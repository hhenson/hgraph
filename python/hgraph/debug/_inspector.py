"""Interactive Python inspector backed by owned native diagnostics snapshots."""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from socket import gethostname

import tornado.web

from _hgraph import GraphDiagnosticEntityKind, GraphDiagnostics
from hgraph import GlobalState, STATE, TS, const, graph, sink_node
from hgraph.adaptors.perspective import PerspectiveTablesManager
from hgraph.adaptors.perspective._perspective import IndexPageHandler
from hgraph.adaptors.tornado._tornado_web import BaseHandler, TornadoWeb
from hgraph._wiring._core import _wiring_stack

from ._inspector_item_id import InspectorItemId, NodeValueType

__all__ = ("inspector",)


_INSPECTOR_SCHEMA = {
    "X": str,
    "name": str,
    "type": str,
    "value": str,
    "modified": datetime,
    "scheduled": datetime,
    "evals": int,
    "time": float,
    "of_graph": float,
    "of_total": float,
    "value_size": int,
    "size": int,
    "total_value_size": int,
    "total_size": int,
    "subgraphs": int,
    "nodes": int,
    "id": str,
    "ord": str,
}

_ENTRY_MODIFIED = object()
_MAX_DISPLAY_LENGTH = 256


def _synchronized(method):
    @wraps(method)
    def guarded(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return guarded


def _seconds(value) -> float:
    total_seconds = getattr(value, "total_seconds", None)
    return float(total_seconds()) if callable(total_seconds) else 0.0


def _perspective_time(value):
    """Use Perspective's JSON-compatible datetime input representation."""
    return value.isoformat() if isinstance(value, datetime) else value


def _node_type(entry) -> str:
    kind = entry.node_kind.name
    if kind != "NESTED":
        return kind
    identity = f"{entry.label} {entry.schema_label} {entry.implementation_label}".lower()
    for token, label in (
        ("map", "MAP"),
        ("mesh", "MESH"),
        ("switch", "SWITCH"),
        ("reduce", "REDUCE"),
    ):
        if token in identity:
            return label
    return "NESTED"


def _entry_name(entry) -> str:
    if entry.kind == GraphDiagnosticEntityKind.NODE and ":" in entry.label:
        # Native diagnostic labels retain ``schema:user-label`` so C++ traces
        # remain self-describing.  The released Python inspector displays the
        # user/wiring label itself.
        return entry.label.split(":", 1)[1]
    return entry.label


def _display_json(value: str, error: str) -> str:
    if error:
        return f"<unavailable: {error}>"
    if not value:
        return ""
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return value
    if isinstance(parsed, str):
        rendered = parsed
    elif isinstance(parsed, (dict, list)):
        return f"{len(parsed)} items"
    elif parsed is None:
        return "INVALID"
    else:
        rendered = repr(parsed)
    return (
        rendered
        if len(rendered) <= _MAX_DISPLAY_LENGTH
        else rendered[: _MAX_DISPLAY_LENGTH - 3] + "..."
    )


def _display_value(value) -> str:
    if isinstance(value, (dict, list)):
        return f"{len(value)} items"
    if value is None:
        return "INVALID"
    rendered = value if isinstance(value, str) else repr(value)
    return (
        rendered
        if len(rendered) <= _MAX_DISPLAY_LENGTH
        else rendered[: _MAX_DISPLAY_LENGTH - 3] + "..."
    )


class _InspectorHttpHandler(BaseHandler):
    def initialize(self, session):
        self.session = session

    async def get(self, command="", item=""):
        try:
            body, content_type = self.session.command(command, item, self.request.query_arguments)
        except ValueError as error:
            self.set_status(500)
            await self.finish(str(error))
            return
        except KeyError as error:
            self.set_status(404)
            await self.finish(str(error))
            return
        except Exception as error:  # pragma: no cover - defensive HTTP boundary
            self.set_status(500)
            await self.finish(f"Inspector error: {error}")
            return
        if content_type:
            self.set_header("Content-Type", content_type)
        await self.finish(body)


class _InspectorValuePageHandler(BaseHandler):
    def initialize(self, session):
        self.session = session

    def get(self, item):
        try:
            self.session.value_for(item)
        except KeyError as error:
            self.set_status(404)
            self.finish(str(error))
            return
        self.render(
            str(Path(__file__).with_name("frame_template.html")),
            table_name=item,
        )


class _InspectorSession:
    def __init__(self, diagnostics, port, publish_interval, global_state):
        if not isinstance(port, int) or not 0 < port < 65536:
            raise ValueError("inspector port must be an integer in [1, 65535]")
        if publish_interval <= 0:
            raise ValueError("inspector publish_interval must be positive")
        self.diagnostics = diagnostics
        self._lock = threading.RLock()
        self.port = port
        self.publish_interval = float(publish_interval)
        self.manager = PerspectiveTablesManager.current(global_state)
        self.web = TornadoWeb.instance(port)
        self.expanded = {""}
        self.rows = {}
        self.values = {}
        self.frames = {}
        self._navigation = {}
        self._output_navigation = {}
        self._known_ids = set()
        self._entry_item_ids = {}
        self._recent_counters = {}
        self._frame_cache = {}
        self._built_frames = {}
        self._search_rows = {}
        self._search_values = {}
        self._search_frames = {}
        self._last_cycle = None
        self._last_stats = 0.0
        self._last_stats_cycles = 0
        self._last_stats_graph_time = 0.0
        self._last_process_time = time.process_time()
        self._last_manager_stats = {}
        try:
            import psutil

            self._process = psutil.Process()
        except ImportError:  # pragma: no cover - optional outside perspective extra
            self._process = None

    @_synchronized
    def start(self):
        self.manager.start()
        if "inspector" not in self.manager.get_table_names():
            self.manager.create_table(
                _INSPECTOR_SCHEMA, index="id", name="inspector")
        if "recent_performance" not in self.manager.get_table_names():
            self.manager.create_table(
                {"time": datetime, "id": str, "eval_count": int, "eval_time": float},
                limit=100_000,
                name="recent_performance",
            )
        if "graph_performance" not in self.manager.get_table_names():
            self.manager.create_table(
                {
                    "time": datetime,
                    "evaluation_time": datetime,
                    "cycles": float,
                    "avg_cycle": float,
                    "avg_os_cycle": float,
                    "max_cycle": float,
                    "graph_time": float,
                    "os_graph_time": float,
                    "graph_load": float,
                    "avg_lag": float,
                    "max_lag": float,
                    "inspection_time": float,
                    "memory": int,
                    "virt_memory": int,
                    "graph_memory": int,
                    "psp_polls": float,
                    "psp_updates": float,
                    "psp_batches": float,
                    "psp_rows": float,
                },
                limit=24 * 3600,
                name="graph_performance",
            )

        self.manager.register_default_layout(
            "view",
            Path(__file__).with_name("inspector_layout.json").read_bytes(),
        )
        layouts = self.manager.configure_layouts_path(
            os.path.join(tempfile.gettempdir(), "inspector_layouts"))
        template = str(Path(__file__).with_name("inspector_template.html"))
        self.web.add_handlers(
            [
                (
                    r"/inspector/(.*)",
                    IndexPageHandler,
                    {
                        "mgr": self.manager,
                        "layouts_path": layouts,
                        "index_template": template,
                        "host": gethostname(),
                        "port": self.port,
                    },
                ),
                (r"/inspect(?:/([^/]*))?(?:/(.*))?", _InspectorHttpHandler, {"session": self}),
                (r"/inspect_value/(.*)", _InspectorValuePageHandler, {"session": self}),
            ]
        )
        self.web.start()

        print(
            f"Inspector running on "
            f"http://{gethostname()}:{self.port}/inspector/view"
        )
        self.publish(force=True)

    @_synchronized
    def stop(self):
        self.web.stop()

    @staticmethod
    def _graph_layout(entries):
        by_id = {entry.id: entry for entry in entries}
        graph_ids = {}
        node_ids = {}
        child_ordinals = {}
        unresolved = [entry for entry in entries if entry.kind == GraphDiagnosticEntityKind.GRAPH]
        while unresolved:
            progress = False
            for entry in tuple(unresolved):
                if entry.parent_id == 0:
                    graph_ids[entry.id] = ()
                else:
                    parent_node = by_id.get(entry.parent_id)
                    parent_graph = graph_ids.get(parent_node.parent_id) if parent_node else None
                    if parent_node is None or parent_graph is None:
                        continue
                    ordinal = child_ordinals.get(parent_node.id, 0)
                    child_ordinals[parent_node.id] = ordinal + 1
                    if _node_type(parent_node) in {"MAP", "MESH", "SWITCH"}:
                        graph_ids[entry.id] = parent_graph + (
                            parent_node.node_index,
                            -(ordinal + 1),
                        )
                    else:
                        graph_ids[entry.id] = parent_graph + (
                            parent_node.node_index,
                        )
                unresolved.remove(entry)
                progress = True
            if not progress:
                break
        for entry in entries:
            if entry.kind == GraphDiagnosticEntityKind.NODE and entry.parent_id in graph_ids:
                graph_id = graph_ids[entry.parent_id]
                node_ids[entry.id] = InspectorItemId(
                    graph=graph_id, node=entry.node_index)
        return by_id, graph_ids, node_ids

    @staticmethod
    def _row(
        entry,
        item_id,
        *,
        name=None,
        type_name=None,
        value="",
        modified=_ENTRY_MODIFIED,
        x="+",
        nodes=None,
        subgraphs=None,
        entity_metrics=True,
    ):
        identifier = item_id.to_str()
        evaluation = entry.evaluation
        if modified is _ENTRY_MODIFIED and entry.kind == GraphDiagnosticEntityKind.NODE:
            candidates = [
                endpoint.last_modified
                for endpoint in (entry.input, entry.output)
                if endpoint.available and endpoint.valid
            ]
            modified = max(candidates, default=None)
        elif modified is _ENTRY_MODIFIED:
            modified = None
        display_name = name if name is not None else _entry_name(entry)
        return {
            "id": identifier,
            "ord": item_id.sort_key(),
            "X": x,
            "name": item_id.indent() + display_name,
            "type": type_name or (
                "GRAPH"
                if entry.kind == GraphDiagnosticEntityKind.GRAPH
                else _node_type(entry)
            ),
            "value": value,
            "modified": _perspective_time(modified),
            "scheduled": (
                _perspective_time(entry.scheduled_time)
                if entity_metrics else None
            ),
            "evals": evaluation.count if entity_metrics else None,
            "time": _seconds(evaluation.total_time) if entity_metrics else None,
            "of_graph": None,
            "of_total": None,
            "value_size": None,
            "size": (
                entry.storage.dynamic_live_bytes if entity_metrics else None
            ),
            "total_value_size": None,
            "total_size": (
                entry.storage.static_bytes + entry.storage.dynamic_live_bytes
                if entity_metrics else None
            ),
            "subgraphs": subgraphs if entity_metrics else None,
            "nodes": nodes if entity_metrics else None,
        }

    def _value_rows(self, entry, item_id, value, navigation_targets=None):
        rows = []
        values = {}
        if isinstance(value, dict):
            items = value.items()
        elif isinstance(value, list):
            items = enumerate(value)
        else:
            return rows, values
        for key, child in items:
            child_id = InspectorItemId(
                graph=item_id.graph,
                node=item_id.node,
                value_type=item_id.value_type,
                value_path=item_id.value_path + (key,),
            )
            child_text = child_id.to_str()
            has_children = isinstance(child, (dict, list)) and bool(child)
            row = self._row(
                entry,
                child_id,
                name=str(key),
                type_name=type(child).__name__,
                value=_display_value(child),
                x="+" if has_children else "º",
                entity_metrics=False,
            )
            values[child_text] = json.dumps(child, ensure_ascii=False)
            if navigation_targets is not None:
                target = navigation_targets.get(child_id.value_path)
                if target is not None:
                    self._navigation[child_text] = target
            if has_children and child_text in self.expanded:
                row["X"] = "-"
                child_rows, child_values = self._value_rows(
                    entry, child_id, child, navigation_targets)
                rows.extend(child_rows)
                values.update(child_values)
            rows.append(row)
        return rows, values

    def _build_rows(self, snapshot):
        entries = tuple(snapshot.entries)
        by_id, graph_ids, node_ids = self._graph_layout(entries)
        self._navigation = {}
        self._output_navigation = {}
        self._entry_item_ids = {
            **{
                entry_id: InspectorItemId(graph=graph_id).to_str()
                for entry_id, graph_id in graph_ids.items()
            },
            **{
                entry_id: item_id.to_str()
                for entry_id, item_id in node_ids.items()
            },
        }

        def path_component(component):
            try:
                value = json.loads(component)
            except (TypeError, ValueError):
                return component
            if isinstance(value, (dict, list)):
                return json.dumps(
                    value, ensure_ascii=False, separators=(",", ":"),
                    sort_keys=True)
            return value

        def output_target(node_id, path):
            target = node_ids.get(node_id)
            if target is None:
                return None
            return InspectorItemId(
                graph=target.graph,
                node=target.node,
                value_type=NodeValueType.Output,
                value_path=tuple(path_component(item) for item in path),
            ).to_str()

        def navigation_targets(diagnostic_value, attribute="targets"):
            exact = {}
            ambiguous = set()
            for target in getattr(diagnostic_value, attribute, ()):
                source_path = tuple(
                    path_component(item) for item in target.source_path)
                destination = output_target(
                    target.node_id, target.target_path)
                if destination is None:
                    continue
                if source_path in exact and exact[source_path] != destination:
                    ambiguous.add(source_path)
                else:
                    exact[source_path] = destination
            for path in ambiguous:
                exact.pop(path, None)
            if exact:
                return exact

            # Compatibility with snapshots created by an older native module.
            # Only REF navigation had a legacy id-only representation; direct
            # producer bindings were not exposed before bound_targets.
            if attribute != "targets":
                return {}
            targets = [
                node_ids[target]
                for target in diagnostic_value.target_node_ids
                if target in node_ids
            ]
            if len(targets) != 1:
                return {}
            target = targets[0]
            return {(): InspectorItemId(
                    graph=target.graph,
                    node=target.node,
                    value_type=NodeValueType.Output,
                ).to_str()}

        def register_navigation(item_id, targets, destination):
            for path, target in targets.items():
                source = InspectorItemId(
                    graph=item_id.graph,
                    node=item_id.node,
                    value_type=item_id.value_type,
                    value_path=item_id.value_path + path,
                ).to_str()
                destination[source] = target

        graph_entry_for_id = {graph_id: by_id[entry_id] for entry_id, graph_id in graph_ids.items()}
        root_entry = next(
            (
                entry
                for entry in entries
                if entry.kind == GraphDiagnosticEntityKind.GRAPH
                and entry.parent_id == 0
            ),
            None,
        )
        # Native diagnostics deliberately retain stopped entries so an owned
        # post-run snapshot remains inspectable.  A live released inspector,
        # however, enumerates the nested node's current graphs and removes a
        # stopped keyed child immediately.  Keep retained entries only for an
        # already-completed graph snapshot.
        retain_stopped = root_entry is None or root_entry.stopped
        if not retain_stopped:
            graph_entry_for_id = {
                graph_id: entry
                for graph_id, entry in graph_entry_for_id.items()
                if not entry.stopped
            }
        nodes_by_graph = {}
        child_graphs_by_node = {}
        for entry in entries:
            if (entry.kind == GraphDiagnosticEntityKind.NODE
                    and entry.id in node_ids
                    and node_ids[entry.id].graph in graph_entry_for_id):
                nodes_by_graph.setdefault(node_ids[entry.id].graph, []).append(entry)
            elif (entry.kind == GraphDiagnosticEntityKind.GRAPH
                  and entry.parent_id
                  and (retain_stopped or not entry.stopped)):
                child_graphs_by_node.setdefault(entry.parent_id, []).append(entry)

        subtree_counts = {}

        def subtree_count(entry_id):
            if entry_id in subtree_counts:
                return subtree_counts[entry_id]
            entry = by_id[entry_id]
            nodes = int(entry.kind == GraphDiagnosticEntityKind.NODE)
            graphs = int(entry.kind == GraphDiagnosticEntityKind.GRAPH)
            for child_id in entry.children:
                child = by_id[child_id]
                if (not retain_stopped
                        and child.kind == GraphDiagnosticEntityKind.GRAPH
                        and child.stopped):
                    continue
                child_nodes, child_graphs = subtree_count(child_id)
                nodes += child_nodes
                graphs += child_graphs
            subtree_counts[entry_id] = (nodes, graphs)
            return nodes, graphs

        def descendant_counts(entry):
            nodes, graphs = subtree_count(entry.id)
            if entry.kind == GraphDiagnosticEntityKind.NODE:
                nodes -= 1
            else:
                graphs -= 1
            return nodes, graphs

        rows = {}
        values = {}
        frames = {}
        frame_cache = getattr(self, "_frame_cache", None)
        if frame_cache is None:
            frame_cache = self._frame_cache = {}

        def retain_frame(identifier, diagnostic_value):
            if not diagnostic_value.has_frame:
                return
            version = (
                diagnostic_value.last_modified,
                diagnostic_value.schema_label,
            )
            cached = frame_cache.get(identifier)
            if cached is None or cached[0] != version:
                # Keep the owned native snapshot value. Converting every
                # captured Frame through the Arrow C stream on the Tornado
                # thread can contend with an evaluating Python graph and is
                # unnecessary until the value endpoint is requested.
                cached = (version, diagnostic_value)
                frame_cache[identifier] = cached
            frames[identifier] = cached[1]

        visible_graphs = {()}
        for graph_id in graph_entry_for_id:
            graph_item = InspectorItemId(graph=graph_id)
            if graph_item.to_str() in self.expanded:
                visible_graphs.add(graph_id)

        for graph_id in visible_graphs:
            for entry in sorted(nodes_by_graph.get(graph_id, ()), key=lambda item: item.node_index):
                item_id = node_ids[entry.id]
                row = self._row(
                    entry,
                    item_id,
                    value=(
                        f"{entry.inspection.pending_items} items in the queue"
                        if entry.inspection.pending_items is not None
                        else (
                            _display_json(entry.output.json, entry.output.error)
                            if entry.output.available
                            else "-"
                        )
                    ),
                    nodes=descendant_counts(entry)[0],
                    subgraphs=descendant_counts(entry)[1],
                )
                if item_id.to_str() in self.expanded:
                    row["X"] = "-"
                rows[row["id"]] = row
                values[row["id"]] = entry.output.json if entry.output.available else ""
                if entry.output.available:
                    retain_frame(row["id"], entry.output)
                output_navigation = (
                    navigation_targets(entry.output)
                    if entry.output.available else {})
                if target := output_navigation.get(()):
                    self._navigation[row["id"]] = target

                if item_id.to_str() not in self.expanded:
                    continue
                categories = []
                if entry.input.available:
                    categories.append((NodeValueType.Inputs, entry.input))
                if child_graphs_by_node.get(entry.id):
                    categories.append((NodeValueType.Graphs, None))
                if entry.output.available:
                    categories.append((NodeValueType.Output, entry.output))
                if entry.scalars.available and (
                    entry.scalars.json or not entry.scalars.error
                ):
                    categories.append((NodeValueType.Scalars, entry.scalars))
                for value_type, diagnostic_value in categories:
                    category_id = InspectorItemId(
                        graph=item_id.graph,
                        node=item_id.node,
                        value_type=value_type,
                    )
                    category_text = category_id.to_str()
                    has_children = value_type is NodeValueType.Graphs
                    decoded = None
                    if diagnostic_value is not None:
                        try:
                            decoded = json.loads(diagnostic_value.json) if diagnostic_value.json else None
                            has_children = isinstance(decoded, (dict, list)) and bool(decoded)
                        except (TypeError, ValueError):
                            pass
                    category_row = self._row(
                        entry,
                        category_id,
                        name=value_type.value,
                        type_name=(
                            "dict"
                            if diagnostic_value is None
                            else diagnostic_value.schema_label or "VALUE"
                        ),
                        value=(
                            f"{len(child_graphs_by_node.get(entry.id, ()))} items"
                            if diagnostic_value is None
                            else _display_json(diagnostic_value.json, diagnostic_value.error)
                        ),
                        modified=(
                            diagnostic_value.last_modified
                            if diagnostic_value is not None
                            and diagnostic_value.valid
                            else None
                        ),
                        x="+" if has_children else "º",
                        entity_metrics=False,
                    )
                    if category_text in self.expanded:
                        category_row["X"] = "-" if has_children else "º"
                    rows[category_text] = category_row
                    if diagnostic_value is not None:
                        category_navigation = navigation_targets(
                            diagnostic_value)
                        register_navigation(
                            category_id,
                            category_navigation,
                            self._navigation,
                        )
                        if value_type is NodeValueType.Inputs:
                            register_navigation(
                                category_id,
                                navigation_targets(
                                    diagnostic_value, "bound_targets"),
                                self._output_navigation,
                            )
                        target = category_navigation.get(())
                        if target is not None:
                            self._navigation[category_text] = target
                        values[category_text] = diagnostic_value.json
                        retain_frame(category_text, diagnostic_value)
                        if category_text in self.expanded and isinstance(
                            decoded, (dict, list)
                        ):
                            child_rows, child_values = self._value_rows(
                                entry, category_id, decoded,
                                category_navigation)
                            for child_row in child_rows:
                                rows[child_row["id"]] = child_row
                            values.update(child_values)

                graphs_id = InspectorItemId(
                    graph=item_id.graph,
                    node=item_id.node,
                    value_type=NodeValueType.Graphs,
                )
                if graphs_id.to_str() in self.expanded:
                    for graph_entry in child_graphs_by_node.get(entry.id, ()):
                        child_graph_id = graph_ids[graph_entry.id]
                        child_item = InspectorItemId(graph=child_graph_id)
                        child_row = self._row(
                            graph_entry,
                            child_item,
                            value=graph_entry.label,
                            nodes=descendant_counts(graph_entry)[0],
                            subgraphs=descendant_counts(graph_entry)[1],
                        )
                        child_row["ord"] = graphs_id.sort_key() + "0" + child_item.sort_key()
                        if child_item.to_str() in self.expanded:
                            child_row["X"] = "-"
                        rows[child_row["id"]] = child_row

        root_time = (
            _seconds(root_entry.evaluation.total_time)
            if root_entry is not None
            else 0.0
        )
        for entry in entries:
            identifier = self._entry_item_ids.get(entry.id)
            row = rows.get(identifier)
            if row is None:
                continue
            parent_graph = None
            if entry.kind == GraphDiagnosticEntityKind.NODE:
                parent_graph = by_id.get(entry.parent_id)
            elif entry.parent_id:
                parent_node = by_id.get(entry.parent_id)
                if parent_node is not None:
                    parent_graph = by_id.get(parent_node.parent_id)
            parent_time = (
                _seconds(parent_graph.evaluation.total_time)
                if parent_graph is not None
                else 0.0
            )
            entry_time = _seconds(entry.evaluation.total_time)
            row["of_graph"] = entry_time / parent_time if parent_time else None
            row["of_total"] = entry_time / root_time if root_time else None
        self._built_frames = frames
        return rows, values

    @staticmethod
    def _expandable_ids(snapshot):
        _, graph_ids, node_ids = _InspectorSession._graph_layout(snapshot.entries)
        expanded = {
            InspectorItemId(graph=graph_id).to_str()
            for graph_id in graph_ids.values()
        }

        def add_value(item_id, value):
            if not isinstance(value, (dict, list)) or not value:
                return
            expanded.add(item_id.to_str())
            items = value.items() if isinstance(value, dict) else enumerate(value)
            for key, child in items:
                add_value(
                    InspectorItemId(
                        graph=item_id.graph,
                        node=item_id.node,
                        value_type=item_id.value_type,
                        value_path=item_id.value_path + (key,),
                    ),
                    child,
                )

        for entry in snapshot.entries:
            item_id = node_ids.get(entry.id)
            if item_id is None:
                continue
            expanded.add(item_id.to_str())
            for value_type, diagnostic_value in (
                (NodeValueType.Inputs, entry.input),
                (NodeValueType.Output, entry.output),
                (NodeValueType.Scalars, entry.scalars),
            ):
                if not diagnostic_value.available:
                    continue
                category = InspectorItemId(
                    graph=item_id.graph,
                    node=item_id.node,
                    value_type=value_type,
                )
                try:
                    decoded = (
                        json.loads(diagnostic_value.json)
                        if diagnostic_value.json
                        else None
                    )
                except (TypeError, ValueError):
                    decoded = None
                add_value(category, decoded)
            expanded.add(
                InspectorItemId(
                    graph=item_id.graph,
                    node=item_id.node,
                    value_type=NodeValueType.Graphs,
                ).to_str()
            )
        return expanded

    @staticmethod
    def _query_argument(query, name, default=None):
        values = query.get(name)
        if values is None:
            values = query.get(name.encode())
        if not values:
            return default
        value = b"".join(values) if isinstance(values[0], bytes) else "".join(values)
        return value.decode() if isinstance(value, bytes) else value

    @staticmethod
    def _search_depth(root, child):
        if root.graph != child.graph:
            return 1 + len(child.graph) - len(root.graph)
        if root.node is None:
            return 1 + (1 if child.value_type is not None else 0) + len(child.value_path)
        if root.node != child.node:
            return 1
        if root.value_type is None:
            return 1 + len(child.value_path)
        if root.value_type != child.value_type:
            return 1
        return len(child.value_path) - len(root.value_path)

    def _search(self, item, query):
        root = InspectorItemId.from_str(item)
        needle = self._query_argument(query, "q")
        if not needle:
            raise ValueError("Search command requires a query parameter")
        try:
            expression = re.compile(needle, re.I)
        except re.error as error:
            raise ValueError(f"invalid search expression: {error}") from error
        try:
            depth = int(self._query_argument(query, "depth", "3"))
            limit = int(self._query_argument(query, "limit", "10"))
        except ValueError as error:
            raise ValueError("search depth and limit must be integers") from error
        if depth < 0 or limit <= 0:
            raise ValueError("search depth must be non-negative and limit positive")

        self._search_rows.clear()
        self._search_values.clear()
        self._search_frames.clear()

        snapshot = self.diagnostics.snapshot()
        visible_expanded = self.expanded
        try:
            self.expanded = self._expandable_ids(snapshot)
            all_rows, all_values = self._build_rows(snapshot)
            all_frames = self._built_frames
        finally:
            self.expanded = visible_expanded
        self._build_rows(snapshot)  # restore navigation for the visible tree

        matches = []
        for identifier, row in sorted(
            all_rows.items(), key=lambda item: item[1]["ord"]
        ):
            candidate = InspectorItemId.from_str(identifier)
            if identifier == item or not root.is_parent_of(candidate):
                continue
            if self._search_depth(root, candidate) > depth:
                continue
            if expression.search(row["name"]) is None:
                continue
            found = dict(row)
            found["X"] = "?"
            self._search_rows[identifier] = found
            if identifier in all_values:
                self._search_values[identifier] = all_values[identifier]
            if identifier in all_frames:
                self._search_frames[identifier] = all_frames[identifier]
            matches.append(identifier)
            if len(matches) >= limit:
                break
        self.publish(force=True)
        return matches[-1] if matches else ""

    def _clear_search(self, apply=False):
        if apply:
            for identifier in self._search_rows:
                item = InspectorItemId.from_str(identifier)
                self.expanded.update(parent.to_str() for parent in item.parent_item_ids())
                self.expanded.add(InspectorItemId(graph=item.graph).to_str())
                self.expanded.add(identifier)
        self._search_rows.clear()
        self._search_values.clear()
        self._search_frames.clear()
        self.publish(force=True)

    @_synchronized
    def publish(self, force=False):
        publish_started = time.perf_counter()
        snapshot = self.diagnostics.snapshot()
        rows, values = self._build_rows(snapshot)
        frames = self._built_frames
        rows.update(self._search_rows)
        values.update(self._search_values)
        frames.update(self._search_frames)
        current = set(rows)
        removals = self._known_ids - current
        if force or snapshot.graph_cycles != self._last_cycle or rows != self.rows or removals:
            self.manager.update_table("inspector", list(rows.values()), removals)
            self.rows = rows
            self.values = values
            self.frames = frames
            self._known_ids = current
            self._last_cycle = snapshot.graph_cycles

        recent = []
        recent_time = datetime.now(timezone.utc).isoformat()
        for entry in snapshot.entries:
            previous_count, previous_time = self._recent_counters.get(
                entry.id, (0, 0.0)
            )
            count = entry.evaluation.count
            total_time = _seconds(entry.evaluation.total_time)
            if count > previous_count:
                recent.append(
                    {
                        "time": recent_time,
                        "id": self._entry_item_ids.get(entry.id, str(entry.id)),
                        "eval_count": count - previous_count,
                        "eval_time": max(0.0, total_time - previous_time),
                    }
                )
            self._recent_counters[entry.id] = (count, total_time)
        if recent:
            self.manager.update_table("recent_performance", recent)

        now = time.perf_counter()
        if force or now - self._last_stats >= self.publish_interval:
            samples = snapshot.scheduling_lag_samples
            elapsed = max(now - self._last_stats, 1e-9) if self._last_stats else 0.0
            cycle_delta = snapshot.graph_cycles - self._last_stats_cycles
            graph_total = _seconds(snapshot.root_evaluation_time)
            graph_delta = max(0.0, graph_total - self._last_stats_graph_time)
            root = next(
                (
                    entry
                    for entry in snapshot.entries
                    if entry.kind == GraphDiagnosticEntityKind.GRAPH
                    and entry.parent_id == 0
                ),
                None,
            )
            manager_stats = self.manager.get_stats()
            process_time = time.process_time()
            process_delta = max(0.0, process_time - self._last_process_time)
            memory = self._process.memory_info() if self._process is not None else None

            def manager_rate(name):
                current = manager_stats.get(name, 0)
                previous = self._last_manager_stats.get(name, 0)
                return max(0, current - previous) / elapsed if elapsed else 0.0

            self.manager.update_table(
                "graph_performance",
                [
                    {
                        "time": datetime.now(timezone.utc).isoformat(),
                        "evaluation_time": (
                            _perspective_time(root.evaluation_time)
                            if root is not None
                            else None
                        ),
                        "cycles": cycle_delta / elapsed if elapsed else 0.0,
                        "avg_cycle": (
                            graph_delta / cycle_delta
                            if cycle_delta
                            else 0.0
                        ),
                        "avg_os_cycle": (
                            process_delta / cycle_delta if cycle_delta else 0.0
                        ),
                        "max_cycle": (
                            _seconds(root.evaluation.max_time)
                            if root is not None
                            else 0.0
                        ),
                        "graph_time": graph_delta,
                        "os_graph_time": process_delta,
                        "graph_load": graph_delta / elapsed if elapsed else 0.0,
                        "avg_lag": (
                            _seconds(snapshot.scheduling_lag_total) / samples if samples else 0.0
                        ),
                        "max_lag": _seconds(snapshot.scheduling_lag_max),
                        "inspection_time": (
                            (time.perf_counter() - publish_started) / graph_delta
                            if graph_delta
                            else 0.0
                        ),
                        "memory": (
                            int(memory.rss / (1024 * 1024))
                            if memory is not None
                            else None
                        ),
                        "virt_memory": (
                            int(memory.vms / (1024 * 1024))
                            if memory is not None
                            else None
                        ),
                        "graph_memory": (
                            snapshot.planned_bytes + snapshot.dynamic_live_bytes
                        ) // (1024 * 1024),
                        "psp_polls": manager_rate("polling"),
                        "psp_updates": manager_rate("updates"),
                        "psp_batches": manager_rate("batches"),
                        "psp_rows": manager_rate("rows"),
                    }
                ],
            )
            self._last_stats = now
            self._last_stats_cycles = snapshot.graph_cycles
            self._last_stats_graph_time = graph_total
            self._last_process_time = process_time
            self._last_manager_stats = dict(manager_stats)

    @_synchronized
    def value_for(self, item):
        if item not in self.values:
            self.publish(force=True)
        if item not in self.values:
            raise KeyError(f"unknown inspector item {item!r}")
        return self.values[item]

    @_synchronized
    def value_ipc_for(self, item):
        raw = self.value_for(item)
        import pyarrow as pa

        if item in self.frames:
            value = self.frames[item]
            if hasattr(value, "has_frame"):
                value = value.frame
            if hasattr(value, "to_arrow"):
                value = value.to_arrow()
            table = value if isinstance(value, pa.Table) else pa.table(value)
        else:
            try:
                value = json.loads(raw) if raw else None
            except (TypeError, ValueError):
                value = raw
            try:
                if isinstance(value, list) and value and all(
                    isinstance(row, dict) for row in value
                ):
                    table = pa.Table.from_pylist(value)
                elif isinstance(value, dict):
                    table = pa.Table.from_pylist([value])
                elif isinstance(value, list):
                    table = pa.table({"value": value})
                else:
                    table = pa.table({"value": [value]})
            except (TypeError, ValueError, pa.ArrowException):
                table = pa.table({"value": [str(value)]})

        stream = pa.BufferOutputStream()
        with pa.ipc.new_stream(stream, table.schema) as writer:
            writer.write_table(table)
        return stream.getvalue().to_pybytes()

    @_synchronized
    def command(self, command, item, query):
        command = command or "expand"
        if command in {"expand", "show"}:
            target = InspectorItemId.from_str(item)
            self.expanded.add(item)
            expand_all = self._query_argument(query, "all", "false").lower()
            if command == "expand" and expand_all in {"1", "true", "yes"}:
                for candidate in self._expandable_ids(
                        self.diagnostics.snapshot()):
                    candidate_id = InspectorItemId.from_str(candidate)
                    if candidate == item or target.is_parent_of(candidate_id):
                        self.expanded.add(candidate)
            self.publish(force=True)
            return "", "text/plain"
        if command == "collapse":
            target = InspectorItemId.from_str(item)
            self.expanded = {
                candidate
                for candidate in self.expanded
                if candidate == item
                or not target.is_parent_of(InspectorItemId.from_str(candidate))
            }
            self.expanded.discard(item)
            self.publish(force=True)
            return "", "text/plain"
        if command in {"pin", "unpin"}:
            return "", "text/plain"
        if command == "search":
            return self._search(item, query), "text/plain"
        if command == "applysearch":
            self._clear_search(apply=True)
            return "OK", "text/plain"
        if command == "stopsearch":
            self._clear_search()
            return "OK", "text/plain"
        if command == "value":
            return self.value_ipc_for(item), "application/vnd.apache.arrow.stream"
        if command == "rows":
            self.publish(force=True)
            return json.dumps(
                sorted(self.rows.values(), key=lambda row: row["ord"]),
                default=str,
            ), "application/json"
        if command in {"output", "ref", "refs"}:
            navigation = (
                self._output_navigation
                if command == "output"
                else self._navigation
            )
            target = navigation.get(item)
            if target is None:
                raise ValueError(f"inspector item {item!r} has no output target")
            visited = {item}
            while command == "refs" and target not in visited:
                visited.add(target)
                next_target = self._navigation.get(target)
                if next_target is None:
                    break
                target = next_target
            target_id = InspectorItemId.from_str(target)
            self.expanded.update(
                parent.to_str() for parent in target_id.parent_item_ids())
            self.expanded.add(target)
            self.expanded.add(
                InspectorItemId(
                    graph=target_id.graph,
                    node=target_id.node,
                ).to_str()
            )
            self.expanded.add(InspectorItemId(graph=target_id.graph).to_str())
            self.publish(force=True)
            return target, "text/plain"
        if command == "pin_ref":
            return "", "text/plain"
        raise KeyError(f"unknown inspector command {command!r}")


class _InspectorPublisher:
    """Publish from the root graph's lifecycle, as the released inspector does.

    Keeping Perspective publication on the graph thread avoids a competing
    Python publisher thread and preserves the executor's phase-scoped GIL
    contract.  HTTP commands may still request an immediate owned snapshot.
    """

    def __init__(self, publish_interval: float):
        self.publish_interval = float(publish_interval)
        self.session = None
        self.last_publish = 0.0

    def on_after_graph_evaluation(self, graph):
        if graph.graph_id or self.session is None:
            return
        now = time.monotonic()
        if now - self.last_publish >= self.publish_interval:
            self.session.publish()
            self.last_publish = now


@sink_node
def _inspector_lifetime(
    signal: TS[bool],
    diagnostics: object,
    publisher: object,
    port: int,
    publish_interval: float,
    state: STATE = None,
):
    # The signal is deliberately one-shot. Publication is driven by the root
    # graph lifecycle observer and must not create ticks or extend graph life.
    pass


@_inspector_lifetime.start
def _start_inspector_lifetime(
    diagnostics: object,
    publisher: object,
    port: int,
    publish_interval: float,
    state: STATE = None,
    _global_state: GlobalState = None,
):
    state.session = _InspectorSession(
        diagnostics, port, publish_interval, _global_state)
    state.session.start()
    publisher.session = state.session
    publisher.last_publish = time.monotonic()


@_inspector_lifetime.stop
def _stop_inspector_lifetime(
    diagnostics: object,
    publisher: object,
    port: int,
    publish_interval: float,
    state: STATE = None,
):
    del diagnostics, port, publish_interval
    publisher.session = None
    if getattr(state, "session", None) is not None:
        state.session.stop()


@graph
def inspector(port: int = 8080, publish_interval: float = 2.5):
    """Expose the released hgraph interactive inspector for this graph run.

    Runtime hierarchy, timing, and values are copied by the native
    ``GraphDiagnostics`` observer.  Python owns only the Perspective/Tornado
    presentation and never retains graph, node, or time-series pointers.
    """
    if not _wiring_stack:
        raise RuntimeError("inspector() must be wired inside a graph")
    diagnostics = GraphDiagnostics(capture_values=True)
    publisher = _InspectorPublisher(publish_interval)
    _wiring_stack[0].add_lifecycle_observer(diagnostics)
    _wiring_stack[0].add_lifecycle_observer(publisher)
    _inspector_lifetime(
        const(True),
        diagnostics=diagnostics,
        publisher=publisher,
        port=port,
        publish_interval=publish_interval,
    )
