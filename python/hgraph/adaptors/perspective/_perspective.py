"""Perspective table ownership and Tornado hosting.

Perspective remains an optional dependency.  The manager accepts an injected
client for deterministic tests and otherwise creates the current Perspective
server/client pair lazily when the first table or web transport is required.
"""

from __future__ import annotations

import base64
import inspect
import json
import logging
import os
import tempfile
import threading
from collections import defaultdict
from datetime import datetime, timezone
from glob import glob
from pathlib import Path
from urllib.parse import unquote

import pyarrow as pa
import tornado.log
import tornado.web

from hgraph import GlobalState, STATE, TS, sink_node
from hgraph.adaptors.tornado._tornado_web import BaseHandler, TornadoWeb

__all__ = (
    "PerspectiveTablesManager",
    "TablePageHandler",
    "IndexPageHandler",
    "perspective_web",
)

logger = logging.getLogger(__name__)


def _sequence(value):
    if value in (None, ""):
        return ()
    return tuple(value) if isinstance(value, (list, tuple)) else (value,)


def _decode_update(delta):
    if delta is None:
        return []
    if isinstance(delta, list):
        return [dict(row) for row in delta]
    if isinstance(delta, dict):
        return [dict(delta)]
    if isinstance(delta, (bytes, bytearray, memoryview, pa.Buffer)):
        return pa.ipc.open_stream(delta).read_all().to_pylist()
    to_pylist = getattr(delta, "to_pylist", None)
    if to_pylist is not None:
        return to_pylist()
    raise TypeError(f"unsupported Perspective update payload {type(delta)!r}")


class PerspectiveTablesManager:
    """Own Perspective tables, edit subscriptions, configuration and hosting."""

    _STATE_KEY = "perspective_manager"

    def __init__(
        self,
        host_server_tables=True,
        table_config_file=(),
        table_configs=None,
        view_config_file=(),
        **kwargs,
    ):
        # Preserve the early hg_cpp injected-client spelling while matching
        # upstream's public positional signature.
        injected = kwargs.pop("client", None)
        if not isinstance(host_server_tables, bool):
            if injected is not None:
                raise TypeError("Perspective client was supplied twice")
            injected = host_server_tables
            host_server_tables = True

        self._client = injected
        self._server = kwargs.pop("server", None)
        self._host_server_tables = host_server_tables
        self._table_configs = dict(table_configs or {})
        self._table_config_files = _sequence(table_config_file)
        self._view_config_files = _sequence(view_config_file)
        self.options = dict(kwargs)

        self._tables = {}
        self._table_options = {}
        self._subscribers = defaultdict(dict)
        self._subscription_views = {}
        self._views = {}
        self._temporary_tables = {}
        self._stats = defaultdict(int)
        self._table_stats = []
        self._callback = lambda fn, *args, **kw: fn(*args, **kw)
        self._started = False
        self._system_tables_started = False
        self._web = None
        self._web_configured = False
        self._lock = threading.RLock()

        self._load_view_configs()
        # Validate table configuration eagerly, as upstream does, without
        # importing Perspective or constructing tables.
        self.read_table_config()

    def is_new_api(self):
        """hg_cpp supports the current Perspective server/client API."""
        return True

    @classmethod
    def set_current(cls, self, global_state: GlobalState = None):
        state = global_state if global_state is not None else GlobalState.instance()
        if state.get(cls._STATE_KEY) is not None:
            raise ValueError("a PerspectiveTablesManager is already configured")
        state[cls._STATE_KEY] = self

    @classmethod
    def current(cls, global_state: GlobalState = None):
        state = global_state if global_state is not None else GlobalState.instance()
        manager = state.get(cls._STATE_KEY)
        if manager is None:
            manager = cls()
            state[cls._STATE_KEY] = manager
        return manager

    @property
    def server_tables(self):
        return self._host_server_tables

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        try:
            import perspective
        except ModuleNotFoundError as error:
            raise RuntimeError(
                "Perspective adaptors require the 'perspective' extra") from error
        self._server = self._server or perspective.Server(
            on_poll_request=self.schedule_poll)
        new_local_client = getattr(self._server, "new_local_client", None)
        self._client = (
            new_local_client()
            if new_local_client is not None
            else perspective.Client.from_server(self._server)
        )
        return self._client

    def table(self, *args, **kwargs):
        return self._ensure_client().table(*args, **kwargs)

    def create_table(
        self,
        *args,
        name,
        editable=False,
        user=True,
        edit_role=None,
        temporary=False,
        **kwargs,
    ):
        with self._lock:
            if name in self._tables:
                raise ValueError(f"Perspective table {name!r} already exists")
            try:
                table = self.table(*args, name=name, **kwargs)
            except TypeError:
                # Injected test clients and older compatible clients may not
                # accept Perspective's optional name argument.
                table = self.table(*args, **kwargs)
            self.add_table(
                name,
                table,
                editable=editable,
                user=user,
                edit_role=edit_role,
                temporary=temporary,
            )
            return table

    def add_table(
        self,
        name,
        table,
        editable=False,
        user=True,
        edit_role=None,
        temporary=False,
    ):
        with self._lock:
            if name in self._tables:
                raise ValueError(f"Perspective table {name!r} already exists")
            self._tables[name] = table
            self._table_options[name] = {
                "editable": bool(editable),
                "user": bool(user),
                "edit_role": edit_role,
                "temporary": bool(temporary),
                "index": (
                    table.get_index()
                    if callable(getattr(table, "get_index", None)) else None
                ),
            }
            if temporary:
                self._temporary_tables[name] = 0
            self._attach_subscribers(name)
            if user:
                self._publish_index_entry(name)
            return table

    def _publish_index_entry(self, name):
        index_table = self._tables.get("index")
        if index_table is None or name == "index":
            return
        table = self._tables[name]
        options = self._table_options[name]
        schema_fn = getattr(table, "schema", None)
        schema = schema_fn() if callable(schema_fn) else {}
        index_fn = getattr(table, "get_index", None)
        index = index_fn() if callable(index_fn) else options.get("index")
        index_table.update([{
            "name": name,
            "type": "table" if self.server_tables else "client_table",
            "editable": options["editable"],
            "edit_role": options.get("edit_role"),
            "url": "",
            "schema": json.dumps({
                key: value if isinstance(value, str)
                else getattr(value, "__name__", str(value))
                for key, value in dict(schema).items()
            }),
            "blank": "",
            "index": index or "",
            "description": "",
        }])

    def _ensure_system_tables(self):
        if self._system_tables_started:
            return
        self._system_tables_started = True
        if "index" not in self._tables:
            self.create_table(
                {
                    "name": str, "type": str, "editable": bool,
                    "edit_role": str, "url": str, "schema": str,
                    "blank": str, "index": str, "description": str,
                },
                index="name", name="index", user=False,
            )
        if "table_stats" not in self._tables:
            self.create_table(
                {"table": str, "batch": int, "rows": int, "time": datetime},
                name="table_stats", user=False,
            )
        for name in tuple(self._tables):
            if self._table_options.get(name, {}).get("user"):
                self._publish_index_entry(name)

    def update_table(self, name, data, removals=None):
        rows = _decode_update(data) if data is not None else []
        with self._lock:
            table = self._tables[name]
            self._stats["updates"] += 1
            if rows:
                self._stats["batches"] += 1
                self._stats["rows"] += len(rows)
                self._table_stats.append({
                    "table": name,
                    "batch": self._stats["batches"],
                    "rows": len(rows),
                    "time": datetime.now(timezone.utc),
                })
                self.schedule_callback(table.update, rows)
            if removals:
                remove = getattr(table, "remove", None)
                if callable(remove):
                    self.schedule_callback(remove, list(removals))

    def replace_table(self, name, data):
        rows = _decode_update(data) if data is not None else []
        with self._lock:
            table = self._tables[name]
            replace = getattr(table, "replace", None)
            if not callable(replace):
                raise TypeError(
                    "the configured Perspective table does not support replace")
            self._stats["updates"] += 1
            self.schedule_callback(replace, rows)

    def get_table_names(self):
        with self._lock:
            return list(self._tables)

    def get_table(self, name):
        with self._lock:
            return self._tables.get(name)

    def is_table_editable(self, name):
        return bool(self._table_options.get(name, {}).get("editable"))

    def subscribe_table_updates(self, name, cb, self_updates=False):
        with self._lock:
            if name in self._tables and not self.is_table_editable(name):
                raise ValueError(f"Table {name!r} is not editable")
            token = object()
            self._subscribers[name][token] = (cb, bool(self_updates))
            self._attach_subscribers(name)
            return token

    def unsubscribe_table_updates(self, name, updater):
        with self._lock:
            subscribers = self._subscribers.get(name)
            if subscribers is not None:
                subscribers.pop(updater, None)

    @staticmethod
    def _callback_arity(callback):
        try:
            signature = inspect.signature(callback)
        except (TypeError, ValueError):
            return 2
        positional = tuple(
            parameter for parameter in signature.parameters.values()
            if parameter.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        )
        if any(parameter.kind is inspect.Parameter.VAR_POSITIONAL
               for parameter in signature.parameters.values()):
            return 2
        return len(positional)

    def publish_edits(self, name, updates=(), removals=(), *, raw=None):
        """Deliver an edit batch; injected clients may call this directly."""
        rows = list(updates)
        removed = list(removals)
        with self._lock:
            subscribers = tuple(self._subscribers.get(name, {}).values())
        for callback, _self_updates in subscribers:
            if self._callback_arity(callback) <= 1:
                callback(raw if raw is not None else rows)
            else:
                callback(rows, removed)

    def _attach_subscribers(self, name):
        if (name in self._subscription_views or name not in self._tables
                or not self._subscribers.get(name)):
            return
        view_factory = getattr(self._tables[name], "view", None)
        if not callable(view_factory):
            return
        view = view_factory()
        on_update = getattr(view, "on_update", None)
        if not callable(on_update):
            return

        def updated(*args):
            delta = args[-1] if args else None
            self.publish_edits(
                name, _decode_update(delta), (), raw=delta)

        on_update(updated, mode="row")
        self._subscription_views[name] = view

    def start(self):
        with self._lock:
            if self._started:
                return
            self._ensure_client()
            self._ensure_system_tables()
            self._started = True
            for name in tuple(self._tables):
                self._attach_subscribers(name)

    def get_table_config_files(self):
        return list(self._table_config_files)

    def read_table_config(self):
        config = {}
        for filename in self._table_config_files:
            with open(filename, encoding="utf-8") as stream:
                config.update(json.load(stream))
        for value in self._table_configs.values():
            config.update(json.loads(value) if isinstance(value, str) else value)
        return config

    def add_table_configs(self, table_configs):
        for value in table_configs.values():
            if isinstance(value, str):
                json.loads(value)
        self._table_configs.update(table_configs)

    def _load_view_configs(self):
        for filename in self._view_config_files:
            with open(filename, encoding="utf-8") as stream:
                self._views.update(json.load(stream))

    def get_view_names(self):
        return list(self._views)

    def get_view(self, name):
        return self._views[name]

    def set_loop_callback(self, cb, *args):
        self._callback = lambda fn, *fn_args, **kwargs: cb(
            *args, fn, *fn_args, **kwargs)

    def get_loop_callback(self):
        return self._callback

    def schedule_callback(self, f, *args, **kwargs):
        return self._callback(f, *args, **kwargs)

    def schedule_poll(self, server):
        self._stats["polling"] += 1
        return server.poll()

    def get_stats(self):
        return self._stats

    def cleanup_temporary_tables(self):
        for name, previous_views in tuple(self._temporary_tables.items()):
            table = self._tables.get(name)
            if table is None:
                self._temporary_tables.pop(name, None)
                continue
            count = getattr(table, "get_num_views", lambda: 0)()
            if previous_views > 0 and count == 0:
                delete = getattr(table, "delete", None)
                if callable(delete):
                    self.schedule_callback(delete)
                self._tables.pop(name, None)
                self._table_options.pop(name, None)
                self._temporary_tables.pop(name, None)
            else:
                self._temporary_tables[name] = count

    def tornado_config(self):
        if self._server is None:
            return []
        try:
            from perspective.handlers.tornado import PerspectiveTornadoHandler
        except (ImportError, ModuleNotFoundError) as error:
            raise RuntimeError(
                "Perspective web hosting requires the 'perspective' extra") from error
        options = {"perspective_server": self._server}
        return [
            (r"/websocket", PerspectiveTornadoHandler, options),
            (r"/perspective", PerspectiveTornadoHandler, options),
        ]

    def start_web(
        self, host, port, *, static=None,
        table_template="table_template.html",
        index_template="index_template.html",
        workspace_template="workspace_template.html",
        layouts_path=None,
    ):
        self.start()
        web = TornadoWeb.instance(port)
        if not self._web_configured:
            layouts = layouts_path or os.path.join(
                tempfile.gettempdir(), "psp_layouts")
            os.makedirs(layouts, exist_ok=True)
            handlers = [
                (r"/table/(.*)", TablePageHandler, {
                    "mgr": self, "template": table_template,
                    "host": host, "port": port,
                }),
                (r"/view/(.*)\.(.*)", ViewPageHandler, {
                    "mgr": self, "host": host, "port": port,
                }),
                (r"/workspace/(.*)", IndexPageHandler, {
                    "mgr": self, "layouts_path": layouts,
                    "index_template": workspace_template,
                    "host": host, "port": port,
                }),
                (r"/layout/(.*)", WorkspacePageHandler, {"path": layouts}),
                (r"/", IndexPageHandler, {
                    "mgr": self, "layouts_path": layouts,
                    "index_template": index_template,
                    "host": host, "port": port,
                }),
                (r"/versions/(.*)", IndexPageHandler, {
                    "mgr": self, "layouts_path": layouts,
                    "index_template": index_template,
                    "host": host, "port": port,
                }),
            ]
            handlers.extend(self.tornado_config())
            handlers.extend(
                (pattern, tornado.web.StaticFileHandler, options)
                for pattern, options in (static or {}).items()
            )
            web.add_handlers(handlers)
            self._web_configured = True
        web.start()
        self._web = web

    def stop_web(self):
        web, self._web = self._web, None
        if web is not None:
            web.stop()

    def close(self):
        self.stop_web()
        with self._lock:
            views = tuple(self._subscription_views.values())
            self._subscription_views.clear()
        for view in views:
            delete = getattr(view, "delete", None)
            if callable(delete):
                delete()


def _resolve_template(template):
    if not template:
        return None
    candidate = Path(template)
    if not candidate.is_absolute():
        candidate = Path(__file__).with_name(template)
    return candidate if candidate.is_file() else None


class TablePageHandler(BaseHandler):
    def initialize(self, mgr, template, host, port):
        self.mgr = mgr
        self.template = template
        self.host = host
        self.port = port

    def get(self, table_name):
        if table_name not in self.mgr.get_table_names():
            self.set_status(404)
            self.finish("Table not found")
            return
        template = _resolve_template(self.template)
        if template is not None:
            self.render(
                str(template), table_name=table_name,
                is_new_api=self.mgr.is_new_api(),
                table=self.mgr.get_table(table_name),
                editable=self.mgr.is_table_editable(table_name),
                host=self.host, port=self.port,
            )
        else:
            self.write({
                "table": table_name,
                "editable": self.mgr.is_table_editable(table_name),
                "host": self.host,
                "port": self.port,
            })


class ViewPageHandler(BaseHandler):
    def initialize(self, mgr, host="localhost", port=8080):
        self.mgr = mgr
        self.host = host
        self.port = port

    def get(self, view_name, output_format):
        if view_name not in self.mgr.get_view_names():
            self.set_status(404)
            self.finish("View not found")
            return
        config = dict(self.mgr.get_view(view_name))
        table_name = config.pop("table")
        view = self.mgr.get_table(table_name).view(**{
            key: value for key, value in config.items()
            if value not in (None, {}, [])
        })
        try:
            if output_format == "json":
                self.set_header("Content-Type", "application/json")
                self.write(json.dumps(view.to_json()))
            elif output_format == "arrow":
                self.set_header("Content-Type", "application/octet-stream")
                self.write(view.to_arrow())
            elif output_format == "csv":
                self.set_header("Content-Type", "text/csv")
                self.write(view.to_csv())
            else:
                self.set_status(400)
                self.write("Unsupported format")
        finally:
            delete = getattr(view, "delete", None)
            if callable(delete):
                delete()


class IndexPageHandler(BaseHandler):
    def initialize(
        self, mgr, layouts_path, index_template, host, port,
    ):
        self.mgr = mgr
        self.layouts_path = layouts_path
        self.index_template = index_template
        self.host = host
        self.port = port

    def get(self, url=""):
        layouts = []
        versions = []
        if self.layouts_path:
            layouts = sorted(
                os.path.basename(filename).removesuffix(".json")
                for filename in glob(os.path.join(
                    self.layouts_path, f"{url or '*'}.json")))
            if url:
                versions = sorted(
                    os.path.basename(filename).split(".")[1]
                    for filename in glob(os.path.join(
                        self.layouts_path, f"{url}.*.version")))
        template = _resolve_template(self.index_template)
        if template is not None:
            self.render(
                str(template), url=url, mgr=self.mgr,
                layouts=layouts, versions=versions,
                host=self.host, port=self.port,
            )
        else:
            self.write({
                "tables": self.mgr.get_table_names(),
                "views": self.mgr.get_view_names(),
                "layouts": layouts,
                "versions": versions,
            })


def _workspace_layout_name(url):
    """Return a single safe layout filename component from a route value."""
    decoded = str(url)
    while True:
        unquoted = unquote(decoded)
        if unquoted == decoded:
            break
        decoded = unquoted
    if (
        not decoded
        or decoded in {".", ".."}
        or "\x00" in decoded
        or "/" in decoded
        or "\\" in decoded
        or Path(decoded).name != decoded
    ):
        raise tornado.web.HTTPError(
            400, reason="invalid workspace layout name")
    return decoded


def _workspace_layout_target(root, url, suffix=".json"):
    root = Path(root).resolve()
    name = _workspace_layout_name(url)
    target = (root / f"{name}{suffix}").resolve()
    if target.parent != root:
        raise tornado.web.HTTPError(
            400, reason="workspace layout must remain inside the layouts directory")
    return name, target


class WorkspacePageHandler(BaseHandler):
    def initialize(self, path):
        self.path = Path(path).resolve()
        self.path.mkdir(parents=True, exist_ok=True)

    def get(self, url):
        name, target = _workspace_layout_target(self.path, url)
        if target.is_file():
            self.finish(target.read_bytes())
            return
        prefix = f"{name}."
        versions = sorted(
            resolved
            for candidate in self.path.iterdir()
            if candidate.name.startswith(prefix)
            and candidate.name.endswith(".version")
            and (resolved := candidate.resolve()).parent == self.path
            and resolved.is_file()
        )
        self.finish(versions[-1].read_bytes() if versions else b"{}")

    def post(self, url):
        name, target = _workspace_layout_target(self.path, url)
        if target.is_file() and target.read_bytes() == self.request.body:
            self.finish("ok")
            return
        if target.is_file():
            stamp = datetime.now().isoformat(timespec="seconds").replace(":", "-")
            _, version = _workspace_layout_target(
                self.path, name, f".{stamp}.version")
            target.replace(version)
        target.write_bytes(self.request.body)
        self.finish("ok")

    def delete(self, url):
        name, target = _workspace_layout_target(self.path, url)
        if not target.is_file():
            self.set_status(404)
            self.finish("not found")
            return
        stamp = datetime.now().isoformat(timespec="seconds").replace(":", "-")
        _, version = _workspace_layout_target(
            self.path, name, f".{stamp}.version")
        target.replace(version)
        self.finish("ok")


@sink_node
def perspective_web(
    host: str,
    port: int,
    static: dict[str, dict[str, str]] = None,
    table_template: str = "table_template.html",
    index_template: str = "index_template.html",
    workspace_template: str = "workspace_template.html",
    layouts_path: str = None,
    _sig: TS[bool] = True,
    logger: logging.Logger = None,
    _global_state: GlobalState = None,
):
    manager = PerspectiveTablesManager.current(_global_state)
    if _sig.value and manager._web is None:
        manager.start_web(
            host, port, static=static,
            table_template=table_template,
            index_template=index_template,
            workspace_template=workspace_template,
            layouts_path=layouts_path,
        )
        (logger or globals()["logger"]).info(
            "Perspective server started at http://%s:%s", host, port)


@perspective_web.stop
def _stop_perspective_web(_global_state: GlobalState = None):
    manager = PerspectiveTablesManager.current(_global_state)
    manager.stop_web()
