import hgraph as hg
import pytest

from hgraph.debug import GraphDiagnostics, inspector
from hgraph.debug._inspector_item_id import (
    InspectorItemId,
    InspectorItemType,
    NodeValueType,
)


def test_inspector_item_id_matches_released_spelling_and_sort_order():
    assert InspectorItemId(graph=(1, 2, 3)).to_str() == "1.2.3"
    assert InspectorItemId(graph=(1, 2, 3), node=4).to_str() == "1.2.3:4"
    assert (
        InspectorItemId(
            graph=(1, 2, 3),
            node=4,
            value_type=NodeValueType.Inputs,
            value_path=(5, 6, 7),
        ).to_str()
        == "1.2.3:4/INPUTS/5/6/7"
    )

    parsed = InspectorItemId.from_str("1.2.3:4/INPUTS/5/6/7")
    assert InspectorItemId.from_object(parsed) is parsed
    assert parsed.item_type is InspectorItemType.Value
    assert parsed.graph == (1, 2, 3)
    assert parsed.node_path == ()
    assert parsed.node == 4
    assert parsed.value_type is NodeValueType.Inputs
    assert parsed.value_path == (5, 6, 7)

    InspectorItemId.__reset__()

    assert hash(InspectorItemId(graph=(1,), node=2)) == hash(
        InspectorItemId.from_str("1:2")
    )
    string_key = InspectorItemId(
        graph=(1, 2, 3),
        node=4,
        value_type=NodeValueType.Inputs,
        value_path=(5, "6", 7),
    )
    assert string_key.to_str() == "1.2.3:4/INPUTS/5/x001/7"
    assert InspectorItemId.from_str(string_key.to_str()).value_path == (5, "6", 7)
    InspectorItemId.__reset__()

    assert (
        InspectorItemId(
            graph=(),
            node=1,
            value_type=NodeValueType.Inputs,
            value_path=("i",),
        ).sort_key()
        == "001X01001"
    )
    assert (
        InspectorItemId(
            graph=(1, -1),
            node=1,
            value_type=NodeValueType.Inputs,
            value_path=("i",),
        ).sort_key()
        == "001X02001001X01001"
    )
    assert [
        item.to_str()
        for item in InspectorItemId(
            graph=(1, -1),
            node=2,
            value_type=NodeValueType.Output,
            value_path=("value",),
        ).parent_item_ids()
    ] == [
        ":1",
        ":1/GRAPHS",
        "1.-1",
        "1.-1:2",
        "1.-1:2/OUTPUT",
        "1.-1:2/OUTPUT",
    ]


def test_inspector_registers_native_diagnostics_without_adding_recurring_ticks(monkeypatch):
    import hgraph.debug._inspector as implementation

    sessions = []

    def start(session):
        sessions.append(session)

    monkeypatch.setattr(implementation._InspectorSession, "start", start)
    monkeypatch.setattr(implementation._InspectorSession, "stop", lambda session: None)

    @hg.graph
    def app(value: hg.TS[int]) -> hg.TS[int]:
        inspector(port=18080, publish_interval=0.1)
        return value + 1

    assert hg.eval_node(app, [1, 2, 3]) == [2, 3, 4]
    assert len(sessions) == 1

    snapshot = sessions[0].diagnostics.snapshot()
    assert snapshot.graph_cycles == 3
    assert snapshot.entries
    assert all(entry.stopped for entry in snapshot.entries)
    assert any(
        entry.output.available and entry.output.valid and entry.output.json
        for entry in snapshot.entries
    )


def test_inspector_retains_the_released_perspective_workspace_interactions():
    import hgraph.debug._inspector as implementation

    template = implementation.Path(implementation.__file__).with_name(
        "inspector_template.html"
    ).read_text()
    assert '<perspective-workspace id="workspace">' in template
    assert "table='inspector'" in template
    assert 'fetch_alert("/inspect/expand/"' in template
    assert 'fetch("/inspect/ref/"' in template
    assert 'window.open("/inspect_value/"' in template
    frame_template = implementation.Path(implementation.__file__).with_name(
        "frame_template.html"
    ).read_text()
    assert 'frame("/inspect/value/{{table_name}}")' in frame_template


def test_inspector_starts_publishes_and_serves_the_released_workspace(capsys):
    pytest.importorskip("perspective")
    import json
    import socket
    from urllib.parse import quote
    from urllib.request import urlopen

    import pyarrow as pa

    import hgraph.debug._inspector as implementation

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    @hg.graph
    def add_one(value: hg.TS[int]) -> hg.TS[int]:
        return value + 1

    diagnostics = GraphDiagnostics(capture_values=True)
    assert hg.eval_node(add_one, [1, 2], __observers__=[diagnostics]) == [2, 3]

    session = implementation._InspectorSession(
        diagnostics,
        port,
        0.1,
        hg.GlobalState(),
    )
    session.start()
    try:
        assert capsys.readouterr().out == (
            f"Inspector running on "
            f"http://{implementation.gethostname()}:{port}/inspector/view\n"
        )
        html = urlopen(
            f"http://127.0.0.1:{port}/inspector/view", timeout=5
        ).read().decode()
        rows = urlopen(
            f"http://127.0.0.1:{port}/inspect/rows/", timeout=5
        ).read().decode()
        assert '<perspective-workspace id="workspace">' in html
        assert rows.startswith("[") and rows != "[]"

        value_id = next(
            identifier
            for identifier, value in session.values.items()
            if "/" not in identifier and value == "3"
        )
        value_page = urlopen(
            f"http://127.0.0.1:{port}/inspect_value/{quote(value_id, safe=':.-')}",
            timeout=5,
        ).read().decode()
        value_stream = urlopen(
            f"http://127.0.0.1:{port}/inspect/value/{quote(value_id, safe=':.-')}",
            timeout=5,
        ).read()
        assert '<perspective-viewer id="viewer">' in value_page
        assert pa.ipc.RecordBatchStreamReader(value_stream).read_all().to_pylist() == [
            {"value": 3}
        ]

        node_id = next(
            row["id"]
            for row in json.loads(rows)
            if "/" not in row["id"] and ":" in row["id"]
        )
        found = urlopen(
            f"http://127.0.0.1:{port}/inspect/search/{quote(node_id, safe=':.-')}?q=OUTPUT",
            timeout=5,
        ).read().decode()
        assert found.endswith("/OUTPUT")
        assert session.rows[found]["X"] == "?"
        assert urlopen(
            f"http://127.0.0.1:{port}/inspect/stopsearch/", timeout=5
        ).read() == b"OK"
        assert found not in session.rows

        recent_view = session.manager.get_table("recent_performance").view()
        graph_view = session.manager.get_table("graph_performance").view()
        try:
            assert recent_view.to_records()
            assert graph_view.to_records()
        finally:
            recent_view.delete()
            graph_view.delete()
    finally:
        session.stop()
        session.manager.close()


def test_inspector_serves_packaged_workspace_assets_and_default_layout(
        monkeypatch, tmp_path):
    pytest.importorskip("perspective")
    import json
    import socket
    from urllib.request import urlopen

    import hgraph.adaptors.perspective._perspective as perspective_impl
    import hgraph.debug._inspector as implementation

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    node_modules = tmp_path / "node_modules"
    for asset_name in perspective_impl._PERSPECTIVE_WEB_ASSETS:
        asset = node_modules / asset_name
        asset.parent.mkdir(parents=True, exist_ok=True)
        asset.write_text(asset_name, encoding="utf-8")
    monkeypatch.setattr(
        perspective_impl, "_node_modules_root", lambda: node_modules)
    monkeypatch.setattr(implementation.tempfile, "tempdir", str(tmp_path))

    diagnostics = GraphDiagnostics(capture_values=True)
    assert hg.eval_node(hg.pass_through, [1], __observers__=[diagnostics]) == [1]
    session = implementation._InspectorSession(
        diagnostics, port, 0.1, hg.GlobalState())
    session.start()
    try:
        session.manager.start_web("127.0.0.1", port)
        assert "connectWorkspaceTables" in urlopen(
            f"http://127.0.0.1:{port}/workspace_code/workspace_tables.js",
            timeout=5,
        ).read().decode()
        assert "installTableWorkarounds" in urlopen(
            f"http://127.0.0.1:{port}/workspace_code/table_workarounds.js",
            timeout=5,
        ).read().decode()
        for asset_name in perspective_impl._PERSPECTIVE_WEB_ASSETS:
            assert urlopen(
                f"http://127.0.0.1:{port}/node_modules/{asset_name}",
                timeout=5,
            ).read().decode() == asset_name
        layout = json.loads(urlopen(
            f"http://127.0.0.1:{port}/layout/view", timeout=5
        ).read())
        assert next(iter(layout["viewers"].values()))["table"] == "inspector"
    finally:
        session.manager.stop_web()
        session.stop()
        session.manager.close()


def test_inspector_reuses_a_port_without_serving_the_stopped_session():
    pytest.importorskip("perspective")
    import json
    import socket
    from urllib.request import urlopen

    import hgraph.debug._inspector as implementation

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    def diagnostics_for(value):
        diagnostics = GraphDiagnostics(capture_values=True)
        assert hg.eval_node(
            hg.pass_through, [value], __observers__=[diagnostics]
        ) == [value]
        return diagnostics

    def served_values():
        rows = json.loads(urlopen(
            f"http://127.0.0.1:{port}/inspect/rows/", timeout=5
        ).read())
        return {row["value"] for row in rows}

    first = implementation._InspectorSession(
        diagnostics_for(11), port, 0.1, hg.GlobalState())
    first.start()
    try:
        assert "11" in served_values()
    finally:
        first.stop()
        first.manager.close()

    second = implementation._InspectorSession(
        diagnostics_for(29), port, 0.1, hg.GlobalState())
    second.start()
    try:
        values = served_values()
        assert "29" in values
        assert "11" not in values
    finally:
        second.stop()
        second.manager.close()


def test_inspector_serves_and_expands_the_graph_while_it_is_running():
    pytest.importorskip("perspective")
    import json
    import socket
    import threading
    import time
    from datetime import timedelta
    from urllib.error import URLError
    from urllib.parse import quote
    from urllib.request import urlopen

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    stop_requested = threading.Event()
    feeders = []

    @hg.push_queue(hg.TS[int])
    def live_values(sender):
        def feed():
            sender(1)

        feeder = threading.Thread(target=feed, daemon=True)
        feeders.append(feeder)
        feeder.start()

    @hg.push_queue(hg.TS[bool])
    def stop_signal(sender):
        def feed():
            if stop_requested.wait(timeout=10.0):
                sender(True)

        feeder = threading.Thread(target=feed, daemon=True)
        feeders.append(feeder)
        feeder.start()

    @hg.sink_node
    def capture(value: hg.TS[int], label: str):
        del value, label

    @hg.graph
    def app():
        inspector(port=port, publish_interval=0.05)
        ticks = hg.sum_(live_values())
        values = hg.convert[hg.TSD[int, hg.TS[int]]](
            key=ticks, ts=ticks)
        hg.map_(lambda value: value * 2, values)
        capture(ticks, label="live")
        hg.stop_engine(stop_signal())

    failures = []

    def run():
        try:
            hg.run_graph(
                app,
                run_mode=hg.EvaluationMode.REAL_TIME,
                end_time=timedelta(seconds=10),
            )
        except BaseException as error:
            failures.append(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    rows = None
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and thread.is_alive():
        try:
            rows = json.loads(urlopen(
                f"http://127.0.0.1:{port}/inspect/rows/", timeout=1
            ).read())
            if rows:
                break
        except (URLError, TimeoutError, json.JSONDecodeError):
            time.sleep(0.02)

    try:
        assert rows, failures
        def expand(identifier):
            assert urlopen(
                f"http://127.0.0.1:{port}/inspect/expand/"
                f"{quote(identifier, safe=':.-')}",
                timeout=2,
            ).status == 200
            return json.loads(urlopen(
                f"http://127.0.0.1:{port}/inspect/rows/", timeout=2
            ).read())

        def children_of(all_rows, identifier):
            return {
                row["name"].strip("\u00a0 ")
                for row in all_rows
                if row["id"].startswith(identifier + "/")
                and row["id"].count("/") == 1
            }

        map_id = next(row["id"] for row in rows if row["type"] == "MAP")
        map_row = next(row for row in rows if row["id"] == map_id)
        assert map_row["name"].strip("\u00a0 ") == "app.map_"
        expanded = expand(map_id)
        assert children_of(expanded, map_id) == {"INPUTS", "OUTPUT", "GRAPHS"}
        assert all(
            row["evals"] is None and row["nodes"] is None
            for row in expanded
            if row["id"].startswith(map_id + "/")
            and row["id"].count("/") == 1
        )

        graphs_id = map_id + "/GRAPHS"
        graphs_row = next(row for row in expanded if row["id"] == graphs_id)
        assert graphs_row["type"] == "dict"
        assert graphs_row["value"] == "1 items"
        nested = expand(graphs_id)
        nested_graph = next(
            row
            for row in nested
            if row["type"] == "GRAPH"
            and row["ord"].startswith(
                next(item["ord"] for item in expanded if item["id"] == graphs_id) + "0")
        )
        assert nested_graph["nodes"] > 0
        assert map_row["nodes"] == nested_graph["nodes"]
        assert map_row["subgraphs"] == 1

        push_id = next(
            row["id"] for row in rows if row["type"] == "PUSH_SOURCE")
        assert next(
            row for row in rows if row["id"] == push_id
        )["name"].strip("\u00a0 ") == "app.live_values"
        assert next(
            row for row in rows if row["id"] == push_id
        )["value"].endswith(" items in the queue")
        assert children_of(expand(push_id), push_id) == {"OUTPUT", "SCALARS"}

        sink_id = next(
            row["id"]
            for row in rows
            if row["type"] == "SINK" and "capture" in row["name"])
        assert next(
            row for row in rows if row["id"] == sink_id
        )["name"].strip("\u00a0 ") == "app.capture"
        assert children_of(expand(sink_id), sink_id) == {"INPUTS", "SCALARS"}
    finally:
        stop_requested.set()
        for feeder in feeders:
            feeder.join(timeout=2.0)
        thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert not failures


def test_inspector_value_endpoint_preserves_native_frame_columns_and_rows():
    pa = pytest.importorskip("pyarrow")
    import hgraph.debug._inspector as implementation

    table = pa.table({"instrument": ["A", "B"], "value": [1.5, 2.5]})
    diagnostics = GraphDiagnostics(capture_values=True)
    assert hg.eval_node(
        hg.pass_through,
        [table],
        resolution_dict={"tsd": hg.TS[hg.Frame]},
        __observers__=[diagnostics],
    )[0].equals(table)

    session = object.__new__(implementation._InspectorSession)
    session._lock = implementation.threading.RLock()
    session.expanded = {""}
    session._navigation = {}
    session._entry_item_ids = {}
    session._frame_cache = {}
    rows, values = session._build_rows(diagnostics.snapshot())
    session.rows = rows
    session.values = values
    session.frames = session._built_frames

    item = next(iter(session.frames))
    restored = pa.ipc.RecordBatchStreamReader(
        session.value_ipc_for(item)
    ).read_all()
    assert restored.equals(table)


def test_inspector_value_endpoint_preserves_released_tsd_table_shape():
    pytest.importorskip("pyarrow")

    diagnostics = GraphDiagnostics(capture_values=True)

    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return values

    assert hg.eval_node(
        app,
        [{"a": 1}],
        __observers__=[diagnostics],
    ) == [{"a": 1}]

    entry = next(
        item
        for item in diagnostics.snapshot().entries
        if item.output.has_frame
        and item.output.frame.column_names
        == ["__key_1_removed__", "__key_1__", "value"]
    )
    assert entry.output.table_error == ""
    assert entry.output.frame.to_pylist() == [
        {"__key_1_removed__": False, "__key_1__": "a", "value": 1}
    ]


def test_inspector_value_endpoint_preserves_released_partial_ref_bundle_shape():
    pytest.importorskip("pyarrow")

    class AB(hg.TimeSeriesSchema):
        a: hg.TS[int]
        b: hg.TS[str]

    @hg.compute_node
    def pass_ref(value: hg.REF[hg.TSB[AB]]) -> hg.REF[hg.TSB[AB]]:
        return value.value

    @hg.graph
    def partial_ref_bundle(value: hg.TS[int]) -> hg.REF[hg.TSB[AB]]:
        return pass_ref(hg.combine[hg.TSB[AB]](a=value))

    diagnostics = GraphDiagnostics(capture_values=True)
    assert hg.eval_node(
        partial_ref_bundle,
        [1],
        __observers__=[diagnostics],
    ) == [{"a": 1}]

    entry = next(
        item
        for item in diagnostics.snapshot().entries
        if item.label.endswith((".pass_ref", ":pass_ref"))
    )
    assert entry.output.table_error == ""
    assert entry.output.frame.column_names == ["a", "b"]
    assert entry.output.frame.to_pylist() == [{"a": 1, "b": None}]


def test_inspector_uses_released_dynamic_nested_graph_id_shape():
    import hgraph.debug._inspector as implementation

    diagnostics = GraphDiagnostics(capture_values=True)

    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_("add_", values, hg.const(1, tp=hg.TS[int]))

    assert hg.eval_node(
        app,
        [{"a": 1, "b": 2}],
        __observers__=[diagnostics],
    ) == [{"a": 2, "b": 3}]

    _, graph_ids, _ = implementation._InspectorSession._graph_layout(
        diagnostics.snapshot().entries)
    dynamic_ids = [
        graph_id
        for graph_id in graph_ids.values()
        if len(graph_id) >= 2 and graph_id[-1] < 0
    ]
    assert dynamic_ids


def test_inspector_expands_nested_owned_values_and_serves_each_value():
    import hgraph.debug._inspector as implementation

    diagnostics = GraphDiagnostics(capture_values=True)

    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return values

    assert hg.eval_node(
        app,
        [{"outer": 1}],
        __observers__=[diagnostics],
    ) == [{"outer": 1}]

    entry = next(
        item
        for item in diagnostics.snapshot().entries
        if item.output.available and item.output.json.startswith("{")
    )
    category = InspectorItemId(
        node=entry.node_index,
        value_type=NodeValueType.Output,
    )
    child = InspectorItemId(
        node=entry.node_index,
        value_type=NodeValueType.Output,
        value_path=("outer",),
    )
    session = object.__new__(implementation._InspectorSession)
    session.expanded = {category.to_str(), child.to_str()}

    rows, values = session._value_rows(
        entry, category, {"outer": {"inner": 1}})
    assert {row["id"] for row in rows} == {
        child.to_str(),
        InspectorItemId(
            node=entry.node_index,
            value_type=NodeValueType.Output,
            value_path=("outer", "inner"),
        ).to_str(),
    }
    assert values[child.to_str()] == '{"inner": 1}'


def test_inspector_maps_owned_reference_metadata_to_output_navigation():
    import hgraph.debug._inspector as implementation

    @hg.compute_node
    def forward(value: hg.REF[hg.TS[int]]) -> hg.REF[hg.TS[int]]:
        return value.value

    diagnostics = GraphDiagnostics(capture_values=True)
    assert hg.eval_node(forward, [42], __observers__=[diagnostics]) == [42]

    snapshot = diagnostics.snapshot()
    _, _, node_ids = implementation._InspectorSession._graph_layout(
        snapshot.entries
    )
    forward_entry = next(
        entry for entry in snapshot.entries if entry.label.endswith(":forward")
    )
    assert len(forward_entry.output.target_node_ids) == 1
    source_id = node_ids[forward_entry.output.target_node_ids[0]]
    expected = InspectorItemId(
        graph=source_id.graph,
        node=source_id.node,
        value_type=NodeValueType.Output,
    ).to_str()

    session = object.__new__(implementation._InspectorSession)
    session.expanded = {""}
    session._build_rows(snapshot)
    assert session._navigation[node_ids[forward_entry.id].to_str()] == expected


def test_inspector_distinguishes_direct_output_from_reference_navigation():
    import hgraph.debug._inspector as implementation

    @hg.compute_node
    def forward(value: hg.REF[hg.TS[int]]) -> hg.REF[hg.TS[int]]:
        return value.value

    @hg.graph
    def twice(value: hg.TS[int]) -> hg.REF[hg.TS[int]]:
        return forward(forward(value))

    diagnostics = GraphDiagnostics(capture_values=True)
    assert hg.eval_node(twice, [42], __observers__=[diagnostics]) == [42]

    snapshot = diagnostics.snapshot()
    _, _, node_ids = implementation._InspectorSession._graph_layout(
        snapshot.entries)
    forwards = sorted(
        (
            entry for entry in snapshot.entries
            if entry.label.endswith(".forward")
            or entry.label.endswith(":forward")
        ),
        key=lambda entry: entry.node_index,
    )
    assert len(forwards) == 2
    first, second = forwards
    assert second.input.json == '{"value":42}'
    assert len(second.input.targets) == 1
    assert tuple(second.input.bound_targets[0].source_path) == ('"value"',)
    assert tuple(second.input.targets[0].source_path) == ('"value"',)
    assert second.input.table_error == ""
    assert second.input.frame.column_names == ["value"]
    assert second.input.bound_targets[0].node_id == first.id
    assert second.input.targets[0].node_id != first.id

    second_id = node_ids[second.id]
    input_id = InspectorItemId(
        graph=second_id.graph,
        node=second_id.node,
        value_type=NodeValueType.Inputs,
        value_path=("value",),
    ).to_str()
    session = object.__new__(implementation._InspectorSession)
    session.expanded = {"", second_id.to_str()}
    session._frame_cache = {}
    session._build_rows(snapshot)

    assert input_id in session._output_navigation
    direct = session._output_navigation[input_id]
    assert input_id in session._navigation, session._navigation
    referenced = session._navigation[input_id]
    assert direct == InspectorItemId(
        graph=node_ids[first.id].graph,
        node=node_ids[first.id].node,
        value_type=NodeValueType.Output,
    ).to_str()
    assert referenced != direct


def test_inspector_maps_reference_fields_to_their_exact_output_paths():
    import hgraph.debug._inspector as implementation

    class AB(hg.TimeSeriesSchema):
        a: hg.TS[int]
        b: hg.TS[int]

    @hg.compute_node
    def pass_ref(value: hg.REF[hg.TS[int]]) -> hg.REF[hg.TS[int]]:
        return value.value

    @hg.graph
    def select_a(value: hg.TSB[AB]) -> hg.REF[hg.TS[int]]:
        return pass_ref(value.a)

    diagnostics = GraphDiagnostics(capture_values=True)
    assert hg.eval_node(
        select_a,
        [{"a": 1, "b": 2}],
        __observers__=[diagnostics],
    ) == [1]

    snapshot = diagnostics.snapshot()
    _, _, node_ids = implementation._InspectorSession._graph_layout(
        snapshot.entries)
    entry = next(
        item for item in snapshot.entries
        if item.label.endswith((".pass_ref", ":pass_ref"))
    )
    assert len(entry.output.targets) == 1
    target = entry.output.targets[0]
    source = node_ids[target.node_id]
    expected = InspectorItemId(
        graph=source.graph,
        node=source.node,
        value_type=NodeValueType.Output,
        value_path=("a",),
    ).to_str()

    session = object.__new__(implementation._InspectorSession)
    session.expanded = {""}
    session._build_rows(snapshot)
    assert session._navigation[node_ids[entry.id].to_str()] == expected


def test_inspector_publishes_released_node_categories_from_owned_snapshot():
    import hgraph.debug._inspector as implementation

    diagnostics = GraphDiagnostics(capture_values=True)

    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_("add_", values, hg.const(1, tp=hg.TS[int]))

    assert hg.eval_node(
        app,
        [{"a": 1}],
        __observers__=[diagnostics],
    ) == [{"a": 2}]

    snapshot = diagnostics.snapshot()
    _, _, node_ids = implementation._InspectorSession._graph_layout(
        snapshot.entries)
    map_entry = next(entry for entry in snapshot.entries if "map" in entry.label)
    map_id = node_ids[map_entry.id]
    graphs_id = InspectorItemId(
        graph=map_id.graph,
        node=map_id.node,
        value_type=NodeValueType.Graphs,
    )

    session = object.__new__(implementation._InspectorSession)
    session.expanded = {"", map_id.to_str(), graphs_id.to_str()}
    rows, _ = session._build_rows(snapshot)

    children = {
        row["name"].strip("\u00a0 ")
        for identifier, row in rows.items()
        if identifier.startswith(map_id.to_str() + "/")
        and identifier.count("/") == 1
    }
    assert children == {"INPUTS", "OUTPUT", "GRAPHS"}
    assert any(row["type"] == "GRAPH" for row in rows.values())


def test_inspector_recursive_expand_opens_all_descendants():
    import threading
    import hgraph.debug._inspector as implementation

    diagnostics = GraphDiagnostics(capture_values=True)

    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_("add_", values, hg.const(1, tp=hg.TS[int]))

    assert hg.eval_node(
        app, [{"a": 1}], __observers__=[diagnostics]
    ) == [{"a": 2}]

    snapshot = diagnostics.snapshot()
    _, _, node_ids = implementation._InspectorSession._graph_layout(
        snapshot.entries)
    map_entry = next(entry for entry in snapshot.entries if "map" in entry.label)
    map_id = node_ids[map_entry.id]

    session = object.__new__(implementation._InspectorSession)
    session._lock = threading.RLock()
    session.diagnostics = diagnostics
    session.expanded = {""}
    session.publish = lambda force=False: None
    assert session.command(
        "expand", map_id.to_str(), {"all": ["true"]}
    ) == ("", "text/plain")

    descendants = {
        identifier
        for identifier in implementation._InspectorSession._expandable_ids(snapshot)
        if map_id.is_parent_of(InspectorItemId.from_str(identifier))
    }
    assert descendants
    assert descendants <= session.expanded
