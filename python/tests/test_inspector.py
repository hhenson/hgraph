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
    assert parsed.item_type is InspectorItemType.Value
    assert parsed.graph == (1, 2, 3)
    assert parsed.node == 4
    assert parsed.value_type is NodeValueType.Inputs
    assert parsed.value_path == (5, 6, 7)

    InspectorItemId.__reset__()
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


def test_inspector_starts_publishes_and_serves_the_released_workspace():
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
        row["name"]
        for identifier, row in rows.items()
        if identifier.startswith(map_id.to_str() + "/")
        and identifier.count("/") == 1
    }
    assert children == {"INPUTS", "OUTPUT", "GRAPHS"}
    assert any(row["type"] == "GRAPH" for row in rows.values())
