import hgraph as hg
import pyarrow as pa

from hgraph.debug import (
    GraphDiagnosticEntityKind,
    GraphDiagnostics,
    graph_diagnostics_rows,
)


def test_native_diagnostics_observes_eval_node_and_owns_its_snapshot():
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_("add_", values, hg.const(10, tp=hg.TS[int]))

    diagnostics = GraphDiagnostics(recent_window=4)
    assert hg.eval_node(
        app,
        [
            {"one": 1, "two": 2},
            {"one": hg.REMOVE},
            {"two": hg.REMOVE},
        ],
        __observers__=[diagnostics],
    ) == [
        {"one": 11, "two": 12},
        {"one": hg.REMOVE},
        {"two": hg.REMOVE},
    ]

    snapshot = diagnostics.snapshot()
    assert snapshot.graph_cycles == 3
    assert snapshot.planned_bytes > 0
    assert snapshot.dynamic_live_bytes == 0
    assert snapshot.dynamic_reserved_bytes == 0
    assert snapshot.entries
    assert all(entry.stopped for entry in snapshot.entries)
    assert any(entry.kind == GraphDiagnosticEntityKind.GRAPH for entry in snapshot.entries)
    assert any(
        entry.kind == GraphDiagnosticEntityKind.NODE and entry.evaluation.count > 0
        for entry in snapshot.entries
    )
    mapped = next(entry for entry in snapshot.entries if "map" in entry.label)
    assert mapped.peak_storage.nested_graph_count == 2
    assert mapped.peak_storage.dynamic_reserved_bytes > 0
    assert mapped.storage.nested_graph_count == 0
    assert any(
        entry.kind == GraphDiagnosticEntityKind.NODE
        and entry.peak_storage.nested_graph_capacity == 0
        and entry.peak_storage.dynamic_reserved_bytes > 0
        for entry in snapshot.entries
    )

    rows = graph_diagnostics_rows(snapshot)
    assert len(rows) == len(snapshot.entries)
    assert all(row["path"] and row["schema"] for row in rows)
    assert any(row["kind"] == "node" and row["node_kind"] for row in rows)
    assert any(row["kind"] == "graph" and row["node_kind"] is None for row in rows)

    diagnostics.reset()
    assert diagnostics.snapshot().entries == []


def test_native_diagnostics_rejects_reset_from_an_active_python_node():
    diagnostics = GraphDiagnostics()

    @hg.compute_node
    def reset_while_active(value: hg.TS[int]) -> hg.TS[bool]:
        try:
            diagnostics.reset()
        except RuntimeError:
            return True
        return False

    assert hg.eval_node(
        reset_while_active,
        [1],
        __observers__=[diagnostics],
    ) == [True]
    assert diagnostics.snapshot().entries


def test_native_diagnostics_attributes_tsw_buffers_through_python_wiring():
    @hg.graph
    def app(value: hg.TS[int]) -> hg.TSW[int]:
        return hg.to_window(value, 8, 1)

    diagnostics = GraphDiagnostics()
    assert hg.eval_node(
        app,
        [1, 2, 3],
        __observers__=[diagnostics],
    ) == [1, 2, 3]

    window = next(
        entry for entry in diagnostics.snapshot().entries if "to_window" in entry.label
    )
    assert window.storage.dynamic_reserved_bytes == 0
    assert window.peak_storage.dynamic_live_bytes > 0
    assert (
        window.peak_storage.dynamic_reserved_bytes
        > window.peak_storage.dynamic_live_bytes
    )


def test_native_diagnostics_recurses_into_python_visible_value_payloads():
    @hg.graph
    def app(value: hg.TS[str]) -> hg.TSW[str]:
        return hg.to_window(value, 8, 1)

    values = ["a" * 256, "b" * 320, "c" * 384]
    diagnostics = GraphDiagnostics()
    assert hg.eval_node(app, values, __observers__=[diagnostics]) == values

    window = next(
        entry for entry in diagnostics.snapshot().entries if "to_window" in entry.label
    )
    assert window.storage.dynamic_reserved_bytes == 0
    assert window.peak_storage.dynamic_live_bytes > sum(len(value) for value in values)
    assert (
        window.peak_storage.dynamic_reserved_bytes
        > window.peak_storage.dynamic_live_bytes
    )


def test_native_diagnostics_renders_reference_targets_for_python_inspector():
    @hg.compute_node
    def forward(value: hg.REF[hg.TS[int]]) -> hg.REF[hg.TS[int]]:
        return value.value

    diagnostics = GraphDiagnostics(capture_values=True)
    assert hg.eval_node(
        forward,
        [42],
        __observers__=[diagnostics],
    ) == [42]

    entry = next(
        item
        for item in diagnostics.snapshot().entries
        if item.label.endswith(":forward")
    )
    assert entry.output.valid
    assert entry.output.error == ""
    assert entry.output.json == "42"


def test_native_diagnostics_renders_reference_targets_nested_in_containers():
    @hg.compute_node
    def wrap(value: hg.REF[hg.TS[int]]) -> hg.TSD[str, hg.REF[hg.TS[int]]]:
        return {"selected": value.value}

    diagnostics = GraphDiagnostics(capture_values=True)
    assert hg.eval_node(wrap, [42], __observers__=[diagnostics]) == [
        {"selected": 42}
    ]

    entry = next(
        item
        for item in diagnostics.snapshot().entries
        if item.label.endswith(":wrap")
    )
    assert entry.output.valid
    assert entry.output.error == ""
    assert entry.output.json == '{"selected":42}'


def test_native_diagnostics_preserves_partial_composite_reference_fields():
    class AB(hg.TimeSeriesSchema):
        a: hg.TS[int]
        b: hg.TS[str]

    @hg.compute_node
    def forward(value: hg.REF[hg.TSB[AB]]) -> hg.REF[hg.TSB[AB]]:
        return value.value

    @hg.graph
    def app(a: hg.TS[int], b: hg.TS[str]) -> hg.REF[hg.TSB[AB]]:
        return forward(hg.combine[hg.TSB[AB]](a=a, b=b))

    diagnostics = GraphDiagnostics(capture_values=True)
    assert hg.eval_node(
        app,
        a=[42],
        b=[None],
        __observers__=[diagnostics],
    ) == [{"a": 42}]

    entry = next(
        item
        for item in diagnostics.snapshot().entries
        if item.label.endswith(":forward")
    )
    assert entry.output.valid
    assert entry.output.error == ""
    assert entry.output.json == '{"a":42,"b":null}'


def test_native_diagnostics_retains_frame_values_as_owned_arrow_handles():
    table = pa.table({"value": [1, 2]})
    diagnostics = GraphDiagnostics(capture_values=True)

    assert hg.eval_node(
        hg.pass_through,
        [table],
        resolution_dict={"tsd": hg.TS[hg.Frame]},
        __observers__=[diagnostics],
    )[0].equals(table)

    captured = next(
        entry.output
        for entry in diagnostics.snapshot().entries
        if entry.output.available and entry.output.has_frame
    )
    assert captured.error == ""
    assert captured.json == '"frame[2 x 1]"'
    assert captured.frame.equals(table)
