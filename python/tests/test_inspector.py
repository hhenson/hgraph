import hgraph as hg

from hgraph.debug import InspectionEntityKind, Inspector, inspection_rows


def test_native_inspector_observes_eval_node_and_owns_its_snapshot():
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_("add_", values, hg.const(10, tp=hg.TS[int]))

    inspector = Inspector(recent_window=4)
    assert hg.eval_node(
        app,
        [
            {"one": 1, "two": 2},
            {"one": hg.REMOVE},
            {"two": hg.REMOVE},
        ],
        __observers__=[inspector],
    ) == [
        {"one": 11, "two": 12},
        {"one": hg.REMOVE},
        {"two": hg.REMOVE},
    ]

    snapshot = inspector.snapshot()
    assert snapshot.graph_cycles == 3
    assert snapshot.planned_bytes > 0
    assert snapshot.dynamic_live_bytes == 0
    assert snapshot.dynamic_reserved_bytes == 0
    assert snapshot.entries
    assert all(entry.stopped for entry in snapshot.entries)
    assert any(entry.kind == InspectionEntityKind.GRAPH for entry in snapshot.entries)
    assert any(
        entry.kind == InspectionEntityKind.NODE and entry.evaluation.count > 0
        for entry in snapshot.entries
    )
    mapped = next(entry for entry in snapshot.entries if "map" in entry.label)
    assert mapped.peak_storage.nested_graph_count == 2
    assert mapped.peak_storage.dynamic_reserved_bytes > 0
    assert mapped.storage.nested_graph_count == 0
    assert any(
        entry.kind == InspectionEntityKind.NODE
        and entry.peak_storage.nested_graph_capacity == 0
        and entry.peak_storage.dynamic_reserved_bytes > 0
        for entry in snapshot.entries
    )

    rows = inspection_rows(snapshot)
    assert len(rows) == len(snapshot.entries)
    assert all(row["path"] and row["schema"] for row in rows)
    assert any(row["kind"] == "node" and row["node_kind"] for row in rows)
    assert any(row["kind"] == "graph" and row["node_kind"] is None for row in rows)

    inspector.reset()
    assert inspector.snapshot().entries == []


def test_native_inspector_rejects_reset_from_an_active_python_node():
    inspector = Inspector()

    @hg.compute_node
    def reset_while_active(value: hg.TS[int]) -> hg.TS[bool]:
        try:
            inspector.reset()
        except RuntimeError:
            return True
        return False

    assert hg.eval_node(
        reset_while_active,
        [1],
        __observers__=[inspector],
    ) == [True]
    assert inspector.snapshot().entries
