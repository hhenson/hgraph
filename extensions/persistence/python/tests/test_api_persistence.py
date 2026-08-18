"""Durable record/replay API behaviours (moved with hgraph-persistence,
RFC 0025 checkpoint 4)."""

import pyarrow as pa

import hgraph as hg
from hgraph import TS, eval_node, graph


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def test_durable_replay_overload_registers_partition_signature():
    # Moved from core's test_native_docstrings when the partitioned frame
    # replay overload became extension-registered (RFC 0025 checkpoint 4).
    import _hgraph

    import hgraph_persistence  # noqa: F401  (registers the frame overloads)

    replay_overloads = _hgraph.operator_overload_signatures("replay")
    assert any(
        ("partition_names", False, "tuple[str, ...]", True) in parameters
        and ("removed_names", False, "tuple[str, ...]", True) in parameters
        for parameters, *_ in replay_overloads
    )


def test_component_record_replay_modes():
    hg.set_record_replay_config(hg.DATA_FRAME)
    M = hg.RecordReplayEnum

    @hg.component
    def calc(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return lhs + rhs

    @graph
    def recording(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.RECORD):
            return calc(a, b)

    out = eval_node(recording, [1, None, 3], [10, 20, None])
    check(out == [11, 21, 23], f"record: {out}")
    for key in ("calc.lhs", "calc.rhs", "calc.__out__"):
        check(hg.frame_store_contains(key), f"missing frame {key}")

    @graph
    def replaying(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.REPLAY):
            return calc(a, b)

    # The recordings win over garbage live inputs.
    out = eval_node(replaying, [100, 100, 100], [100, 100, 100])
    check(out == [11, 21, 23], f"replay: {out}")

    @graph
    def comparing(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.COMPARE):
            return calc(a, b)

    eval_node(comparing, [100, 100, 100], [100, 100, 100])
    check(hg.comparison_summary("calc.__compare__") == (3, 0), "compare clean")

    @graph
    def recovering(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.RECOVER):
            return calc(a, b)

    # Seeded from the recordings at start (1+10), live overrides (100+10).
    out = eval_node(recovering, [None, 100], [None, None])
    check(out == [11, 110], f"recover: {out}")

    hg.set_record_replay_config(hg.IN_MEMORY)




def test_frame_pyarrow_round_trip():
    # Frames cross the boundary as pyarrow.Tables (the Arrow C stream
    # protocol - zero copy): store reads return Tables, and Tables convert
    # back to Frame values.
    import pyarrow as pa

    hg.set_record_replay_config(hg.DATA_FRAME)

    @hg.component
    def snap(x: TS[int]) -> TS[int]:
        return x + x

    @graph
    def recording(x: TS[int]) -> TS[int]:
        with hg.record_replay_scope(hg.RecordReplayEnum.RECORD):
            return snap(x)

    eval_node(recording, [1, 2, 3])
    table = hg.frame_store_read("snap.__out__")
    check(isinstance(table, pa.Table), f"expected a pyarrow.Table, got {type(table)}")
    check(table.column("value").to_pylist() == [2, 4, 6], f"values: {table.to_pydict()}")
    check(table.num_columns == 3, "bitemporal columns present")
    hg.set_record_replay_config(hg.IN_MEMORY)


