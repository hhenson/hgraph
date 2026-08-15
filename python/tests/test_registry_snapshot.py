from typing import Set, Tuple

import pytest

from hgraph import (
    MIN_ST,
    MIN_TD,
    TS,
    TSD,
    TSS,
    collect,
    convert,
    eq_,
    eval_node,
    from_json,
    generator,
    graph,
    if_,
    keys_,
    map_,
    match_,
    nothing,
    null_sink,
    race,
    register_adaptor,
    run_graph,
    service_adaptor,
    service_adaptor_impl,
    to_json,
)
from hgraph import OUT

from hgraph.debug import RuntimeRegistrySnapshot, runtime_registry_snapshot


@graph
def _registry_reuse_graph(ts: TS[int]) -> TS[int]:
    return ts + 1


@service_adaptor
def _registry_reuse_adaptor(
    request: TS[int], path: str = "registry_reuse"
) -> TS[int]: ...


@service_adaptor_impl(interfaces=_registry_reuse_adaptor)
def _registry_reuse_adaptor_impl(
    request: TSD[int, TS[int]], path: str
) -> TSD[int, TS[int]]:
    return map_(_registry_reuse_graph, request)


@graph
def _registry_reuse_adaptor_graph(ts: TS[int]) -> TS[int]:
    register_adaptor("registry_reuse", _registry_reuse_adaptor_impl)
    return _registry_reuse_adaptor(ts, path="registry_reuse")


@graph
def _registry_reuse_map_graph(
    ts: TSD[int, TS[int]],
) -> TSD[int, TS[int]]:
    return map_(_registry_reuse_graph, ts)


def test_runtime_registry_snapshot_exposes_cold_path_cardinalities():
    snapshot = runtime_registry_snapshot()

    assert isinstance(snapshot, RuntimeRegistrySnapshot)
    assert snapshot.node_runtime_types >= 0
    assert snapshot.graph_programs >= 0
    assert snapshot.graph_runtime_types >= snapshot.graph_programs
    assert snapshot.executor_runtime_types >= 0
    assert snapshot.type_records >= (
        snapshot.node_runtime_types
        + snapshot.graph_runtime_types
        + snapshot.executor_runtime_types
    )


def test_repeated_equivalent_graph_construction_reuses_runtime_types():
    assert eval_node(_registry_reuse_graph, [1, 2]) == [2, 3]
    after_first = runtime_registry_snapshot()

    assert eval_node(_registry_reuse_graph, [1, 2]) == [2, 3]
    after_repeated = runtime_registry_snapshot()

    assert after_repeated.node_runtime_types == after_first.node_runtime_types
    assert after_repeated.graph_programs == after_first.graph_programs
    assert after_repeated.graph_runtime_types == after_first.graph_runtime_types
    assert (
        after_repeated.executor_runtime_types
        == after_first.executor_runtime_types
    )
    assert after_repeated.type_records == after_first.type_records


def test_repeated_service_adaptor_wiring_reuses_runtime_types():
    eval_node(_registry_reuse_adaptor_graph, [1, 2])
    after_first = runtime_registry_snapshot()

    eval_node(_registry_reuse_adaptor_graph, [1, 2])
    after_repeated = runtime_registry_snapshot()

    assert after_repeated.node_runtime_types == after_first.node_runtime_types
    assert after_repeated.graph_programs == after_first.graph_programs
    assert after_repeated.graph_runtime_types == after_first.graph_runtime_types
    assert (
        after_repeated.executor_runtime_types
        == after_first.executor_runtime_types
    )
    assert after_repeated.type_records == after_first.type_records


@generator
def _dense_tsd_pulse(cycles: int) -> TSD[int, TS[int]]:
    for i in range(cycles):
        yield MIN_TD, {0: i, 1: i + 1}


def _dense_sink_graph(cycles: int):
    @graph
    def _g():
        null_sink(map_(_registry_reuse_graph, _dense_tsd_pulse(cycles)))

    return _g


def _lock_delta_for(sink_graph, cycles: int) -> int:
    before = runtime_registry_snapshot().type_system_lock_acquisitions
    run_graph(
        sink_graph,
        start_time=MIN_ST,
        end_time=MIN_ST + (cycles + 2) * MIN_TD,
    )
    return runtime_registry_snapshot().type_system_lock_acquisitions - before


def test_wired_evaluation_acquires_no_type_system_locks():
    """The 2026-07-02 ruling, enforceable: type resolution is what wiring is
    for. Evaluating a wired graph must never touch a type-system mutex, so
    runs of the same warm graph at different cycle counts acquire IDENTICAL
    lock counts — any difference is per-tick registry traffic.

    Driven through run_graph with a Python generator source and a native
    sink: that is the production tick path. (eval_node is deliberately not
    the harness here — its own input-injection and per-tick output recording
    convert through delta values and are measured separately.)"""
    short_graph = _dense_sink_graph(6)
    long_graph = _dense_sink_graph(24)
    # Warm every wiring/interning cache; a cold first run may intern.
    _lock_delta_for(short_graph, 6)
    _lock_delta_for(long_graph, 24)

    short_delta = _lock_delta_for(short_graph, 6)
    long_delta = _lock_delta_for(long_graph, 24)

    assert long_delta == short_delta


@generator
def _int_pulse(cycles: int) -> TS[int]:
    for i in range(cycles):
        yield MIN_TD, i


def _ref_route_sink_graph(cycles: int):
    @graph
    def _g():
        source = _int_pulse(cycles)
        null_sink(if_(source % 2 == 0, source).true)

    return _g


def test_reference_routing_acquires_no_type_system_locks():
    """if_ routes references every cycle; the empty reference is state-held,
    so steady-state routing must not touch a type-system mutex."""
    short_graph = _ref_route_sink_graph(6)
    long_graph = _ref_route_sink_graph(24)
    _lock_delta_for(short_graph, 6)
    _lock_delta_for(long_graph, 24)

    short_delta = _lock_delta_for(short_graph, 6)
    long_delta = _lock_delta_for(long_graph, 24)

    assert long_delta == short_delta


# ---------------------------------------------------------------------------
# Operator-family lock matrix (audit 2026-08-15).
#
# One family per operator group the audit found acquiring type-system locks
# per tick. The invariant is the same N-vs-2N equality as above; families
# that still violate the ruling carry xfail(strict=True), so fixing the
# operator forces the marker's removal in the same change (an XPASS under
# strict is a failure). The matrix is the enforcement the audit found
# missing: the original test wired only two graphs.
# ---------------------------------------------------------------------------


@generator
def _tuple_pulse(cycles: int) -> TS[Tuple[int, ...]]:
    for i in range(cycles):
        yield MIN_TD, (i, i + 1)


@generator
def _str_pulse(cycles: int) -> TS[str]:
    for i in range(cycles):
        yield MIN_TD, f"value-{i}"


@generator
def _churn_tsd_pulse(cycles: int) -> TSD[int, TS[int]]:
    # A rotating key insert + removal per cycle, so key-set-shaped outputs
    # (keys_, set conversions) re-emit every cycle rather than settling.
    from hgraph import REMOVE_IF_EXISTS

    for i in range(cycles):
        yield MIN_TD, {i % 3: i, (i + 1) % 3: REMOVE_IF_EXISTS}


def _convert_ts_to_set_graph(cycles: int):
    @graph
    def _g():
        null_sink(convert[TS[Set[int]]](_int_pulse(cycles)))

    return _g


def _collect_tuple_graph(cycles: int):
    @graph
    def _g():
        null_sink(collect[TS[Tuple[int, ...]]](_int_pulse(cycles)))

    return _g


def _keys_tsd_graph(cycles: int):
    @graph
    def _g():
        null_sink(keys_[OUT : TS[Set[int]]](_churn_tsd_pulse(cycles)))

    return _g


def _getitem_tsd_graph(cycles: int):
    @graph
    def _g():
        null_sink(_dense_tsd_pulse(cycles)[_int_pulse(cycles) % 2])

    return _g


def _eq_tuple_graph(cycles: int):
    @graph
    def _g():
        source = _tuple_pulse(cycles)
        null_sink(eq_(source, source))

    return _g


def _match_str_graph(cycles: int):
    @graph
    def _g():
        null_sink(match_("value-([0-9]+)", _str_pulse(cycles)).is_match)

    return _g


def _json_roundtrip_graph(cycles: int):
    @graph
    def _g():
        null_sink(from_json[TS[int]](to_json(_int_pulse(cycles))))

    return _g


def _race_graph(cycles: int):
    @graph
    def _g():
        null_sink(race(_int_pulse(cycles), nothing[TS[int]]()))

    return _g


_LOCK_MATRIX = [
    pytest.param(_convert_ts_to_set_graph, id="convert_ts_to_set"),
    pytest.param(_collect_tuple_graph, id="collect_tuple"),
    pytest.param(_keys_tsd_graph, id="keys_tsd_as_set"),
    pytest.param(_getitem_tsd_graph, id="getitem_tsd_by_key"),
    pytest.param(_eq_tuple_graph, id="eq_tuple_fallback"),
    pytest.param(_match_str_graph, id="match_str"),
    pytest.param(_json_roundtrip_graph, id="json_roundtrip"),
    # race_ is deliberately unmarked: its Out<REF>::set locks fire on winner/
    # reference CHANGES, which this stable-winner driver never produces, so
    # the family holds as a steady-state guard while the rebind-path fix
    # lands in the race_ rework.
    pytest.param(_race_graph, id="race_ref"),
]


@pytest.mark.parametrize("builder", _LOCK_MATRIX)
def test_operator_family_lock_matrix(builder):
    short_graph = builder(6)
    long_graph = builder(24)
    # Warm every wiring/interning cache; a cold first run may intern.
    _lock_delta_for(short_graph, 6)
    _lock_delta_for(long_graph, 24)

    short_delta = _lock_delta_for(short_graph, 6)
    long_delta = _lock_delta_for(long_graph, 24)

    assert long_delta == short_delta


def test_repeated_higher_order_wiring_reuses_runtime_types():
    eval_node(_registry_reuse_map_graph, [{1: 1}, {1: 2}])
    after_first = runtime_registry_snapshot()

    eval_node(_registry_reuse_map_graph, [{1: 1}, {1: 2}])
    after_repeated = runtime_registry_snapshot()

    assert after_repeated.node_runtime_types == after_first.node_runtime_types
    assert after_repeated.graph_programs == after_first.graph_programs
    assert after_repeated.graph_runtime_types == after_first.graph_runtime_types
    assert (
        after_repeated.executor_runtime_types
        == after_first.executor_runtime_types
    )
    assert after_repeated.type_records == after_first.type_records
