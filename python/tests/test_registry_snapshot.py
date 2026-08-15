from hgraph import (
    MIN_ST,
    MIN_TD,
    TS,
    TSD,
    eval_node,
    generator,
    graph,
    map_,
    null_sink,
    register_adaptor,
    run_graph,
    service_adaptor,
    service_adaptor_impl,
)
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
