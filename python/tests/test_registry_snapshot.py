from hgraph import TS, eval_node, graph
from hgraph.debug import RuntimeRegistrySnapshot, runtime_registry_snapshot


@graph
def _registry_reuse_graph(ts: TS[int]) -> TS[int]:
    return ts + 1


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
