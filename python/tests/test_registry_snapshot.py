from hgraph.debug import RuntimeRegistrySnapshot, runtime_registry_snapshot


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
