"""Durable mesh recovery compared with uninterrupted public Python wiring."""

import hgraph as hg
import pytest

from .test_component_recovery_scenarios import CUTS, compare_restarts, running_total


_mesh_stopped = []


@hg.compute_node(valid=("value",))
def add_optional_peer(value: hg.TS[int], peer: hg.TS[int]) -> hg.TS[int]:
    return value.value + (peer.value if peer.valid else 0)


@hg.compute_node(active=("key",), valid=("key",))
def remember_mesh_peer_reference(
    key: hg.TS[int], peer: hg.REF[hg.TS[int]]
) -> hg.REF[hg.TS[int]]:
    # The key ticks once, so later subscription retargeting must follow the
    # stored generic REF endpoint without reevaluating this wrapper.
    return peer.value


@hg.graph
def accumulated_dependency(
    key: hg.TS[int], value: hg.TS[int], link: hg.TS[int]
) -> hg.TS[int]:
    peer = remember_mesh_peer_reference(key, hg.mesh_(accumulated_dependency)[link])
    return add_optional_peer(running_total(value), peer)


@hg.graph
def key_dependency(key: hg.TS[int], link: hg.TS[int]) -> hg.TS[int]:
    return add_optional_peer(key, hg.mesh_(key_dependency)[link])


@hg.compute_node
def count_keys(keys: hg.TSS[int]) -> hg.TS[int]:
    return len(keys.value)


@hg.graph
def key_count(key: hg.TS[int]) -> hg.TS[int]:
    return count_keys(hg.mesh_(key_count).key_set)


@pytest.mark.parametrize("cuts", CUTS)
def test_mesh_state_and_membership_survive_every_restart_boundary(tmp_path, cuts):
    schema = hg.TSD[int, hg.TS[int]]
    events = [None, {1: 2, 2: 10}, {1: 3}, None, {1: hg.REMOVE},
              {2: 1}, {1: 7}, None]

    @hg.component
    def scenario(values: schema) -> schema:
        return hg.mesh_(running_total, values)

    compare_restarts(tmp_path, scenario, (schema,), schema, (events,), cuts)


@pytest.mark.parametrize("cuts", CUTS)
def test_mesh_generic_references_recursive_state_and_retargeting_survive_every_boundary(tmp_path, cuts):
    schema = hg.TSD[int, hg.TS[int]]
    values = [None, {1: 10, 2: 2, 3: 3}, {1: 5}, None, {2: 1}, None, {3: 4}, None]
    links = [None, {2: 1, 3: 2}, None, None, {3: 1}, {2: hg.REMOVE}, None, None]

    @hg.component
    def scenario(values: schema, links: schema) -> schema:
        return hg.mesh_(accumulated_dependency, values, links)

    actual = compare_restarts(tmp_path, scenario, (schema, schema), schema, (values, links), cuts)
    assert actual[2] == {1: 15, 2: 17, 3: 20}
    assert actual[3] is None


@pytest.mark.parametrize("cuts", CUTS)
def test_mesh_demand_keys_retirement_and_recreation_survive_every_boundary(tmp_path, cuts):
    schema = hg.TSD[int, hg.TS[int]]
    links = [None, {1: 0, 2: 1}, None, {2: 0}, {1: hg.REMOVE},
             {2: hg.REMOVE}, {3: 2}, None]

    @hg.component
    def scenario(links: schema) -> schema:
        return hg.mesh_(key_dependency, links)

    actual = compare_restarts(tmp_path, scenario, (schema,), schema, (links,), cuts)
    assert actual[1] == {0: 0, 1: 1, 2: 3}
    assert actual[6] == {2: 2, 3: 5}


@pytest.mark.parametrize("cuts", CUTS)
def test_mesh_key_set_subscription_survives_every_boundary(tmp_path, cuts):
    output = hg.TSD[int, hg.TS[int]]
    keys = [None, {1, 2}, None, {hg.Removed(1), 3}, {hg.Removed(2)},
            {hg.Removed(3)}, {1}, None]

    @hg.component
    def scenario(keys: hg.TSS[int]) -> output:
        return hg.mesh_(key_count, __keys__=keys)

    compare_restarts(tmp_path, scenario, (hg.TSS[int],), output, (keys,), cuts)


def test_mesh_failed_child_stop_does_not_publish_completed_day(tmp_path):
    import hgraph_persistence as persistence
    from .test_component_recovery_scenarios import Total, _run

    @hg.compute_node
    def accumulated(value: hg.TS[int], state: hg.RECORDABLE_STATE[Total] = None) -> hg.TS[int]:
        state.value.value = (state.value.value if state.value.valid else 0) + value.value
        return state.value.value

    _mesh_stopped.clear()

    @accumulated.stop
    def accumulated_stop(state: hg.RECORDABLE_STATE[Total] = None):
        _mesh_stopped.append(state.value.value)
        if state.value.value < 0:
            raise RuntimeError("mesh child stop failed")

    schema = hg.TSD[int, hg.TS[int]]

    @hg.component
    def scenario(values: schema) -> schema:
        return hg.mesh_(accumulated, values)

    store = persistence.ComponentCheckpointStore(tmp_path)
    _run(scenario, (schema,), schema, ([{1: 2, 2: 3}],), 0, store)
    _mesh_stopped.clear()
    with pytest.raises(RuntimeError, match="mesh child stop failed"):
        _run(scenario, (schema,), schema, ([{1: -10, 2: 4}],), 1, store, "cut-1")
    assert sorted(_mesh_stopped) == [-8, 7]
    assert store.contains("cut-1")
    assert not store.contains("cut-2")
