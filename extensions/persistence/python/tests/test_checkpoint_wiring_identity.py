"""Checkpoint compatibility includes component results and child ownership."""

import hgraph as hg
import hgraph_persistence as persistence
import pytest

from .test_component_recovery_scenarios import _run


class ResultPair(hg.TimeSeriesSchema):
    left: hg.TS[int]
    right: hg.TS[int]


@hg.compute_node
def produce_pair(value: hg.TS[int]) -> hg.TSB[ResultPair]:
    return {"left": value.value * 10, "right": value.value * 100}


@pytest.mark.parametrize("explicit_node_id", [False, True])
@pytest.mark.parametrize("structural", [False, True])
def test_changed_component_result_binding_is_incompatible(tmp_path, explicit_node_id, structural):
    @hg.component
    def strategy(value: hg.TS[int], right: bool) -> hg.TSB[ResultPair] if structural else hg.TS[int]:
        pair = (produce_pair(value, __recordable_id__="pair") if explicit_node_id
                else produce_pair(value))
        if structural:
            return hg.combine[hg.TSB[ResultPair]](
                left=pair.right if right else pair.left,
                right=pair.left if right else pair.right)
        return pair.right if right else pair.left

    output_schema = hg.TSB[ResultPair] if structural else hg.TS[int]

    @hg.graph
    def application(value: hg.TS[int], tick: hg.SIGNAL, right: bool) -> output_schema:
        return hg.sample(tick, strategy(value, right))

    store = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, strategy.recordable_id, "first", global_state=state)
        first = hg.eval_node(application, [1], [True], right=False,
                             __end_time__=hg.MIN_ST + hg.MIN_TD)
    expected = {"left": 10, "right": 100} if structural else 10
    assert first == [expected]
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, strategy.recordable_id, "same", "first", global_state=state)
        assert hg.eval_node(application, [None, None], [None, True], right=False,
                            __start_time__=hg.MIN_ST + hg.MIN_TD,
                            __end_time__=hg.MIN_ST + hg.MIN_TD * 3) == [None, expected]
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, strategy.recordable_id, "changed", "first", global_state=state)
        with pytest.raises(RuntimeError, match="incompatible"):
            hg.eval_node(application, [None, None], [None, True], right=True,
                         __start_time__=hg.MIN_ST + hg.MIN_TD,
                         __end_time__=hg.MIN_ST + hg.MIN_TD * 3)
    assert not store.contains("changed")


@pytest.mark.parametrize("unconfigured_first", [False, True])
def test_signal_map_probe_keeps_checkpoint_ownership(tmp_path, unconfigured_first):
    @hg.compute_node
    def copy_signal(value: hg.SIGNAL) -> hg.SIGNAL:
        return value.delta_value

    schema = hg.TSD[str, hg.SIGNAL]

    @hg.component
    def scenario(values: schema) -> schema:
        return hg.map_(copy_signal, values)

    if unconfigured_first:
        assert _run(scenario, (schema,), schema, ([{"a": True}],), 0) == [{"a": True}]
    store = persistence.ComponentCheckpointStore(tmp_path)
    assert _run(scenario, (schema,), schema, ([{"a": True}],), 0, store) == [{"a": True}]
    assert _run(scenario, (schema,), schema, ([None, {"a": hg.REMOVE}, {"a": True}],),
                1, store, "cut-1") == [None, {"a": hg.REMOVE}, {"a": True}]
