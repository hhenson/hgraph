from __future__ import annotations

import pytest

from hgraph import (
    MIN_DT,
    MIN_ST,
    MIN_TD,
    NODE,
    REF,
    TS,
    TSB,
    TSB_OUT,
    TS_OUT,
    TimeSeriesSchema,
    compute_node,
)
from hgraph.test import eval_node


def test_input_and_output_views_expose_generic_time_series_interrogation():
    observations = []
    retained = []

    @compute_node
    def inspect(value: TS[int], node: NODE = None, _output: TS_OUT[int] = None) -> TS[int]:
        observations.append(
            (
                value.owning_node.node_id,
                value.owning_graph.graph_id,
                value.is_reference(),
                _output.owning_node.node_id,
                _output.owning_graph.graph_id,
                _output.is_reference(),
                _output.valid,
                _output.all_valid,
                _output.last_modified_time,
                node.node_id,
            )
        )
        retained.extend(
            (
                value.owning_node,
                value.owning_graph,
                _output.owning_node,
                _output.owning_graph,
            )
        )

        # Binding and ownership mutation are native engine responsibilities,
        # not Python runtime-view APIs.
        assert not hasattr(value, "bind_output")
        assert not hasattr(value, "un_bind_output")
        assert not hasattr(value, "do_bind_output")
        assert not hasattr(value, "do_un_bind_output")
        assert not hasattr(value, "re_parent")
        assert not hasattr(_output, "re_parent")
        return value.value

    assert eval_node(inspect, value=[1, 2]) == [1, 2]
    assert observations == [
        ((1,), (), False, (1,), (), False, False, False, MIN_DT, (1,)),
        ((1,), (), False, (1,), (), False, True, True, MIN_ST, (1,)),
    ]

    for owner in retained[::4] + retained[2::4]:
        with pytest.raises(RuntimeError, match="outside its node's evaluation"):
            _ = owner.node_id
    for owner in retained[1::4] + retained[3::4]:
        with pytest.raises(RuntimeError, match="outside its lifecycle callback"):
            _ = owner.graph_id


def test_input_and_output_owners_keep_the_callback_scheduler():
    evaluations = []

    @compute_node
    def reschedule(value: TS[int], _output: TS_OUT[int] = None) -> TS[int]:
        evaluations.append(value.value)
        if len(evaluations) == 1:
            value.owning_node.notify_next_cycle()
            _output.owning_node.notify()
        return value.value

    eval_node(
        reschedule,
        value=[1],
        __end_time__=MIN_ST + 2 * MIN_TD,
    )
    assert evaluations == [1, 1]


def test_fast_compute_input_owner_keeps_the_callback_scheduler():
    evaluations = []

    @compute_node
    def reschedule(value: TS[int]) -> TS[int]:
        evaluations.append(value.value)
        if len(evaluations) == 1:
            value.owning_node.notify_next_cycle()
        return value.value

    eval_node(
        reschedule,
        value=[1],
        __end_time__=MIN_ST + 2 * MIN_TD,
    )
    assert evaluations == [1, 1]


def test_reference_input_and_output_views_identify_their_runtime_schema():
    observations = []

    @compute_node
    def inspect(value: REF[TS[int]], _output: REF[TS[int]] = None) -> REF[TS[int]]:
        observations.append((value.is_reference(), _output.is_reference()))
        return value.value

    assert eval_node(inspect, value=[1]) == [1]
    assert observations == [(True, True)]


class RuntimeApiPair(TimeSeriesSchema):
    left: TS[int]
    right: TS[str]


def test_structural_child_views_keep_their_node_and_graph_owners():
    observations = []

    @compute_node
    def inspect(
        value: TSB[RuntimeApiPair],
        node: NODE = None,
        _output: TSB_OUT[RuntimeApiPair] = None,
    ) -> TSB[RuntimeApiPair]:
        observations.append(
            (
                value.left.owning_node.node_id,
                value.left.owning_graph.graph_id,
                _output.left.owning_node.node_id,
                _output.left.owning_graph.graph_id,
                node.node_id,
            )
        )
        assert not hasattr(_output, "not_a_field")
        return value.delta_value

    assert eval_node(inspect, value=[{"left": 1, "right": "a"}]) == [
        {"left": 1, "right": "a"}
    ]
    assert observations == [((1,), (), (1,), (), (1,))]
