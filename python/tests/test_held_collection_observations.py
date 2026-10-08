"""Held collection values preserve validity independently of retained payloads."""

import hgraph as hg
from hgraph.test import eval_node


def test_fixed_list_value_keeps_invalid_positions_and_owned_snapshot():
    observed = []

    @hg.compute_node
    def source(action: hg.TS[int], _output: hg.TSL = None) -> hg.TSL[hg.TS[int], hg.Size[2]]:
        if action.value == 1:
            _output[0].value = 7
            _output[1].value = 100
        elif action.value == 2:
            _output[0].invalidate()
        elif action.value == 3:
            _output[0].value = 9

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: hg.TS[int], value: hg.TSL[hg.TS[int], hg.Size[2]]) -> hg.TS[int]:
        observed.append((tuple(value.value), len(value), value[0].valid, value.all_valid))
        return step.value

    @hg.graph
    def run(action: hg.TS[int], step: hg.TS[int]) -> hg.TS[int]:
        return observe(step, source(action))

    eval_node(run, [1, None, 2, None, None, 3, None], [None, 1, None, 1, 1, None, 1])
    assert observed == [
        ((7, 100), 2, True, True),
        ((None, 100), 2, False, False),
        ((None, 100), 2, False, False),
        ((9, 100), 2, True, True),
    ]


def test_map_value_keeps_invalid_member_and_owned_snapshot():
    observed = []

    @hg.compute_node
    def source(action: hg.TS[int], _output: hg.TSD = None) -> hg.TSD[int, hg.TS[int]]:
        if action.value == 1:
            _output.get_or_create(1).value = 7
            _output.get_or_create(2).value = 100
        elif action.value == 2:
            _output[1].invalidate()
        elif action.value == 3:
            _output[1].value = 9

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: hg.TS[int], value: hg.TSD[int, hg.TS[int]]) -> hg.TS[int]:
        observed.append((dict(value.value), 1 in value, value[1].valid, value.all_valid))
        return step.value

    @hg.graph
    def run(action: hg.TS[int], step: hg.TS[int]) -> hg.TS[int]:
        return observe(step, source(action))

    eval_node(run, [1, None, 2, None, None, 3, None], [None, 1, None, 1, 1, None, 1])
    assert observed == [
        ({1: 7, 2: 100}, True, True, True),
        ({1: None, 2: 100}, True, False, False),
        ({1: None, 2: 100}, True, False, False),
        ({1: 9, 2: 100}, True, True, True),
    ]
