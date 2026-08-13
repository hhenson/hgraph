# Ported from release/0.5:hgraph_unit_tests/_wiring/test_tsl_wiring.py
import pytest

from datetime import timedelta
from hgraph import (
    REF,
    TS,
    combine,
    dereference,
    generator,
    graph,
    TSL,
    Size,
    SCALAR,
    compute_node,
    SIZE,
    getitem_,
    const,
    if_,
    TimeSeriesReference,
)
from hgraph.nodes import flatten_tsl_values
from hgraph.test import eval_node



import pytest
pytestmark = pytest.mark.smoke


@compute_node
def my_tsl_maker(ts1: TS[int], ts2: TS[int]) -> TSL[TS[int], Size[2]]:
    out = {}
    if ts1.modified:
        out[0] = ts1.delta_value
    if ts2.modified:
        out[1] = ts2.delta_value
    return out


def test_fixed_tsl_non_peered_input():
    @graph
    def my_tsl(ts1: TS[int], ts2: TS[int]) -> TS[tuple[int, ...]]:
        tsl = TSL.from_ts(ts1, ts2)
        return flatten_tsl_values[SCALAR:int](tsl)

    assert eval_node(my_tsl, ts1=[1, 2], ts2=[3, 4]) == [(1, 3), (2, 4)]


def test_fixed_tsl_non_peered_input_generator():
    @graph
    def my_tsl(ts1: TS[int], ts2: TS[int]) -> TS[tuple[int, ...]]:
        tsl = TSL.from_ts((g for g in (ts1, ts2)))
        return flatten_tsl_values[SCALAR:int](tsl)

    assert eval_node(my_tsl, ts1=[1, 2], ts2=[3, 4]) == [(1, 3), (2, 4)]


def test_fixed_tsl_peered():
    @graph
    def my_tsl(ts1: TS[int], ts2: TS[int]) -> TS[int]:
        tsl = my_tsl_maker(ts1, ts2)
        return tsl[0]

    assert eval_node(my_tsl, ts1=[1, 2], ts2=[3, 4]) == [1, 2]


def test_peered_to_peered_tsl():
    @graph
    def my_tsl(ts1: TS[int], ts2: TS[int]) -> TS[tuple[int, ...]]:
        tsl = my_tsl_maker(ts1, ts2)
        return flatten_tsl_values[SCALAR:int](tsl)

    assert eval_node(my_tsl, ts1=[1, 2], ts2=[3, 4]) == [(1, 3), (2, 4)]


def test_len_tsl_wiring():
    @graph
    def l_test(tsl: TSL[TS[int], SIZE]) -> TS[int]:
        return const(len(tsl))

    assert eval_node(l_test, tsl=[None], resolution_dict={"tsl": TSL[TS[int], Size[5]]}) == [5]


def test_tsl_compatible_types():
    @graph
    def tsl_test(ts1: TS[object], ts2: TS[int]) -> TSL[TS[object], Size[2]]:
        tsl = TSL.from_ts(ts1, ts2, tp=TS[object])
        return tsl

    assert eval_node(tsl_test, ts1=[1, 2], ts2=[3, 4]) == [{0: 1, 1: 3}, {0: 2, 1: 4}]


def test_tsl_from_ts_unifies_reference_and_direct_elements():
    @graph
    def app(condition: TS[bool], value: TS[int]) -> TSL[TS[int], Size[2]]:
        selected, _ = if_(condition, value)
        return TSL.from_ts(selected, value)

    assert eval_node(app, [True], [7]) == [{0: 7, 1: 7}]


def test_tsl_get_item():
    assert eval_node(getitem_, [(1, 2), (2, 3), (4, 5)], 0, resolution_dict={"ts": TSL[TS[int], Size[2]]}) == [1, 2, 4]


def test_tsl_ref_flipping():
    @generator
    def null_ref() -> REF[TSL[TS[int], Size[2]]]:
        yield timedelta(), TimeSeriesReference.make()
    
    @graph
    def g(tsb1: TSL[TS[int], Size[2]], tsb2: TSL[TS[int], Size[2]], i: TS[int]) -> TSL[TS[int], Size[2]]:
        return combine[TSL](tsb1, tsb2, null_ref())[i]
    
    assert eval_node(g, [(1, 1)], [(2, 2)], [0, 2, 1, 2]) == [
        {0: 1, 1: 1},
        None,
        {0: 2, 1: 2},
        None,
    ]


def test_dereference_materializes_tsl_element_references():
    tsl_type = TSL[TS[int], Size[2]]

    @compute_node
    def as_reference(tsl: REF[tsl_type]) -> REF[tsl_type]:
        return tsl.value

    @graph
    def g(tsl: tsl_type) -> tsl_type:
        elements = dereference(as_reference(tsl))
        assert elements.output_type == TSL[REF[TS[int]], Size[2]]
        return TSL.from_ts(elements[0], elements[1])

    assert eval_node(g, [{0: 1}, {1: 2}, {0: 3}]) == [
        {0: 1},
        {1: 2},
        {0: 3},
    ]


def test_dereference_direct_tsl_normalizes_nested_reference_elements():
    @graph
    def g(condition: TS[bool], value: TS[int]) -> TS[int]:
        routed = if_(condition, value)
        direct = TSL.from_ts(routed.true, routed.false)
        elements = dereference(direct)
        assert elements.output_type == TSL[REF[TS[int]], Size[2]]
        return elements[0]

    assert eval_node(g, [True, True, False, True], [1, 2, 3, 4]) == [
        1,
        2,
        None,
        4,
    ]


def test_dereference_rejects_incompatible_non_peered_tsl_child_references():
    expected_type = TSL[TS[int], Size[2]]
    wrong_type = TSL[TS[str], Size[2]]

    @compute_node
    def misdeclare_reference(tsl: REF[wrong_type]) -> REF[expected_type]:
        return tsl.value

    @graph
    def g(first: TS[str], second: TS[str]) -> TS[int]:
        tsl = TSL.from_ts(first, second)
        return dereference(misdeclare_reference(tsl))[0]

    with pytest.raises(RuntimeError, match="reference targets schema"):
        eval_node(g, ["wrong"], ["value"])
