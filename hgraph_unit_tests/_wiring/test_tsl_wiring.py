from datetime import timedelta
from hgraph import REF, TS, combine, generator, graph, TSL, Size, SCALAR, compute_node, SIZE, getitem_, const, TimeSeriesReference
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
