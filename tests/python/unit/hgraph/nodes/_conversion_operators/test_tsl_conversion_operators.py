from typing import Tuple

from hgraph import TIME_SERIES_TYPE, TS, graph, combine, TSL, Size, convert, emit
from hgraph.test import eval_node


def test_combine_tsl_implicit():
    @graph
    def g(a: TS[int], b: TS[int]) -> TSL[TS[int], Size[2]]:
        return combine(a, b)

    assert eval_node(g, 1, 2) == [{0: 1, 1: 2}]


def test_combine_tsl_explicit():
    @graph
    def g(a: TS[int], b: TS[int]) -> TSL[TS[int], Size[2]]:
        return combine[TSL](a, b)

    assert eval_node(g, 1, 2) == [{0: 1, 1: 2}]


def test_combine_tsl_very_explicit():
    @graph
    def g(a: TS[int], b: TS[int]) -> TSL[TS[int], Size[2]]:
        return combine[TSL[TS[int], Size[2]]](a, b)

    assert eval_node(g, 1, 2) == [{0: 1, 1: 2}]


def test_convert_tuple_to_tsl():
    @graph
    def g(a: TS[Tuple[int, ...]]) -> TSL[TS[int], Size[2]]:
        return convert[TSL[TS[int], Size[2]]](a)

    assert eval_node(g, [(1, 2)]) == [{0: 1, 1: 2}]


def test_emit_tsl():
    @graph
    def g(m: TSL[TS[int], Size[2]]) -> TS[int]:
        return emit(m)

    assert eval_node(g, [(1, None), None, (4, None), (5, 6)]) == [1, None, 4, 5, 6]

