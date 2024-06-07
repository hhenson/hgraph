from hgraph import TIME_SERIES_TYPE, TS, graph, combine, TSL, Size
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

