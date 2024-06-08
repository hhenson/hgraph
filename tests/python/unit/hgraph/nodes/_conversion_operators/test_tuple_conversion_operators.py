from typing import Tuple

from hgraph import TIME_SERIES_TYPE, combine, TS, graph, HgTypeMetaData, collect
from hgraph.test import eval_node


def test_combine_tuple():
    @graph
    def g(a: TS[int], b: TS[int]) -> TIME_SERIES_TYPE:
        return combine[TS[Tuple]](a, b)

    assert eval_node(g, [None, 1], 2) == [None, (1, 2)]

    @graph
    def h(a: TS[int], b: TS[int]) -> TIME_SERIES_TYPE:
        return combine[TS[Tuple[int, ...]]](a, b)

    assert eval_node(h, [None, 1], 2) == [None, (1, 2)]

    @graph
    def f(a: TS[int], b: TS[int]) -> TIME_SERIES_TYPE:
        return combine[TS[Tuple[int, int]]](a, b)

    assert eval_node(f, [None, 1], 2) == [None, (1, 2)]


def test_combine_tuple_relaxed():
    @graph
    def g(a: TS[int], b: TS[int]) -> TIME_SERIES_TYPE:
        return combine[TS[Tuple]](a, b, __strict__=False)

    assert eval_node(g, [None, 1], 2) == [(None, 2), (1, 2)]


def test_collect_tuple():
    @graph
    def g(a: TS[int], b: TS[bool]) -> TIME_SERIES_TYPE:
        return collect[TS[Tuple]](a, reset=b)

    assert eval_node(g, [None, 1, 2, 3], [None, None, None, True]) == [None, (1,), (1, 2), (3,)]