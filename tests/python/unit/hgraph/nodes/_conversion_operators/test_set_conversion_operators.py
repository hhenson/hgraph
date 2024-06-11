from typing import Set

from hgraph import graph, TS, convert, collect
from hgraph.test import eval_node


def test_convert_ts_to_set():
    @graph
    def g(a: TS[int]) -> TS[Set[int]]:
        return convert[TS[Set]](a)

    assert eval_node(g, 1) == [{1}]

    @graph
    def g(a: TS[int]) -> TS[Set[int]]:
        return convert[TS[Set[int]]](a)

    assert eval_node(g, 1) == [{1}]


def test_collect_set():
    @graph
    def g(a: TS[int], b: TS[bool]) -> TS[Set[int]]:
        return collect[TS[Set]](a, reset=b)

    assert eval_node(g, [1, 2, 3, 4], [None, None, True]) == [{1}, {1, 2}, {3}, {3, 4}]

    @graph
    def g(a: TS[int], b: TS[bool]) -> TS[Set[int]]:
        return collect[TS[Set[int]]](a, reset=b)

    assert eval_node(g, [1, 2, 3, 4], [None, None, True]) == [{1}, {1, 2}, {3}, {3, 4}]
