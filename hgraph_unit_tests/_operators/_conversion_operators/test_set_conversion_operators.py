from typing import Set, Tuple

from hgraph import graph, TS, convert, collect, TSS, Removed, emit
from hgraph.test import eval_node


def test_convert_ts_to_set():
    @graph
    def g(a: TS[int]) -> TS[Set[int]]:
        return convert[TS[Set]](a)

    assert eval_node(g, 1) == [{1}]

    @graph
    def h(a: TS[int]) -> TS[Set[int]]:
        return convert[TS[Set[int]]](a)

    assert eval_node(h, 1) == [{1}]


def test_convert_tuple_to_set():
    @graph
    def g(a: TS[Tuple[int, ...]]) -> TS[Set[int]]:
        return convert[TS[Set]](a)

    assert eval_node(g, [None, (1,), (1, 2, 3, 2, 1)]) == [None, {1}, {1, 2, 3}]

    @graph
    def h(a: TS[Tuple[int, ...]]) -> TS[Set[int]]:
        return convert[TS[Set[int]]](a)

    assert eval_node(h, [None, (1,), (1, 2, 3, 2, 1)]) == [None, {1}, {1, 2, 3}]


def test_convert_tss_to_set():
    @graph
    def g(a: TSS[int]) -> TS[Set[int]]:
        return convert[TS[Set]](a)

    assert eval_node(g, [None, {1}, {1, 2, 3}, {Removed(2)}]) == [None, {1}, {1, 2, 3}, {1, 3}]


def test_collect_set():
    @graph
    def g(a: TS[int], b: TS[bool]) -> TS[Set[int]]:
        return collect[TS[Set]](a, reset=b)

    assert eval_node(g, [1, 2, 3, 4], [None, None, True]) == [{1}, {1, 2}, {3}, {3, 4}]

    @graph
    def g(a: TS[int], b: TS[bool]) -> TS[Set[int]]:
        return collect[TS[Set[int]]](a, reset=b)

    assert eval_node(g, [1, 2, 3, 4], [None, None, True]) == [{1}, {1, 2}, {3}, {3, 4}]


def test_collect_set_from_tuples():
    @graph
    def g(a: TS[Tuple[int, ...]], b: TS[bool]) -> TS[Set[int]]:
        return collect[TS[Set]](a, reset=b)

    assert eval_node(g, [(1,), (2, 3), (3,), (4, 5)], [None, None, True]) == [{1}, {1, 2, 3}, {3}, {3, 4, 5}]


def test_emit_set():
    @graph
    def g(m: TS[Set[int]]) -> TS[int]:
        return emit(m)

    assert eval_node(g, [{1, 2, 3}, None, {4}]) == [1, 2, 3, 4]
