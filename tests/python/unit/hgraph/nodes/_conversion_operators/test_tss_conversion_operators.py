from typing import Tuple, Set

from hgraph import graph, TS, TSS, convert, Removed, collect, emit, combine
from hgraph.test import eval_node


def test_convert_ts_to_tss():
    @graph
    def g(a: TS[int]) -> TSS[int]:
        return convert[TSS](a)

    assert eval_node(g, [1, 2, 3]) == [{1}, {2, Removed(1)}, {3, Removed(2)}]

    @graph
    def g(a: TS[int]) -> TSS[int]:
        return convert[TSS[int]](a)

    assert eval_node(g, [1, 2, 3]) == [{1}, {2, Removed(1)}, {3, Removed(2)}]


def test_convert_tuple_to_tss():
    @graph
    def g(a: TS[Tuple[int, ...]]) -> TSS[int]:
        return convert[TSS](a)

    assert (eval_node(g, [None, (1,), (1, 2), (3,)]) ==
            [None, {1}, {2}, {3, Removed(2), Removed(1)}])


def test_convert_set_to_tss():
    @graph
    def g(a: TS[Set[int]]) -> TSS[int]:
        return convert[TSS](a)

    assert (eval_node(g, [None, {1}, {1, 2}, {3}]) ==
            [None, {1}, {2}, {3, Removed(2), Removed(1)}])


def test_combine_tsl():
    @graph
    def g(a: TS[int], b: TS[int]) -> TSS[int]:
        return combine[TSS](a, b)

    assert (eval_node(g, [None, 1, None], [None, None, 2, 1]) ==
            [None, {1}, {2}, {Removed(2)}])


def test_collect_tss_from_ts():
    @graph
    def g(a: TS[int], b: TS[bool]) -> TSS[int]:
        return collect[TSS](a, reset=b)

    assert (eval_node(g, [1, 2, 3], [None, None, True]) ==
            [{1}, {2}, {3, Removed(2), Removed(1)}])


def test_collect_tss_from_tuples():
    @graph
    def g(a: TS[Tuple[int, ...]], b: TS[bool]) -> TSS[int]:
        return collect[TSS](a, reset=b)

    assert (eval_node(g, [(1,), (2,), (3, 4)], [None, None, True], __trace_wiring__=True) ==
            [{1}, {2}, {3, 4, Removed(2), Removed(1)}])


def test_collect_tss_from_sets():
    @graph
    def g(a: TS[Set[int]], b: TS[bool]) -> TSS[int]:
        return collect[TSS](a, reset=b)

    assert (eval_node(g, [{1, }, {2, }, {3, 4}], [None, None, True], __trace_wiring__=True) ==
            [{1}, {2}, {3, 4, Removed(2), Removed(1)}])


def test_emit_tss():
    @graph
    def g(m: TSS[int]) -> TS[int]:
        return emit(m)

    assert eval_node(g, [{1, 2, 3}, None, {4}]) == [1, 2, 3, 4]
