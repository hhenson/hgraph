from typing import Tuple, Set

from hgraph import TIME_SERIES_TYPE, combine, TS, graph, HgTypeMetaData, collect, convert, TSS, Removed, TSL, Size, emit
from hgraph.test import eval_node


def test_convert_ts_to_tuple():
    @graph
    def g(a: TS[int]) -> TIME_SERIES_TYPE:
        return convert[TS[Tuple]](a)

    assert eval_node(g, [None, 1, 2]) == [None, (1,), (2,)]

    @graph
    def h(a: TS[int]) -> TIME_SERIES_TYPE:
        return convert[TS[Tuple[int, ...]]](a)

    assert eval_node(h, [None, 1, 2]) == [None, (1,), (2,)]


def test_convert_set_to_tuple():
    @graph
    def g(a: TS[Set[int]]) -> TIME_SERIES_TYPE:
        return convert[TS[Tuple]](a)

    assert eval_node(g, [set(), {1}, {2, 3}]) == [tuple(), (1,), (2, 3)]

    @graph
    def h(a: TS[Set[int]]) -> TIME_SERIES_TYPE:
        return convert[TS[Tuple[int, ...]]](a)

    assert eval_node(h, [set(), {1}, {2, 3}]) == [tuple(), (1,), (2, 3)]


def test_convert_tss_to_tuple():
    @graph
    def g(a: TSS[int]) -> TIME_SERIES_TYPE:
        return convert[TS[Tuple]](a)

    assert eval_node(g, [set(), {1}, {2, 3, Removed(1)}]) == [tuple(), (1,), (2, 3)]

    @graph
    def g(a: TSS[int]) -> TIME_SERIES_TYPE:
        return convert[TS[Tuple[int, ...]]](a)

    assert eval_node(g, [set(), {1}, {2, 3, Removed(1)}]) == [tuple(), (1,), (2, 3)]


def test_convert_tsl_to_tuple():
    @graph
    def g(a: TSL[TS[int], Size[2]]) -> TIME_SERIES_TYPE:
        return convert[TS[Tuple]](a)

    assert eval_node(g, [None, {0: 1}, {0: 2, 1: 3}]) == [None, None, (2, 3)]

    @graph
    def g(a: TSL[TS[int], Size[2]]) -> TIME_SERIES_TYPE:
        return convert[TS[Tuple[int, ...]]](a, __strict__=False)

    assert eval_node(g, [None, {0: 1}, {0: 2, 1: 3}]) == [None, (1, None), (2, 3)]


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


def test_combine_tuple_nonuniform():
    @graph
    def g(a: TS[int], b: TS[str]) -> TIME_SERIES_TYPE:
        return combine[TS[Tuple[int, str]]](a, b, __strict__=False)

    assert eval_node(g, [None, 1], '2') == [(None, '2'), (1, '2')]


def test_collect_tuple():
    @graph
    def g(a: TS[int], b: TS[bool]) -> TIME_SERIES_TYPE:
        return collect[TS[Tuple]](a, reset=b)

    assert eval_node(g, [None, 1, 2, 3], [None, None, None, True]) == [None, (1,), (1, 2), (3,)]

    @graph
    def g(a: TS[int], b: TS[bool]) -> TIME_SERIES_TYPE:
        return collect[TS[Tuple[int, ...]]](a, reset=b)

    assert eval_node(g, [None, 1, 2, 3], [None, None, None, True]) == [None, (1,), (1, 2), (3,)]


def test_emit_tuple():
    @graph
    def g(m: TS[Tuple[int, ...]]) -> TS[int]:
        return emit(m)

    assert eval_node(g, [(1, 2, 3), None, (4,)]) == [1, 2, 3, 4]

