from typing import Mapping

from hgraph import TS, combine, TSD, graph, convert, REMOVE, Size, TSL, collect
from hgraph.test import eval_node


def test_convert_ts_to_tsd():
    @graph
    def g(a: TS[str], b: TS[int]) -> TSD[str, TS[int]]:
        return convert[TSD](a, b)

    assert eval_node(g, ['a', 'b'], [1, 2]) == [{'a': 1}, {'b': 2, 'a': REMOVE}]

    @graph
    def g(a: TS[str], b: TS[int]) -> TSD[str, TS[int]]:
        return convert[TSD[str, TS[int]]](a, b)

    assert eval_node(g, ['a', 'b'], [1, 2]) == [{'a': 1}, {'b': 2, 'a': REMOVE}]


def test_convert_tsl_to_tsd():
    @graph
    def g(a: TSL[TS[int], Size[2]]) -> TSD[int, TS[int]]:
        return convert[TSD](a)

    assert eval_node(g, [(None, 1), (2, None)]) == [{1: 1}, {0: 2}]


def test_convert_mapping_to_tsd():
    @graph
    def g(a: TS[Mapping[int, int]]) -> TSD[int, TS[int]]:
        return convert[TSD](a)

    assert eval_node(g, [{1: 2}, {3: 4}]) == [{1: 2}, {3: 4, 1: REMOVE}]


def test_combine_tsd_from_tuple_and_tsl():
    @graph
    def g(a: TS[int], b: TS[int]) -> TSD[str, TS[int]]:
        return combine[TSD](('a', 'b'), a, b)

    assert eval_node(g, [1, 2], [3, None]) == [{'a': 1, 'b': 3}, {'a': 2}]

    @graph
    def g(a: TS[int], b: TS[int]) -> TSD[str, TS[int]]:
        return combine[TSD[str, TS[int]]](('a', 'b'), a, b)

    assert eval_node(g, [1, 2], [3, None]) == [{'a': 1, 'b': 3}, {'a': 2}]


def test_collect_tsd():
    @graph
    def g(k: TS[str], v: TS[int], b: TS[bool]) -> TSD[str, TS[int]]:
        return collect[TSD](k, v, reset=b)

    assert (eval_node(g, ['a', 'b', 'c'], [1, 2, 3, 4], [None, None, True]) ==
            [{'a': 1}, {'b': 2}, {'c': 3, 'a': REMOVE, 'b': REMOVE}, {'c': 4}])
