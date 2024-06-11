from typing import Set, Mapping, Tuple

from hgraph import graph, TS, convert, collect, emit, TSB, Size, TSL
from hgraph.nodes import KeyValue
from hgraph.test import eval_node


def test_convert_ts_to_mapping():
    @graph
    def g(k: TS[str], v: TS[int]) -> TS[Mapping[str, int]]:
        return convert[TS[Mapping]](k, v)

    assert eval_node(g, 'a', 1) == [{'a': 1}]

    @graph
    def g(k: TS[str], v: TS[int]) -> TS[Mapping[str, int]]:
        return convert[TS[Mapping[str, int]]](k, v)

    assert eval_node(g, 'a', 1) == [{'a': 1}]


def test_collect_mapping():
    @graph
    def g(k: TS[str], v: TS[int], b: TS[bool]) -> TS[Mapping[str, int]]:
        return collect[TS[Mapping]](k, v, reset=b)

    assert (eval_node(g, ['a', 'b', 'c'], [1, 2, 3, 4], [None, None, True]) ==
            [{'a': 1}, {'a': 1, 'b': 2}, {'c': 3}, {'c': 4}])

    @graph
    def g(k: TS[str], v: TS[int], b: TS[bool]) -> TS[Mapping[str, int]]:
        return collect[TS[Mapping[str, int]]](k, v, reset=b)

    assert (eval_node(g, ['a', 'b', 'c'], [1, 2, 3, 4], [None, None, True]) ==
            [{'a': 1}, {'a': 1, 'b': 2}, {'c': 3}, {'c': 4}])


def test_emit_mapping():
    @graph
    def g(m: TS[Mapping[str, int]]) -> TSB[KeyValue[str, TS[int]]]:
        return emit(m)

    assert eval_node(g, [{'a': 1, 'b': 2, 'c': 3}, None, {'a': 4}]) == [
        {'key': 'a', 'value': 1}, {'key': 'b', 'value': 2}, {'key': 'c', 'value': 3}, {'key': 'a', 'value': 4}]

    @graph
    def g(m: TS[Mapping[str, Tuple[int, int]]]) -> TSB[KeyValue[str, TSL[TS[int], Size[2]]]]:
        return emit[TSL[TS[int], Size[2]]](m)

    assert eval_node(g, [{'a': (1, 1), 'b': (2, 2), 'c': (3, 3)}, None, {'a': (4, 4)}]) == [
        {'key': 'a', 'value': {0: 1, 1: 1}}, {'key': 'b', 'value': {0: 2, 1: 2}},
        {'key': 'c', 'value': {0: 3, 1: 3}}, {'key': 'a', 'value': {0: 4, 1: 4}}]
