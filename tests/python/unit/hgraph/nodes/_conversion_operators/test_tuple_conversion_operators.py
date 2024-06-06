from typing import Tuple

from hgraph import TIME_SERIES_TYPE, combine, TS, graph, HgTypeMetaData
from hgraph.nodes import combine_tuple
from hgraph.test import eval_node


def test_combine_tuple():
    @graph
    def g(a: TS[int], b: TS[int]) -> TIME_SERIES_TYPE:
        return combine[TS[Tuple]](a, b)

    assert eval_node(g, [None, 1], 2) == [None, (1, 2)]


def test_combine_tuple_relaxed():
    @graph
    def g(a: TS[int], b: TS[int]) -> TIME_SERIES_TYPE:
        return combine[TS[Tuple]](a, b, strict=False)

    assert eval_node(g, [None, 1], 2) == [(None, 2), (1, 2)]
