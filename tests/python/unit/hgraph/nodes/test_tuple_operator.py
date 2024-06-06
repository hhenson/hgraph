from typing import Tuple

from hgraph import SCALAR, TS, sink_node, graph
from hgraph.nodes._tuple_operators import unroll
from hgraph.test import eval_node


def test_unroll():
    assert eval_node(unroll[SCALAR: int], [(1, 2, 3), (4,), None, None, (5, 6)]) == [1, 2, 3, 4, 5, 6]


def test_tuple_compatibility():
    @sink_node
    def n(x: TS[Tuple[int, ...]]):
        ...

    @graph
    def g(i: TS[Tuple[int, int]]):
        return n(i)

    assert eval_node(g, [(1, 2), (3, 4)]) == None