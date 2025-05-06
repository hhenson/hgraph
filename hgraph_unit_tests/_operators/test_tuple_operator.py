from typing import Tuple

from hgraph import TS, sink_node, graph
from hgraph.test import eval_node


def test_tuple_compatibility():
    @sink_node
    def n(x: TS[Tuple[int, ...]]): ...

    @graph
    def g(i: TS[Tuple[int, int]]):
        return n(i)

    assert eval_node(g, [(1, 2), (3, 4)]) == None
