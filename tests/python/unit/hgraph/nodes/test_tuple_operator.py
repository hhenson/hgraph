from hgraph import SCALAR
from hgraph.nodes._tuple_operators import unroll
from hgraph.test import eval_node


def test_unroll():
    assert eval_node(unroll[SCALAR: int], [(1, 2, 3), (4,), None, None, (5, 6)]) == [1, 2, 3, 4, 5, 6]
