from hgraph import compute_node
from hgraph import pass_through_node
from hgraph.test._node_unit_tester import eval_node


def test_eval_node():
    assert eval_node(pass_through_node, ts=[1, 2, 3]) == [1, 2, 3]
