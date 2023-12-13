from hg import compute_node
from hg.nodes._pass_through import pass_through
from hg.test._node_unit_tester import eval_node


def test_eval_node():
    assert eval_node(pass_through, ts=[1, 2, 3]) == [1, 2, 3]

