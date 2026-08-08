from hgraph import not_
from hgraph.test import eval_node


def test_not():
    assert eval_node(not_, [True, False]) == [False, True]
