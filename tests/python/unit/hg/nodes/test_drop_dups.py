from hgraph.nodes._drop_dups import drop_dups
from hgraph.test import eval_node


def test_drop_dups():

    assert eval_node(drop_dups, [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]) == [1, 2, None, 3, None, None, 4, None, None, None]
