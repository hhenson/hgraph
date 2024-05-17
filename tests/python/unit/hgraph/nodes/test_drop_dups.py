from hgraph.nodes._drop_dups import drop_dups
from hgraph.test import eval_node


def test_drop_dups():
    assert eval_node(drop_dups, [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]) == [1, 2, None, 3, None, None, 4, None, None, None]


def test_drop_dups_float():
    assert eval_node(drop_dups,
                     [1.0, 2.0, 2.0, 3.0, 3.0 + 1e-15, 3.0, 4.0, 4.0, 4.0, 4.0, 4.00001],
                     abs_tol=1e-15) == [1.0, 2.0, None, 3.0, None, None, 4.0, None, None, None, 4.00001]
