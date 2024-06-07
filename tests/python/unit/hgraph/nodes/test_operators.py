from hgraph.nodes import cast_, drop, take
from hgraph.test import eval_node


def test_cast():
    expected = [
        1.0,
        2.0,
        3.0
    ]

    assert eval_node(cast_, float, [1, 2, 3]) == expected


def test_drop():
    assert eval_node(drop, [1, 2, 3, 4, 5], 3) == [None, None, None, 4, 5]


def test_take():
    assert eval_node(take, [1, 2, 3, 4, 5], 3) == [1, 2, 3, None, None]
