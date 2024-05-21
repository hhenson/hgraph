import pytest

from hgraph import sub_, add_, eq_, lt_, gt_, le_, ge_
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


@pytest.mark.parametrize(
    ['op', 'd1', 'd2', 'expected'],
    [
        [sub_, 3, 1, 2],
        [add_, 3, 1, 4],
        [eq_, 3, 1, False],
        [eq_, 3, 3, True],
        [lt_, 3, 2, False],
        [lt_, 2, 3, True],
        [gt_, 3, 2, True],
        [gt_, 2, 3, False],
        [le_, 3, 2, False],
        [le_, 2, 3, True],
        [le_, 3, 3, True],
        [ge_, 3, 2, True],
        [ge_, 3, 3, True],
        [ge_, 2, 3, False],
    ]
)
def test_date_ops(op, d1, d2, expected):
    assert eval_node(op, d1, d2) == [expected]
