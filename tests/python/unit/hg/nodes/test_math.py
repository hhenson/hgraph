import pytest

from hgraph import WiringError
from hgraph.nodes import add_, sub_, mult_, div_
from hgraph.test import eval_node


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3], [4, 5, 6], [5, 7, 9]),
    ([1, 2, 3], [4, 5, 6, 7], [5, 7, 9, 10]),
    ([None, 2, None, 4], [4, 5, 6], [None, 7, 8, 10]),
])
def test_add(lhs, rhs, expected):
    assert eval_node(add_, lhs, rhs) == expected
    assert eval_node(add_, rhs, lhs) == expected


def test_add_fail():
    with pytest.raises(WiringError):
        eval_node(add_, [1, 2, 3], [4.0, 5.0, 6.0])


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3], [6, 5, 4], [-5, -3, -1]),
    ([1, 2, 3], [6, 5, 4, 3], [-5, -3, -1, 0]),
    ([None, 2, None, 4], [4, 5, 6], [None, -3, -4, -2]),
])
def test_sub(lhs, rhs, expected):
    assert eval_node(sub_, lhs, rhs) == expected


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3], [6, 5, 4], [6, 10, 12]),
    ([1, 2, 3], [6, 5, 4, 3], [6, 10, 12, 9]),
    ([None, 2, None, 4], [4, 5, 6], [None, 10, 12, 24]),
])
def test_mult(lhs, rhs, expected):
    assert eval_node(mult_, lhs, rhs) == expected


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([6, 4, 2], [3, 2, 1], [2.0, 2.0, 2.0]),
    ([6, 4, 2], [3, 2, 1, 2], [2.0, 2.0, 2.0, 1.0]),
    ([None, 6, None, 12], [2, 3, 6], [None, 2.0, 1.0, 2.0]),
])
def test_div(lhs, rhs, expected):
    assert eval_node(div_, lhs, rhs) == expected