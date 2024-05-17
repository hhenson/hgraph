import pytest

from hgraph import WiringError, min_
from hgraph.nodes import add_ts, sub_ts, mult_ts, div_ts
from hgraph.test import eval_node


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3], [4, 5, 6], [5, 7, 9]),
    ([1, 2, 3], [4, 5, 6, 7], [5, 7, 9, 10]),
    ([None, 2, None, 4], [4, 5, 6], [None, 7, 8, 10]),
])
def test_add(lhs, rhs, expected):
    assert eval_node(add_ts, lhs, rhs) == expected
    assert eval_node(add_ts, rhs, lhs) == expected


def test_add_fail():
    with pytest.raises(WiringError):
        eval_node(add_ts, [1, 2, 3], [4.0, 5.0, 6.0])


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3], [6, 5, 4], [-5, -3, -1]),
    ([1, 2, 3], [6, 5, 4, 3], [-5, -3, -1, 0]),
    ([None, 2, None, 4], [4, 5, 6], [None, -3, -4, -2]),
])
def test_sub(lhs, rhs, expected):
    assert eval_node(sub_ts, lhs, rhs) == expected


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3], [6, 5, 4], [6, 10, 12]),
    ([1, 2, 3], [6, 5, 4, 3], [6, 10, 12, 9]),
    ([None, 2, None, 4], [4, 5, 6], [None, 10, 12, 24]),
])
def test_mult(lhs, rhs, expected):
    assert eval_node(mult_ts, lhs, rhs) == expected


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([6, 4, 2], [3, 2, 1], [2.0, 2.0, 2.0]),
    ([6, 4, 2], [3, 2, 1, 2], [2.0, 2.0, 2.0, 1.0]),
    ([None, 6, None, 12], [2, 3, 6], [None, 2.0, 1.0, 2.0]),
])
def test_div(lhs, rhs, expected):
    assert eval_node(div_ts, lhs, rhs) == expected


def test_min():
    assert eval_node(min_, [1, 2, 3]) == [1, 2, 3]
    assert eval_node(min_, [1, 2, 3], [3, 2, 1]) == [1, 2, 1]
