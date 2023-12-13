import pytest

from hgraph.nodes import eq_, if_
from hgraph.nodes._operators import if_true
from hgraph.test import eval_node


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3], [1, 3, 3], [True, False, True]),
    ([1, 2], [4, 2, 3], [False, True, False]),
    ([None, 1, None, 2], [1, None, 2], [None, True, False, True]),
])
def test_eq(lhs, rhs, expected):
    assert eval_node(eq_, lhs, rhs) == expected


@pytest.mark.parametrize("condition,true_value,false_value,tick_on_condition,expected", [
    ([True, False], [1, None], [2, None], True, [1, 2]),
    ([True, False], [1, None], [2, None], False, [1, None]),
])
def test_if(condition, true_value, false_value, tick_on_condition, expected):
    assert eval_node(if_, condition, true_value, false_value, tick_on_condition) == expected


@pytest.mark.parametrize("condition,tick_once_only,expected", [
    ([True, False, True], False, [True, None, True]),
    ([True, False, True], True, [True, None, None]),
])
def test_if_true(condition, tick_once_only, expected):
    assert eval_node(if_true, condition, tick_once_only) == expected