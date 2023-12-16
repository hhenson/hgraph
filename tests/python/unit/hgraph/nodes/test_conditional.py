import pytest

from hgraph.nodes._conditional import if_then_else, if_true
from hgraph.test import eval_node


def test_if_then_else():
    expected = [
        None,
        2,
        6,
        3
    ]

    assert eval_node(if_then_else, [None, True, False, True], [1, 2, 3], [4, 5, 6]) == expected


@pytest.mark.parametrize("condition,tick_once_only,expected", [
    ([True, False, True], False, [True, None, True]),
    ([True, False, True], True, [True, None, None]),
])
def test_if_true(condition, tick_once_only, expected):
    assert eval_node(if_true, condition, tick_once_only) == expected
