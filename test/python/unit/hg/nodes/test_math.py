import pytest

from hg import ParseError
from hg._wiring._wiring_errors import WiringError
from hg.nodes import add_
from hg.test import eval_node


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