import pytest

from hgraph import compute_node, TS
from hgraph.test import eval_node


@compute_node
def add(a: TS[int], b: TS[int]) -> TS[int]:
    return a.value + b.value


@pytest.mark.parametrize(
    "a,b,expected", [
       [[1, 2, 3], [2, 3, 4], [3, 5, 7]],
       [[None, 2, None], [2, 3, 4], [None, 5, 6]],
    ])
def test_add(a, b, expected):
    assert eval_node(add, a=a, b=b) == expected
