from dataclasses import dataclass

from hgraph import take, cast_, graph, CompoundScalar, TS, setattr_
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke


def test_cast():
    expected = [1.0, 2.0, 3.0]

    assert eval_node(cast_, float, [1, 2, 3]) == expected


def test_take():
    assert eval_node(take, [1, 2, 3, 4, 5], 3) == [1, 2, 3, None, None]


def test_setattr():

    @dataclass
    class Simple(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts: TS[Simple], p: TS[int]) -> TS[Simple]:
        return setattr_(ts, "a", p)

    assert eval_node(g, [Simple(1, "a")], [2]) == [Simple(2, "a")]