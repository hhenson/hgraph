from dataclasses import dataclass

from hgraph import CompoundScalar, graph, TS
from hgraph.nodes import cs_from_ts
from hgraph.test import eval_node


@dataclass
class TestCS(CompoundScalar):
    a: int
    b: str = ''


def test_cs_getatttr():
    @graph
    def g(cs: TS[TestCS]) -> TS[int]:
        return cs.a

    assert eval_node(g, [TestCS(a=1), TestCS(a=2)]) == [1, 2]


def test_cs_from_ts():
    @graph
    def g(a: TS[int], b: TS[str]) -> TS[TestCS]:
        return cs_from_ts(TestCS, a=a, b=b)

    assert eval_node(g, a=[1, 2], b=['1', '2']) == [TestCS(a=1, b='1'), TestCS(a=2, b='2')]


def test_cs_from_ts_scalar():
    @graph
    def g(a: TS[int]) -> TS[TestCS]:
        return cs_from_ts(TestCS, a=a, b='-')

    assert eval_node(g, a=[1, 2]) == [TestCS(a=1, b='-'), TestCS(a=2, b='-')]


def test_cs_from_ts_defaults():
    @graph
    def g(a: TS[int]) -> TS[TestCS]:
        return cs_from_ts(TestCS, a=a)

    assert eval_node(g, a=[1, 2]) == [TestCS(a=1, b=''), TestCS(a=2, b='')]
