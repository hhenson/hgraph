from dataclasses import dataclass

from hgraph import CompoundScalar, graph, TS, eq_, getattr_, type_, str_
from hgraph.test import eval_node


@dataclass
class TestCS(CompoundScalar):
    a: int
    b: str = ''


def test_getatttr_cs():
    @graph
    def g(cs: TS[TestCS]) -> TS[int]:
        return cs.a

    assert eval_node(g, [TestCS(a=1), TestCS(a=2)]) == [1, 2]


def test_eq_cs():
    @graph
    def app(lhs: TS[TestCS], rhs: TS[TestCS]) -> TS[bool]:
        return eq_(lhs, rhs)

    assert eval_node(app, lhs=[TestCS(a=1), TestCS(a=2)], rhs=[TestCS(a=1), TestCS(a=3)]) == [True, False]


def test_ne_cs():
    @graph
    def g(lhs: TS[TestCS], rhs: TS[TestCS]) -> TS[bool]:
        return lhs != rhs

    assert eval_node(g, lhs=[TestCS(a=1), TestCS(a=2)], rhs=[TestCS(a=1), TestCS(a=3)]) == [False, True]


def test_getattr_cs():
    @graph
    def g(ts: TS[TestCS]) -> TS[int]:
        return ts.a

    assert eval_node(g, [TestCS(a=1)]) == [1]


def test_getattr_cs_default():
    @dataclass
    class Test(CompoundScalar):
        b: str = None

    @graph
    def g(ts: TS[Test]) -> TS[str]:
        return getattr_(ts, 'b', "DEFAULT")

    assert eval_node(g, [Test()]) == ["DEFAULT"]
    assert eval_node(g, [Test(b="")]) == [""]
    assert eval_node(g, [Test(b=None)]) == ["DEFAULT"]


def test_type_cs():
    @graph
    def g(ts: TS[TestCS]) -> TS[type]:
        return type_(ts)

    assert eval_node(g, [TestCS(a=1)]) == [TestCS]


def test_str_cs():
    @graph
    def g(ts: TS[TestCS]) -> TS[str]:
        return str_(ts)

    assert eval_node(g, [TestCS(a=1)]) == ["TestCS(a=1, b='')"]
