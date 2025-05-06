from dataclasses import dataclass

from hgraph import CompoundScalar, compute_node, TS, graph
from hgraph.test import eval_node


def test_auto_cast():
    @dataclass
    class Base(CompoundScalar):
        i: int

    @dataclass
    class Derived(Base):
        s: str

    @compute_node
    def f(x: TS[Base]) -> TS[bool]:
        return isinstance(x.value, Derived)

    @graph
    def g(x: TS[Derived]) -> TS[bool]:
        return f(x)

    assert eval_node(g, [Derived(i=1, s="a")]) == [True]
