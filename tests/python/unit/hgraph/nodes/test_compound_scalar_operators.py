from dataclasses import dataclass

from hgraph import CompoundScalar, graph, TS
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
