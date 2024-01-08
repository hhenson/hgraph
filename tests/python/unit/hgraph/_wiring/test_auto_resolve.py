from hgraph import graph, TSL, TS, SIZE, Size, AUTO_RESOLVE
from hgraph.nodes import const
from hgraph.test import eval_node


def test_auto_resolve():

    @graph
    def g(tsl: TSL[TS[int], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[int]:
        return const(_sz.SIZE)

    assert eval_node(g, [(1, 2)], resolution_dict={"tsl": TSL[TS[int], Size[2]]}) == [2]
