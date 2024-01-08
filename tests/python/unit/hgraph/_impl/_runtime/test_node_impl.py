from hgraph import compute_node, TSL, TS, Size
from hgraph.test import eval_node


def test_all_valid():
    @compute_node(all_valid=('tsl',))
    def a_node(tsl: TSL[TS[int], Size[2]]) -> TS[bool]:
        return True

    assert eval_node(a_node, [{0: 1}, {1: 1}]) == [None, True]


def test_valid():
    @compute_node(valid=('tsl',))
    def a_node(tsl: TSL[TS[int], Size[2]]) -> TS[bool]:
        return True

    assert eval_node(a_node, [{0: 1}, {1: 1}]) == [True, True]
