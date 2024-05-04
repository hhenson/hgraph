from hgraph import TSS, graph, TS, Removed, not_, is_empty
from hgraph.test import eval_node


def test_is_empty():
    @graph
    def is_empty_test(tss: TSS[int]) -> TS[bool]:
        return is_empty(tss)

    assert eval_node(is_empty_test, [None, {1}, {2}, {Removed(1)}, {Removed(2)}]) == [True, False, None, None, True]


def test_not():
    @graph
    def is_empty_test(tss: TSS[int]) -> TS[bool]:
        return not_(tss)

    assert eval_node(is_empty_test, [None, {1}, {2}, {Removed(1)}, {Removed(2)}]) == [True, False, None, None, True]
