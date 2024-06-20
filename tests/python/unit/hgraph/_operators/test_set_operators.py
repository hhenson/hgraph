from hgraph import graph, TSS, union, difference, symmetric_difference
from hgraph.test import eval_node


def test_union_tss_multi():
    @graph
    def app(ts1: TSS[int], ts2: TSS[int], ts3: TSS[int]) -> TSS[int]:
        return union(ts1, ts2, ts3)

    assert eval_node(app, [{1, 2, 3}], [{3, 4, 5}], [{4, 5, 6}]) == [{1, 2, 3, 4, 5, 6}]


def test_difference_tss_binary():
    @graph
    def app(ts1: TSS[int], ts2: TSS[int]) -> TSS[int]:
        return difference(ts1, ts2)

    assert eval_node(app, [{1, 2, 3}], [{3, 4, 5}]) == [{1, 2}]


def test_symmetric_difference_tss_binary():
    @graph
    def app(ts1: TSS[int], ts2: TSS[int]) -> TSS[int]:
        return symmetric_difference(ts1, ts2)

    assert eval_node(app, [{1, 2, 3}], [{3, 4, 5}]) == [{1, 2, 4, 5}]
