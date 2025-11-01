from hgraph import graph, TSS, union, difference, symmetric_difference, if_then_else, TS, nothing, Removed
from hgraph.test import eval_node


import pytest
pytestmark = pytest.mark.smoke

def test_union_tss_multi():
    @graph
    def app(ts1: TSS[int], ts2: TSS[int], ts3: TSS[int]) -> TSS[int]:
        return union(ts1, ts2, ts3)

    assert eval_node(app, [{1, 2, 3}], [{3, 4, 5}], [{4, 5, 6}]) == [{1, 2, 3, 4, 5, 6}]


def test_union_tss_multi_rebind_to_nothing():
    @graph
    def app(ts1_flag: TS[bool], ts1: TSS[int], ts2_flag: TS[bool], ts2: TSS[int]) -> TSS[int]:
        return union(if_then_else(ts1_flag, ts1, nothing(TSS[int])), if_then_else(ts2_flag, ts2, nothing(TSS[int])))

    assert eval_node(app, [True, False], [{1, 2, 3}], [True, None, False], [{3, 4, 5}]) == [
        {1, 2, 3, 4, 5},
        {Removed(1), Removed(2)},
        {Removed(3), Removed(4), Removed(5)},
    ]


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
