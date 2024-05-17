from hgraph import graph, TS, TSS, compute_node, PythonSetDelta, Removed, contains_
from hgraph.nodes import pass_through, tss_intersection
from hgraph.test import eval_node


@compute_node
def create_tss(key: TS[str], add: TS[bool]) -> TSS[str]:
    if add.value:
        return PythonSetDelta(frozenset([key.value]), frozenset())
    else:
        return PythonSetDelta(frozenset(), frozenset([key.value]))


def test_tss_strait():

    assert eval_node(create_tss, key=["a", "b", "a"], add=[True, True, False]) == [
        PythonSetDelta(frozenset("a"), frozenset()),
        PythonSetDelta(frozenset("b"), frozenset()),
        PythonSetDelta(frozenset(), frozenset("a"))]


def test_tss_pass_through():

        @graph
        def pass_through_test(key: TS[str], add: TS[bool]) -> TSS[str]:
            tss = create_tss(key, add)
            return pass_through(tss)

        assert eval_node(pass_through_test, key=["a", "b", "a"], add=[True, True, False]) == [
            PythonSetDelta(frozenset("a"), frozenset()),
            PythonSetDelta(frozenset("b"), frozenset()),
            PythonSetDelta(frozenset(), frozenset("a"))]


def test_tss_contains():

    @graph
    def contains(ts: TSS[int], key: TS[int]) -> TS[bool]:
        return contains_(ts, key)

    assert eval_node(contains, [{1}, {2}, {Removed(1)}, {}, {3}], [0, 1, None, 3]) \
           == [False, True, False, False, True]


def test_tss_sub_or_difference():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 - tss2

    assert eval_node(app,
                     [{1}, {2},  None,         None, {Removed(2)}],
                     [{},  None, {1},          {1},  None]) \
           ==        [{1}, {2},  {Removed(1)}, None, {Removed(2)}]


def test_tss_add_or_union():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 + tss2

    assert eval_node(app,
                     [{1}, {2},  None, {4},    {5},  None,         {Removed(5)}],
                     [{1}, None, {3},  {5},    None, {Removed(5)}, None]) \
           ==        [{1}, {2},  {3},  {4, 5}, None, None,         {Removed(5)}]


def test_tss_intersection():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss_intersection(tss1, tss2)

    assert eval_node(app,
                     [{1, 2, 3, 4}, {5, 6}, {Removed(1)}, {Removed(2)},     None],
                     [{0, 2, 3, 5}, None,   None,         None,             {-1, 1, 4}]) \
           ==        [{2, 3},       {5},    None,         {Removed(2)},     {4}]
