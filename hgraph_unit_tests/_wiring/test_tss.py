from hgraph import graph, TS, TSS, compute_node, PythonSetDelta, Removed, contains_, SIGNAL
from hgraph.nodes import pass_through_node
from hgraph.test import eval_node


@compute_node
def create_tss(key: TS[str], add: TS[bool]) -> TSS[str]:
    if add.value:
        return PythonSetDelta(added=frozenset([key.value]), removed=frozenset())
    else:
        return PythonSetDelta(added=frozenset(), removed=frozenset([key.value]))


def test_tss_strait():
    assert eval_node(create_tss, key=["a", "a", "b", "a"], add=[True, True, True, False]) == [
        PythonSetDelta(frozenset("a"), frozenset()),
        None,
        PythonSetDelta(frozenset("b"), frozenset()),
        PythonSetDelta(frozenset(), frozenset("a")),
    ]


def test_tss_pass_through():

    @graph
    def pass_through_test(key: TS[str], add: TS[bool]) -> TSS[str]:
        tss = create_tss(key, add)
        return pass_through_node(tss)

    assert eval_node(pass_through_test, key=["a", "b", "a"], add=[True, True, False]) == [
        PythonSetDelta(frozenset("a"), frozenset()),
        PythonSetDelta(frozenset("b"), frozenset()),
        PythonSetDelta(frozenset(), frozenset("a")),
    ]


def test_tss_contains():

    @graph
    def contains(ts: TSS[int], key: TS[int]) -> TS[bool]:
        return contains_(ts, key)

    assert eval_node(contains, [{1}, {2}, {Removed(1)}, {}, {3}], [0, 1, None, 3]) == [False, True, False, False, True]


def test_tss_empty():
    @compute_node
    def empty(s: TS[bool]) -> TSS[int]:
        return set()

    assert eval_node(empty, [True]) == [set()]


def test_set_delta_addition():
    d = PythonSetDelta(added = {1, 2, 3}, removed = set())
    d1 = d + PythonSetDelta(added = {4, 5}, removed = {3})
    assert d1 == PythonSetDelta(added = {1, 2, 4, 5}, removed = set())
