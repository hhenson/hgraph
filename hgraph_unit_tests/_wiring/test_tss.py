from hgraph import graph, TS, TSS, compute_node, Removed, contains_, set_delta
from hgraph import pass_through_node
from hgraph.test import eval_node


import pytest
pytestmark = pytest.mark.smoke


@compute_node
def create_tss(key: TS[str], add: TS[bool]) -> TSS[str]:
    if add.value:
        return set_delta(added=frozenset([key.value]), removed=frozenset(), tp=str)
    else:
        return set_delta(added=frozenset(), removed=frozenset([key.value]), tp=str)


def test_tss_strait():
    assert eval_node(create_tss, key=["a", "a", "b", "a"], add=[True, True, True, False]) == [
        set_delta(frozenset("a"), frozenset(), tp=str),
        None,
        set_delta(frozenset("b"), frozenset(), tp=str),
        set_delta(frozenset(), frozenset("a"), tp=str),
    ]


def test_tss_set_frozenset():
    @compute_node
    def c(ts: TS[frozenset[str]]) -> TSS[str]:
        return ts.value

    assert eval_node(c, [frozenset({"a", "b"}), frozenset({"a"}), frozenset({"b"})]) == [
        set_delta(frozenset({"a", "b"}), frozenset(), tp=str),
        set_delta(frozenset(), frozenset({"b"}), tp=str),
        set_delta(frozenset("b"), frozenset({"a"}), tp=str),
    ]

def test_tss_pass_through():

    @graph
    def pass_through_test(key: TS[str], add: TS[bool]) -> TSS[str]:
        tss = create_tss(key, add)
        return pass_through_node(tss)

    assert eval_node(pass_through_test, key=["a", "b", "a"], add=[True, True, False]) == [
        set_delta(frozenset("a"), frozenset(), tp=str),
        set_delta(frozenset("b"), frozenset(), tp=str),
        set_delta(frozenset(), frozenset("a"), tp=str),
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
    d = set_delta(added={1, 2, 3}, removed=set(), tp=int)
    d1 = d + set_delta(added={4, 5}, removed={3}, tp=int)
    assert d1 == set_delta(added={1, 2, 4, 5}, removed=set(), tp=int)
