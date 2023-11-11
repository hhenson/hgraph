from hg import graph, TS, TSS, compute_node, PythonSetDelta
from hg.test import eval_node


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