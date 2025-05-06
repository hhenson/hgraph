import pytest
from frozendict import frozendict

from hgraph import compute_node, TS, TIME_SERIES_TYPE, graph, SCALAR, TSL, Size, TSS, TSD, operator
from hgraph.test import eval_node


@pytest.mark.parametrize(
    ("ts_tp", "value", "should_work"),
    (
        (TS[int], 1, True),
        (TS[int], "1", False),
        (TS[bool], True, True),
        (TSL[TS[int], Size[2]], (1, 2), True),
        (TSL[TS[int], Size[2]], frozendict({0: 1}), True),
        (TSL[TS[int], Size[2]], (1, 2, 3), False),
        (TSS[int], frozenset({1, 2}), True),
        (TSS[int], {0: 1, 1: 2}, False),
        (TSS[int], 1, False),
        (TSD[int, TS[int]], frozenset({1, 2}), False),
        (TSD[int, TS[int]], frozendict({1: 1, 2: 2}), True),
        (TSD[int, TSL[TS[int], Size[2]]], frozendict({1: (1, 2)}), True),
    ),
)
def test_auto_const(ts_tp, value, should_work):
    @compute_node
    def n(t: ts_tp) -> TS[bool]:
        return bool(t.value)

    @graph
    def g() -> TS[bool]:
        return n(value)

    if should_work:
        assert eval_node(g) == [True]
    else:
        with pytest.raises(Exception):
            eval_node(g)


def test_auto_cons_with_overload():

    @operator
    def op(a: TS[int], b: TS[int]) -> TS[int]: ...

    @compute_node(overloads=op)
    def op_default(a: TS[int], b: TS[int]) -> TS[int]:
        return a.value + b.value + 1

    @compute_node(overloads=op)
    def op_scalar(a: TS[int], b: int) -> TS[int]:
        return a.value + b

    @graph
    def g(a: TS[int]) -> TS[int]:
        return op(a, 1)

    assert eval_node(g, [1]) == [2]
