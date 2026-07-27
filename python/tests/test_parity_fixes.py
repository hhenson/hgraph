"""Public Python wiring regressions for fixed parity issues #69, #70, #72.

Each test pins the released-hgraph trace the differential harness verified;
the corpus retains the minimized recipes as passing regressions.
"""

import hgraph as hg
from hgraph import TS, REF, compute_node, graph
from hgraph.test import eval_node


def test_float_dedup_applies_default_tolerance():
    # Issue #69: upstream's float dedup overload defaults abs_tol=1e-15;
    # a sub-tolerance change from the last emitted value does not tick.
    @graph
    def dedup_product(lhs: TS[float], rhs: TS[float]) -> TS[float]:
        return hg.dedup(rhs * lhs)

    out = eval_node(dedup_product, [0.0, 1.0], [9.395605309808467e-37, None])
    assert out == [0.0, None]

    # The explicit-tolerance arity is unchanged.
    @graph
    def dedup_tol(v: TS[float]) -> TS[float]:
        return hg.dedup(v, 0.5)

    assert eval_node(dedup_tol, [1.0, 1.4, 2.0]) == [1.0, None, 2.0]


def _selection(choose_minimum, lhs, rhs):
    return hg.if_then_else(choose_minimum, hg.min_(lhs, rhs), hg.max_(lhs, rhs))


def test_format_renders_ref_arguments_dereferenced():
    # Issue #72: a REF-valued format argument renders its referenced VALUE,
    # never the reference object.
    @graph
    def formatted(lhs: TS[int], rhs: TS[int], choose_minimum: TS[bool]) -> TS[str]:
        return hg.format_("{}:{}", _selection(choose_minimum, lhs, rhs), lhs % rhs)

    assert eval_node(formatted, [8], [-6], [True]) == ["-6:-4"]


def test_valid_over_silent_ref_produces_no_tick():
    # Issue #70: valid over a REF-valued source that never ticks produces NO
    # output (upstream's valid_impl requires the REF input valid); a plain
    # statically-referenced source still ticks False at start.
    @graph
    def selection_valid(lhs: TS[int], rhs: TS[int], choose_minimum: TS[bool]) -> TS[bool]:
        return hg.valid(_selection(choose_minimum, lhs, rhs))

    assert eval_node(selection_valid, [None], [None], [None]) is None
    assert eval_node(selection_valid, [8], [-6], [True]) == [True]

    @compute_node
    def never_ref(i: TS[int]) -> REF[TS[int]]:
        return None

    @graph
    def never_valid(i: TS[int]) -> TS[bool]:
        return hg.valid(never_ref(i))

    assert eval_node(never_valid, [None]) is None

    # Plain (non-REF-valued) sources keep the start False tick.
    @graph
    def plain_valid(i: TS[int]) -> TS[bool]:
        return hg.valid(i)

    assert eval_node(plain_valid, [None, 1]) == [False, True]
