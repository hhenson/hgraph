"""Comparison across numeric types is a type error, not a convenience.

Ruling 2026-09-10 (issue #818 item 5.7): this type system does no automatic
type casting. ``TS[float] > 1`` silently promoted the ``int`` through an
explicitly registered mixed ``Kernel<Float, Int>`` overload -- that is an
implicit cast wearing an overload's clothes, and it is not part of the type
system's design.

Released hgraph rejects the same spellings, so the strict reading is also the
parity-matching one:

    TypeVar 'SCALAR' has already been resolved to 'float'
    which does not match the type 'rhs: int'

``eq_`` and ``cmp_`` are the two exceptions, kept ONLY because released hgraph
accepts them. They are pinned here so the asymmetry stays deliberate.
"""
import pytest

from hgraph import TS, cmp_, compute_node, eq_, ge_, graph, gt_, le_, lt_, max_, min_, ne_, null_sink
from hgraph.test import eval_node

#: The operators that must reject a mixed Int/Float pair.
STRICT = pytest.mark.parametrize(
    "op",
    [ne_, lt_, le_, gt_, ge_, min_, max_],
    ids=["ne_", "lt_", "le_", "gt_", "ge_", "min_", "max_"],
)


@STRICT
def test_rejects_a_scalar_of_the_other_numeric_type(op):
    """``op(TS[float], 1)`` -- the int literal must not lift as a float."""

    @graph
    def g(a: TS[float]):
        null_sink(op(a, 1))

    with pytest.raises(Exception):
        eval_node(g, [1.5])


@STRICT
def test_rejects_a_time_series_of_the_other_numeric_type(op):
    """``op(TS[float], TS[int])`` -- neither operand may be converted."""

    @graph
    def g(a: TS[float], b: TS[int]):
        null_sink(op(a, b))

    with pytest.raises(Exception):
        eval_node(g, [1.5], [1])


@STRICT
def test_same_type_still_works(op):
    """The removal must not touch the homogeneous overloads."""

    @graph
    def g(a: TS[float], b: TS[float]):
        null_sink(op(a, b))

    eval_node(g, [1.5], [1.0])


@pytest.mark.parametrize("op", [eq_, cmp_], ids=["eq_", "cmp_"])
def test_eq_and_cmp_keep_mixed_numerics_for_parity(op):
    """Deliberate exception: released hgraph accepts these two mixed.

    Removing them would satisfy the no-implicit-cast rule but create a NEW
    divergence, so parity wins here and the asymmetry is recorded rather than
    tidied away.
    """

    @graph
    def scalar(a: TS[float]):
        null_sink(op(a, 1))

    @graph
    def series(a: TS[float], b: TS[int]):
        null_sink(op(a, b))

    eval_node(scalar, [1.5])
    eval_node(series, [1.5], [1])


def test_plain_input_binding_was_always_strict():
    """The core type system never had this hole -- only the operator overload
    set did. Pinned so a future 'convenience' overload cannot reopen it by
    claiming binding already allowed it."""

    @compute_node
    def takes_float(x: TS[float]) -> TS[float]:
        return x.value * 2.0

    @graph
    def g(a: TS[int]) -> TS[float]:
        return takes_float(a)

    with pytest.raises(Exception):
        eval_node(g, [3])
