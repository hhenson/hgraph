"""A graph's declared output type is enforced at wiring (issue #811).

A graph whose body returned a different time-series type from the one it
declared was accepted and evaluated, so ``int`` values were delivered through a
declared ``TS[bool]`` and a ``TS`` through a declared ``TSD``. Released hgraph
rejects each of these at wiring, with the wording pinned below.

The issue's own reading is worth keeping in view: if a declaration is never
enforced where it is made, a mismatch survives to wherever something downstream
trusts it -- which is why this is a wiring-safety hole rather than a value
difference.
"""
import pytest

from hgraph import (
    TS,
    TSD,
    TIME_SERIES_TYPE,
    WiringError,
    compute_node,
    graph,
    invert_,
)
from hgraph.test import eval_node


@compute_node
def _to_float(x: TS[int]) -> TS[float]:
    return float(x.value)


@compute_node
def _to_str(x: TS[int]) -> TS[str]:
    return str(x.value)


def test_operator_result_type_must_match_the_declaration():
    """The issue's own case: invert_ over TS[bool] yields TS[int]."""

    @graph
    def g(ts: TS[bool]) -> TS[bool]:
        return invert_(ts)

    with pytest.raises(WiringError, match=r"declares its output as 'TS\[bool\]' but 'TS\[int\]'"):
        eval_node(g, [True, False])


def test_widened_arithmetic_result_must_match_the_declaration():
    """Mixed arithmetic legitimately widens to TS[float]; the DECLARATION is
    what was wrong, and it used to deliver a float inside a TS[int]."""

    @graph
    def g(a: TS[int]) -> TS[int]:
        from hgraph import add_
        return add_(a, 1.7)

    with pytest.raises(WiringError, match=r"declares its output as 'TS\[int\]' but 'TS\[float\]'"):
        eval_node(g, [10])


def test_node_result_type_must_match_the_declaration():
    @graph
    def g(a: TS[int]) -> TS[int]:
        return _to_float(a)

    with pytest.raises(WiringError, match="declares its output as"):
        eval_node(g, [3])


def test_a_different_shape_is_rejected():
    """Not just the payload: a TS returned through a declared TSD."""

    @graph
    def g(a: TS[int]) -> TSD[str, TS[int]]:
        return a

    with pytest.raises(WiringError, match="declares its output as"):
        eval_node(g, [1])


def test_unrelated_payload_is_rejected():
    @graph
    def g(a: TS[int]) -> TS[int]:
        return _to_str(a)

    with pytest.raises(WiringError, match=r"'TS\[int\]' but 'TS\[str\]'"):
        eval_node(g, [3])


def test_a_correct_declaration_still_wires():
    @graph
    def g(a: TS[int]) -> TS[float]:
        return _to_float(a)

    assert eval_node(g, [3]) == [3.0]


def test_a_nested_graph_is_checked_too():
    """map_ bodies go through the same wiring path, so a mis-declared body is
    caught where it is declared rather than at the outer boundary."""
    from hgraph import map_

    @graph
    def bad_body(x: TS[int]) -> TS[int]:
        return _to_float(x)

    @graph
    def outer(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return map_(bad_body, ts)

    with pytest.raises(WiringError, match="'bad_body' declares its output as"):
        eval_node(outer, [{"a": 1}])


def test_a_generic_declaration_still_resolves():
    """``TIME_SERIES_TYPE`` is treated as an absent annotation -- it resolves
    FROM the wired ports rather than constraining them, so enforcing it would
    break every generic graph. This is the acceptance criterion's second half.
    """

    @graph
    def g(a: TS[int]) -> TIME_SERIES_TYPE:
        return _to_float(a)

    assert eval_node(g, [3]) == [3.0]


def test_covariance_is_still_accepted():
    """Assignability, not equality: a subclass payload satisfies a declared
    base. Writing the check as handle equality rejected 87 working graphs."""
    from dataclasses import dataclass

    from hgraph import CompoundScalar, const

    @dataclass(frozen=True)
    class Base(CompoundScalar):
        a: int

    @dataclass(frozen=True)
    class Derived(Base):
        b: int = 0

    @graph
    def g() -> TS[Base]:
        return const(Derived(1, 2), TS[Derived])

    assert eval_node(g) == [Derived(1, 2)]


def test_an_erased_payload_does_not_excuse_a_different_shape():
    """The payload widenings must not admit a different OUTER shape.

    ``TS[object]`` widens over any payload and ``TSW`` defers its handle
    comparison, but both are about what a time series CARRIES. Applied without
    checking the outer kind they reopened the hole this check exists to close,
    in both directions -- and no C++ graph signature can admit either return
    (issue #811 review).
    """

    @compute_node
    def _to_tsd(x: TS[int]) -> TSD[str, TS[int]]:
        return {"a": x.value}

    @compute_node
    def _to_object(x: TS[int]) -> TS[object]:
        return x.value

    @graph
    def declared_scalar(a: TS[int]) -> TS[object]:
        return _to_tsd(a)

    with pytest.raises(WiringError, match="declares its output as"):
        eval_node(declared_scalar, [1])

    @graph
    def declared_keyed(a: TS[int]) -> TSD[str, TS[int]]:
        return _to_object(a)

    with pytest.raises(WiringError, match="declares its output as"):
        eval_node(declared_keyed, [1])


def test_an_erased_payload_is_still_accepted_at_the_same_shape():
    """The widening itself survives: same outer shape, erased payload."""

    @compute_node
    def _to_object(x: TS[int]) -> TS[object]:
        return x.value

    @graph
    def g(a: TS[int]) -> TS[object]:
        return _to_object(a)

    assert eval_node(g, [1]) == [1]
