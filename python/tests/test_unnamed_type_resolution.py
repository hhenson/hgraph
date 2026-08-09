"""Unnamed ``[]`` pre-resolution: ``my_node[TS[int]]`` as well as ``[OUT: ...]``.

Upstream resolves a single unnamed subscript item against the signature's sole
type variable, or against the one marked ``DEFAULT[...]``, and raises when
neither rule applies. This adds enumerated positional binding on top: several
unnamed items fill type variables in order of first appearance.

The ordering comes from the lowered C++ pattern (``TypePattern.variables``), so
it matches what the overload matcher unifies against.
"""

from __future__ import annotations

import pytest
from frozendict import frozendict as fd

from hgraph import (
    DEFAULT,
    K,
    OUT,
    SCALAR,
    TS,
    TSD,
    TSL,
    SIZE,
    NUMBER,
    TS_SCHEMA,
    TSB,
    V,
    WiringError,
    compute_node,
    graph,
)
from hgraph._types import _pattern_of
from hgraph.test import eval_node


def test_pattern_variables_follow_declaration_order():
    def names(annotation):
        return list(_pattern_of(annotation).variables)

    assert names(TS[SCALAR]) == ["SCALAR"]
    assert names(OUT) == ["OUT"]
    # A TSD reports its key before its value.
    assert names(TSD[K, V]) == ["K", "V"]
    # A TSL reports its element before its size, matching TSL[element, size].
    assert names(TSL[TS[NUMBER], SIZE]) == ["NUMBER", "SIZE"]
    assert names(TSB[TS_SCHEMA]) == ["TS_SCHEMA"]
    # A concrete annotation contributes nothing.
    assert names(TS[int]) == []


def test_single_unnamed_item_binds_the_sole_type_var():
    @compute_node
    def my_add(lhs: TS[float], rhs: TS[int]) -> OUT:
        return lhs.value + rhs.value

    @graph
    def g(lhs: TS[float], rhs: TS[int]) -> TS[float]:
        return my_add[TS[float]](lhs, rhs)

    assert eval_node(g, [1.0, 2.0], [4, 5]) == [5.0, 7.0]


def test_named_form_still_works():
    @compute_node
    def my_add(lhs: TS[float], rhs: TS[int]) -> OUT:
        return lhs.value + rhs.value

    @graph
    def g(lhs: TS[float], rhs: TS[int]) -> TS[float]:
        return my_add[OUT: TS[float]](lhs, rhs)

    assert eval_node(g, [1.0], [4]) == [5.0]


def test_default_marker_selects_the_target_when_several_type_vars_exist():
    @compute_node
    def pick(ts: TS[SCALAR], count: int) -> DEFAULT[OUT]:
        return str(ts.value) * count

    @graph
    def g(ts: TS[int]) -> TS[str]:
        return pick[TS[str]](ts, 2)

    assert eval_node(g, [7]) == ["77"]


def test_enumerated_items_bind_in_declaration_order():
    @compute_node
    def probe(values: TSD[K, TS[V]]) -> TS[str]:
        return ",".join(f"{k}={v}" for k, v in sorted(values.value.items()))

    assert eval_node(probe[str, int], [fd(a=1, b=2)]) == ["a=1,b=2"]


def test_named_and_unnamed_compose_without_the_named_being_overwritten():
    @compute_node
    def probe(values: TSD[K, TS[V]]) -> TS[str]:
        return ",".join(f"{k}={v}" for k, v in sorted(values.value.items()))

    # V is named, so the unnamed `str` fills the remaining variable, K.
    assert eval_node(probe[str, V: int], [fd(a=1)]) == ["a=1"]


def test_ambiguous_unnamed_item_raises_rather_than_being_dropped():
    @compute_node
    def ambiguous(lhs: TS[K], rhs: TS[V]) -> TS[bool]:
        return True

    with pytest.raises(WiringError) as raised:
        ambiguous[str]
    message = str(raised.value)
    # The error names the candidates rather than surfacing later as an
    # unrelated complaint about the return annotation.
    assert "K, V" in message
    assert "DEFAULT" in message


def test_too_many_unnamed_items_raises():
    @compute_node
    def one_var(ts: TS[SCALAR]) -> TS[SCALAR]:
        return ts.value

    with pytest.raises(WiringError):
        one_var[int, str]
