"""
Tests for ``InputsKey``, the cache key used to de-dup wiring node instances.

``WiringPort`` replaces ``__eq__`` so that ``a == b`` wires an ``eq_`` node, stashing the real
implementation on ``__orig_eq__``. ``InputsKey`` has to call that stashed implementation directly,
which bypasses the interpreter's handling of ``NotImplemented``. ``WiringPort`` is declared
``eq=False``, so ``__orig_eq__`` is ``object.__eq__``, which returns ``NotImplemented`` for any two
distinct objects.

Mishandling that has two consequences: every non-identical pair of ports compares *equal*, because
``NotImplemented`` is truthy, and from Python 3.14 using it in a boolean context raises
``TypeError``.
"""

import warnings

import pytest
from frozendict import frozendict

from hgraph._wiring._wiring_node_instance import InputsKey
from hgraph._wiring._wiring_port import TSLWiringPort, WiringPort


def _port(path=(0,)):
    """A wiring port that hashes by field value (``unsafe_hash=True``), so distinct instances collide."""
    return TSLWiringPort(node_instance=None, path=path)


def test_identical_inputs_are_equal():
    port = _port()
    assert InputsKey(frozendict(a=port)) == InputsKey(frozendict(a=port))


def test_distinct_ports_are_not_equal():
    """Two distinct ports are not equal, per ``object.__eq__``, even when their fields match."""
    assert InputsKey(frozendict(a=_port())) != InputsKey(frozendict(a=_port()))


def test_different_ports_are_not_equal():
    """Regression: this returned True, because ``object.__eq__`` gave NotImplemented, which is truthy."""
    assert InputsKey(frozendict(a=_port(path=(0,)))) != InputsKey(frozendict(a=_port(path=(9,))))


def test_field_equal_ports_hash_alike_so_eq_is_reached():
    """Guards the premise of the tests above: these ports collide, so __eq__ is actually exercised."""
    assert hash(_port()) == hash(_port())
    assert hash(InputsKey(frozendict(a=_port()))) == hash(InputsKey(frozendict(a=_port())))


def test_comparison_does_not_leak_not_implemented():
    """``__eq__`` must return a bool for ports, never the ``NotImplemented`` singleton."""
    result = InputsKey(frozendict(a=_port())).__eq__(InputsKey(frozendict(a=_port())))
    assert result is False


def test_no_deprecation_warning_from_not_implemented():
    """On 3.12/3.13 this warns; on 3.14 the same code path raises TypeError."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert InputsKey(frozendict(a=_port())) != InputsKey(frozendict(a=_port()))


def test_differing_key_sets_are_not_equal():
    """Must not raise KeyError, and a subset of matching entries must not count as equal."""
    port = _port()
    assert InputsKey(frozendict(a=port)) != InputsKey(frozendict(a=port, b=port))
    assert InputsKey(frozendict(a=port, b=port)) != InputsKey(frozendict(a=port))


def test_disjoint_key_sets_are_not_equal():
    port = _port()
    assert InputsKey(frozendict(a=port)) != InputsKey(frozendict(b=port))


def test_non_inputs_key_is_not_equal():
    """Comparing against an unrelated type must defer rather than blow up."""
    key = InputsKey(frozendict(a=_port()))
    assert key.__eq__(object()) is NotImplemented
    assert key != object()
    assert key != 42
    assert key is not None


def test_scalar_inputs_compare_by_value():
    """Non-port inputs keep ordinary value semantics."""
    assert InputsKey(frozendict(a=1, b="x")) == InputsKey(frozendict(a=1, b="x"))
    assert InputsKey(frozendict(a=1, b="x")) != InputsKey(frozendict(a=2, b="x"))


def test_unhashable_input_degrades_to_best_effort_hash():
    """Logically immutable but unhashable values must not break key construction."""
    value = [1, 2, 3]
    key = InputsKey(frozendict(a=value))
    assert isinstance(hash(key), int)
    assert key == InputsKey(frozendict(a=value))


def test_mixed_scalar_and_port_does_not_wire_a_node():
    """
    A plain value on one side and a port on the other must not reach the ``eq_``-wiring ``__eq__``.

    ``==`` would reflect into it when the left operand returns NotImplemented, which would build a
    graph node as a side effect of a dictionary lookup, and outside a wiring context that raises.
    """
    port = _port()
    assert InputsKey(frozendict(a=1)) != InputsKey(frozendict(a=port))
    assert InputsKey(frozendict(a=port)) != InputsKey(frozendict(a=1))


def test_wiring_port_orig_eq_is_identity_based():
    """Documents the premise: __orig_eq__ is object.__eq__, so it yields NotImplemented for distinct objects."""
    assert WiringPort.__orig_eq__ is object.__eq__
    assert _port().__orig_eq__(_port()) is NotImplemented


@pytest.mark.smoke
def test_node_instance_cache_lookup_survives_port_comparison():
    """
    End-to-end: wiring must not raise while probing the node-instance cache.

    Marked smoke so it runs in the cross-version CI matrix, where a NotImplemented
    leak would surface as a TypeError on Python 3.14.
    """
    from hgraph import graph, const, debug_print, wire_graph
    from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext

    @graph
    def main():
        c = const(1)
        d = const(1)
        debug_print("sum", c + d)

    with WiringNodeInstanceContext():
        assert wire_graph(main) is not None
