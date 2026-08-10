"""Schema identity is process-wide; redeclaration is a duplicate, not a conflict.

A schema is keyed by ``(module, name)``, so the same name in two modules is
fine. Re-executing an *unchanged* declaration in one module - a notebook cell,
an ``importlib.reload``, a doctest - produces a new class object for the same
schema and rebinds. Redeclaring the same name with a different shape still
raises, because the existing schema is already in use.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from hgraph import TS, CompoundScalar


def _declare(*, defaulted=False, extra_field=False):
    """Build a fresh BidAsk class object, optionally with a different shape."""
    if extra_field:
        @dataclass(frozen=True)
        class BidAsk(CompoundScalar):
            bid: float
            ask: float
            venue: str
    elif defaulted:
        @dataclass(frozen=True)
        class BidAsk(CompoundScalar):
            bid: float
            ask: float = 0.0
    else:
        @dataclass(frozen=True)
        class BidAsk(CompoundScalar):
            bid: float
            ask: float
    return BidAsk


def test_reexecuting_an_unchanged_declaration_rebinds():
    first = _declare()
    TS[first]
    second = _declare()
    assert first is not second
    TS[second]  # a duplicate, not a conflict


def test_redeclaring_with_a_different_reconstruction_shape_raises():
    TS[_declare()]
    # Same fields, but one now has a constructor default, so the class is
    # reconstructed differently.
    with pytest.raises(TypeError, match="different reconstruction shape"):
        TS[_declare(defaulted=True)]


def test_redeclaring_with_a_different_field_set_raises():
    TS[_declare()]
    # A changed field set never reaches the class registry: the named-bundle
    # schema itself conflicts.
    with pytest.raises((TypeError, ValueError)):
        TS[_declare(extra_field=True)]
