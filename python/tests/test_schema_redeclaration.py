"""Schema identity is process-wide; redeclaration is a duplicate, not a conflict.

A schema is keyed by ``(module, qualname)``, so the same name in two modules is
fine. Re-executing an *unchanged* declaration in one module - a notebook cell,
an ``importlib.reload``, a doctest - produces a new class object for the same
schema and rebinds. Redeclaring the same name with a different shape still
raises, because the existing schema is already in use.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from hgraph import TS, TSB, CompoundScalar, TimeSeriesSchema, pass_through
from hgraph.test import eval_node


def _schema_class(module, qualname, field_type, *, compound=False):
    """Create independent declarations without relying on import order."""
    base = CompoundScalar if compound else TimeSeriesSchema
    schema = type(qualname.rsplit(".", 1)[-1], (base,), {
        "__module__": module,
        "__qualname__": qualname,
        "__annotations__": {"value": field_type if compound else TS[field_type]},
    })
    return dataclass(frozen=True)(schema) if compound else schema


@pytest.mark.parametrize("compound", [False, True], ids=["tsb", "compound"])
@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
@pytest.mark.parametrize("same_shape", [False, True], ids=["different-shape", "same-shape"])
def test_same_short_schema_name_in_different_modules(compound, reverse, same_shape):
    prefix = f"{__name__}.modules_{compound}_{reverse}_{same_shape}"
    left = _schema_class(f"{prefix}.left", "Pair", int, compound=compound)
    right_type = int if same_shape else str
    right = _schema_class(f"{prefix}.right", "Pair", right_type, compound=compound)
    schemas = (right, left) if reverse else (left, right)
    annotation = TS if compound else TSB
    registered = {schema: annotation[schema] for schema in schemas}

    assert registered[left].handle != registered[right].handle
    for schema, value in ((left, 42), (right, 7 if same_shape else "seven")):
        sample = schema(value) if compound else {"value": value}
        assert eval_node(
            pass_through, [sample], resolution_dict={"ts": registered[schema]}
        ) == [sample]
        assert repr(registered[schema]) == f"{'TS' if compound else 'TSB'}[Pair]"


@pytest.mark.parametrize("compound", [False, True], ids=["tsb", "compound"])
def test_same_short_schema_name_in_different_enclosing_classes(compound):
    module = f"{__name__}.nested_{compound}"
    left = _schema_class(module, "Left.Pair", int, compound=compound)
    right = _schema_class(module, "Right.Pair", str, compound=compound)
    annotation = TS if compound else TSB
    assert annotation[left].handle != annotation[right].handle


@pytest.mark.parametrize("compound", [False, True], ids=["tsb", "compound"])
def test_module_and_qualname_boundary_is_unambiguous(compound):
    prefix = f"{__name__}.boundary_{compound}"
    top_level = _schema_class(f"{prefix}.models", "Pair", int, compound=compound)
    nested = _schema_class(prefix, "models.Pair", str, compound=compound)
    annotation = TS if compound else TSB

    assert annotation[top_level].handle != annotation[nested].handle


@pytest.mark.parametrize("reverse", [False, True], ids=["tsb-first", "compound-first"])
def test_time_series_and_compound_schema_families_do_not_share_identity(reverse):
    module = f"{__name__}.cross_family_{reverse}"
    time_series = _schema_class(module, "Quote", str)
    compound = _schema_class(module, "Quote", int, compound=True)
    registrations = (TS[compound], TSB[time_series]) if reverse else (TSB[time_series], TS[compound])

    assert registrations[0].handle != registrations[1].handle


def test_time_series_schema_redeclaration_preserves_nominal_identity():
    module = f"{__name__}.redeclare_tsb"
    first = _schema_class(module, "Pair", int)
    duplicate = _schema_class(module, "Pair", int)
    changed = _schema_class(module, "Pair", str)
    assert first is not duplicate
    assert TSB[first].handle == TSB[duplicate].handle
    with pytest.raises(ValueError, match="already registered with a different schema"):
        TSB[changed]


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
