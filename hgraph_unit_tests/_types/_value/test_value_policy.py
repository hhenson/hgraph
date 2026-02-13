"""
Value API surface regression tests.

The Value layer now exposes a single owning type (`PlainValue`) with typed-null
semantics. Policy-specific variants are intentionally not exposed.
"""

import pytest

_hgraph = pytest.importorskip("hgraph._hgraph", reason="C++ extension not available")
value = _hgraph.value


def test_plain_value_is_exposed():
    assert hasattr(value, "PlainValue")


def test_policy_value_variants_not_exposed():
    assert not hasattr(value, "CachedValue")
    assert not hasattr(value, "TSValue")
    assert not hasattr(value, "ValidatedValue")


def test_plain_value_typed_null_roundtrip():
    tr = value.TypeRegistry.instance()
    int_schema = tr.get_int_type()

    v = value.PlainValue(int_schema)
    assert not v.has_value()
    assert not v.valid()
    assert v.schema is int_schema
    assert v.to_python() is None

    v.from_python(123)
    assert v.has_value()
    assert v.valid()
    assert v.as_int() == 123

    v.from_python(None)
    assert not v.has_value()
    assert not v.valid()
    assert v.schema is int_schema
    assert v.to_python() is None


def test_plain_value_emplace_and_reset():
    tr = value.TypeRegistry.instance()
    int_schema = tr.get_int_type()

    v = value.PlainValue(int_schema)
    assert not v.has_value()

    v.emplace()
    assert v.has_value()
    assert v.as_int() == 0

    v.reset()
    assert not v.has_value()
    assert v.schema is int_schema
