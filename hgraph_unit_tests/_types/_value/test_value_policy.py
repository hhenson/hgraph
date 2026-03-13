"""
Value API surface regression tests.

The Value layer now exposes a single owning type (`Value`) with default-live
schema-bound storage. Policy-specific variants are intentionally not exposed.
"""

import pytest

from hgraph._feature_switch import is_feature_enabled
if not is_feature_enabled("use_cpp"):
    pytest.skip("Buffer protocol tests require C++ extension", allow_module_level=True)

_hgraph = pytest.importorskip("hgraph._hgraph", reason="C++ extension not available")
value = _hgraph.value


def test_plain_value_is_exposed():
    assert hasattr(value, "Value")


def test_policy_value_variants_not_exposed():
    assert not hasattr(value, "CachedValue")
    assert not hasattr(value, "TSValue")
    assert not hasattr(value, "ValidatedValue")


def test_plain_value_is_live_when_schema_bound():
    int_schema = value.scalar_type_meta_int64()

    v = value.Value(int_schema)
    assert v.has_value()
    assert v.has_value()
    assert v.schema is int_schema
    assert v.to_python() == 0

    v.from_python(123)
    assert v.has_value()
    assert v.has_value()
    assert v.as_int() == 123

    v.reset()
    assert v.has_value()
    assert v.has_value()
    assert v.schema is int_schema
    assert v.as_int() == 0


def test_plain_value_python_ctor_is_schema_bound():
    v = value.Value(456)
    assert v.has_value()
    assert v.has_value()
    assert v.schema is value.scalar_type_meta_int64()
    assert v.as_int() == 456


def test_plain_value_emplace_and_reset():
    int_schema = value.scalar_type_meta_int64()

    v = value.Value(int_schema)
    assert v.has_value()
    assert v.as_int() == 0

    v.reset()
    assert v.has_value()
    assert v.as_int() == 0

    v.reset()
    assert v.has_value()
    assert v.as_int() == 0
    assert v.schema is int_schema
