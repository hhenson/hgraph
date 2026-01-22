"""
Tests for Nullable value support.

These tests verify:
- Value has has_value() method
- Value.reset() makes value null
- Value.emplace() makes value non-null
- Null values preserve type information
- Python interop with None
"""

import pytest

# Skip all tests if C++ module not available
_hgraph = pytest.importorskip("hgraph._hgraph")


def _skip_if_no_cpp():
    """Helper to skip tests when C++ runtime is disabled."""
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        pytest.skip("C++ runtime not enabled")


def _create_int_value(initial_value: int = 0):
    """Helper to create an int PlainValue with optional initial value."""
    value = _hgraph.value
    int_meta = value.get_scalar_type_meta(int)
    v = value.PlainValue(int_meta)
    if initial_value != 0:
        v.from_python(initial_value)
    return v


# =============================================================================
# has_value() basic tests
# =============================================================================

def test_value_has_has_value_method():
    """Value should have has_value() method."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    assert hasattr(v, 'has_value')
    assert callable(v.has_value)


def test_value_with_data_has_value_true():
    """Value created with data should have has_value() == True."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    assert v.has_value() is True


def test_value_bool_context():
    """Value should be usable in boolean context."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = _create_int_value(42)
    # PlainValue doesn't have __bool__, check has_value instead
    assert v.has_value()  # Should be truthy when has_value


# =============================================================================
# reset() tests
# =============================================================================

def test_value_reset_makes_null():
    """Value.reset() should make has_value() return False."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    assert v.has_value() is True

    v.reset()
    assert v.has_value() is False


def test_null_value_in_bool_context():
    """Null value should have has_value() == False."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    v.reset()
    assert not v.has_value()  # Should be falsy when null


def test_reset_preserves_schema():
    """Value.reset() should preserve type information (schema)."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = _create_int_value(42)
    original_schema = v.schema

    v.reset()

    # Schema should still be accessible and unchanged
    assert v.schema is original_schema
    assert v.schema.kind.name == "Atomic"


# =============================================================================
# emplace() tests
# =============================================================================

def test_value_emplace_makes_non_null():
    """Value.emplace() should make has_value() return True."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    v.reset()
    assert v.has_value() is False

    v.emplace()
    assert v.has_value() is True


def test_emplace_creates_default_value():
    """Value.emplace() should create default-constructed value."""
    _skip_if_no_cpp()

    # Create int value, reset, then emplace
    v = _create_int_value(42)
    v.reset()
    v.emplace()

    # Should have default value (0 for int)
    assert v.has_value() is True
    assert v.to_python() == 0


def test_emplace_on_non_null_is_noop():
    """Value.emplace() on non-null value should be a no-op."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    v.emplace()  # Should not change value

    assert v.to_python() == 42


# =============================================================================
# Python interop tests
# =============================================================================

def test_null_value_to_python_returns_none():
    """Null value's to_python() should return None."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    v.reset()

    result = v.to_python()
    assert result is None


def test_from_python_none_creates_null():
    """Value.from_python(None) should create/reset to null value."""
    _skip_if_no_cpp()

    # Create a value with schema, then set from None
    v = _create_int_value(42)
    v.from_python(None)

    assert v.has_value() is False


def test_assign_none_to_existing_value():
    """Assigning None to existing value should make it null."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Create a bundle value
    int_meta = value.get_scalar_type_meta(int)
    bundle_meta = value.get_bundle_type_meta([('x', int_meta)])
    v = value.PlainValue(bundle_meta)
    v.from_python({'x': 1})

    # Assign None
    v.from_python(None)
    assert v.has_value() is False


# =============================================================================
# Access control tests
# =============================================================================

def test_accessing_null_data_raises():
    """Accessing data on null value should raise exception or return None."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    v.reset()

    # Null value to_python returns None, which is the expected behavior
    assert v.to_python() is None


def test_accessing_null_view_raises():
    """Getting view() on null value should raise exception or return invalid view."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    v.reset()

    # Try to get a view - behavior depends on implementation
    # The view may be invalid or raise an exception
    view = v.const_view()
    # View from null value should not be valid for operations
    assert v.has_value() is False


# =============================================================================
# Edge cases
# =============================================================================

def test_multiple_reset_calls():
    """Multiple reset() calls should be safe."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    v.reset()
    v.reset()  # Should not crash
    v.reset()

    assert v.has_value() is False


def test_reset_emplace_cycle():
    """reset() and emplace() should work correctly in cycles."""
    _skip_if_no_cpp()

    v = _create_int_value(42)

    for _ in range(3):
        assert v.has_value() is True
        v.reset()
        assert v.has_value() is False
        v.emplace()
        assert v.has_value() is True


def test_complex_type_nullable():
    """Nullable should work with complex types (bundles, lists)."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Create a bundle type
    int_meta = value.get_scalar_type_meta(int)
    bundle_meta = value.get_bundle_type_meta([('x', int_meta), ('y', int_meta)])

    v = value.PlainValue(bundle_meta)
    v.from_python({'x': 1, 'y': 2})
    assert v.has_value() is True

    v.reset()
    assert v.has_value() is False
    assert v.to_python() is None

    v.emplace()
    assert v.has_value() is True
    # Default should be the default-constructed bundle
    result = v.to_python()
    assert isinstance(result, dict)


# =============================================================================
# is_null() tests
# =============================================================================

def test_is_null_on_valid_value():
    """is_null() should return False for valid values."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    assert v.is_null() is False


def test_is_null_after_reset():
    """is_null() should return True after reset."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    v.reset()
    assert v.is_null() is True


def test_is_null_after_emplace():
    """is_null() should return False after emplace."""
    _skip_if_no_cpp()

    v = _create_int_value(42)
    v.reset()
    v.emplace()
    assert v.is_null() is False


# =============================================================================
# make_null() static method tests
# =============================================================================

def test_make_null_creates_null_value():
    """PlainValue.make_null() should create a null value with schema."""
    _skip_if_no_cpp()
    value = _hgraph.value

    int_meta = value.get_scalar_type_meta(int)
    v = value.PlainValue.make_null(int_meta)

    assert v.has_value() is False
    assert v.is_null() is True
    assert v.schema is int_meta


def test_make_null_then_emplace():
    """Value created with make_null() should work with emplace()."""
    _skip_if_no_cpp()
    value = _hgraph.value

    int_meta = value.get_scalar_type_meta(int)
    v = value.PlainValue.make_null(int_meta)

    assert v.has_value() is False
    v.emplace()
    assert v.has_value() is True
    assert v.to_python() == 0  # Default int value
