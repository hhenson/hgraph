"""
Tests for Phase 3: Nullable value support.

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
    from hgraph import _features
    if not _features.USE_CPP_RUNTIME:
        pytest.skip("C++ runtime not enabled")


# =============================================================================
# has_value() basic tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_value_has_has_value_method():
    """Value should have has_value() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    assert hasattr(v, 'has_value')
    assert callable(v.has_value)


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_value_with_data_has_value_true():
    """Value created with data should have has_value() == True."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    assert v.has_value() is True


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_value_bool_context():
    """Value should be usable in boolean context."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    assert v  # Should be truthy when has_value


# =============================================================================
# reset() tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_value_reset_makes_null():
    """Value.reset() should make has_value() return False."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    assert v.has_value() is True

    v.reset()
    assert v.has_value() is False


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_null_value_in_bool_context():
    """Null value should be falsy in boolean context."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    v.reset()
    assert not v  # Should be falsy when null


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_reset_preserves_schema():
    """Value.reset() should preserve type information (schema)."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    original_schema = v.meta()

    v.reset()

    # Schema should still be accessible and unchanged
    assert v.meta() is original_schema
    assert v.meta().kind.name == "Atomic"


# =============================================================================
# emplace() tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_value_emplace_makes_non_null():
    """Value.emplace() should make has_value() return True."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    v.reset()
    assert v.has_value() is False

    v.emplace()
    assert v.has_value() is True


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_emplace_creates_default_value():
    """Value.emplace() should create default-constructed value."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Create int value, reset, then emplace
    v = value.Value.from_python(42)
    v.reset()
    v.emplace()

    # Should have default value (0 for int)
    assert v.has_value() is True
    assert v.to_python() == 0


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_emplace_on_non_null_is_noop():
    """Value.emplace() on non-null value should be a no-op."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    v.emplace()  # Should not change value

    assert v.to_python() == 42


# =============================================================================
# Python interop tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_null_value_to_python_returns_none():
    """Null value's to_python() should return None."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    v.reset()

    result = v.to_python()
    assert result is None


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_from_python_none_creates_null():
    """Value.from_python(None) should create/reset to null value."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Create a value with schema, then set from None
    v = value.Value.from_python(42)
    v.from_python(None)  # or similar API

    assert v.has_value() is False


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_assign_none_to_existing_value():
    """Assigning None to existing value should make it null."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python({"x": 1})
    # The exact API for assignment depends on implementation
    # This might be v.assign(None) or v.set_python(None)
    pass  # TODO: Implement when API is defined


# =============================================================================
# Access control tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_accessing_null_data_raises():
    """Accessing data() on null value should raise exception."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    v.reset()

    with pytest.raises(Exception):  # Could be RuntimeError or specific exception
        _ = v.data()


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_accessing_null_view_raises():
    """Getting view() on null value should raise exception."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    v.reset()

    with pytest.raises(Exception):
        _ = v.view()


# =============================================================================
# Edge cases
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_multiple_reset_calls():
    """Multiple reset() calls should be safe."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)
    v.reset()
    v.reset()  # Should not crash
    v.reset()

    assert v.has_value() is False


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_reset_emplace_cycle():
    """reset() and emplace() should work correctly in cycles."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python(42)

    for _ in range(3):
        assert v.has_value() is True
        v.reset()
        assert v.has_value() is False
        v.emplace()
        assert v.has_value() is True


@pytest.mark.skip(reason="Awaiting Phase 3 implementation")
def test_complex_type_nullable():
    """Nullable should work with complex types (bundles, lists)."""
    _skip_if_no_cpp()
    value = _hgraph.value

    v = value.Value.from_python({"x": [1, 2, 3], "y": {"nested": True}})
    assert v.has_value() is True

    v.reset()
    assert v.has_value() is False
    assert v.to_python() is None

    v.emplace()
    assert v.has_value() is True
    # Default should be empty structure
