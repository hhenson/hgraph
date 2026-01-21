"""
Tests for Phase 4: DeltaValue with explicit storage architecture.

These tests verify:
- DeltaValue can be created for different schema types
- SetDeltaView provides access to added/removed elements
- MapDeltaView provides access to added/updated/removed entries
- ListDeltaView provides access to updated items
- DeltaValue can be applied to target values
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
# DeltaValue creation tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_deltavalue_can_be_created_for_set():
    """DeltaValue should be creatable for a Set schema."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Get set TypeMeta
    set_meta = value.get_set_type_meta(value.get_scalar_type_meta(int))

    # Create DeltaValue
    delta = value.DeltaValue(set_meta)
    assert delta is not None


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_deltavalue_can_be_created_for_map():
    """DeltaValue should be creatable for a Map schema."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Get map TypeMeta
    key_meta = value.get_scalar_type_meta(str)
    value_meta = value.get_scalar_type_meta(int)
    map_meta = value.get_dict_type_meta(key_meta, value_meta)

    # Create DeltaValue
    delta = value.DeltaValue(map_meta)
    assert delta is not None


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_deltavalue_can_be_created_for_list():
    """DeltaValue should be creatable for a List schema."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Get list TypeMeta
    element_meta = value.get_scalar_type_meta(int)
    list_meta = value.get_dynamic_list_type_meta(element_meta)

    # Create DeltaValue
    delta = value.DeltaValue(list_meta)
    assert delta is not None


# =============================================================================
# DeltaValue basic properties tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_deltavalue_empty_property():
    """DeltaValue should have empty property."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = value.get_set_type_meta(value.get_scalar_type_meta(int))
    delta = value.DeltaValue(set_meta)

    assert hasattr(delta, 'empty')
    assert delta.empty is True  # Initially empty


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_deltavalue_change_count_property():
    """DeltaValue should have change_count property."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = value.get_set_type_meta(value.get_scalar_type_meta(int))
    delta = value.DeltaValue(set_meta)

    assert hasattr(delta, 'change_count')
    assert delta.change_count == 0  # Initially no changes


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_deltavalue_clear():
    """DeltaValue.clear() should reset the delta."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = value.get_set_type_meta(value.get_scalar_type_meta(int))
    delta = value.DeltaValue(set_meta)

    # Add some changes (API depends on implementation)
    # delta.add_added(...)

    delta.clear()
    assert delta.empty is True


# =============================================================================
# SetDeltaView tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_set_delta_view_added_is_iterable():
    """SetDeltaView.added() should return an iterable."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = value.get_set_type_meta(value.get_scalar_type_meta(int))
    delta = value.DeltaValue(set_meta)
    view = delta.const_view().as_set_delta()

    added = view.added()
    # Should be iterable (ViewRange)
    count = 0
    for elem in added:
        count += 1
    assert count >= 0  # Empty is fine


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_set_delta_view_removed_is_iterable():
    """SetDeltaView.removed() should return an iterable."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = value.get_set_type_meta(value.get_scalar_type_meta(int))
    delta = value.DeltaValue(set_meta)
    view = delta.const_view().as_set_delta()

    removed = view.removed()
    # Should be iterable
    for elem in removed:
        pass


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_set_delta_view_counts():
    """SetDeltaView should have added_count() and removed_count()."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = value.get_set_type_meta(value.get_scalar_type_meta(int))
    delta = value.DeltaValue(set_meta)
    view = delta.const_view().as_set_delta()

    assert hasattr(view, 'added_count')
    assert hasattr(view, 'removed_count')
    assert view.added_count() == 0
    assert view.removed_count() == 0


# =============================================================================
# MapDeltaView tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_map_delta_view_added_keys():
    """MapDeltaView.added_keys() should return iterable of keys."""
    _skip_if_no_cpp()
    value = _hgraph.value

    key_meta = value.get_scalar_type_meta(str)
    val_meta = value.get_scalar_type_meta(int)
    map_meta = value.get_dict_type_meta(key_meta, val_meta)
    delta = value.DeltaValue(map_meta)
    view = delta.const_view().as_map_delta()

    added_keys = view.added_keys()
    # Should be iterable (ViewRange)
    for key in added_keys:
        pass


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_map_delta_view_added_items():
    """MapDeltaView.added_items() should yield (key, value) tuples."""
    _skip_if_no_cpp()
    value = _hgraph.value

    key_meta = value.get_scalar_type_meta(str)
    val_meta = value.get_scalar_type_meta(int)
    map_meta = value.get_dict_type_meta(key_meta, val_meta)
    delta = value.DeltaValue(map_meta)
    view = delta.const_view().as_map_delta()

    added_items = view.added_items()
    # Should yield pairs (ViewPairRange)
    for key, val in added_items:
        assert key is not None
        assert val is not None


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_map_delta_view_updated_keys():
    """MapDeltaView.updated_keys() should return iterable of updated keys."""
    _skip_if_no_cpp()
    value = _hgraph.value

    key_meta = value.get_scalar_type_meta(str)
    val_meta = value.get_scalar_type_meta(int)
    map_meta = value.get_dict_type_meta(key_meta, val_meta)
    delta = value.DeltaValue(map_meta)
    view = delta.const_view().as_map_delta()

    assert hasattr(view, 'updated_keys')
    assert hasattr(view, 'updated_items')


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_map_delta_view_removed_keys():
    """MapDeltaView.removed_keys() should return iterable of removed keys."""
    _skip_if_no_cpp()
    value = _hgraph.value

    key_meta = value.get_scalar_type_meta(str)
    val_meta = value.get_scalar_type_meta(int)
    map_meta = value.get_dict_type_meta(key_meta, val_meta)
    delta = value.DeltaValue(map_meta)
    view = delta.const_view().as_map_delta()

    removed_keys = view.removed_keys()
    # Should be iterable
    for key in removed_keys:
        pass


# =============================================================================
# ListDeltaView tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_list_delta_view_updated_items():
    """ListDeltaView.updated_items() should yield (index, value) tuples."""
    _skip_if_no_cpp()
    value = _hgraph.value

    element_meta = value.get_scalar_type_meta(int)
    list_meta = value.get_dynamic_list_type_meta(element_meta)
    delta = value.DeltaValue(list_meta)
    view = delta.const_view().as_list_delta()

    updated_items = view.updated_items()
    # Should yield (index, value) pairs
    for idx, val in updated_items:
        assert isinstance(idx, int) or hasattr(idx, 'to_python')
        assert val is not None


# =============================================================================
# apply_to() tests
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_deltavalue_apply_to_set():
    """DeltaValue.apply_to() should modify a Set value."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # Create a set value
    set_meta = value.get_set_type_meta(value.get_scalar_type_meta(int))
    v = value.Value(set_meta)
    # Initialize with {1, 2, 3}
    v.from_python({1, 2, 3})

    # Create delta with additions and removals
    delta = value.DeltaValue(set_meta)
    # Add logic to populate delta (API depends on implementation)
    # delta.record_added(4)
    # delta.record_removed(1)

    # Apply delta
    delta.apply_to(v)

    # Verify result
    # result = v.to_python()
    # assert 4 in result
    # assert 1 not in result


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_deltavalue_apply_to_map():
    """DeltaValue.apply_to() should modify a Map value."""
    _skip_if_no_cpp()
    value = _hgraph.value

    key_meta = value.get_scalar_type_meta(str)
    val_meta = value.get_scalar_type_meta(int)
    map_meta = value.get_dict_type_meta(key_meta, val_meta)

    # Create map value
    v = value.Value(map_meta)
    v.from_python({"a": 1, "b": 2})

    # Create and apply delta
    delta = value.DeltaValue(map_meta)
    # delta.record_added("c", 3)
    # delta.record_updated("a", 10)
    # delta.record_removed("b")

    delta.apply_to(v)

    # result = v.to_python()
    # assert result["c"] == 3
    # assert result["a"] == 10
    # assert "b" not in result


@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_empty_delta_apply_is_noop():
    """Applying empty delta should not modify value."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = value.get_set_type_meta(value.get_scalar_type_meta(int))
    v = value.Value(set_meta)
    v.from_python({1, 2, 3})

    delta = value.DeltaValue(set_meta)
    assert delta.empty is True

    original = v.to_python()
    delta.apply_to(v)
    assert v.to_python() == original


# =============================================================================
# Integration with existing SetDeltaValue
# =============================================================================

@pytest.mark.skip(reason="Awaiting Phase 4 implementation")
def test_deltavalue_compatible_with_legacy():
    """New DeltaValue should be compatible with existing SetDeltaValue patterns."""
    _skip_if_no_cpp()
    value = _hgraph.value

    # The existing SetDeltaValue (in tracked_set.py tests) should work
    # alongside the new DeltaValue architecture
    pass
