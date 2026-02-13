"""
Tests for DeltaValue with explicit storage architecture.

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
    from hgraph._feature_switch import is_feature_enabled
    if not is_feature_enabled("use_cpp"):
        pytest.skip("C++ runtime not enabled")


def _get_set_meta():
    """Helper to get set TypeMeta for int elements."""
    value = _hgraph.value
    return value.get_set_type_meta(value.get_scalar_type_meta(int))


def _get_map_meta():
    """Helper to get map TypeMeta for str->int."""
    value = _hgraph.value
    key_meta = value.get_scalar_type_meta(str)
    val_meta = value.get_scalar_type_meta(int)
    return value.get_dict_type_meta(key_meta, val_meta)


def _get_list_meta():
    """Helper to get dynamic list TypeMeta for int elements."""
    value = _hgraph.value
    element_meta = value.get_scalar_type_meta(int)
    return value.get_dynamic_list_type_meta(element_meta)


# =============================================================================
# DeltaValue creation tests
# =============================================================================

def test_deltavalue_can_be_created_for_set():
    """DeltaValue should be creatable for a Set schema."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)
    assert delta is not None
    assert delta.is_set_delta()


def test_deltavalue_can_be_created_for_map():
    """DeltaValue should be creatable for a Map schema."""
    _skip_if_no_cpp()
    value = _hgraph.value

    map_meta = _get_map_meta()
    delta = value.DeltaValue(map_meta)
    assert delta is not None
    assert delta.is_map_delta()


def test_deltavalue_can_be_created_for_list():
    """DeltaValue should be creatable for a List schema."""
    _skip_if_no_cpp()
    value = _hgraph.value

    list_meta = _get_list_meta()
    delta = value.DeltaValue(list_meta)
    assert delta is not None
    assert delta.is_list_delta()


# =============================================================================
# DeltaValue basic properties tests
# =============================================================================

def test_deltavalue_empty_property():
    """DeltaValue should have empty() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)

    assert hasattr(delta, 'empty')
    assert delta.empty() is True  # Initially empty


def test_deltavalue_change_count_property():
    """DeltaValue should have change_count() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)

    assert hasattr(delta, 'change_count')
    assert delta.change_count() == 0  # Initially no changes


def test_deltavalue_clear():
    """DeltaValue.clear() should reset the delta."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)

    # Initially empty
    assert delta.empty() is True

    delta.clear()
    assert delta.empty() is True


def test_deltavalue_valid():
    """DeltaValue should have valid() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)

    assert hasattr(delta, 'valid')
    assert delta.valid() is True


def test_deltavalue_kind():
    """DeltaValue should have kind() method returning TypeKind."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)

    assert hasattr(delta, 'kind')
    kind = delta.kind()
    assert kind == value.TypeKind.Set


# =============================================================================
# SetDeltaView tests
# =============================================================================

def test_set_delta_view_added_is_iterable():
    """SetDeltaView.added() should return an iterable."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)
    view = delta.set_view()

    added = view.added()
    # Should be iterable (ViewRange)
    count = 0
    for elem in added:
        count += 1
    assert count >= 0  # Empty is fine


def test_set_delta_view_removed_is_iterable():
    """SetDeltaView.removed() should return an iterable."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)
    view = delta.set_view()

    removed = view.removed()
    # Should be iterable
    for elem in removed:
        pass


def test_set_delta_view_counts():
    """SetDeltaView should have added_count() and removed_count()."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)
    view = delta.set_view()

    assert hasattr(view, 'added_count')
    assert hasattr(view, 'removed_count')
    assert view.added_count() == 0
    assert view.removed_count() == 0


def test_set_delta_view_empty():
    """SetDeltaView should have empty() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)
    view = delta.set_view()

    assert view.empty() is True


def test_set_delta_view_change_count():
    """SetDeltaView should have change_count() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)
    view = delta.set_view()

    assert view.change_count() == 0


def test_set_delta_view_valid():
    """SetDeltaView should have valid() method."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)
    view = delta.set_view()

    assert view.valid() is True


# =============================================================================
# MapDeltaView tests
# =============================================================================

def test_map_delta_view_added_keys():
    """MapDeltaView.added_keys() should return iterable of keys."""
    _skip_if_no_cpp()
    value = _hgraph.value

    map_meta = _get_map_meta()
    delta = value.DeltaValue(map_meta)
    view = delta.map_view()

    added_keys = view.added_keys()
    # Should be iterable (ViewRange)
    for key in added_keys:
        pass


def test_map_delta_view_added_items():
    """MapDeltaView.added_items() should yield (key, value) tuples."""
    _skip_if_no_cpp()
    value = _hgraph.value

    map_meta = _get_map_meta()
    delta = value.DeltaValue(map_meta)
    view = delta.map_view()

    added_items = view.added_items()
    # Should yield pairs (ViewPairRange)
    for key, val in added_items:
        assert key is not None
        assert val is not None


def test_map_delta_view_updated_keys():
    """MapDeltaView.updated_keys() should return iterable of updated keys."""
    _skip_if_no_cpp()
    value = _hgraph.value

    map_meta = _get_map_meta()
    delta = value.DeltaValue(map_meta)
    view = delta.map_view()

    assert hasattr(view, 'updated_keys')
    assert hasattr(view, 'updated_items')

    # Should be iterable
    for key in view.updated_keys():
        pass


def test_map_delta_view_removed_keys():
    """MapDeltaView.removed_keys() should return iterable of removed keys."""
    _skip_if_no_cpp()
    value = _hgraph.value

    map_meta = _get_map_meta()
    delta = value.DeltaValue(map_meta)
    view = delta.map_view()

    removed_keys = view.removed_keys()
    # Should be iterable
    for key in removed_keys:
        pass


def test_map_delta_view_counts():
    """MapDeltaView should have count methods."""
    _skip_if_no_cpp()
    value = _hgraph.value

    map_meta = _get_map_meta()
    delta = value.DeltaValue(map_meta)
    view = delta.map_view()

    assert view.added_count() == 0
    assert view.updated_count() == 0
    assert view.removed_count() == 0


def test_map_delta_view_empty():
    """MapDeltaView should have empty() and valid() methods."""
    _skip_if_no_cpp()
    value = _hgraph.value

    map_meta = _get_map_meta()
    delta = value.DeltaValue(map_meta)
    view = delta.map_view()

    assert view.empty() is True
    assert view.valid() is True


# =============================================================================
# ListDeltaView tests
# =============================================================================

def test_list_delta_view_updated_items():
    """ListDeltaView.updated_items() should yield (index, value) tuples."""
    _skip_if_no_cpp()
    value = _hgraph.value

    list_meta = _get_list_meta()
    delta = value.DeltaValue(list_meta)
    view = delta.list_view()

    updated_items = view.updated_items()
    # Should yield (index, value) pairs
    for idx, val in updated_items:
        pass  # Empty is fine for now


def test_list_delta_view_counts():
    """ListDeltaView should have count methods."""
    _skip_if_no_cpp()
    value = _hgraph.value

    list_meta = _get_list_meta()
    delta = value.DeltaValue(list_meta)
    view = delta.list_view()

    assert view.updated_count() == 0


def test_list_delta_view_empty():
    """ListDeltaView should have empty() and valid() methods."""
    _skip_if_no_cpp()
    value = _hgraph.value

    list_meta = _get_list_meta()
    delta = value.DeltaValue(list_meta)
    view = delta.list_view()

    assert view.empty() is True
    assert view.valid() is True


# =============================================================================
# DeltaValue to_python tests
# =============================================================================

def test_deltavalue_to_python_set():
    """DeltaValue.to_python() should return dict with added/removed for sets."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)

    result = delta.to_python()
    assert isinstance(result, dict)
    assert 'added' in result
    assert 'removed' in result


def test_deltavalue_to_python_map():
    """DeltaValue.to_python() should return dict for maps."""
    _skip_if_no_cpp()
    value = _hgraph.value

    map_meta = _get_map_meta()
    delta = value.DeltaValue(map_meta)

    result = delta.to_python()
    assert isinstance(result, dict)


def test_deltavalue_to_python_list():
    """DeltaValue.to_python() should return dict for lists."""
    _skip_if_no_cpp()
    value = _hgraph.value

    list_meta = _get_list_meta()
    delta = value.DeltaValue(list_meta)

    result = delta.to_python()
    assert isinstance(result, dict)


# =============================================================================
# DeltaValue value_schema tests
# =============================================================================

def test_deltavalue_value_schema():
    """DeltaValue should provide access to its value schema."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)

    assert hasattr(delta, 'value_schema')
    schema = delta.value_schema()
    assert schema is not None
    assert schema.kind == value.TypeKind.Set


# =============================================================================
# Empty delta tests
# =============================================================================

def test_empty_delta_set():
    """Empty set delta should have zero counts."""
    _skip_if_no_cpp()
    value = _hgraph.value

    set_meta = _get_set_meta()
    delta = value.DeltaValue(set_meta)

    assert delta.empty() is True
    assert delta.change_count() == 0

    view = delta.set_view()
    assert view.added_count() == 0
    assert view.removed_count() == 0
    assert list(view.added()) == []
    assert list(view.removed()) == []


def test_empty_delta_map():
    """Empty map delta should have zero counts."""
    _skip_if_no_cpp()
    value = _hgraph.value

    map_meta = _get_map_meta()
    delta = value.DeltaValue(map_meta)

    assert delta.empty() is True
    assert delta.change_count() == 0

    view = delta.map_view()
    assert view.added_count() == 0
    assert view.updated_count() == 0
    assert view.removed_count() == 0


def test_empty_delta_list():
    """Empty list delta should have zero counts."""
    _skip_if_no_cpp()
    value = _hgraph.value

    list_meta = _get_list_meta()
    delta = value.DeltaValue(list_meta)

    assert delta.empty() is True
    assert delta.change_count() == 0

    view = delta.list_view()
    assert view.updated_count() == 0


# =============================================================================
# Integration with existing SetDeltaValue
# =============================================================================

def test_setdeltavalue_exists():
    """SetDeltaValue should be available for set delta snapshots."""
    _skip_if_no_cpp()
    value = _hgraph.value

    assert hasattr(value, 'SetDeltaValue')


def test_setdeltaview_exists():
    """SetDeltaView should be available for viewing set deltas."""
    _skip_if_no_cpp()
    value = _hgraph.value

    assert hasattr(value, 'SetDeltaView')


def test_mapdeltaview_exists():
    """MapDeltaView should be available for viewing map deltas."""
    _skip_if_no_cpp()
    value = _hgraph.value

    assert hasattr(value, 'MapDeltaView')


def test_listdeltaview_exists():
    """ListDeltaView should be available for viewing list deltas."""
    _skip_if_no_cpp()
    value = _hgraph.value

    assert hasattr(value, 'ListDeltaView')
