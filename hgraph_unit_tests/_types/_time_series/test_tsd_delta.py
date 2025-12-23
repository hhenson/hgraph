"""
Tests for TSD (Time-Series Dict) delta tracking.

These tests verify the key addition/removal and delta tracking for TSD outputs
directly via the C++ Python bindings, without full graph execution.

Note: Many TSD delta operations are currently stubbed in C++ (marked TODO).
This test file documents the expected behavior and will help verify when
the implementation is complete.
"""
import pytest
from datetime import datetime, timedelta

from hgraph import _hgraph


# Skip all tests if TSOutput not available
pytestmark = pytest.mark.skipif(
    not hasattr(_hgraph, 'TSOutput'),
    reason="TSOutput not available in _hgraph module"
)


# Helper to create engine times
def make_time(microseconds: int):
    """Create an engine_time_t from microseconds since epoch."""
    return datetime(1970, 1, 1) + timedelta(microseconds=microseconds)


T0 = make_time(0)
T100 = make_time(100)
T200 = make_time(200)
T300 = make_time(300)


def create_tsd_str_int_output():
    """Helper to create a TSD[str, TS[int]] output."""
    str_meta = _hgraph.get_scalar_type_meta(str)
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsd_meta = _hgraph.get_tsd_type_meta(str_meta, ts_int_meta)
    return _hgraph.TSOutput(tsd_meta)


def create_tsd_int_str_output():
    """Helper to create a TSD[int, TS[str]] output."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    str_meta = _hgraph.get_scalar_type_meta(str)
    ts_str_meta = _hgraph.get_ts_type_meta(str_meta)
    tsd_meta = _hgraph.get_tsd_type_meta(int_meta, ts_str_meta)
    return _hgraph.TSOutput(tsd_meta)


# =============================================================================
# TSD Construction Tests
# =============================================================================


def test_tsd_construction():
    """Test constructing a TSD[str, TS[int]] output."""
    ts_output = create_tsd_str_int_output()

    assert ts_output.valid
    # ts_kind should indicate TSD
    assert ts_output.ts_kind is not None
    assert ts_output.kind == _hgraph.TypeKind.Dict


def test_tsd_initial_state():
    """Test that a new TSD has no value initially."""
    ts_output = create_tsd_str_int_output()

    assert not ts_output.has_value
    assert not ts_output.modified_at(T100)


def test_tsd_int_keys():
    """Test constructing a TSD[int, TS[str]] with integer keys."""
    ts_output = create_tsd_int_str_output()

    assert ts_output.valid
    assert ts_output.kind == _hgraph.TypeKind.Dict


# =============================================================================
# TSD Value Setting Tests
# =============================================================================


def test_tsd_set_value_dict():
    """Test setting a dict value on TSD."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2, "c": 3}, time=T100)

    assert ts_output.has_value
    assert ts_output.modified_at(T100)
    value = ts_output.py_value
    assert value == {"a": 1, "b": 2, "c": 3}


def test_tsd_set_empty():
    """Test setting an empty dict."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({}, time=T100)

    assert ts_output.has_value
    assert ts_output.py_value == {}


def test_tsd_dict_size():
    """Test dict_size property."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2, "c": 3}, time=T100)

    assert view.dict_size == 3

    # Empty dict
    view.set_value({}, time=T200)
    assert view.dict_size == 0


# =============================================================================
# TSD Modification Tracking Tests
# =============================================================================


def test_tsd_modification_tracking_basic():
    """Test modification tracking for TSD."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1}, time=T100)

    assert view.modified_at(T100)
    assert not view.modified_at(T200)
    assert view.last_modified_time == T100


def test_tsd_modification_tracking_update():
    """Test modification tracking updates on value change."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()

    view.set_value({"a": 1}, time=T100)
    assert view.last_modified_time == T100

    view.set_value({"a": 2, "b": 3}, time=T200)
    assert view.last_modified_time == T200
    assert view.modified_at(T200)


# =============================================================================
# TSD Entry Navigation Tests
# =============================================================================


@pytest.mark.xfail(reason="TSOutputView.entry() not exposed to Python")
def test_tsd_entry_navigation():
    """Test navigating to TSD entries via view.

    The C++ TSView.entry() method exists but is not exposed to Python
    via the TSOutputView binding.
    """
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2}, time=T100)

    # Navigate to entry 'a'
    entry_a = view.entry("a")
    assert entry_a.valid
    # The entry should have a value
    assert entry_a.has_value


@pytest.mark.xfail(reason="TSOutputView.entry() not exposed to Python")
def test_tsd_entry_not_found():
    """Test navigating to non-existent entry."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1}, time=T100)

    # Navigate to non-existent entry
    entry_z = view.entry("z")
    assert not entry_z.valid


# =============================================================================
# TSD Multiple Updates Tests
# =============================================================================


def test_tsd_multiple_updates():
    """Test multiple updates to a TSD."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()

    # First tick
    view.set_value({"a": 1, "b": 2}, time=T100)
    assert view.dict_size == 2

    # Second tick - add more and update existing
    view.set_value({"a": 10, "b": 2, "c": 3}, time=T200)
    assert view.dict_size == 3
    value = ts_output.py_value
    assert value["a"] == 10
    assert value["c"] == 3


def test_tsd_replace_all():
    """Test completely replacing TSD contents."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()

    view.set_value({"a": 1, "b": 2}, time=T100)
    assert view.dict_size == 2

    # Complete replacement
    view.set_value({"x": 100, "y": 200}, time=T200)
    assert view.dict_size == 2
    value = ts_output.py_value
    assert "a" not in value
    assert value["x"] == 100


# =============================================================================
# TSD Type Variant Tests
# =============================================================================


def test_tsd_int_keys_operations():
    """Test TSD with integer keys."""
    ts_output = create_tsd_int_str_output()

    view = ts_output.view()
    view.set_value({1: "a", 2: "b", 3: "c"}, time=T100)

    assert ts_output.has_value
    assert view.dict_size == 3
    value = ts_output.py_value
    assert value[1] == "a"
    assert value[3] == "c"


# =============================================================================
# TSD Edge Case Tests
# =============================================================================


def test_tsd_single_entry():
    """Test TSD with a single entry."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"only": 42}, time=T100)

    assert view.dict_size == 1
    assert ts_output.py_value == {"only": 42}


def test_tsd_large_dict():
    """Test TSD with many entries."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    large_dict = {f"key_{i}": i for i in range(100)}
    view.set_value(large_dict, time=T100)

    assert view.dict_size == 100
    value = ts_output.py_value
    assert value["key_0"] == 0
    assert value["key_99"] == 99


def test_tsd_mark_invalid():
    """Test marking TSD as invalid."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1}, time=T100)
    assert ts_output.has_value

    view.mark_invalid()
    assert not ts_output.has_value


# =============================================================================
# TSD Nested Types Tests
# =============================================================================


def test_tsd_with_bundle_value():
    """Test TSD with a bundle as value type."""
    str_meta = _hgraph.get_scalar_type_meta(str)
    int_meta = _hgraph.get_scalar_type_meta(int)
    float_meta = _hgraph.get_scalar_type_meta(float)

    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_float_meta = _hgraph.get_ts_type_meta(float_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_float_meta),
    ], "Point")

    tsd_meta = _hgraph.get_tsd_type_meta(str_meta, tsb_meta)
    ts_output = _hgraph.TSOutput(tsd_meta)

    assert ts_output.valid
    assert ts_output.kind == _hgraph.TypeKind.Dict


# =============================================================================
# TSD Integration Tests
# =============================================================================


def test_tsd_behavior_test_compatibility():
    """Verify TSD works the way behavior tests expect."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()

    # Initial set
    view.set_value({"a": 1, "b": 2}, time=T100)
    assert ts_output.has_value
    assert ts_output.py_value == {"a": 1, "b": 2}

    # Update - modify a, add c
    view.set_value({"a": 10, "b": 2, "c": 3}, time=T200)
    value = ts_output.py_value
    assert value["a"] == 10
    assert value["c"] == 3


# =============================================================================
# TSD Delta Tracking Tests (Documents expected behavior for TODO items)
# =============================================================================


@pytest.mark.xfail(reason="TSD delta tracking not fully implemented")
def test_tsd_added_keys_tracking():
    """Test that added keys are tracked.

    This test documents expected behavior once TSD delta tracking is complete.
    Currently, added_keys() is stubbed with TODO in the C++ code.
    """
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2}, time=T100)

    # Should return the keys that were added
    # This would require integrating DictModificationStorage
    added = list(view.added_keys(T100))
    assert "a" in added
    assert "b" in added


@pytest.mark.xfail(reason="TSD delta tracking not fully implemented")
def test_tsd_removed_keys_tracking():
    """Test that removed keys are tracked.

    This test documents expected behavior once TSD delta tracking is complete.
    Currently, removed_keys() is stubbed with TODO in the C++ code.
    """
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2, "c": 3}, time=T100)

    # Remove 'a' by not including it in new value
    view.set_value({"b": 2, "c": 3}, time=T200)

    # Should return the keys that were removed
    removed = list(view.removed_keys(T200))
    assert "a" in removed


@pytest.mark.xfail(reason="TSD delta tracking not fully implemented")
def test_tsd_modified_keys_via_view():
    """Test that modified keys can be queried via view.

    Documents expected behavior for per-entry modification tracking.
    """
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2}, time=T100)
    view.set_value({"a": 10, "b": 2}, time=T200)  # Only 'a' modified

    # Should return only 'a' as modified at T200
    modified = list(view.modified_keys(T200))
    assert "a" in modified
    assert "b" not in modified  # Value unchanged
