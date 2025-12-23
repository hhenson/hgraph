"""
Tests for TSS (Time-Series Set) delta tracking.

These tests verify the add/remove operations and delta tracking for TSS outputs
directly via the C++ Python bindings, without full graph execution.
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


def create_tss_int_output():
    """Helper to create a TSS[int] output."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    tss_meta = _hgraph.get_tss_type_meta(int_meta)
    return _hgraph.TSOutput(tss_meta)


def create_tss_str_output():
    """Helper to create a TSS[str] output."""
    str_meta = _hgraph.get_scalar_type_meta(str)
    tss_meta = _hgraph.get_tss_type_meta(str_meta)
    return _hgraph.TSOutput(tss_meta)


# =============================================================================
# TSS Construction Tests
# =============================================================================


def test_tss_construction():
    """Test constructing a TSS[int] output."""
    ts_output = create_tss_int_output()

    assert ts_output.valid
    # ts_kind is an integer enum value - TSS is typically 2
    assert ts_output.ts_kind is not None
    assert ts_output.kind == _hgraph.TypeKind.Set


def test_tss_initial_state():
    """Test that a new TSS has no value initially."""
    ts_output = create_tss_int_output()

    assert not ts_output.has_value
    assert not ts_output.modified_at(T100)


# =============================================================================
# TSS Value Setting Tests
# =============================================================================


def test_tss_set_value_frozenset():
    """Test setting a frozenset value on TSS."""
    ts_output = create_tss_int_output()

    view = ts_output.view()
    view.set_value(frozenset({1, 2, 3}), time=T100)

    assert ts_output.has_value
    assert ts_output.modified_at(T100)
    # The value should be the frozenset we set
    value = ts_output.py_value
    assert value == frozenset({1, 2, 3})


def test_tss_set_value_set():
    """Test setting a mutable set value on TSS (should convert to frozenset)."""
    ts_output = create_tss_int_output()

    view = ts_output.view()
    view.set_value({1, 2, 3}, time=T100)

    assert ts_output.has_value
    value = ts_output.py_value
    # Depending on implementation, may store as frozenset
    assert set(value) == {1, 2, 3}


def test_tss_set_empty():
    """Test setting an empty set."""
    ts_output = create_tss_int_output()

    view = ts_output.view()
    view.set_value(frozenset(), time=T100)

    assert ts_output.has_value
    assert ts_output.py_value == frozenset()


def test_tss_set_size():
    """Test set_size property."""
    ts_output = create_tss_int_output()

    view = ts_output.view()
    view.set_value(frozenset({1, 2, 3}), time=T100)

    assert view.set_size == 3

    # Empty set
    view.set_value(frozenset(), time=T200)
    assert view.set_size == 0


# =============================================================================
# TSS Modification Tracking Tests
# =============================================================================


def test_tss_modification_tracking_basic():
    """Test modification tracking for TSS."""
    ts_output = create_tss_int_output()

    view = ts_output.view()

    # Initial set
    view.set_value(frozenset({1, 2, 3}), time=T100)

    assert view.modified_at(T100)
    assert not view.modified_at(T200)
    assert view.last_modified_time == T100


def test_tss_modification_tracking_update():
    """Test modification tracking updates on value change."""
    ts_output = create_tss_int_output()

    view = ts_output.view()

    view.set_value(frozenset({1, 2}), time=T100)
    assert view.last_modified_time == T100

    view.set_value(frozenset({1, 2, 3}), time=T200)
    assert view.last_modified_time == T200
    assert view.modified_at(T200)
    assert not view.modified_at(T100)


# =============================================================================
# TSS String Type Tests
# =============================================================================


def test_tss_str_basic():
    """Test TSS[str] basic operations."""
    ts_output = create_tss_str_output()

    view = ts_output.view()
    view.set_value(frozenset({"a", "b", "c"}), time=T100)

    assert ts_output.has_value
    value = ts_output.py_value
    assert value == frozenset({"a", "b", "c"})


def test_tss_str_empty():
    """Test TSS[str] with empty set."""
    ts_output = create_tss_str_output()

    view = ts_output.view()
    view.set_value(frozenset(), time=T100)

    assert ts_output.has_value
    assert ts_output.py_value == frozenset()


# =============================================================================
# TSS Multiple Updates Tests
# =============================================================================


def test_tss_multiple_updates():
    """Test multiple updates to a TSS."""
    ts_output = create_tss_int_output()

    view = ts_output.view()

    # First tick
    view.set_value(frozenset({1, 2}), time=T100)
    assert view.set_size == 2

    # Second tick - add more
    view.set_value(frozenset({1, 2, 3, 4}), time=T200)
    assert view.set_size == 4

    # Third tick - remove some
    view.set_value(frozenset({3, 4, 5}), time=T300)
    assert view.set_size == 3
    assert set(ts_output.py_value) == {3, 4, 5}


def test_tss_replace_all():
    """Test completely replacing a TSS value."""
    ts_output = create_tss_int_output()

    view = ts_output.view()

    view.set_value(frozenset({1, 2, 3}), time=T100)
    assert view.set_size == 3

    # Complete replacement
    view.set_value(frozenset({10, 20}), time=T200)
    assert view.set_size == 2
    assert set(ts_output.py_value) == {10, 20}


# =============================================================================
# TSS Type Variant Tests
# =============================================================================


@pytest.mark.parametrize("scalar_type,values", [
    (int, frozenset({1, 2, 3})),
    (str, frozenset({"a", "b", "c"})),
    (float, frozenset({1.0, 2.0, 3.0})),
    (bool, frozenset({True, False})),
])
def test_tss_scalar_types(scalar_type, values):
    """Test TSS with various scalar types."""
    meta = _hgraph.get_scalar_type_meta(scalar_type)
    tss_meta = _hgraph.get_tss_type_meta(meta)
    ts_output = _hgraph.TSOutput(tss_meta)

    view = ts_output.view()
    view.set_value(values, time=T100)

    assert ts_output.has_value
    assert ts_output.py_value == values


# =============================================================================
# TSS Integration with Behavior Tests
# =============================================================================


def test_tss_behavior_test_compatibility():
    """Verify TSS works the way the behavior tests expect."""
    # This test documents the expected behavior for graph-level tests
    ts_output = create_tss_int_output()

    view = ts_output.view()

    # Initial set - all elements are "added"
    view.set_value(frozenset({1, 2, 3}), time=T100)
    assert ts_output.has_value
    assert set(ts_output.py_value) == {1, 2, 3}

    # Update - adds 4, removes 1
    view.set_value(frozenset({2, 3, 4}), time=T200)
    assert set(ts_output.py_value) == {2, 3, 4}


# =============================================================================
# Edge Case Tests
# =============================================================================


def test_tss_single_element():
    """Test TSS with a single element."""
    ts_output = create_tss_int_output()

    view = ts_output.view()
    view.set_value(frozenset({42}), time=T100)

    assert view.set_size == 1
    assert ts_output.py_value == frozenset({42})


def test_tss_large_set():
    """Test TSS with a large number of elements."""
    ts_output = create_tss_int_output()

    view = ts_output.view()
    large_set = frozenset(range(1000))
    view.set_value(large_set, time=T100)

    assert view.set_size == 1000
    assert ts_output.py_value == large_set


def test_tss_mark_invalid():
    """Test marking TSS as invalid."""
    ts_output = create_tss_int_output()

    view = ts_output.view()
    view.set_value(frozenset({1, 2, 3}), time=T100)
    assert ts_output.has_value

    view.mark_invalid()
    assert not ts_output.has_value
