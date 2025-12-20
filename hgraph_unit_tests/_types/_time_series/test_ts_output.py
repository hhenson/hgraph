"""
Tests for TSOutput and TSOutputView Python bindings.

These tests verify that TSOutput works correctly for testing without a Node.
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


# =============================================================================
# Construction Tests
# =============================================================================


def test_construction_ts_int():
    """Test constructing a TS[int] output."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)

    ts_output = _hgraph.TSOutput(ts_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TS
    assert ts_output.kind == _hgraph.TypeKind.Scalar
    assert "TS[int]" in ts_output.type_name


def test_construction_ts_float():
    """Test constructing a TS[float] output."""
    float_meta = _hgraph.get_scalar_type_meta(float)
    ts_meta = _hgraph.get_ts_type_meta(float_meta)

    ts_output = _hgraph.TSOutput(ts_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TS


def test_construction_ts_str():
    """Test constructing a TS[str] output."""
    str_meta = _hgraph.get_scalar_type_meta(str)
    ts_meta = _hgraph.get_ts_type_meta(str_meta)

    ts_output = _hgraph.TSOutput(ts_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TS


def test_construction_tss():
    """Test constructing a TSS[int] output."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    tss_meta = _hgraph.get_tss_type_meta(int_meta)

    ts_output = _hgraph.TSOutput(tss_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSS
    assert ts_output.kind == _hgraph.TypeKind.Set


def test_construction_tsb():
    """Test constructing a TSB[{x: TS[int], y: TS[float]}] output."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    float_meta = _hgraph.get_scalar_type_meta(float)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_float_meta = _hgraph.get_ts_type_meta(float_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_float_meta),
    ], "Point")

    ts_output = _hgraph.TSOutput(tsb_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSB
    assert ts_output.kind == _hgraph.TypeKind.Bundle


def test_construction_tsl():
    """Test constructing a TSL[TS[int], 3] output."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 3)

    ts_output = _hgraph.TSOutput(tsl_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSL
    assert ts_output.kind == _hgraph.TypeKind.List


# =============================================================================
# Initial State Tests
# =============================================================================


def test_initial_state():
    """Test that a new TSOutput has no value."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    assert not ts_output.has_value
    assert not ts_output.modified_at(T100)


# =============================================================================
# Value Setting Tests via View
# =============================================================================


def test_view_set_value_int():
    """Test setting an int value via view."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    view = ts_output.view()
    view.set_value(42, time=T100)

    assert ts_output.has_value
    assert ts_output.modified_at(T100)
    assert ts_output.py_value == 42


def test_view_set_value_float():
    """Test setting a float value via view."""
    float_meta = _hgraph.get_scalar_type_meta(float)
    ts_meta = _hgraph.get_ts_type_meta(float_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    view = ts_output.view()
    view.set_value(3.14, time=T100)

    assert ts_output.py_value == pytest.approx(3.14)


def test_view_set_value_str():
    """Test setting a string value via view."""
    str_meta = _hgraph.get_scalar_type_meta(str)
    ts_meta = _hgraph.get_ts_type_meta(str_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    view = ts_output.view()
    view.set_value("hello", time=T100)

    assert ts_output.py_value == "hello"


def test_view_modification_tracking():
    """Test modification tracking via view."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    view = ts_output.view()
    view.set_value(10, time=T100)

    assert view.modified_at(T100)
    assert not view.modified_at(T200)
    assert view.last_modified_time == T100

    view.set_value(20, time=T200)

    assert view.modified_at(T200)
    assert view.last_modified_time == T200


# =============================================================================
# Direct Value Access Tests
# =============================================================================


def test_direct_set_value():
    """Test setting value directly on TSOutput."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    ts_output.set_value(99, time=T100)

    assert ts_output.has_value
    assert ts_output.py_value == 99


# =============================================================================
# Path Tracking Tests
# =============================================================================


def test_view_path_root():
    """Test that root view has 'root' path."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    view = ts_output.view()

    assert view.path == "root"


def test_view_path_field():
    """Test that field navigation extends the path."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    float_meta = _hgraph.get_scalar_type_meta(float)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_float_meta = _hgraph.get_ts_type_meta(float_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_float_meta),
    ], "Point")

    ts_output = _hgraph.TSOutput(tsb_meta)

    field_view = ts_output.view().field(0)

    assert "field(0)" in field_view.path


def test_view_path_field_by_name():
    """Test that field navigation by name extends the path."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_int_meta),
    ], "Point")

    ts_output = _hgraph.TSOutput(tsb_meta)

    field_view = ts_output.view().field_by_name("x")

    assert '"x"' in field_view.path or "x" in field_view.path


# =============================================================================
# Bundle Field Navigation Tests
# =============================================================================


def test_bundle_field_access():
    """Test accessing bundle fields via view."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    float_meta = _hgraph.get_scalar_type_meta(float)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_float_meta = _hgraph.get_ts_type_meta(float_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_float_meta),
    ], "Point")

    ts_output = _hgraph.TSOutput(tsb_meta)

    # Navigate to field and set value
    x_view = ts_output.view().field(0)
    x_view.set_value(10, time=T100)

    y_view = ts_output.view().field(1)
    y_view.set_value(3.14, time=T100)

    # Verify values
    assert ts_output.view().field(0).py_value == 10
    assert ts_output.view().field(1).py_value == pytest.approx(3.14)


def test_bundle_field_count():
    """Test field_count property."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("a", ts_int_meta),
        ("b", ts_int_meta),
        ("c", ts_int_meta),
    ])

    ts_output = _hgraph.TSOutput(tsb_meta)

    assert ts_output.view().field_count == 3


def test_bundle_field_modification_tracking():
    """Test per-field modification tracking."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_int_meta),
    ], "Point")

    ts_output = _hgraph.TSOutput(tsb_meta)

    # Modify only field 0
    ts_output.view().field(0).set_value(10, time=T100)

    view = ts_output.view()
    assert view.field_modified_at(0, T100)
    # Field 1 was not modified at T100
    assert not view.field_modified_at(1, T100)


# =============================================================================
# List Element Navigation Tests
# =============================================================================


def test_list_element_access():
    """Test accessing list elements via view."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 3)

    ts_output = _hgraph.TSOutput(tsl_meta)

    # Set values for each element
    ts_output.view().element(0).set_value(10, time=T100)
    ts_output.view().element(1).set_value(20, time=T100)
    ts_output.view().element(2).set_value(30, time=T100)

    # Verify
    assert ts_output.view().element(0).py_value == 10
    assert ts_output.view().element(1).py_value == 20
    assert ts_output.view().element(2).py_value == 30


def test_list_size():
    """Test list_size property."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 5)

    ts_output = _hgraph.TSOutput(tsl_meta)

    assert ts_output.view().list_size == 5


# =============================================================================
# Set Operation Tests
# =============================================================================


def test_set_size():
    """Test set_size property."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    tss_meta = _hgraph.get_tss_type_meta(int_meta)

    ts_output = _hgraph.TSOutput(tss_meta)

    # Initial set is empty
    assert ts_output.view().set_size == 0


# =============================================================================
# String Representation Tests
# =============================================================================


def test_str_repr():
    """Test string representation."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    ts_output.set_value(42, time=T100)

    s = str(ts_output)
    r = repr(ts_output)

    assert "42" in s or "TSOutput" in r


def test_view_debug_string():
    """Test debug string for view."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    ts_output.set_value(42, time=T100)

    view = ts_output.view()
    debug = view.to_debug_string(T100)

    assert "TSOutputView" in debug
    assert "path=" in debug


# =============================================================================
# Mark Invalid Tests
# =============================================================================


def test_mark_invalid():
    """Test marking output as invalid."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    ts_output.set_value(42, time=T100)
    assert ts_output.has_value

    ts_output.mark_invalid()
    assert not ts_output.has_value


# =============================================================================
# Error Handling Tests
# =============================================================================


def test_field_on_non_bundle_raises():
    """Test that field() on non-bundle raises error."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    with pytest.raises(RuntimeError, match="Bundle"):
        ts_output.view().field(0)


def test_element_on_non_list_raises():
    """Test that element() on non-list raises error."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    with pytest.raises(RuntimeError, match="List"):
        ts_output.view().element(0)


def test_invalid_field_index_raises():
    """Test that invalid field index raises error."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
    ])

    ts_output = _hgraph.TSOutput(tsb_meta)

    with pytest.raises(RuntimeError, match="Invalid field index"):
        ts_output.view().field(99)


def test_invalid_element_index_raises():
    """Test that invalid element index raises error."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 3)

    ts_output = _hgraph.TSOutput(tsl_meta)

    with pytest.raises(RuntimeError, match="Invalid element index"):
        ts_output.view().element(99)
