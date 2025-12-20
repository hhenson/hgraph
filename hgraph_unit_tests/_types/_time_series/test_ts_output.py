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


# =============================================================================
# Additional Scalar Type Tests
# =============================================================================


def test_construction_ts_bool():
    """Test constructing a TS[bool] output."""
    bool_meta = _hgraph.get_scalar_type_meta(bool)
    ts_meta = _hgraph.get_ts_type_meta(bool_meta)

    ts_output = _hgraph.TSOutput(ts_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TS


def test_view_set_value_bool():
    """Test setting a bool value via view."""
    bool_meta = _hgraph.get_scalar_type_meta(bool)
    ts_meta = _hgraph.get_ts_type_meta(bool_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    view = ts_output.view()
    view.set_value(True, time=T100)

    assert ts_output.py_value is True

    view.set_value(False, time=T200)
    assert ts_output.py_value is False


def test_construction_ts_datetime():
    """Test constructing a TS[datetime] output."""
    from datetime import datetime
    datetime_meta = _hgraph.get_scalar_type_meta(datetime)
    ts_meta = _hgraph.get_ts_type_meta(datetime_meta)

    ts_output = _hgraph.TSOutput(ts_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TS


def test_view_set_value_datetime():
    """Test setting a datetime value via view."""
    from datetime import datetime
    datetime_meta = _hgraph.get_scalar_type_meta(datetime)
    ts_meta = _hgraph.get_ts_type_meta(datetime_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    test_dt = datetime(2025, 12, 20, 10, 30, 0)
    view = ts_output.view()
    view.set_value(test_dt, time=T100)

    assert ts_output.py_value == test_dt


def test_construction_ts_date():
    """Test constructing a TS[date] output."""
    from datetime import date
    date_meta = _hgraph.get_scalar_type_meta(date)
    ts_meta = _hgraph.get_ts_type_meta(date_meta)

    ts_output = _hgraph.TSOutput(ts_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TS


def test_view_set_value_date():
    """Test setting a date value via view."""
    from datetime import date
    date_meta = _hgraph.get_scalar_type_meta(date)
    ts_meta = _hgraph.get_ts_type_meta(date_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    test_date = date(2025, 12, 20)
    view = ts_output.view()
    view.set_value(test_date, time=T100)

    assert ts_output.py_value == test_date


# =============================================================================
# Value Update Tests
# =============================================================================


def test_value_overwrite():
    """Test that setting a value overwrites the previous value."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    ts_output.set_value(10, time=T100)
    assert ts_output.py_value == 10

    ts_output.set_value(20, time=T100)
    assert ts_output.py_value == 20

    ts_output.set_value(30, time=T200)
    assert ts_output.py_value == 30


def test_multiple_time_modifications():
    """Test modification tracking across multiple times."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    ts_output.set_value(10, time=T100)
    ts_output.set_value(20, time=T200)
    ts_output.set_value(30, time=T300)

    # Should only be modified at the last time
    assert not ts_output.modified_at(T100)
    assert not ts_output.modified_at(T200)
    assert ts_output.modified_at(T300)
    assert ts_output.last_modified_time == T300


# =============================================================================
# Nested Type Tests
# =============================================================================


def test_nested_bundle():
    """Test TSB with nested TSB fields."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    # Inner bundle: {x: TS[int], y: TS[int]}
    inner_tsb_meta = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_int_meta),
    ], "Point")

    # Outer bundle: {origin: Point, destination: Point}
    outer_tsb_meta = _hgraph.get_tsb_type_meta([
        ("origin", inner_tsb_meta),
        ("destination", inner_tsb_meta),
    ], "Route")

    ts_output = _hgraph.TSOutput(outer_tsb_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSB
    assert ts_output.view().field_count == 2

    # Navigate to nested field
    origin_view = ts_output.view().field(0)
    assert origin_view.field_count == 2

    # Set value in deeply nested field
    origin_view.field(0).set_value(100, time=T100)
    origin_view.field(1).set_value(200, time=T100)

    # Verify values
    assert ts_output.view().field(0).field(0).py_value == 100
    assert ts_output.view().field(0).field(1).py_value == 200


def test_list_of_bundles():
    """Test TSL containing TSB elements."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    # Element type: TSB{value: TS[int]}
    elem_tsb_meta = _hgraph.get_tsb_type_meta([
        ("value", ts_int_meta),
    ], "Item")

    # List of 3 Items
    tsl_meta = _hgraph.get_tsl_type_meta(elem_tsb_meta, 3)

    ts_output = _hgraph.TSOutput(tsl_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSL
    assert ts_output.view().list_size == 3

    # Set values in each element's bundle field
    for i in range(3):
        elem_view = ts_output.view().element(i)
        elem_view.field(0).set_value((i + 1) * 10, time=T100)

    # Verify
    for i in range(3):
        assert ts_output.view().element(i).field(0).py_value == (i + 1) * 10


def test_bundle_of_lists():
    """Test TSB containing TSL fields."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 2)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("first_list", tsl_meta),
        ("second_list", tsl_meta),
    ], "TwoLists")

    ts_output = _hgraph.TSOutput(tsb_meta)

    assert ts_output.valid
    assert ts_output.view().field_count == 2

    # Navigate and set values
    first_list = ts_output.view().field(0)
    assert first_list.list_size == 2

    first_list.element(0).set_value(10, time=T100)
    first_list.element(1).set_value(20, time=T100)

    # Verify
    assert ts_output.view().field(0).element(0).py_value == 10
    assert ts_output.view().field(0).element(1).py_value == 20


# =============================================================================
# Path Tracking Extended Tests
# =============================================================================


def test_nested_path_tracking():
    """Test path tracking through nested navigation."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    inner_tsb = _hgraph.get_tsb_type_meta([("value", ts_int_meta)])
    outer_tsb = _hgraph.get_tsb_type_meta([("inner", inner_tsb)])

    ts_output = _hgraph.TSOutput(outer_tsb)

    deep_view = ts_output.view().field(0).field(0)
    path = deep_view.path

    # Path should contain both navigation steps
    assert "field(0)" in path


def test_list_element_path_tracking():
    """Test path tracking for list element navigation."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 3)

    ts_output = _hgraph.TSOutput(tsl_meta)

    elem_view = ts_output.view().element(1)
    path = elem_view.path

    assert "element(1)" in path


# =============================================================================
# Observer Tests
# =============================================================================


def test_has_observers_initially_false():
    """Test that new TSOutput has no observers."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)
    ts_output = _hgraph.TSOutput(ts_meta)

    assert not ts_output.has_observers


# =============================================================================
# TSW (Window) Tests
# =============================================================================


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_tsw_type_meta'),
    reason="TSW type meta not available"
)
def test_construction_tsw():
    """Test constructing a TSW[float, 10] output."""
    float_meta = _hgraph.get_scalar_type_meta(float)
    tsw_meta = _hgraph.get_tsw_type_meta(float_meta, 10, 0)

    ts_output = _hgraph.TSOutput(tsw_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSW


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_tsw_type_meta'),
    reason="TSW type meta not available"
)
def test_window_initial_state():
    """Test initial window state."""
    float_meta = _hgraph.get_scalar_type_meta(float)
    tsw_meta = _hgraph.get_tsw_type_meta(float_meta, 10, 0)

    ts_output = _hgraph.TSOutput(tsw_meta)
    view = ts_output.view()

    assert view.window_size == 0
    assert view.window_empty


# =============================================================================
# REF (Reference) Tests
# =============================================================================


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_ref_type_meta'),
    reason="REF type meta not available"
)
def test_construction_ref():
    """Test constructing a REF[TS[int]] output."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    ref_meta = _hgraph.get_ref_type_meta(ts_int_meta)

    ts_output = _hgraph.TSOutput(ref_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.REF


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_ref_type_meta'),
    reason="REF type meta not available"
)
def test_ref_initial_state():
    """Test initial ref state."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    ref_meta = _hgraph.get_ref_type_meta(ts_int_meta)

    ts_output = _hgraph.TSOutput(ref_meta)
    view = ts_output.view()

    assert view.ref_is_empty


# =============================================================================
# TSD (Dict) Tests
# =============================================================================


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_tsd_type_meta'),
    reason="TSD type meta not available"
)
def test_construction_tsd():
    """Test constructing a TSD[int, TS[float]] output."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    float_meta = _hgraph.get_scalar_type_meta(float)
    ts_float_meta = _hgraph.get_ts_type_meta(float_meta)
    tsd_meta = _hgraph.get_tsd_type_meta(int_meta, ts_float_meta)

    ts_output = _hgraph.TSOutput(tsd_meta)

    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSD


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_tsd_type_meta'),
    reason="TSD type meta not available"
)
def test_dict_initial_state():
    """Test initial dict state."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    float_meta = _hgraph.get_scalar_type_meta(float)
    ts_float_meta = _hgraph.get_ts_type_meta(float_meta)
    tsd_meta = _hgraph.get_tsd_type_meta(int_meta, ts_float_meta)

    ts_output = _hgraph.TSOutput(tsd_meta)
    view = ts_output.view()

    assert view.dict_size == 0


# =============================================================================
# Large Structure Tests
# =============================================================================


def test_large_bundle():
    """Test bundle with many fields."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    fields = [(f"field_{i}", ts_int_meta) for i in range(10)]
    tsb_meta = _hgraph.get_tsb_type_meta(fields, "LargeBundle")

    ts_output = _hgraph.TSOutput(tsb_meta)

    assert ts_output.valid
    assert ts_output.view().field_count == 10

    # Set all fields
    for i in range(10):
        ts_output.view().field(i).set_value(i * 100, time=T100)

    # Verify all fields
    for i in range(10):
        assert ts_output.view().field(i).py_value == i * 100


def test_large_list():
    """Test list with many elements."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 20)

    ts_output = _hgraph.TSOutput(tsl_meta)

    assert ts_output.valid
    assert ts_output.view().list_size == 20

    # Set all elements
    for i in range(20):
        ts_output.view().element(i).set_value(i, time=T100)

    # Verify all elements
    for i in range(20):
        assert ts_output.view().element(i).py_value == i


# =============================================================================
# Comprehensive Type Construction Tests
# =============================================================================


@pytest.mark.parametrize("scalar_type", [int, float, str, bool])
def test_ts_all_scalar_types(scalar_type):
    """Test TS with all supported scalar types."""
    meta = _hgraph.get_scalar_type_meta(scalar_type)
    ts_meta = _hgraph.get_ts_type_meta(meta)
    ts_output = _hgraph.TSOutput(ts_meta)
    assert ts_output.valid, f"TS[{scalar_type.__name__}] should be valid"
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TS


def test_ts_datetime_scalar():
    """Test TS with datetime scalar type."""
    from datetime import datetime
    meta = _hgraph.get_scalar_type_meta(datetime)
    ts_meta = _hgraph.get_ts_type_meta(meta)
    ts_output = _hgraph.TSOutput(ts_meta)
    assert ts_output.valid


def test_ts_date_scalar():
    """Test TS with date scalar type."""
    from datetime import date
    meta = _hgraph.get_scalar_type_meta(date)
    ts_meta = _hgraph.get_ts_type_meta(meta)
    ts_output = _hgraph.TSOutput(ts_meta)
    assert ts_output.valid


@pytest.mark.parametrize("scalar_type", [int, float, str])
def test_tss_all_scalar_types(scalar_type):
    """Test TSS with all supported scalar types."""
    meta = _hgraph.get_scalar_type_meta(scalar_type)
    tss_meta = _hgraph.get_tss_type_meta(meta)
    ts_output = _hgraph.TSOutput(tss_meta)
    assert ts_output.valid, f"TSS[{scalar_type.__name__}] should be valid"
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSS


@pytest.mark.parametrize("size", [1, 2, 5, 10, 100])
def test_tsl_various_sizes(size):
    """Test TSL with various sizes."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, size)
    ts_output = _hgraph.TSOutput(tsl_meta)
    assert ts_output.valid, f"TSL[TS[int], {size}] should be valid"
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSL
    assert ts_output.view().list_size == size


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_tsw_type_meta'),
    reason="TSW type meta not available"
)
@pytest.mark.parametrize("size", [1, 5, 10, 100])
def test_tsw_various_sizes(size):
    """Test TSW with various window sizes."""
    float_meta = _hgraph.get_scalar_type_meta(float)
    tsw_meta = _hgraph.get_tsw_type_meta(float_meta, size, 0)
    ts_output = _hgraph.TSOutput(tsw_meta)
    assert ts_output.valid, f"TSW[float, {size}] should be valid"
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSW


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_ref_type_meta'),
    reason="REF type meta not available"
)
def test_ref_ts_inner_type():
    """Test REF[TS[int]]."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    ref_ts_meta = _hgraph.get_ref_type_meta(ts_int_meta)
    ts_output = _hgraph.TSOutput(ref_ts_meta)
    assert ts_output.valid
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.REF


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_ref_type_meta'),
    reason="REF type meta not available"
)
def test_ref_tss_inner_type():
    """Test REF[TSS[int]]."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    tss_int_meta = _hgraph.get_tss_type_meta(int_meta)
    ref_tss_meta = _hgraph.get_ref_type_meta(tss_int_meta)
    ts_output = _hgraph.TSOutput(ref_tss_meta)
    assert ts_output.valid


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_tsd_type_meta'),
    reason="TSD type meta not available"
)
@pytest.mark.parametrize("key_type", [int, str])
def test_tsd_various_key_types(key_type):
    """Test TSD with various key types."""
    float_meta = _hgraph.get_scalar_type_meta(float)
    ts_float_meta = _hgraph.get_ts_type_meta(float_meta)
    key_meta = _hgraph.get_scalar_type_meta(key_type)
    tsd_meta = _hgraph.get_tsd_type_meta(key_meta, ts_float_meta)
    ts_output = _hgraph.TSOutput(tsd_meta)
    assert ts_output.valid, f"TSD[{key_type.__name__}, TS[float]] should be valid"
    assert ts_output.ts_kind == _hgraph.TimeSeriesKind.TSD


# =============================================================================
# Nested Type Tests - All Combinations
# =============================================================================


def test_nested_tsb_containing_ts():
    """Test TSB containing TS fields."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    float_meta = _hgraph.get_scalar_type_meta(float)
    str_meta = _hgraph.get_scalar_type_meta(str)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("a", _hgraph.get_ts_type_meta(int_meta)),
        ("b", _hgraph.get_ts_type_meta(float_meta)),
        ("c", _hgraph.get_ts_type_meta(str_meta)),
    ], "Mixed")

    ts_output = _hgraph.TSOutput(tsb_meta)
    assert ts_output.valid
    assert ts_output.view().field_count == 3

    ts_output.view().field(0).set_value(42, time=T100)
    ts_output.view().field(1).set_value(3.14, time=T100)
    ts_output.view().field(2).set_value("hello", time=T100)

    assert ts_output.view().field(0).py_value == 42
    assert ts_output.view().field(1).py_value == pytest.approx(3.14)
    assert ts_output.view().field(2).py_value == "hello"


def test_nested_tsb_containing_tss():
    """Test TSB containing TSS fields."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    tss_meta = _hgraph.get_tss_type_meta(int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("set1", tss_meta),
        ("set2", tss_meta),
    ], "TwoSets")

    ts_output = _hgraph.TSOutput(tsb_meta)
    assert ts_output.valid
    assert ts_output.view().field_count == 2
    assert ts_output.view().field(0).set_size == 0


def test_nested_tsb_containing_tsl():
    """Test TSB containing TSL fields."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 3)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("list1", tsl_meta),
        ("list2", tsl_meta),
    ], "TwoLists")

    ts_output = _hgraph.TSOutput(tsb_meta)
    assert ts_output.valid
    assert ts_output.view().field_count == 2
    assert ts_output.view().field(0).list_size == 3


def test_nested_tsb_containing_tsb():
    """Test TSB containing nested TSB fields."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    inner_tsb = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_int_meta),
    ], "Point")

    outer_tsb = _hgraph.get_tsb_type_meta([
        ("start", inner_tsb),
        ("end", inner_tsb),
    ], "Line")

    ts_output = _hgraph.TSOutput(outer_tsb)
    assert ts_output.valid
    assert ts_output.view().field_count == 2
    assert ts_output.view().field(0).field_count == 2


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_tsd_type_meta'),
    reason="TSD type meta not available"
)
def test_nested_tsb_containing_tsd():
    """Test TSB containing TSD fields."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    float_meta = _hgraph.get_scalar_type_meta(float)
    ts_float_meta = _hgraph.get_ts_type_meta(float_meta)
    tsd_meta = _hgraph.get_tsd_type_meta(int_meta, ts_float_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("prices", tsd_meta),
        ("quantities", tsd_meta),
    ], "OrderBook")

    ts_output = _hgraph.TSOutput(tsb_meta)
    assert ts_output.valid
    assert ts_output.view().field_count == 2
    assert ts_output.view().field(0).dict_size == 0


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_tsw_type_meta'),
    reason="TSW type meta not available"
)
def test_nested_tsb_containing_tsw():
    """Test TSB containing TSW fields."""
    float_meta = _hgraph.get_scalar_type_meta(float)
    tsw_meta = _hgraph.get_tsw_type_meta(float_meta, 10, 0)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("recent_prices", tsw_meta),
        ("recent_volumes", tsw_meta),
    ], "MarketData")

    ts_output = _hgraph.TSOutput(tsb_meta)
    assert ts_output.valid
    assert ts_output.view().field_count == 2
    assert ts_output.view().field(0).window_size == 0


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_ref_type_meta'),
    reason="REF type meta not available"
)
def test_nested_tsb_containing_ref():
    """Test TSB containing REF fields."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    ref_meta = _hgraph.get_ref_type_meta(ts_int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("ref1", ref_meta),
        ("ref2", ref_meta),
    ], "TwoRefs")

    ts_output = _hgraph.TSOutput(tsb_meta)
    assert ts_output.valid
    assert ts_output.view().field_count == 2
    assert ts_output.view().field(0).ref_is_empty


def test_nested_tsl_containing_ts():
    """Test TSL containing TS elements."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 5)

    ts_output = _hgraph.TSOutput(tsl_meta)
    assert ts_output.valid
    assert ts_output.view().list_size == 5

    for i in range(5):
        ts_output.view().element(i).set_value(i * 10, time=T100)

    for i in range(5):
        assert ts_output.view().element(i).py_value == i * 10


def test_nested_tsl_containing_tss():
    """Test TSL containing TSS elements."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    tss_meta = _hgraph.get_tss_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(tss_meta, 3)

    ts_output = _hgraph.TSOutput(tsl_meta)
    assert ts_output.valid
    assert ts_output.view().list_size == 3

    for i in range(3):
        elem = ts_output.view().element(i)
        assert elem.valid
        assert elem.set_size == 0


def test_nested_tsl_containing_tsb():
    """Test TSL containing TSB elements."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    elem_tsb = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_int_meta),
    ], "Point")

    tsl_meta = _hgraph.get_tsl_type_meta(elem_tsb, 4)

    ts_output = _hgraph.TSOutput(tsl_meta)
    assert ts_output.valid
    assert ts_output.view().list_size == 4

    for i in range(4):
        elem = ts_output.view().element(i)
        assert elem.field_count == 2
        elem.field(0).set_value(i, time=T100)
        elem.field(1).set_value(i * 2, time=T100)

    for i in range(4):
        elem = ts_output.view().element(i)
        assert elem.field(0).py_value == i
        assert elem.field(1).py_value == i * 2


def test_nested_tsl_containing_tsl():
    """Test TSL containing nested TSL elements (2D list)."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    inner_tsl = _hgraph.get_tsl_type_meta(ts_int_meta, 3)
    outer_tsl = _hgraph.get_tsl_type_meta(inner_tsl, 2)

    ts_output = _hgraph.TSOutput(outer_tsl)
    assert ts_output.valid
    assert ts_output.view().list_size == 2
    assert ts_output.view().element(0).list_size == 3

    for i in range(2):
        for j in range(3):
            ts_output.view().element(i).element(j).set_value(i * 10 + j, time=T100)

    for i in range(2):
        for j in range(3):
            assert ts_output.view().element(i).element(j).py_value == i * 10 + j


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_tsd_type_meta'),
    reason="TSD type meta not available"
)
def test_nested_tsl_containing_tsd():
    """Test TSL containing TSD elements."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    float_meta = _hgraph.get_scalar_type_meta(float)
    ts_float_meta = _hgraph.get_ts_type_meta(float_meta)
    tsd_meta = _hgraph.get_tsd_type_meta(int_meta, ts_float_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(tsd_meta, 2)

    ts_output = _hgraph.TSOutput(tsl_meta)
    assert ts_output.valid
    assert ts_output.view().list_size == 2
    assert ts_output.view().element(0).dict_size == 0


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_tsw_type_meta'),
    reason="TSW type meta not available"
)
def test_nested_tsl_containing_tsw():
    """Test TSL containing TSW elements."""
    float_meta = _hgraph.get_scalar_type_meta(float)
    tsw_meta = _hgraph.get_tsw_type_meta(float_meta, 5, 0)
    tsl_meta = _hgraph.get_tsl_type_meta(tsw_meta, 3)

    ts_output = _hgraph.TSOutput(tsl_meta)
    assert ts_output.valid
    assert ts_output.view().list_size == 3
    assert ts_output.view().element(0).window_size == 0


@pytest.mark.skipif(
    not hasattr(_hgraph, 'get_ref_type_meta'),
    reason="REF type meta not available"
)
def test_nested_tsl_containing_ref():
    """Test TSL containing REF elements."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    ref_meta = _hgraph.get_ref_type_meta(ts_int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ref_meta, 3)

    ts_output = _hgraph.TSOutput(tsl_meta)
    assert ts_output.valid
    assert ts_output.view().list_size == 3
    assert ts_output.view().element(0).ref_is_empty


def test_three_level_nesting_tsb_tsl_tsb():
    """Test three-level nesting: TSB -> TSL -> TSB."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    inner_tsb = _hgraph.get_tsb_type_meta([("value", ts_int_meta)], "Item")
    tsl_of_tsb = _hgraph.get_tsl_type_meta(inner_tsb, 2)
    outer_tsb = _hgraph.get_tsb_type_meta([
        ("items", tsl_of_tsb),
        ("count", ts_int_meta),
    ], "Container")

    ts_output = _hgraph.TSOutput(outer_tsb)
    assert ts_output.valid

    ts_output.view().field(0).element(0).field(0).set_value(42, time=T100)
    ts_output.view().field(0).element(1).field(0).set_value(99, time=T100)
    ts_output.view().field(1).set_value(2, time=T100)

    assert ts_output.view().field(0).element(0).field(0).py_value == 42
    assert ts_output.view().field(0).element(1).field(0).py_value == 99
    assert ts_output.view().field(1).py_value == 2


def test_three_level_nesting_tsl_tsb_tsl():
    """Test three-level nesting: TSL -> TSB -> TSL."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    inner_tsl = _hgraph.get_tsl_type_meta(ts_int_meta, 2)
    middle_tsb = _hgraph.get_tsb_type_meta([("data", inner_tsl)], "DataHolder")
    outer_tsl = _hgraph.get_tsl_type_meta(middle_tsb, 2)

    ts_output = _hgraph.TSOutput(outer_tsl)
    assert ts_output.valid

    for i in range(2):
        for j in range(2):
            ts_output.view().element(i).field(0).element(j).set_value(i * 10 + j, time=T100)

    for i in range(2):
        for j in range(2):
            assert ts_output.view().element(i).field(0).element(j).py_value == i * 10 + j


def test_four_level_nesting():
    """Test four-level nesting: TSB -> TSL -> TSB -> TSL."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    level4_tsl = _hgraph.get_tsl_type_meta(ts_int_meta, 2)
    level3_tsb = _hgraph.get_tsb_type_meta([("values", level4_tsl)], "Level3")
    level2_tsl = _hgraph.get_tsl_type_meta(level3_tsb, 2)
    level1_tsb = _hgraph.get_tsb_type_meta([("data", level2_tsl)], "Level1")

    ts_output = _hgraph.TSOutput(level1_tsb)
    assert ts_output.valid

    ts_output.view().field(0).element(0).field(0).element(0).set_value(1234, time=T100)
    assert ts_output.view().field(0).element(0).field(0).element(0).py_value == 1234


# =============================================================================
# Modification Tracking Tests - Isolated Path Notifications
# =============================================================================


def test_modification_tsb_only_modified_field_notified():
    """Test that modifying one field doesn't mark siblings as modified."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("a", ts_int_meta),
        ("b", ts_int_meta),
        ("c", ts_int_meta),
    ], "ThreeFields")

    ts_output = _hgraph.TSOutput(tsb_meta)

    # Modify only field 1 at T100
    ts_output.view().field(1).set_value(42, time=T100)

    view = ts_output.view()

    # Field 1 should be modified
    assert view.field_modified_at(1, T100), "Field 1 should be modified at T100"

    # Fields 0 and 2 should NOT be modified
    assert not view.field_modified_at(0, T100), "Field 0 should NOT be modified at T100"
    assert not view.field_modified_at(2, T100), "Field 2 should NOT be modified at T100"


def test_modification_tsl_only_modified_element_notified():
    """Test that modifying one element doesn't mark siblings as modified."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 5)

    ts_output = _hgraph.TSOutput(tsl_meta)

    # Modify only element 2 at T100
    ts_output.view().element(2).set_value(42, time=T100)

    view = ts_output.view()

    # Element 2 should be modified
    assert view.element_modified_at(2, T100), "Element 2 should be modified at T100"

    # Other elements should NOT be modified
    for i in [0, 1, 3, 4]:
        assert not view.element_modified_at(i, T100), f"Element {i} should NOT be modified at T100"


@pytest.mark.xfail(reason="Modification tracking doesn't isolate sibling fields correctly")
def test_modification_nested_tsb_only_modified_path_notified():
    """Test nested TSB modification tracking."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    inner_tsb = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_int_meta),
    ], "Point")

    outer_tsb = _hgraph.get_tsb_type_meta([
        ("origin", inner_tsb),
        ("destination", inner_tsb),
    ], "Route")

    ts_output = _hgraph.TSOutput(outer_tsb)

    # Modify only origin.x
    ts_output.view().field(0).field(0).set_value(100, time=T100)

    # Check modification at outer level
    assert ts_output.view().field_modified_at(0, T100), "origin should be modified"
    assert not ts_output.view().field_modified_at(1, T100), "destination should NOT be modified"

    # Check modification at inner level (origin)
    origin = ts_output.view().field(0)
    assert origin.field_modified_at(0, T100), "origin.x should be modified"
    assert not origin.field_modified_at(1, T100), "origin.y should NOT be modified"

    # Check destination is completely unmodified
    destination = ts_output.view().field(1)
    assert not destination.field_modified_at(0, T100), "destination.x should NOT be modified"
    assert not destination.field_modified_at(1, T100), "destination.y should NOT be modified"


@pytest.mark.xfail(reason="Modification tracking doesn't isolate sibling elements correctly")
def test_modification_tsl_of_tsb_only_modified_path_notified():
    """Test TSL of TSB modification tracking."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    elem_tsb = _hgraph.get_tsb_type_meta([
        ("value", ts_int_meta),
        ("count", ts_int_meta),
    ], "Item")

    tsl_meta = _hgraph.get_tsl_type_meta(elem_tsb, 3)

    ts_output = _hgraph.TSOutput(tsl_meta)

    # Modify only element[1].value
    ts_output.view().element(1).field(0).set_value(42, time=T100)

    view = ts_output.view()

    # Element 1 should be modified
    assert view.element_modified_at(1, T100), "Element 1 should be modified"

    # Elements 0 and 2 should NOT be modified
    assert not view.element_modified_at(0, T100), "Element 0 should NOT be modified"
    assert not view.element_modified_at(2, T100), "Element 2 should NOT be modified"

    # Within element 1, only field 0 should be modified
    elem1 = ts_output.view().element(1)
    assert elem1.field_modified_at(0, T100), "Element 1, field 0 should be modified"
    assert not elem1.field_modified_at(1, T100), "Element 1, field 1 should NOT be modified"


@pytest.mark.xfail(reason="Modification tracking doesn't isolate sibling elements correctly")
def test_modification_tsb_of_tsl_only_modified_path_notified():
    """Test TSB of TSL modification tracking."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 3)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("list_a", tsl_meta),
        ("list_b", tsl_meta),
    ], "TwoLists")

    ts_output = _hgraph.TSOutput(tsb_meta)

    # Modify only list_a[1]
    ts_output.view().field(0).element(1).set_value(99, time=T100)

    # list_a should be modified, list_b should NOT
    assert ts_output.view().field_modified_at(0, T100), "list_a should be modified"
    assert not ts_output.view().field_modified_at(1, T100), "list_b should NOT be modified"

    # Within list_a, only element 1 should be modified
    list_a = ts_output.view().field(0)
    assert not list_a.element_modified_at(0, T100), "list_a[0] should NOT be modified"
    assert list_a.element_modified_at(1, T100), "list_a[1] should be modified"
    assert not list_a.element_modified_at(2, T100), "list_a[2] should NOT be modified"

    # list_b should have no modifications
    list_b = ts_output.view().field(1)
    for i in range(3):
        assert not list_b.element_modified_at(i, T100), f"list_b[{i}] should NOT be modified"


def test_modification_multiple_same_time():
    """Test multiple modifications at the same time in different paths."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("a", ts_int_meta),
        ("b", ts_int_meta),
        ("c", ts_int_meta),
    ])

    ts_output = _hgraph.TSOutput(tsb_meta)

    # Modify fields 0 and 2 at T100
    ts_output.view().field(0).set_value(10, time=T100)
    ts_output.view().field(2).set_value(30, time=T100)

    view = ts_output.view()

    # Fields 0 and 2 should be modified
    assert view.field_modified_at(0, T100), "Field 0 should be modified"
    assert view.field_modified_at(2, T100), "Field 2 should be modified"

    # Field 1 should NOT be modified
    assert not view.field_modified_at(1, T100), "Field 1 should NOT be modified"


def test_modification_at_different_times():
    """Test modifications at different times."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("x", ts_int_meta),
        ("y", ts_int_meta),
    ])

    ts_output = _hgraph.TSOutput(tsb_meta)

    # Modify field 0 at T100
    ts_output.view().field(0).set_value(10, time=T100)

    # Modify field 1 at T200
    ts_output.view().field(1).set_value(20, time=T200)

    # At T100: only field 0 was modified
    assert ts_output.view().field_modified_at(0, T100), "Field 0 modified at T100"
    assert not ts_output.view().field_modified_at(1, T100), "Field 1 NOT modified at T100"

    # At T200: only field 1 was modified
    assert not ts_output.view().field_modified_at(0, T200), "Field 0 NOT modified at T200"
    assert ts_output.view().field_modified_at(1, T200), "Field 1 modified at T200"


@pytest.mark.xfail(reason="Deep nested modification propagation needs work")
def test_modification_deep_nesting_isolation():
    """Test that modifications in deep nesting only affect the path."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    # Create 3-level structure: TSB -> TSL[2] -> TSB[2 fields]
    inner_tsb = _hgraph.get_tsb_type_meta([
        ("value", ts_int_meta),
        ("flag", ts_int_meta),
    ])

    mid_tsl = _hgraph.get_tsl_type_meta(inner_tsb, 2)

    outer_tsb = _hgraph.get_tsb_type_meta([
        ("items", mid_tsl),
        ("total", ts_int_meta),
    ])

    ts_output = _hgraph.TSOutput(outer_tsb)

    # Modify only items[0].value
    ts_output.view().field(0).element(0).field(0).set_value(42, time=T100)

    # Check outer level
    assert ts_output.view().field_modified_at(0, T100), "items should be modified"
    assert not ts_output.view().field_modified_at(1, T100), "total should NOT be modified"

    # Check middle level (items TSL)
    items = ts_output.view().field(0)
    assert items.element_modified_at(0, T100), "items[0] should be modified"
    assert not items.element_modified_at(1, T100), "items[1] should NOT be modified"

    # Check inner level (items[0] TSB)
    item0 = ts_output.view().field(0).element(0)
    assert item0.field_modified_at(0, T100), "items[0].value should be modified"
    assert not item0.field_modified_at(1, T100), "items[0].flag should NOT be modified"

    # Check sibling structure (items[1])
    item1 = ts_output.view().field(0).element(1)
    assert not item1.field_modified_at(0, T100), "items[1].value should NOT be modified"
    assert not item1.field_modified_at(1, T100), "items[1].flag should NOT be modified"


# =============================================================================
# Root-Level Modification Propagation Tests
# =============================================================================


def test_propagation_tsb_root_modified_when_field_modified():
    """Test TSB root is marked modified when any field is modified."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("a", ts_int_meta),
        ("b", ts_int_meta),
    ])

    ts_output = _hgraph.TSOutput(tsb_meta)

    # Initially not modified
    assert not ts_output.modified_at(T100)

    # Modify a field
    ts_output.view().field(0).set_value(42, time=T100)

    # Root should now be modified
    assert ts_output.modified_at(T100), "Root should be modified when field is modified"


def test_propagation_tsl_root_modified_when_element_modified():
    """Test TSL root is marked modified when any element is modified."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 3)

    ts_output = _hgraph.TSOutput(tsl_meta)

    # Initially not modified
    assert not ts_output.modified_at(T100)

    # Modify an element
    ts_output.view().element(1).set_value(42, time=T100)

    # Root should now be modified
    assert ts_output.modified_at(T100), "Root should be modified when element is modified"


@pytest.mark.xfail(reason="Deep nested modification propagation to root needs work")
def test_propagation_nested_modification_propagates_to_root():
    """Test deep modification propagates to root."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    inner_tsb = _hgraph.get_tsb_type_meta([("value", ts_int_meta)])
    tsl_meta = _hgraph.get_tsl_type_meta(inner_tsb, 2)
    outer_tsb = _hgraph.get_tsb_type_meta([("items", tsl_meta)])

    ts_output = _hgraph.TSOutput(outer_tsb)

    # Initially not modified
    assert not ts_output.modified_at(T100)

    # Modify deeply nested value
    ts_output.view().field(0).element(0).field(0).set_value(42, time=T100)

    # Root should be modified
    assert ts_output.modified_at(T100), "Root should be modified when deep value is modified"

    # Intermediate levels should also be modified
    assert ts_output.view().field_modified_at(0, T100), "Intermediate field should be modified"


# =============================================================================
# Edge Cases and Boundary Tests
# =============================================================================


def test_edge_single_field_bundle():
    """Test bundle with single field."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    tsb_meta = _hgraph.get_tsb_type_meta([("only", ts_int_meta)])

    ts_output = _hgraph.TSOutput(tsb_meta)
    assert ts_output.valid
    assert ts_output.view().field_count == 1

    ts_output.view().field(0).set_value(42, time=T100)
    assert ts_output.view().field(0).py_value == 42


def test_edge_single_element_list():
    """Test list with single element."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)
    tsl_meta = _hgraph.get_tsl_type_meta(ts_int_meta, 1)

    ts_output = _hgraph.TSOutput(tsl_meta)
    assert ts_output.valid
    assert ts_output.view().list_size == 1

    ts_output.view().element(0).set_value(42, time=T100)
    assert ts_output.view().element(0).py_value == 42


def test_edge_same_value_different_times():
    """Test setting same value at different times."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_meta = _hgraph.get_ts_type_meta(int_meta)

    ts_output = _hgraph.TSOutput(ts_meta)

    ts_output.set_value(42, time=T100)
    assert ts_output.py_value == 42
    assert ts_output.modified_at(T100)
    assert ts_output.last_modified_time == T100

    ts_output.set_value(42, time=T200)  # Same value, different time
    assert ts_output.py_value == 42
    assert ts_output.modified_at(T200)
    assert ts_output.last_modified_time == T200
    # Previous time should no longer be "last modified"
    assert not ts_output.modified_at(T100)


def test_edge_deeply_nested_same_structure():
    """Test deeply nested identical structures."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    ts_int_meta = _hgraph.get_ts_type_meta(int_meta)

    # Create identical nested bundles at multiple levels
    level1 = _hgraph.get_tsb_type_meta([("value", ts_int_meta)], "L1")
    level2 = _hgraph.get_tsb_type_meta([("inner", level1)], "L2")
    level3 = _hgraph.get_tsb_type_meta([("inner", level2)], "L3")
    level4 = _hgraph.get_tsb_type_meta([("inner", level3)], "L4")

    ts_output = _hgraph.TSOutput(level4)
    assert ts_output.valid

    # Navigate to deepest level
    deep = ts_output.view().field(0).field(0).field(0).field(0)
    deep.set_value(12345, time=T100)

    # Verify value
    assert ts_output.view().field(0).field(0).field(0).field(0).py_value == 12345


def test_edge_mixed_types_in_bundle():
    """Test bundle with all different field types."""
    int_meta = _hgraph.get_scalar_type_meta(int)
    float_meta = _hgraph.get_scalar_type_meta(float)
    str_meta = _hgraph.get_scalar_type_meta(str)
    bool_meta = _hgraph.get_scalar_type_meta(bool)

    ts_int = _hgraph.get_ts_type_meta(int_meta)
    ts_float = _hgraph.get_ts_type_meta(float_meta)
    ts_str = _hgraph.get_ts_type_meta(str_meta)
    ts_bool = _hgraph.get_ts_type_meta(bool_meta)
    tss_int = _hgraph.get_tss_type_meta(int_meta)
    tsl_int = _hgraph.get_tsl_type_meta(ts_int, 2)

    tsb_meta = _hgraph.get_tsb_type_meta([
        ("int_field", ts_int),
        ("float_field", ts_float),
        ("str_field", ts_str),
        ("bool_field", ts_bool),
        ("set_field", tss_int),
        ("list_field", tsl_int),
    ], "AllTypes")

    ts_output = _hgraph.TSOutput(tsb_meta)
    assert ts_output.valid
    assert ts_output.view().field_count == 6

    # Set values for each type
    ts_output.view().field(0).set_value(42, time=T100)
    ts_output.view().field(1).set_value(3.14, time=T100)
    ts_output.view().field(2).set_value("hello", time=T100)
    ts_output.view().field(3).set_value(True, time=T100)

    # Verify
    assert ts_output.view().field(0).py_value == 42
    assert ts_output.view().field(1).py_value == pytest.approx(3.14)
    assert ts_output.view().field(2).py_value == "hello"
    assert ts_output.view().field(3).py_value is True
    assert ts_output.view().field(4).set_size == 0
    assert ts_output.view().field(5).list_size == 2
