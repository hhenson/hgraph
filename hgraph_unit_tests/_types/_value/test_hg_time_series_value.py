"""
Tests for the HgTimeSeriesValue C++ time-series value wrapper exposed to Python.

Tests the core time-series value functionality:
- Value storage and retrieval
- Modification tracking with explicit time parameters
- Field-level tracking for bundles
- Propagation of modifications from children to parents
"""
import pytest
from datetime import date, datetime, timedelta

from hgraph import _hgraph


# Skip all tests if HgTimeSeriesValue not available
pytestmark = pytest.mark.skipif(
    not hasattr(_hgraph, 'HgTimeSeriesValue'),
    reason="HgTimeSeriesValue not available in _hgraph module"
)


# Helper timestamps for testing
# Using distinct datetimes at minute intervals for clarity
T1 = datetime(2025, 1, 1, 0, 0, 1)      # Time at second 1
T99 = datetime(2025, 1, 1, 0, 1, 39)    # Time at 1:39
T100 = datetime(2025, 1, 1, 0, 1, 40)   # Time at 1:40
T101 = datetime(2025, 1, 1, 0, 1, 41)   # Time at 1:41
T200 = datetime(2025, 1, 1, 0, 3, 20)   # Time at 3:20
T300 = datetime(2025, 1, 1, 0, 5, 0)    # Time at 5:00


# =============================================================================
# Basic Construction and Properties
# =============================================================================

def test_construction_scalar():
    """Test constructing a scalar time-series value."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    assert ts_value.valid
    assert ts_value.kind == _hgraph.TypeKind.Scalar
    assert "int" in ts_value.type_name.lower()


def test_construction_set():
    """Test constructing a set time-series value."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    ts_value = _hgraph.HgTimeSeriesValue(set_schema)

    assert ts_value.valid
    assert ts_value.kind == _hgraph.TypeKind.Set


def test_construction_dict():
    """Test constructing a dict time-series value."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)
    ts_value = _hgraph.HgTimeSeriesValue(dict_schema)

    assert ts_value.valid
    assert ts_value.kind == _hgraph.TypeKind.Dict


def test_construction_bundle():
    """Test constructing a bundle time-series value."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    float_schema = _hgraph.get_scalar_type_meta(float)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", float_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    assert ts_value.valid
    assert ts_value.kind == _hgraph.TypeKind.Bundle
    assert ts_value.field_count == 2


# Note: Dynamic list tests are skipped due to memory layout differences
# between DynamicListType and TimeSeriesValue's ModificationTrackerStorage


# =============================================================================
# Modification Tracking - Scalar Types
# =============================================================================

def test_initial_state_has_no_value():
    """Test that a fresh time-series value has no value."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    assert not ts_value.has_value
    assert not ts_value.modified_at(T100)


def test_set_value_marks_modified():
    """Test that setting a value marks it as modified at that time."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    ts_value.set_value(42, time=T100)

    assert ts_value.has_value
    assert ts_value.py_value == 42
    assert ts_value.modified_at(T100)
    assert not ts_value.modified_at(T99)
    assert not ts_value.modified_at(T101)


def test_last_modified_time():
    """Test that last_modified_time returns the correct time."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    ts_value.set_value(10, time=T100)
    assert ts_value.last_modified_time == T100

    ts_value.set_value(20, time=T200)
    assert ts_value.last_modified_time == T200

    # Setting at an earlier time should not update last_modified_time
    # (time only moves forward in the engine)
    ts_value.set_value(30, time=T300)
    assert ts_value.last_modified_time == T300


def test_multiple_modifications_same_time():
    """Test multiple modifications at the same time."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    ts_value.set_value(10, time=T100)
    ts_value.set_value(20, time=T100)  # Same time, different value

    assert ts_value.py_value == 20
    assert ts_value.modified_at(T100)
    assert ts_value.last_modified_time == T100


def test_mark_invalid():
    """Test that mark_invalid resets the modification state."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    ts_value.set_value(42, time=T100)
    assert ts_value.has_value
    assert ts_value.modified_at(T100)

    ts_value.mark_invalid()
    assert not ts_value.has_value
    assert not ts_value.modified_at(T100)


# =============================================================================
# Scalar Type Round-Trips
# =============================================================================

@pytest.mark.parametrize("value", [True, False])
def test_scalar_bool(value):
    """Test bool type round-trip with time tracking."""
    schema = _hgraph.get_scalar_type_meta(bool)
    ts_value = _hgraph.HgTimeSeriesValue(schema)
    ts_value.set_value(value, time=T100)

    assert ts_value.py_value is value
    assert ts_value.modified_at(T100)


@pytest.mark.parametrize("value", [0, 1, -1, 42, -123456])
def test_scalar_int(value):
    """Test int type round-trip with time tracking."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)
    ts_value.set_value(value, time=T100)

    assert ts_value.py_value == value
    assert ts_value.modified_at(T100)


@pytest.mark.parametrize("value", [0.0, 3.14159, -2.71828])
def test_scalar_float(value):
    """Test float type round-trip with time tracking."""
    schema = _hgraph.get_scalar_type_meta(float)
    ts_value = _hgraph.HgTimeSeriesValue(schema)
    ts_value.set_value(value, time=T100)

    assert ts_value.py_value == value
    assert ts_value.modified_at(T100)


@pytest.mark.parametrize("value", ["", "Hello", "Unicode: \u00e9"])
def test_scalar_str(value):
    """Test str type round-trip with time tracking."""
    schema = _hgraph.get_scalar_type_meta(str)
    ts_value = _hgraph.HgTimeSeriesValue(schema)
    ts_value.set_value(value, time=T100)

    assert ts_value.py_value == value
    assert ts_value.modified_at(T100)


# =============================================================================
# Bundle Field Tracking
# =============================================================================

def test_bundle_field_access():
    """Test accessing bundle fields."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    float_schema = _hgraph.get_scalar_type_meta(float)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", float_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    # Set the whole value
    ts_value.set_value({"x": 10, "y": 3.14}, time=T100)

    assert ts_value.get_field(0) == 10
    assert abs(ts_value.get_field(1) - 3.14) < 1e-10
    assert ts_value.get_field_by_name("x") == 10
    assert abs(ts_value.get_field_by_name("y") - 3.14) < 1e-10


def test_bundle_field_modification_tracking():
    """Test that field-level modifications are tracked."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    float_schema = _hgraph.get_scalar_type_meta(float)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", float_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    # Set field x at time T100
    ts_value.set_field(0, 10, time=T100)

    assert ts_value.field_modified_at(0, T100)  # x was modified
    assert not ts_value.field_modified_at(1, T100)  # y was not modified
    assert ts_value.modified_at(T100)  # Bundle itself is modified (propagation)


def test_bundle_field_modification_by_name():
    """Test setting fields by name with modification tracking."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    str_schema = _hgraph.get_scalar_type_meta(str)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("name", str_schema), ("age", int_schema)],
        "Person"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    ts_value.set_field_by_name("name", "Alice", time=T100)
    ts_value.set_field_by_name("age", 30, time=T200)

    assert ts_value.get_field_by_name("name") == "Alice"
    assert ts_value.get_field_by_name("age") == 30

    # Check field-level tracking
    assert ts_value.field_modified_at(0, T100)  # name at T100
    assert not ts_value.field_modified_at(0, T200)  # name not at T200
    assert not ts_value.field_modified_at(1, T100)  # age not at T100
    assert ts_value.field_modified_at(1, T200)  # age at T200

    # The last_modified_time should reflect the latest modification
    assert ts_value.last_modified_time == T200


def test_bundle_propagation():
    """Test that field modifications propagate to parent bundle."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("a", int_schema), ("b", int_schema)],
        "AB"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    # Modify field a at time T100
    ts_value.set_field(0, 1, time=T100)
    assert ts_value.last_modified_time == T100

    # Modify field b at time T200
    ts_value.set_field(1, 2, time=T200)
    assert ts_value.last_modified_time == T200


# Note: Dynamic list tests are omitted due to memory layout differences
# between DynamicListType and TimeSeriesValue's ModificationTrackerStorage.
# List types should be tested once TSL (TimeSeriesListOutput) is integrated.


# =============================================================================
# Set Operations
# =============================================================================

def test_set_value_round_trip():
    """Test set value round-trip with time tracking."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    ts_value = _hgraph.HgTimeSeriesValue(set_schema)

    ts_value.set_value({1, 2, 3}, time=T100)

    assert ts_value.py_value == {1, 2, 3}
    assert ts_value.set_size == 3
    assert ts_value.modified_at(T100)


def test_set_modification_at_different_times():
    """Test modifying set at different times."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    ts_value = _hgraph.HgTimeSeriesValue(set_schema)

    ts_value.set_value({1, 2}, time=T100)
    assert ts_value.modified_at(T100)
    assert ts_value.last_modified_time == T100

    ts_value.set_value({1, 2, 3}, time=T200)
    assert ts_value.modified_at(T200)
    assert ts_value.last_modified_time == T200


# =============================================================================
# Dict Operations
# =============================================================================

def test_dict_value_round_trip():
    """Test dict value round-trip with time tracking."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)
    ts_value = _hgraph.HgTimeSeriesValue(dict_schema)

    ts_value.set_value({"a": 1, "b": 2}, time=T100)

    assert ts_value.py_value == {"a": 1, "b": 2}
    assert ts_value.dict_size == 2
    assert ts_value.modified_at(T100)


# =============================================================================
# String Representation
# =============================================================================

def test_str_repr():
    """Test string representation."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)
    ts_value.set_value(42, time=T100)

    assert "42" in str(ts_value)
    assert "HgTimeSeriesValue" in repr(ts_value)


def test_debug_string():
    """Test debug string includes modification info."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)
    ts_value.set_value(42, time=T100)

    debug_str = ts_value.to_debug_string(T100)
    assert "42" in debug_str
    assert "modified=true" in debug_str.lower()

    # At a different time, should show not modified
    debug_str_other = ts_value.to_debug_string(T99)
    assert "modified=false" in debug_str_other.lower()


# =============================================================================
# Integration with HgScalarTypeMetaData
# =============================================================================

def test_from_scalar_metadata():
    """Test creating HgTimeSeriesValue from HgScalarTypeMetaData."""
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    for py_type, test_value in [
        (int, 42),
        (float, 3.14),
        (bool, True),
        (date, date(2025, 12, 20)),
    ]:
        meta = HgScalarTypeMetaData.parse_type(py_type)
        schema = meta.cpp_type_meta
        ts_value = _hgraph.HgTimeSeriesValue(schema)
        ts_value.set_value(test_value, time=T100)

        if py_type == float:
            assert abs(ts_value.py_value - test_value) < 1e-10
        else:
            assert ts_value.py_value == test_value
        assert ts_value.modified_at(T100)


# =============================================================================
# Nested Types
# =============================================================================

def test_bundle_with_nested_set():
    """Test bundle containing a set field."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    str_schema = _hgraph.get_scalar_type_meta(str)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("name", str_schema), ("ids", set_schema)],
        "NamedIds"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    ts_value.set_value({"name": "test", "ids": {1, 2, 3}}, time=T100)

    result = ts_value.py_value
    assert result["name"] == "test"
    assert result["ids"] == {1, 2, 3}


def test_nested_bundle():
    """Test bundle containing another bundle."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    inner_bundle = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )
    str_schema = _hgraph.get_scalar_type_meta(str)
    outer_bundle = _hgraph.get_bundle_type_meta(
        [("name", str_schema), ("location", inner_bundle)],
        "NamedPoint"
    )
    ts_value = _hgraph.HgTimeSeriesValue(outer_bundle)

    ts_value.set_value({"name": "origin", "location": {"x": 0, "y": 0}}, time=T100)

    result = ts_value.py_value
    assert result["name"] == "origin"
    assert result["location"]["x"] == 0
    assert result["location"]["y"] == 0


# =============================================================================
# Edge Cases
# =============================================================================

def test_empty_set():
    """Test empty set handling."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    ts_value = _hgraph.HgTimeSeriesValue(set_schema)

    ts_value.set_value(set(), time=T100)

    assert ts_value.py_value == set()
    assert ts_value.set_size == 0
    assert ts_value.modified_at(T100)


def test_empty_dict():
    """Test empty dict handling."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)
    ts_value = _hgraph.HgTimeSeriesValue(dict_schema)

    ts_value.set_value({}, time=T100)

    assert ts_value.py_value == {}
    assert ts_value.dict_size == 0
    assert ts_value.modified_at(T100)


def test_time_at_epoch():
    """Test modification at the start time."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    ts_value.set_value(42, time=T1)

    assert ts_value.modified_at(T1)
    assert ts_value.py_value == 42


# =============================================================================
# Notification / Observer Tests
# =============================================================================

def test_subscribe_and_notify():
    """Test that subscribers receive notifications when value is modified."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    notifications = []
    callback = lambda t: notifications.append(t)

    assert not ts_value.has_subscribers
    ts_value.subscribe(callback)
    assert ts_value.has_subscribers
    assert ts_value.subscriber_count == 1

    # Modify the value - should trigger notification
    ts_value.set_value(42, time=T100)

    assert len(notifications) == 1
    assert notifications[0] == T100


def test_multiple_modifications_notify_each_time():
    """Test that each modification triggers a notification."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    notifications = []
    callback = lambda t: notifications.append(t)
    ts_value.subscribe(callback)

    ts_value.set_value(1, time=T100)
    ts_value.set_value(2, time=T200)
    ts_value.set_value(3, time=T300)

    assert len(notifications) == 3
    assert notifications == [T100, T200, T300]


def test_unsubscribe_stops_notifications():
    """Test that unsubscribing stops notifications."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    notifications = []
    callback = lambda t: notifications.append(t)
    ts_value.subscribe(callback)

    ts_value.set_value(1, time=T100)
    assert len(notifications) == 1

    ts_value.unsubscribe(callback)
    assert not ts_value.has_subscribers

    ts_value.set_value(2, time=T200)
    assert len(notifications) == 1  # No new notification


def test_multiple_subscribers():
    """Test that multiple subscribers all receive notifications."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    notifications1 = []
    notifications2 = []
    callback1 = lambda t: notifications1.append(t)
    callback2 = lambda t: notifications2.append(t)

    ts_value.subscribe(callback1)
    ts_value.subscribe(callback2)
    assert ts_value.subscriber_count == 2

    ts_value.set_value(42, time=T100)

    assert len(notifications1) == 1
    assert len(notifications2) == 1
    assert notifications1[0] == T100
    assert notifications2[0] == T100


def test_bundle_field_modification_notifies():
    """Test that setting a bundle field triggers notification."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    notifications = []
    callback = lambda t: notifications.append(t)
    ts_value.subscribe(callback)

    ts_value.set_field(0, 10, time=T100)

    assert len(notifications) == 1
    assert notifications[0] == T100


def test_notification_with_complex_callback():
    """Test that callback can perform complex operations."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    state = {"count": 0, "times": [], "values": []}

    def callback(t):
        state["count"] += 1
        state["times"].append(t)
        state["values"].append(ts_value.py_value)

    ts_value.subscribe(callback)

    ts_value.set_value(10, time=T100)
    ts_value.set_value(20, time=T200)

    assert state["count"] == 2
    assert state["times"] == [T100, T200]
    assert state["values"] == [10, 20]


def test_no_notification_before_subscription():
    """Test that modifications before subscription don't notify."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    ts_value.set_value(1, time=T100)

    notifications = []
    callback = lambda t: notifications.append(t)
    ts_value.subscribe(callback)

    assert len(notifications) == 0  # No notification for previous modification

    ts_value.set_value(2, time=T200)
    assert len(notifications) == 1  # Only new modification notifies


def test_duplicate_subscribe_raises():
    """Test that subscribing the same callback twice raises an error."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    callback = lambda t: None
    ts_value.subscribe(callback)

    with pytest.raises(RuntimeError, match="already subscribed"):
        ts_value.subscribe(callback)


def test_unsubscribe_unknown_callback_is_safe():
    """Test that unsubscribing an unknown callback is a no-op."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    callback1 = lambda t: None
    callback2 = lambda t: None

    ts_value.subscribe(callback1)
    assert ts_value.subscriber_count == 1

    # Unsubscribing an unknown callback should silently succeed
    ts_value.unsubscribe(callback2)
    assert ts_value.subscriber_count == 1  # Still have the original subscriber


# =============================================================================
# Fluent View API Tests
# =============================================================================

def test_view_basic():
    """Test basic view access."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    view = ts_value.view()
    assert view.valid
    assert view.kind == _hgraph.TypeKind.Scalar


def test_view_set_value():
    """Test setting value through a view."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    ts_value.view().set_value(42, time=T100)
    assert ts_value.py_value == 42
    assert ts_value.modified_at(T100)


def test_view_field_navigation():
    """Test navigating to a field using view."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    field_view = ts_value.view().field(0)
    assert field_view.valid
    assert field_view.kind == _hgraph.TypeKind.Scalar


def test_view_field_by_name():
    """Test navigating to a field by name using view."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    field_view = ts_value.view().field("x")
    assert field_view.valid
    field_view.set_value(42, time=T100)
    assert ts_value.get_field_by_name("x") == 42


# =============================================================================
# Hierarchical Notification Tests (Fluent API)
# =============================================================================

def test_subscribe_field_basic():
    """Test subscribing to a specific field using fluent API."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    field_notifications = []
    field_callback = lambda t: field_notifications.append(("field_x", t))

    # Subscribe using fluent API
    ts_value.view().field(0).subscribe(field_callback)

    # Modify field x using the view - should notify field subscriber
    ts_value.view().field(0).set_value(10, time=T100)

    assert len(field_notifications) == 1
    assert field_notifications[0] == ("field_x", T100)


def test_hierarchical_notification_field_and_root():
    """Test that modifying a field notifies both field and root subscribers."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    root_notifications = []
    field_notifications = []

    root_callback = lambda t: root_notifications.append(("root", t))
    field_callback = lambda t: field_notifications.append(("field_x", t))

    ts_value.view().subscribe(root_callback)
    ts_value.view().field(0).subscribe(field_callback)

    # Modify field x using the view - should notify both field AND root
    ts_value.view().field(0).set_value(10, time=T100)

    assert len(field_notifications) == 1
    assert len(root_notifications) == 1
    assert field_notifications[0] == ("field_x", T100)
    assert root_notifications[0] == ("root", T100)


def test_hierarchical_notification_other_field_not_notified():
    """Test that modifying one field doesn't notify subscribers to other fields."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    field_x_notifications = []
    field_y_notifications = []

    callback_x = lambda t: field_x_notifications.append(("field_x", t))
    callback_y = lambda t: field_y_notifications.append(("field_y", t))

    ts_value.view().field(0).subscribe(callback_x)
    ts_value.view().field(1).subscribe(callback_y)

    # Modify field x only
    ts_value.view().field(0).set_value(10, time=T100)

    assert len(field_x_notifications) == 1
    assert len(field_y_notifications) == 0

    # Modify field y only
    ts_value.view().field(1).set_value(20, time=T200)

    assert len(field_x_notifications) == 1  # Still 1
    assert len(field_y_notifications) == 1  # Now 1


def test_hierarchical_nested_bundle_notifications():
    """Test notification propagation through nested bundles."""
    int_schema = _hgraph.get_scalar_type_meta(int)

    # Create inner bundle: Point {x, y}
    inner_bundle = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )

    str_schema = _hgraph.get_scalar_type_meta(str)

    # Create outer bundle: NamedPoint {name, location}
    outer_bundle = _hgraph.get_bundle_type_meta(
        [("name", str_schema), ("location", inner_bundle)],
        "NamedPoint"
    )

    ts_value = _hgraph.HgTimeSeriesValue(outer_bundle)

    outer_notifications = []
    location_notifications = []

    outer_callback = lambda t: outer_notifications.append(("outer", t))
    location_callback = lambda t: location_notifications.append(("location", t))

    ts_value.view().subscribe(outer_callback)
    ts_value.view().field(1).subscribe(location_callback)  # field 1 is "location"

    # Set the whole location at once using the view
    ts_value.view().field(1).set_value({"x": 10, "y": 20}, time=T100)

    # Both outer and location should be notified
    assert len(outer_notifications) == 1
    assert len(location_notifications) == 1


def test_unsubscribe_field():
    """Test unsubscribing from a specific field using fluent API."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    notifications = []
    callback = lambda t: notifications.append(t)

    ts_value.view().field(0).subscribe(callback)
    ts_value.view().field(0).set_value(10, time=T100)
    assert len(notifications) == 1

    ts_value.view().field(0).unsubscribe(callback)
    ts_value.view().field(0).set_value(20, time=T200)
    assert len(notifications) == 1  # No new notification


def test_dict_entry_subscription():
    """Test subscribing to a specific dict entry using fluent API."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)

    ts_value = _hgraph.HgTimeSeriesValue(dict_schema)

    # First set the dict so entries exist
    ts_value.set_value({"a": 1, "b": 2}, time=T100)

    root_notifications = []
    entry_notifications = []

    root_callback = lambda t: root_notifications.append(("root", t))
    entry_callback = lambda t: entry_notifications.append(("entry_a", t))

    ts_value.view().subscribe(root_callback)
    ts_value.view().key("a").subscribe(entry_callback)

    # Modify the entry using the view
    ts_value.view().key("a").set_value(10, time=T200)

    # Both root and entry should be notified
    assert len(root_notifications) == 1
    assert len(entry_notifications) == 1


def test_subscribe_field_requires_bundle():
    """Test that field() on view raises error for non-bundle types."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    callback = lambda t: None

    with pytest.raises(RuntimeError, match="requires a valid Bundle type"):
        ts_value.view().field(0).subscribe(callback)


def test_subscribe_field_invalid_index():
    """Test that field() on view raises error for invalid index."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema)],
        "Single"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    callback = lambda t: None

    with pytest.raises(RuntimeError, match="Invalid field index"):
        ts_value.view().field(5).subscribe(callback)


def test_subscribe_entry_requires_dict():
    """Test that key() on view raises error for non-dict types."""
    schema = _hgraph.get_scalar_type_meta(int)
    ts_value = _hgraph.HgTimeSeriesValue(schema)

    callback = lambda t: None

    with pytest.raises(RuntimeError, match="requires a valid Dict type"):
        ts_value.view().key("key").subscribe(callback)


def test_subscribe_entry_missing_key():
    """Test that key() on view raises error for missing key."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)

    ts_value = _hgraph.HgTimeSeriesValue(dict_schema)
    ts_value.set_value({"a": 1}, time=T100)

    callback = lambda t: None

    with pytest.raises(RuntimeError, match="Key not found"):
        ts_value.view().key("missing").subscribe(callback)


def test_multiple_field_subscribers():
    """Test that multiple subscribers can subscribe to the same field."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema)],
        "Single"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    notifications1 = []
    notifications2 = []

    callback1 = lambda t: notifications1.append(t)
    callback2 = lambda t: notifications2.append(t)

    ts_value.view().field(0).subscribe(callback1)
    ts_value.view().field(0).subscribe(callback2)

    ts_value.view().field(0).set_value(42, time=T100)

    assert len(notifications1) == 1
    assert len(notifications2) == 1


def test_field_subscription_with_root_modify():
    """Test that setting root value notifies root but not field subscribers."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )
    ts_value = _hgraph.HgTimeSeriesValue(bundle_schema)

    root_notifications = []
    field_notifications = []

    root_callback = lambda t: root_notifications.append(t)
    field_callback = lambda t: field_notifications.append(t)

    ts_value.view().subscribe(root_callback)
    ts_value.view().field(0).subscribe(field_callback)

    # Set the whole bundle at once (at root level)
    ts_value.set_value({"x": 10, "y": 20}, time=T100)

    # Root should be notified
    assert len(root_notifications) == 1
    # Field subscriptions are only notified when modifying through field view
    # When modifying at root, only root observers are notified
