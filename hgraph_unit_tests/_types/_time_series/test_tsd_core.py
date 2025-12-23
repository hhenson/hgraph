"""
Tests for TSD (Time-Series Dict) core operations via TSOutputView.

These tests focus on the fundamental TSD storage operations:
- dict_size: checking the dict size
- py_value: getting the Python dict value
- set_value: setting the dict value
- modified_at: modification tracking

Note: TSD-specific methods like contains, keys, entry are on
PyTimeSeriesDictOutput (graph wrapper), not TSOutputView.
Those are tested via test_tsd_behavior.py using full graph execution.
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
# TSD Size Tests (via dict_size)
# =============================================================================


def test_tsd_size_empty():
    """Test dict_size of an empty TSD."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    assert view.dict_size == 0


def test_tsd_size_after_set():
    """Test dict_size after setting values."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1}, time=T100)
    assert view.dict_size == 1

    view.set_value({"a": 1, "b": 2, "c": 3}, time=T200)
    assert view.dict_size == 3


def test_tsd_size_after_replace():
    """Test dict_size after replacing values."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2, "c": 3}, time=T100)
    assert view.dict_size == 3

    # Replace with smaller dict
    view.set_value({"x": 10}, time=T200)
    assert view.dict_size == 1


def test_tsd_is_empty_via_size():
    """Test checking if TSD is empty via dict_size."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()

    # Initially no value
    assert view.dict_size == 0
    assert not ts_output.has_value

    # Set empty dict
    view.set_value({}, time=T100)
    assert view.dict_size == 0
    assert ts_output.has_value  # Has value, but empty

    # Set non-empty dict
    view.set_value({"a": 1}, time=T200)
    assert view.dict_size == 1


def test_tsd_size_int_keys():
    """Test dict_size with integer keys."""
    ts_output = create_tsd_int_str_output()

    view = ts_output.view()
    view.set_value({1: "a", 2: "b", 100: "c"}, time=T100)
    assert view.dict_size == 3


# =============================================================================
# TSD py_value Tests
# =============================================================================


def test_tsd_py_value_empty():
    """Test py_value on empty TSD."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({}, time=T100)

    assert ts_output.py_value == {}


def test_tsd_py_value_basic():
    """Test py_value returns the dict."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2, "c": 3}, time=T100)

    value = ts_output.py_value
    assert value == {"a": 1, "b": 2, "c": 3}


def test_tsd_py_value_after_update():
    """Test py_value after updating."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2}, time=T100)
    assert ts_output.py_value == {"a": 1, "b": 2}

    view.set_value({"a": 10, "b": 2, "c": 3}, time=T200)
    assert ts_output.py_value == {"a": 10, "b": 2, "c": 3}


def test_tsd_py_value_int_keys():
    """Test py_value with integer keys."""
    ts_output = create_tsd_int_str_output()

    view = ts_output.view()
    view.set_value({1: "a", 2: "b", 100: "c"}, time=T100)

    value = ts_output.py_value
    assert value == {1: "a", 2: "b", 100: "c"}


# =============================================================================
# TSD Modification Tracking Tests
# =============================================================================


def test_tsd_modification_tracking_initial():
    """Test modification tracking on initial set."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2}, time=T100)

    assert view.modified_at(T100)
    assert not view.modified_at(T200)
    assert view.last_modified_time == T100


def test_tsd_modification_tracking_update():
    """Test modification tracking on update."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1}, time=T100)
    assert view.last_modified_time == T100

    view.set_value({"a": 2, "b": 3}, time=T200)
    assert view.last_modified_time == T200
    assert view.modified_at(T200)


def test_tsd_has_value():
    """Test has_value property."""
    ts_output = create_tsd_str_int_output()

    assert not ts_output.has_value

    view = ts_output.view()
    view.set_value({"a": 1}, time=T100)

    assert ts_output.has_value


def test_tsd_mark_invalid():
    """Test marking TSD as invalid."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1}, time=T100)
    assert ts_output.has_value

    view.mark_invalid()
    assert not ts_output.has_value


# =============================================================================
# TSD Type Metadata Tests
# =============================================================================


def test_tsd_kind():
    """Test TSD kind is Dict."""
    ts_output = create_tsd_str_int_output()

    assert ts_output.kind == _hgraph.TypeKind.Dict


def test_tsd_ts_kind():
    """Test TSD ts_kind."""
    ts_output = create_tsd_str_int_output()

    # ts_kind should be TSD (typically 2)
    assert ts_output.ts_kind is not None


def test_tsd_valid():
    """Test TSD valid property."""
    ts_output = create_tsd_str_int_output()

    assert ts_output.valid

    view = ts_output.view()
    assert view.valid


# =============================================================================
# TSD Edge Cases
# =============================================================================


def test_tsd_empty_string_key():
    """Test TSD with empty string as key."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"": 42, "a": 1}, time=T100)

    assert view.dict_size == 2
    value = ts_output.py_value
    assert value[""] == 42
    assert value["a"] == 1


def test_tsd_unicode_keys():
    """Test TSD with unicode string keys."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"hello": 1, "世界": 2, "🎉": 3}, time=T100)

    assert view.dict_size == 3
    value = ts_output.py_value
    assert value["hello"] == 1
    assert value["世界"] == 2
    assert value["🎉"] == 3


def test_tsd_negative_int_keys():
    """Test TSD with negative integer keys."""
    ts_output = create_tsd_int_str_output()

    view = ts_output.view()
    view.set_value({-1: "neg", 0: "zero", 1: "pos"}, time=T100)

    assert view.dict_size == 3
    value = ts_output.py_value
    assert value[-1] == "neg"
    assert value[0] == "zero"
    assert value[1] == "pos"


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


def test_tsd_replace_all_keys():
    """Test completely replacing all keys."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2, "c": 3}, time=T100)
    assert view.dict_size == 3

    # Complete replacement with different keys
    view.set_value({"x": 100, "y": 200}, time=T200)
    assert view.dict_size == 2
    value = ts_output.py_value
    assert "a" not in value
    assert value["x"] == 100


def test_tsd_update_single_value():
    """Test updating a single value."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()
    view.set_value({"a": 1, "b": 2}, time=T100)

    # Update just 'a'
    view.set_value({"a": 100, "b": 2}, time=T200)

    value = ts_output.py_value
    assert value["a"] == 100
    assert value["b"] == 2


# =============================================================================
# TSD Multiple Tick Tests
# =============================================================================


def test_tsd_multiple_ticks():
    """Test TSD behavior across multiple ticks."""
    ts_output = create_tsd_str_int_output()

    view = ts_output.view()

    # Tick 1
    view.set_value({"a": 1}, time=T100)
    assert view.dict_size == 1
    assert view.modified_at(T100)

    # Tick 2 - add key
    view.set_value({"a": 1, "b": 2}, time=T200)
    assert view.dict_size == 2
    assert view.modified_at(T200)
    assert not view.modified_at(T100)

    # Tick 3 - remove key
    view.set_value({"b": 2}, time=T300)
    assert view.dict_size == 1
    assert ts_output.py_value == {"b": 2}
