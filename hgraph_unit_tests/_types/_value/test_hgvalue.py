"""
Tests for the HgValue C++ value wrapper exposed to Python.

Tests round-trip storage and retrieval for all supported types:
- Scalar types: bool, int, float, date, datetime, timedelta, str (as object)
- Composite types: set, dict, tuple[T, ...], bundle
"""
import pytest
from datetime import date, datetime, timedelta

from hgraph import _hgraph


# Skip all tests if HgValue not available
pytestmark = pytest.mark.skipif(
    not hasattr(_hgraph, 'HgValue'),
    reason="HgValue not available in _hgraph module"
)


# =============================================================================
# Scalar Type Tests
# =============================================================================

@pytest.mark.parametrize("value", [True, False])
def test_scalar_bool(value):
    """Test bool type round-trip."""
    schema = _hgraph.get_scalar_type_meta(bool)
    hgvalue = _hgraph.HgValue(schema)
    hgvalue.py_value = value
    assert hgvalue.py_value is value


@pytest.mark.parametrize("value", [0, 1, -1, 42, -123456, 2**60])
def test_scalar_int(value):
    """Test int type round-trip."""
    schema = _hgraph.get_scalar_type_meta(int)
    hgvalue = _hgraph.HgValue(schema)
    hgvalue.py_value = value
    assert hgvalue.py_value == value


@pytest.mark.parametrize("value", [0.0, 3.14159, -2.71828, float('inf'), float('-inf')])
def test_scalar_float(value):
    """Test float type round-trip."""
    schema = _hgraph.get_scalar_type_meta(float)
    hgvalue = _hgraph.HgValue(schema)
    hgvalue.py_value = value
    assert hgvalue.py_value == value


@pytest.mark.parametrize("value", [
    date(2025, 1, 1),
    date(2025, 12, 19),
    date(1970, 1, 1),
])
def test_scalar_date(value):
    """Test date type round-trip."""
    schema = _hgraph.get_scalar_type_meta(date)
    hgvalue = _hgraph.HgValue(schema)
    hgvalue.py_value = value
    assert hgvalue.py_value == value


@pytest.mark.parametrize("value", [
    datetime(2025, 12, 19, 14, 30, 45),
    datetime(1970, 1, 1, 0, 0, 0),
    datetime(2025, 6, 15, 12, 0, 0, 500000),
])
def test_scalar_datetime(value):
    """Test datetime type round-trip."""
    schema = _hgraph.get_scalar_type_meta(datetime)
    hgvalue = _hgraph.HgValue(schema)
    hgvalue.py_value = value
    result = hgvalue.py_value
    # Compare with second precision (microseconds may vary due to nanosecond storage)
    assert result.year == value.year
    assert result.month == value.month
    assert result.day == value.day
    assert result.hour == value.hour
    assert result.minute == value.minute
    assert result.second == value.second


@pytest.mark.parametrize("value", [
    timedelta(seconds=0),
    timedelta(days=5, hours=3, minutes=30, seconds=15),
    timedelta(microseconds=12345),
])
def test_scalar_timedelta(value):
    """Test timedelta type round-trip."""
    schema = _hgraph.get_scalar_type_meta(timedelta)
    hgvalue = _hgraph.HgValue(schema)
    hgvalue.py_value = value
    result = hgvalue.py_value
    # Allow microsecond precision differences
    assert abs(result.total_seconds() - value.total_seconds()) < 0.001


@pytest.mark.parametrize("value", [
    "",
    "Hello, HGraph!",
    "Unicode: \u00e9\u00e8\u00ea",
])
def test_scalar_str(value):
    """Test str type (stored as Python object) round-trip."""
    schema = _hgraph.get_scalar_type_meta(str)
    hgvalue = _hgraph.HgValue(schema)
    hgvalue.py_value = value
    assert hgvalue.py_value == value


def test_scalar_object():
    """Test arbitrary Python object storage."""
    schema = _hgraph.get_scalar_type_meta(str)
    hgvalue = _hgraph.HgValue(schema)
    test_list = [1, 2, 3]
    hgvalue.py_value = test_list
    assert hgvalue.py_value == test_list


# =============================================================================
# Composite Type Tests
# =============================================================================

@pytest.mark.parametrize("value", [
    set(),
    {1},
    {1, 2, 3, 4, 5},
])
def test_set_int(value):
    """Test set[int] round-trip."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    hgvalue = _hgraph.HgValue(set_schema)
    hgvalue.py_value = value
    assert hgvalue.py_value == value


@pytest.mark.parametrize("value", [
    set(),
    {"apple"},
    {"apple", "banana", "cherry"},
])
def test_set_str(value):
    """Test set[str] round-trip."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    set_schema = _hgraph.get_set_type_meta(str_schema)
    hgvalue = _hgraph.HgValue(set_schema)
    hgvalue.py_value = value
    assert hgvalue.py_value == value


@pytest.mark.parametrize("value", [
    {},
    {1: 1.1},
    {1: 1.1, 2: 2.2, 3: 3.3},
])
def test_dict_int_float(value):
    """Test dict[int, float] round-trip."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    float_schema = _hgraph.get_scalar_type_meta(float)
    dict_schema = _hgraph.get_dict_type_meta(int_schema, float_schema)
    hgvalue = _hgraph.HgValue(dict_schema)
    hgvalue.py_value = value
    result = hgvalue.py_value
    assert set(result.keys()) == set(value.keys())
    for k in value:
        assert abs(result[k] - value[k]) < 1e-10


@pytest.mark.parametrize("value", [
    {},
    {"one": 1},
    {"one": 1, "two": 2, "three": 3},
])
def test_dict_str_int(value):
    """Test dict[str, int] round-trip."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)
    hgvalue = _hgraph.HgValue(dict_schema)
    hgvalue.py_value = value
    assert hgvalue.py_value == value


@pytest.mark.parametrize("value", [
    (),
    (1,),
    (1, 2, 3, 4, 5),
])
def test_dynamic_list_int(value):
    """Test tuple[int, ...] round-trip."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    list_schema = _hgraph.get_dynamic_list_type_meta(int_schema)
    hgvalue = _hgraph.HgValue(list_schema)
    hgvalue.py_value = value
    assert hgvalue.py_value == value


@pytest.mark.parametrize("value", [
    (),
    (1.1,),
    (1.1, 2.2, 3.3),
])
def test_dynamic_list_float(value):
    """Test tuple[float, ...] round-trip."""
    float_schema = _hgraph.get_scalar_type_meta(float)
    list_schema = _hgraph.get_dynamic_list_type_meta(float_schema)
    hgvalue = _hgraph.HgValue(list_schema)
    hgvalue.py_value = value
    result = hgvalue.py_value
    assert len(result) == len(value)
    for a, b in zip(result, value):
        assert abs(a - b) < 1e-10


def test_bundle_simple():
    """Test simple bundle (struct-like) round-trip."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    float_schema = _hgraph.get_scalar_type_meta(float)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", float_schema)],
        "Point"
    )
    hgvalue = _hgraph.HgValue(bundle_schema)
    test_dict = {"x": 10, "y": 3.14}
    hgvalue.py_value = test_dict
    result = hgvalue.py_value
    assert result["x"] == 10
    assert abs(result["y"] - 3.14) < 1e-10


def test_bundle_with_str():
    """Test bundle with str field round-trip."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    bundle_schema = _hgraph.get_bundle_type_meta(
        [("name", str_schema), ("age", int_schema)],
        "Person"
    )
    hgvalue = _hgraph.HgValue(bundle_schema)
    test_dict = {"name": "Alice", "age": 30}
    hgvalue.py_value = test_dict
    result = hgvalue.py_value
    assert result["name"] == "Alice"
    assert result["age"] == 30


# =============================================================================
# HgValue Operations Tests
# =============================================================================

def test_copy():
    """Test value deep copy."""
    schema = _hgraph.get_scalar_type_meta(int)
    original = _hgraph.HgValue(schema)
    original.py_value = 42

    copy = original.copy()
    assert copy.py_value == 42

    # Modifying copy shouldn't affect original
    copy.py_value = 100
    assert original.py_value == 42
    assert copy.py_value == 100


def test_equality():
    """Test value equality comparison."""
    schema = _hgraph.get_scalar_type_meta(int)

    v1 = _hgraph.HgValue(schema)
    v1.py_value = 42

    v2 = _hgraph.HgValue(schema)
    v2.py_value = 42

    v3 = _hgraph.HgValue(schema)
    v3.py_value = 100

    assert v1 == v2
    assert v1 != v3


def test_hash():
    """Test value hashing (equal values have equal hashes)."""
    schema = _hgraph.get_scalar_type_meta(int)

    v1 = _hgraph.HgValue(schema)
    v1.py_value = 42

    v2 = _hgraph.HgValue(schema)
    v2.py_value = 42

    assert hash(v1) == hash(v2)


def test_str_repr():
    """Test string representation."""
    schema = _hgraph.get_scalar_type_meta(int)
    value = _hgraph.HgValue(schema)
    value.py_value = 42

    assert "42" in str(value)
    assert "HgValue" in repr(value)


def test_from_python_factory():
    """Test the from_python static factory method."""
    schema = _hgraph.get_scalar_type_meta(int)
    value = _hgraph.HgValue.from_python(schema, 42)

    assert value.valid
    assert value.py_value == 42


def test_valid_and_kind():
    """Test valid and kind properties."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    int_value = _hgraph.HgValue(int_schema)
    assert int_value.valid
    assert int_value.kind == _hgraph.TypeKind.Scalar

    set_schema = _hgraph.get_set_type_meta(int_schema)
    set_value = _hgraph.HgValue(set_schema)
    assert set_value.valid
    assert set_value.kind == _hgraph.TypeKind.Set


# =============================================================================
# Integration with HgScalarTypeMetaData
# =============================================================================

def test_from_scalar_metadata():
    """Test creating HgValue from HgScalarTypeMetaData."""
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    for py_type, test_value in [
        (int, 42),
        (float, 3.14),
        (bool, True),
        (date, date(2025, 12, 19)),
    ]:
        meta = HgScalarTypeMetaData.parse_type(py_type)
        schema = meta.cpp_type_meta
        hgvalue = _hgraph.HgValue(schema)
        hgvalue.py_value = test_value
        if py_type == float:
            assert abs(hgvalue.py_value - test_value) < 1e-10
        else:
            assert hgvalue.py_value == test_value


def test_from_set_metadata():
    """Test creating HgValue from HgSetScalarType metadata."""
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData, HgSetScalarType

    inner_meta = HgScalarTypeMetaData.parse_type(int)
    set_meta = HgSetScalarType(inner_meta)
    schema = set_meta.cpp_type_meta

    hgvalue = _hgraph.HgValue(schema)
    test_set = {1, 2, 3}
    hgvalue.py_value = test_set
    assert hgvalue.py_value == test_set


def test_from_dict_metadata():
    """Test creating HgValue from HgDictScalarType metadata."""
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData, HgDictScalarType

    key_meta = HgScalarTypeMetaData.parse_type(str)
    value_meta = HgScalarTypeMetaData.parse_type(int)
    dict_meta = HgDictScalarType(key_meta, value_meta)
    schema = dict_meta.cpp_type_meta

    hgvalue = _hgraph.HgValue(schema)
    test_dict = {"a": 1, "b": 2}
    hgvalue.py_value = test_dict
    assert hgvalue.py_value == test_dict
