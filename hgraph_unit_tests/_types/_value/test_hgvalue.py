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


# =============================================================================
# Nested Container Tests
# =============================================================================

def test_nested_set_of_tuples():
    """Test set containing tuples (set[tuple[int, ...]])."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    tuple_schema = _hgraph.get_dynamic_list_type_meta(int_schema)
    set_schema = _hgraph.get_set_type_meta(tuple_schema)

    hgvalue = _hgraph.HgValue(set_schema)
    # Sets of tuples - tuples are hashable
    test_value = {(1, 2, 3), (4, 5), (6,)}
    hgvalue.py_value = test_value
    assert hgvalue.py_value == test_value


def test_nested_dict_of_sets():
    """Test dict with set values (dict[str, set[int]])."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, set_schema)

    hgvalue = _hgraph.HgValue(dict_schema)
    test_value = {"evens": {2, 4, 6}, "odds": {1, 3, 5}, "empty": set()}
    hgvalue.py_value = test_value
    assert hgvalue.py_value == test_value


def test_nested_dict_of_dicts():
    """Test dict with dict values (dict[str, dict[str, int]])."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    inner_dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)
    outer_dict_schema = _hgraph.get_dict_type_meta(str_schema, inner_dict_schema)

    hgvalue = _hgraph.HgValue(outer_dict_schema)
    test_value = {
        "person1": {"age": 30, "score": 100},
        "person2": {"age": 25, "score": 95},
    }
    hgvalue.py_value = test_value
    assert hgvalue.py_value == test_value


def test_nested_tuple_of_dicts():
    """Test tuple containing dicts (tuple[dict[str, int], ...])."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)
    tuple_schema = _hgraph.get_dynamic_list_type_meta(dict_schema)

    hgvalue = _hgraph.HgValue(tuple_schema)
    test_value = ({"a": 1, "b": 2}, {"c": 3}, {})
    hgvalue.py_value = test_value
    assert hgvalue.py_value == test_value


def test_nested_tuple_of_tuples():
    """Test tuple of tuples (tuple[tuple[int, ...], ...])."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    inner_tuple_schema = _hgraph.get_dynamic_list_type_meta(int_schema)
    outer_tuple_schema = _hgraph.get_dynamic_list_type_meta(inner_tuple_schema)

    hgvalue = _hgraph.HgValue(outer_tuple_schema)
    test_value = ((1, 2), (3, 4, 5), (), (6,))
    hgvalue.py_value = test_value
    assert hgvalue.py_value == test_value


def test_nested_bundle_with_containers():
    """Test bundle containing container fields."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    str_schema = _hgraph.get_scalar_type_meta(str)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)

    bundle_schema = _hgraph.get_bundle_type_meta(
        [("ids", set_schema), ("mapping", dict_schema), ("count", int_schema)],
        "ContainerBundle"
    )

    hgvalue = _hgraph.HgValue(bundle_schema)
    test_value = {"ids": {1, 2, 3}, "mapping": {"a": 10, "b": 20}, "count": 42}
    hgvalue.py_value = test_value
    result = hgvalue.py_value
    assert result["ids"] == test_value["ids"]
    assert result["mapping"] == test_value["mapping"]
    assert result["count"] == test_value["count"]


def test_triple_nested_dict():
    """Test three levels of nesting (dict[str, dict[str, dict[str, int]]])."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    level1_dict = _hgraph.get_dict_type_meta(str_schema, int_schema)
    level2_dict = _hgraph.get_dict_type_meta(str_schema, level1_dict)
    level3_dict = _hgraph.get_dict_type_meta(str_schema, level2_dict)

    hgvalue = _hgraph.HgValue(level3_dict)
    test_value = {
        "a": {
            "b": {"c": 1, "d": 2},
            "e": {"f": 3},
        },
        "g": {
            "h": {},
        },
    }
    hgvalue.py_value = test_value
    assert hgvalue.py_value == test_value


def test_nested_bundle_in_bundle():
    """Test bundle containing another bundle."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    str_schema = _hgraph.get_scalar_type_meta(str)

    inner_bundle = _hgraph.get_bundle_type_meta(
        [("x", int_schema), ("y", int_schema)],
        "Point"
    )
    outer_bundle = _hgraph.get_bundle_type_meta(
        [("name", str_schema), ("location", inner_bundle)],
        "NamedPoint"
    )

    hgvalue = _hgraph.HgValue(outer_bundle)
    test_value = {"name": "origin", "location": {"x": 0, "y": 0}}
    hgvalue.py_value = test_value
    result = hgvalue.py_value
    assert result["name"] == "origin"
    assert result["location"]["x"] == 0
    assert result["location"]["y"] == 0


# =============================================================================
# Arithmetic Operator Tests
# =============================================================================

def test_add_int():
    """Test addition with integers."""
    schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(schema, 10)
    v2 = _hgraph.HgValue.from_python(schema, 5)
    result = v1 + v2
    assert result.py_value == 15


def test_add_float():
    """Test addition with floats."""
    schema = _hgraph.get_scalar_type_meta(float)
    v1 = _hgraph.HgValue.from_python(schema, 3.14)
    v2 = _hgraph.HgValue.from_python(schema, 2.86)
    result = v1 + v2
    assert abs(result.py_value - 6.0) < 1e-10


def test_subtract_int():
    """Test subtraction with integers."""
    schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(schema, 10)
    v2 = _hgraph.HgValue.from_python(schema, 3)
    result = v1 - v2
    assert result.py_value == 7


def test_multiply_int():
    """Test multiplication with integers."""
    schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(schema, 6)
    v2 = _hgraph.HgValue.from_python(schema, 7)
    result = v1 * v2
    assert result.py_value == 42


def test_divide_float():
    """Test division with floats."""
    schema = _hgraph.get_scalar_type_meta(float)
    v1 = _hgraph.HgValue.from_python(schema, 10.0)
    v2 = _hgraph.HgValue.from_python(schema, 4.0)
    result = v1 / v2
    assert abs(result.py_value - 2.5) < 1e-10


def test_floor_divide_int():
    """Test floor division with integers."""
    schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(schema, 17)
    v2 = _hgraph.HgValue.from_python(schema, 5)
    result = v1 // v2
    assert result.py_value == 3


def test_modulo_int():
    """Test modulo with integers."""
    schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(schema, 17)
    v2 = _hgraph.HgValue.from_python(schema, 5)
    result = v1 % v2
    assert result.py_value == 2


def test_power_int():
    """Test power with integers."""
    schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(schema, 2)
    v2 = _hgraph.HgValue.from_python(schema, 10)
    result = v1 ** v2
    assert result.py_value == 1024


# =============================================================================
# Unary Operator Tests
# =============================================================================

def test_negate_int():
    """Test unary negation with integers."""
    schema = _hgraph.get_scalar_type_meta(int)
    v = _hgraph.HgValue.from_python(schema, 42)
    result = -v
    assert result.py_value == -42


def test_negate_float():
    """Test unary negation with floats."""
    schema = _hgraph.get_scalar_type_meta(float)
    v = _hgraph.HgValue.from_python(schema, 3.14)
    result = -v
    assert abs(result.py_value - (-3.14)) < 1e-10


def test_positive_int():
    """Test unary positive with integers."""
    schema = _hgraph.get_scalar_type_meta(int)
    v = _hgraph.HgValue.from_python(schema, -42)
    result = +v
    assert result.py_value == -42  # unary + doesn't change sign


def test_absolute_int():
    """Test abs() with integers."""
    schema = _hgraph.get_scalar_type_meta(int)
    v = _hgraph.HgValue.from_python(schema, -42)
    result = abs(v)
    assert result.py_value == 42


def test_absolute_float():
    """Test abs() with floats."""
    schema = _hgraph.get_scalar_type_meta(float)
    v = _hgraph.HgValue.from_python(schema, -3.14)
    result = abs(v)
    assert abs(result.py_value - 3.14) < 1e-10


def test_invert_int():
    """Test bitwise inversion with integers."""
    schema = _hgraph.get_scalar_type_meta(int)
    v = _hgraph.HgValue.from_python(schema, 0)
    result = ~v
    assert result.py_value == -1


# =============================================================================
# Comparison Operator Tests
# =============================================================================

def test_less_than_int():
    """Test less than comparison."""
    schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(schema, 5)
    v2 = _hgraph.HgValue.from_python(schema, 10)
    assert v1 < v2
    assert not v2 < v1


def test_less_equal_int():
    """Test less than or equal comparison."""
    schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(schema, 5)
    v2 = _hgraph.HgValue.from_python(schema, 10)
    v3 = _hgraph.HgValue.from_python(schema, 5)
    assert v1 <= v2
    assert v1 <= v3
    assert not v2 <= v1


def test_greater_than_int():
    """Test greater than comparison."""
    schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(schema, 10)
    v2 = _hgraph.HgValue.from_python(schema, 5)
    assert v1 > v2
    assert not v2 > v1


def test_greater_equal_int():
    """Test greater than or equal comparison."""
    schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(schema, 10)
    v2 = _hgraph.HgValue.from_python(schema, 5)
    v3 = _hgraph.HgValue.from_python(schema, 10)
    assert v1 >= v2
    assert v1 >= v3
    assert not v2 >= v1


def test_comparison_float():
    """Test comparison operators with floats."""
    schema = _hgraph.get_scalar_type_meta(float)
    v1 = _hgraph.HgValue.from_python(schema, 1.5)
    v2 = _hgraph.HgValue.from_python(schema, 2.5)
    assert v1 < v2
    assert v1 <= v2
    assert v2 > v1
    assert v2 >= v1


def test_comparison_str():
    """Test comparison operators with strings."""
    schema = _hgraph.get_scalar_type_meta(str)
    v1 = _hgraph.HgValue.from_python(schema, "apple")
    v2 = _hgraph.HgValue.from_python(schema, "banana")
    assert v1 < v2
    assert v2 > v1


# =============================================================================
# Boolean Conversion Tests
# =============================================================================

def test_bool_true():
    """Test boolean conversion to True."""
    schema = _hgraph.get_scalar_type_meta(int)
    v = _hgraph.HgValue.from_python(schema, 42)
    assert bool(v) is True


def test_bool_false():
    """Test boolean conversion to False."""
    schema = _hgraph.get_scalar_type_meta(int)
    v = _hgraph.HgValue.from_python(schema, 0)
    assert bool(v) is False


def test_bool_empty_set():
    """Test boolean conversion of empty set."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    v = _hgraph.HgValue(set_schema)
    v.py_value = set()
    assert bool(v) is False


def test_bool_nonempty_set():
    """Test boolean conversion of non-empty set."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    v = _hgraph.HgValue(set_schema)
    v.py_value = {1, 2, 3}
    assert bool(v) is True


# =============================================================================
# Container Operation Tests
# =============================================================================

def test_len_tuple():
    """Test len() on tuple."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    tuple_schema = _hgraph.get_dynamic_list_type_meta(int_schema)
    v = _hgraph.HgValue(tuple_schema)
    v.py_value = (1, 2, 3, 4, 5)
    assert len(v) == 5


def test_len_set():
    """Test len() on set."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    v = _hgraph.HgValue(set_schema)
    v.py_value = {1, 2, 3}
    assert len(v) == 3


def test_len_dict():
    """Test len() on dict."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)
    v = _hgraph.HgValue(dict_schema)
    v.py_value = {"a": 1, "b": 2}
    assert len(v) == 2


def test_contains_set():
    """Test 'in' operator on set."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    v = _hgraph.HgValue(set_schema)
    v.py_value = {1, 2, 3}
    assert 2 in v
    assert 5 not in v


def test_contains_dict():
    """Test 'in' operator on dict (checks keys)."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)
    v = _hgraph.HgValue(dict_schema)
    v.py_value = {"a": 1, "b": 2}
    assert "a" in v
    assert "c" not in v


def test_getitem_tuple():
    """Test indexing on tuple."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    tuple_schema = _hgraph.get_dynamic_list_type_meta(int_schema)
    v = _hgraph.HgValue(tuple_schema)
    v.py_value = (10, 20, 30)
    assert v[0] == 10
    assert v[1] == 20
    assert v[-1] == 30


def test_getitem_dict():
    """Test key access on dict."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)
    v = _hgraph.HgValue(dict_schema)
    v.py_value = {"a": 1, "b": 2}
    assert v["a"] == 1
    assert v["b"] == 2


def test_iter_tuple():
    """Test iteration over tuple."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    tuple_schema = _hgraph.get_dynamic_list_type_meta(int_schema)
    v = _hgraph.HgValue(tuple_schema)
    v.py_value = (1, 2, 3)
    items = list(v)
    assert items == [1, 2, 3]


def test_iter_set():
    """Test iteration over set."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)
    v = _hgraph.HgValue(set_schema)
    v.py_value = {1, 2, 3}
    items = set(v)
    assert items == {1, 2, 3}


def test_iter_dict():
    """Test iteration over dict (iterates keys)."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)
    v = _hgraph.HgValue(dict_schema)
    v.py_value = {"a": 1, "b": 2}
    keys = set(v)
    assert keys == {"a", "b"}


# =============================================================================
# Set Algebra Operator Tests
# =============================================================================

def test_set_union():
    """Test set union operator: a | b"""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {3, 4, 5}

    result = v1 | v2
    assert result.py_value == {1, 2, 3, 4, 5}


def test_set_union_inplace():
    """Test set in-place union operator: a |= b"""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {3, 4, 5}

    v1 |= v2
    assert v1.py_value == {1, 2, 3, 4, 5}


def test_set_intersection():
    """Test set intersection operator: a & b"""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3, 4}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {3, 4, 5, 6}

    result = v1 & v2
    assert result.py_value == {3, 4}


def test_set_intersection_inplace():
    """Test set in-place intersection operator: a &= b"""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3, 4}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {3, 4, 5, 6}

    v1 &= v2
    assert v1.py_value == {3, 4}


def test_set_difference():
    """Test set difference operator: a - b"""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3, 4}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {3, 4, 5}

    result = v1 - v2
    assert result.py_value == {1, 2}


def test_set_difference_inplace():
    """Test set in-place difference operator: a -= b"""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3, 4}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {3, 4, 5}

    v1 -= v2
    assert v1.py_value == {1, 2}


def test_set_symmetric_difference():
    """Test set symmetric difference operator: a ^ b"""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {2, 3, 4}

    result = v1 ^ v2
    assert result.py_value == {1, 4}


def test_set_symmetric_difference_inplace():
    """Test set in-place symmetric difference operator: a ^= b"""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {2, 3, 4}

    v1 ^= v2
    assert v1.py_value == {1, 4}


def test_set_union_empty():
    """Test union with empty set."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = set()

    result = v1 | v2
    assert result.py_value == {1, 2, 3}


def test_set_intersection_empty():
    """Test intersection with empty set."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = set()

    result = v1 & v2
    assert result.py_value == set()


def test_set_difference_disjoint():
    """Test difference with disjoint sets."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {4, 5, 6}

    result = v1 - v2
    assert result.py_value == {1, 2, 3}


def test_set_str_union():
    """Test set union with string elements."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    set_schema = _hgraph.get_set_type_meta(str_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {"apple", "banana"}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {"banana", "cherry"}

    result = v1 | v2
    assert result.py_value == {"apple", "banana", "cherry"}


# =============================================================================
# Dict Merge Operator Tests
# =============================================================================

def test_dict_merge():
    """Test dict merge operator: a | b"""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)

    v1 = _hgraph.HgValue(dict_schema)
    v1.py_value = {"a": 1, "b": 2}

    v2 = _hgraph.HgValue(dict_schema)
    v2.py_value = {"b": 20, "c": 3}

    result = v1 | v2
    assert result.py_value == {"a": 1, "b": 20, "c": 3}


def test_dict_merge_inplace():
    """Test dict in-place merge operator: a |= b"""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)

    v1 = _hgraph.HgValue(dict_schema)
    v1.py_value = {"a": 1, "b": 2}

    v2 = _hgraph.HgValue(dict_schema)
    v2.py_value = {"b": 20, "c": 3}

    v1 |= v2
    assert v1.py_value == {"a": 1, "b": 20, "c": 3}


def test_dict_merge_empty():
    """Test merging with empty dict."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)

    v1 = _hgraph.HgValue(dict_schema)
    v1.py_value = {"a": 1, "b": 2}

    v2 = _hgraph.HgValue(dict_schema)
    v2.py_value = {}

    result = v1 | v2
    assert result.py_value == {"a": 1, "b": 2}


def test_dict_merge_disjoint():
    """Test merging dicts with no overlapping keys."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)

    v1 = _hgraph.HgValue(dict_schema)
    v1.py_value = {"a": 1, "b": 2}

    v2 = _hgraph.HgValue(dict_schema)
    v2.py_value = {"c": 3, "d": 4}

    result = v1 | v2
    assert result.py_value == {"a": 1, "b": 2, "c": 3, "d": 4}


# =============================================================================
# List Concatenation Operator Tests
# =============================================================================

def test_list_concat():
    """Test list concatenation operator: a + b"""
    int_schema = _hgraph.get_scalar_type_meta(int)
    list_schema = _hgraph.get_dynamic_list_type_meta(int_schema)

    v1 = _hgraph.HgValue(list_schema)
    v1.py_value = (1, 2, 3)

    v2 = _hgraph.HgValue(list_schema)
    v2.py_value = (4, 5, 6)

    result = v1 + v2
    assert result.py_value == (1, 2, 3, 4, 5, 6)


def test_list_concat_inplace():
    """Test list in-place concatenation operator: a += b"""
    int_schema = _hgraph.get_scalar_type_meta(int)
    list_schema = _hgraph.get_dynamic_list_type_meta(int_schema)

    v1 = _hgraph.HgValue(list_schema)
    v1.py_value = (1, 2, 3)

    v2 = _hgraph.HgValue(list_schema)
    v2.py_value = (4, 5, 6)

    v1 += v2
    assert v1.py_value == (1, 2, 3, 4, 5, 6)


def test_list_concat_empty():
    """Test concatenation with empty list."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    list_schema = _hgraph.get_dynamic_list_type_meta(int_schema)

    v1 = _hgraph.HgValue(list_schema)
    v1.py_value = (1, 2, 3)

    v2 = _hgraph.HgValue(list_schema)
    v2.py_value = ()

    result = v1 + v2
    assert result.py_value == (1, 2, 3)


def test_list_concat_str():
    """Test concatenation with string elements."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    list_schema = _hgraph.get_dynamic_list_type_meta(str_schema)

    v1 = _hgraph.HgValue(list_schema)
    v1.py_value = ("hello",)

    v2 = _hgraph.HgValue(list_schema)
    v2.py_value = ("world",)

    result = v1 + v2
    assert result.py_value == ("hello", "world")


# =============================================================================
# Mixed Operations Tests
# =============================================================================

def test_set_operations_preserve_original():
    """Test that set operations don't modify originals (except in-place)."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    set_schema = _hgraph.get_set_type_meta(int_schema)

    v1 = _hgraph.HgValue(set_schema)
    v1.py_value = {1, 2, 3}
    original_v1 = {1, 2, 3}

    v2 = _hgraph.HgValue(set_schema)
    v2.py_value = {3, 4, 5}
    original_v2 = {3, 4, 5}

    # Non-modifying operations
    _ = v1 | v2
    _ = v1 & v2
    _ = v1 - v2
    _ = v1 ^ v2

    # Check originals are unchanged
    assert v1.py_value == original_v1
    assert v2.py_value == original_v2


def test_dict_operations_preserve_original():
    """Test that dict operations don't modify originals (except in-place)."""
    str_schema = _hgraph.get_scalar_type_meta(str)
    int_schema = _hgraph.get_scalar_type_meta(int)
    dict_schema = _hgraph.get_dict_type_meta(str_schema, int_schema)

    v1 = _hgraph.HgValue(dict_schema)
    v1.py_value = {"a": 1, "b": 2}
    original_v1 = {"a": 1, "b": 2}

    v2 = _hgraph.HgValue(dict_schema)
    v2.py_value = {"b": 20, "c": 3}

    _ = v1 | v2

    assert v1.py_value == original_v1


def test_list_operations_preserve_original():
    """Test that list operations don't modify originals (except in-place)."""
    int_schema = _hgraph.get_scalar_type_meta(int)
    list_schema = _hgraph.get_dynamic_list_type_meta(int_schema)

    v1 = _hgraph.HgValue(list_schema)
    v1.py_value = (1, 2, 3)
    original_v1 = (1, 2, 3)

    v2 = _hgraph.HgValue(list_schema)
    v2.py_value = (4, 5, 6)

    _ = v1 + v2

    assert v1.py_value == original_v1


def test_scalar_subtract_vs_set_difference():
    """Test that - works correctly for both scalars and sets."""
    # Scalar subtraction
    int_schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(int_schema, 10)
    v2 = _hgraph.HgValue.from_python(int_schema, 3)
    result = v1 - v2
    assert result.py_value == 7

    # Set difference
    set_schema = _hgraph.get_set_type_meta(int_schema)
    s1 = _hgraph.HgValue(set_schema)
    s1.py_value = {1, 2, 3}
    s2 = _hgraph.HgValue(set_schema)
    s2.py_value = {2, 3, 4}
    result = s1 - s2
    assert result.py_value == {1}


def test_scalar_add_vs_list_concat():
    """Test that + works correctly for both scalars and lists."""
    # Scalar addition
    int_schema = _hgraph.get_scalar_type_meta(int)
    v1 = _hgraph.HgValue.from_python(int_schema, 10)
    v2 = _hgraph.HgValue.from_python(int_schema, 5)
    result = v1 + v2
    assert result.py_value == 15

    # List concatenation
    list_schema = _hgraph.get_dynamic_list_type_meta(int_schema)
    l1 = _hgraph.HgValue(list_schema)
    l1.py_value = (1, 2)
    l2 = _hgraph.HgValue(list_schema)
    l2.py_value = (3, 4)
    result = l1 + l2
    assert result.py_value == (1, 2, 3, 4)
