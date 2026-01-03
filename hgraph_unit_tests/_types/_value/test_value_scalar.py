"""
Test Value Type System - Scalar Values

Tests for scalar Value creation and access based on the Value_USER_GUIDE.md
Sections 2-3: Creating Scalar Values and Accessing Scalar Values

These tests verify the C++ implementation once available.
The tests are designed to work with the hgraph._hgraph extension module.
"""
import pytest

# Try to import the C++ extension module
_hgraph = pytest.importorskip("hgraph._hgraph", reason="C++ extension not available")
value = _hgraph.value  # Value types are in the value submodule

# Import Value types from the C++ extension
try:
    Value = value.PlainValue
    ValueView = value.ValueView
    ConstValueView = value.ConstValueView
except AttributeError:
    pytest.skip("Value types not yet exposed in C++ extension", allow_module_level=True)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def int_value():
    """Create an integer Value for testing."""
    return Value(42)


@pytest.fixture
def large_int_value():
    """Create a large integer Value (>256) to avoid Python's small integer cache."""
    return Value(123456789)


@pytest.fixture
def double_value():
    """Create a double/float Value for testing."""
    return Value(3.14)


@pytest.fixture
def bool_value():
    """Create a boolean Value for testing."""
    return Value(True)


@pytest.fixture
def string_value():
    """Create a string Value for testing."""
    return Value("hello")


# =============================================================================
# Section 2: Creating Scalar Values
# =============================================================================

def test_create_int_value():
    """Value can be created from an integer (int64_t)."""
    v = Value(42)
    assert v.valid()


def test_create_large_int_value():
    """Value can be created from a large integer (>256)."""
    v = Value(123456789)
    assert v.valid()


def test_create_negative_int_value():
    """Value can be created from a negative integer."""
    v = Value(-42)
    assert v.valid()


def test_create_zero_int_value():
    """Value can be created from zero."""
    v = Value(0)
    assert v.valid()


def test_create_double_value():
    """Value can be created from a double/float."""
    v = Value(3.14)
    assert v.valid()


def test_create_negative_double_value():
    """Value can be created from a negative double."""
    v = Value(-3.14)
    assert v.valid()


def test_create_bool_true_value():
    """Value can be created from True."""
    v = Value(True)
    assert v.valid()


def test_create_bool_false_value():
    """Value can be created from False."""
    v = Value(False)
    assert v.valid()


def test_create_string_value():
    """Value can be created from a string."""
    v = Value("hello")
    assert v.valid()


def test_create_empty_string_value():
    """Value can be created from an empty string."""
    v = Value("")
    assert v.valid()


def test_create_unicode_string_value():
    """Value can be created from a unicode string."""
    v = Value("hello \u4e16\u754c")
    assert v.valid()


def test_create_date_value():
    """Value can be created from a date."""
    from datetime import date
    v = Value(date(2024, 1, 15))
    assert v.valid()


def test_create_datetime_value():
    """Value can be created from a datetime."""
    from datetime import datetime
    v = Value(datetime(2024, 1, 15, 10, 30, 0))
    assert v.valid()


def test_create_timedelta_value():
    """Value can be created from a timedelta."""
    from datetime import timedelta
    v = Value(timedelta(hours=1, minutes=30))
    assert v.valid()


# =============================================================================
# Section 2.1: Scalar Value Boundaries
# =============================================================================

def test_int_max_value():
    """Value can hold maximum int64 value."""
    import sys
    max_int = 2**63 - 1
    v = Value(max_int)
    assert v.as_int() == max_int


def test_int_min_value():
    """Value can hold minimum int64 value."""
    min_int = -(2**63)
    v = Value(min_int)
    assert v.as_int() == min_int


def test_double_very_small():
    """Value can hold very small double values."""
    v = Value(1e-300)
    assert abs(v.as_double() - 1e-300) < 1e-310


def test_double_very_large():
    """Value can hold very large double values."""
    v = Value(1e300)
    assert abs(v.as_double() - 1e300) < 1e290


def test_double_special_values():
    """Value handles special double values."""
    import math
    # Positive infinity
    v_inf = Value(float('inf'))
    assert math.isinf(v_inf.as_double())

    # Negative infinity
    v_neg_inf = Value(float('-inf'))
    assert math.isinf(v_neg_inf.as_double())


def test_long_string_value():
    """Value can hold very long strings."""
    long_str = "x" * 10000
    v = Value(long_str)
    assert v.as_string() == long_str


# =============================================================================
# Section 3: Accessing Scalar Values
# =============================================================================

def test_as_int_returns_correct_value(int_value):
    """as_int() returns the stored integer value."""
    assert int_value.as_int() == 42


def test_as_double_returns_correct_value(double_value):
    """as_double() returns the stored double value."""
    assert abs(double_value.as_double() - 3.14) < 1e-10


def test_as_bool_true_returns_true(bool_value):
    """as_bool() returns True for true values."""
    assert bool_value.as_bool() is True


def test_as_bool_false_returns_false():
    """as_bool() returns False for false values."""
    v = Value(False)
    assert v.as_bool() is False


def test_as_string_returns_correct_value(string_value):
    """as_string() returns the stored string value."""
    assert string_value.as_string() == "hello"


def test_try_as_returns_value_on_match(int_value):
    """try_as_int() returns the value when types match."""
    result = int_value.const_view().try_as_int()
    assert result == 42


def test_try_as_returns_none_on_mismatch(int_value):
    """try_as_double() returns None when called on int."""
    result = int_value.const_view().try_as_double()
    assert result is None


def test_checked_as_succeeds_on_match(int_value):
    """as_int() returns the value when types match (checked access)."""
    result = int_value.as_int()
    assert result == 42


def test_checked_as_throws_on_mismatch(int_value):
    """as_double() throws when called on int (checked access)."""
    with pytest.raises((TypeError, RuntimeError)):
        int_value.as_double()


# =============================================================================
# Section 3.1: Value Views
# =============================================================================

def test_const_view_preserves_type_info(double_value):
    """ConstValueView preserves type information."""
    cv = double_value.const_view()
    assert cv.schema is not None


def test_view_schema_matches_value(int_value):
    """View's schema matches the original Value's schema."""
    v_schema = int_value.schema
    cv_schema = int_value.const_view().schema
    assert v_schema == cv_schema


# =============================================================================
# Section 3.2: Value Modification
# =============================================================================

def test_view_allows_modification(int_value):
    """ValueView allows modifying the underlying value."""
    v = int_value.view()
    v.set_int(100)
    assert int_value.as_int() == 100


def test_modification_reflects_in_original():
    """Changes through view reflect in original Value."""
    v = Value(42)
    view = v.view()
    view.set_int(100)
    assert v.as_int() == 100


def test_const_view_cannot_modify():
    """ConstValueView provides read-only access."""
    v = Value(42)
    cv = v.const_view()
    assert cv.as_int() == 42
    # Note: ConstValueView doesn't have set methods


# =============================================================================
# Section 3.3: Value Validity
# =============================================================================

def test_valid_returns_true_for_initialized(int_value):
    """valid() returns True for properly initialized values."""
    assert int_value.valid() is True


def test_valid_returns_true_for_string(string_value):
    """valid() returns True for string values."""
    assert string_value.valid() is True


# =============================================================================
# Section 3.4: Value Equality
# =============================================================================

def test_equal_int_values():
    """Two Values with same int are equal."""
    v1 = Value(42)
    v2 = Value(42)
    assert v1.equals(v2.const_view())


def test_unequal_int_values():
    """Two Values with different ints are not equal."""
    v1 = Value(42)
    v2 = Value(100)
    assert not v1.equals(v2.const_view())


def test_equal_double_values():
    """Two Values with same double are equal."""
    v1 = Value(3.14)
    v2 = Value(3.14)
    assert v1.equals(v2.const_view())


def test_equal_string_values():
    """Two Values with same string are equal."""
    v1 = Value("hello")
    v2 = Value("hello")
    assert v1.equals(v2.const_view())


def test_equal_bool_values():
    """Two Values with same bool are equal."""
    v1 = Value(True)
    v2 = Value(True)
    assert v1.equals(v2.const_view())


def test_type_mismatch_not_equal():
    """Values of different types are not equal."""
    v_int = Value(42)
    v_str = Value("42")
    assert not v_int.equals(v_str.const_view())


# =============================================================================
# Section 3.5: Value Hashing
# =============================================================================

def test_hash_returns_consistent_value():
    """hash() returns same value for same Value."""
    v = Value(42)
    h1 = v.hash()
    h2 = v.hash()
    assert h1 == h2


def test_equal_values_have_same_hash():
    """Equal values have the same hash."""
    v1 = Value(42)
    v2 = Value(42)
    assert v1.hash() == v2.hash()


def test_hash_returns_int():
    """hash() returns an integer."""
    v = Value(42)
    h = v.hash()
    assert isinstance(h, int)


# =============================================================================
# Section 3.6: Value Cloning
# =============================================================================

def test_clone_int_value():
    """Cloning creates a new Value with same content."""
    v = Value(42)
    cloned = v.const_view().clone()
    assert cloned.as_int() == 42


def test_clone_is_independent():
    """Cloned value is independent of original."""
    v = Value(42)
    cloned = v.const_view().clone()
    v.view().set_int(100)
    assert cloned.as_int() == 42  # Unchanged


def test_clone_from_mutable_view():
    """clone() works on ValueView too."""
    v = Value(42)
    clone = v.const_view().clone()
    assert clone.as_int() == 42


def test_clone_string_value(string_value):
    """Cloning string values works correctly."""
    clone = string_value.const_view().clone()
    assert clone.as_string() == "hello"


# =============================================================================
# Error Handling
# =============================================================================

def test_type_mismatch_raises_exception():
    """Type mismatch in checked conversion raises exception."""
    int_value = Value(42)
    with pytest.raises((TypeError, RuntimeError)):
        int_value.as_double()


def test_type_mismatch_message_is_informative():
    """Type mismatch exception has informative message."""
    int_value = Value(42)
    try:
        int_value.as_double()
    except (TypeError, RuntimeError) as e:
        assert "type" in str(e).lower() or "mismatch" in str(e).lower()


def test_try_as_safe_on_mismatch():
    """try_as returns None instead of throwing on mismatch."""
    int_value = Value(42)
    result = int_value.const_view().try_as_double()
    assert result is None


# =============================================================================
# Python Interop
# =============================================================================

def test_to_python_int():
    """to_python() converts int Value to Python int."""
    v = Value(42)
    py_val = v.to_python()
    assert py_val == 42
    assert isinstance(py_val, int)


def test_to_python_double():
    """to_python() converts double Value to Python float."""
    v = Value(3.14)
    py_val = v.to_python()
    assert abs(py_val - 3.14) < 1e-10
    assert isinstance(py_val, float)


def test_to_python_bool():
    """to_python() converts bool Value to Python bool."""
    v = Value(True)
    py_val = v.to_python()
    assert py_val is True


def test_to_python_string():
    """to_python() converts string Value to Python str."""
    v = Value("hello")
    py_val = v.to_python()
    assert py_val == "hello"
    assert isinstance(py_val, str)


def test_from_python_int():
    """from_python() can update Value from Python int."""
    v = Value(0)
    v.from_python(42)
    assert v.as_int() == 42


def test_from_python_preserves_type():
    """from_python() uses the Value's type for conversion."""
    v = Value(0)
    v.from_python(42)
    # Should still be stored as int64_t internally
    assert v.as_int() == 42


# =============================================================================
# To String
# =============================================================================

def test_to_string_int():
    """to_string() converts int Value to string representation."""
    v = Value(42)
    s = v.to_string()
    assert "42" in s


def test_to_string_double():
    """to_string() converts double Value to string representation."""
    v = Value(3.14)
    s = v.to_string()
    assert "3.14" in s or "3.1" in s


def test_to_string_bool_true():
    """to_string() converts true bool to string."""
    v = Value(True)
    s = v.to_string()
    assert "true" in s.lower() or "1" in s


def test_to_string_string_value():
    """to_string() on string Value shows the string."""
    v = Value("hello")
    s = v.to_string()
    assert "hello" in s
