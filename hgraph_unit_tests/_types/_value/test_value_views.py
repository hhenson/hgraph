"""
Test Value Type System - Views

Tests for ValueView and ConstValueView based on the Value_DESIGN.md Section 6:
Value and View Classes

These tests verify the C++ implementation once available.
The tests are designed to work with the hgraph._hgraph extension module.
"""
import pytest

# Try to import the C++ extension module
_hgraph = pytest.importorskip("hgraph._hgraph", reason="C++ extension not available")
value = _hgraph.value  # Value types are in the value submodule

# Import types from the C++ extension
try:
    Value = value.PlainValue
    ValueView = value.ValueView
    ConstValueView = value.ConstValueView
    TypeRegistry = value.TypeRegistry
except AttributeError:
    pytest.skip("Value view types not yet exposed in C++ extension", allow_module_level=True)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def int_value():
    """Create an integer Value for testing."""
    return Value(42)


@pytest.fixture
def string_value():
    """Create a string Value for testing."""
    return Value("hello")


@pytest.fixture
def bundle_schema():
    """Create a bundle schema with x, y, name fields."""
    try:
        return (
            TypeRegistry.instance()
            .bundle()
            .field("x", value.scalar_type_meta_int64())
            .field("y", value.scalar_type_meta_double())
            .field("name", value.scalar_type_meta_string())
            .build()
        )
    except AttributeError:
        pytest.skip("TypeRegistry not available in C++ extension")


@pytest.fixture
def tuple_schema():
    """Create a tuple schema with (int, string, double)."""
    try:
        return (
            TypeRegistry.instance()
            .tuple()
            .element(value.scalar_type_meta_int64())
            .element(value.scalar_type_meta_string())
            .element(value.scalar_type_meta_double())
            .build()
        )
    except AttributeError:
        pytest.skip("TypeRegistry not available in C++ extension")


@pytest.fixture
def list_schema():
    """Create a list schema for int64 elements."""
    try:
        return (
            TypeRegistry.instance()
            .list(value.scalar_type_meta_int64())
            .build()
        )
    except AttributeError:
        pytest.skip("TypeRegistry not available in C++ extension")


# =============================================================================
# Section 6.2: ConstValueView Creation and Validity
# =============================================================================

def test_const_view_from_value(int_value):
    """ConstValueView can be created from Value."""
    cv = int_value.const_view()
    assert cv.valid()


def test_const_view_validity(int_value):
    """ConstValueView reports valid() correctly."""
    cv = int_value.const_view()
    assert cv.valid() is True


def test_const_view_schema_access(int_value):
    """ConstValueView provides access to schema."""
    cv = int_value.const_view()
    schema = cv.schema  # schema is a property
    assert schema is not None


# =============================================================================
# Section 6.3: ConstValueView Type Queries
# =============================================================================

def test_is_scalar_for_int(int_value):
    """is_scalar() returns True for integer values."""
    cv = int_value.const_view()
    assert cv.is_scalar() is True


def test_is_scalar_for_string(string_value):
    """is_scalar() returns True for string values."""
    cv = string_value.const_view()
    assert cv.is_scalar() is True


def test_is_bundle_for_scalar(int_value):
    """is_bundle() returns False for scalar values."""
    cv = int_value.const_view()
    assert cv.is_bundle() is False


def test_is_list_for_scalar(int_value):
    """is_list() returns False for scalar values."""
    cv = int_value.const_view()
    assert cv.is_list() is False


def test_is_set_for_scalar(int_value):
    """is_set() returns False for scalar values."""
    cv = int_value.const_view()
    assert cv.is_set() is False


def test_is_map_for_scalar(int_value):
    """is_map() returns False for scalar values."""
    cv = int_value.const_view()
    assert cv.is_map() is False


def test_is_tuple_for_scalar(int_value):
    """is_tuple() returns False for scalar values."""
    cv = int_value.const_view()
    assert cv.is_tuple() is False


# =============================================================================
# Composite Type Queries (skipped - TypeOps not implemented)
# =============================================================================

def test_is_bundle_for_bundle_value(bundle_schema):
    """is_bundle() returns True for bundle values."""
    v = Value(bundle_schema)
    cv = v.const_view()
    assert cv.is_bundle() is True


def test_is_scalar_for_bundle(bundle_schema):
    """is_scalar() returns False for bundle values."""
    v = Value(bundle_schema)
    cv = v.const_view()
    assert cv.is_scalar() is False


def test_is_tuple_for_tuple_value(tuple_schema):
    """is_tuple() returns True for tuple values."""
    v = Value(tuple_schema)
    cv = v.const_view()
    assert cv.is_tuple() is True


def test_is_list_for_list_value(list_schema):
    """is_list() returns True for list values."""
    v = Value(list_schema)
    cv = v.const_view()
    assert cv.is_list() is True


# =============================================================================
# ConstValueView Scalar Access
# =============================================================================

def test_is_scalar_type_correct(int_value):
    """is_int() returns True for integer values."""
    cv = int_value.const_view()
    assert cv.is_int() is True


def test_is_scalar_type_incorrect(int_value):
    """is_double() returns False for integer values."""
    cv = int_value.const_view()
    assert cv.is_double() is False


def test_as_scalar_read(int_value):
    """as<T>() provides read access to scalar value."""
    cv = int_value.const_view()
    assert cv.as_int() == 42


def test_try_as_scalar_success(int_value):
    """try_as<T>() returns value when type matches."""
    cv = int_value.const_view()
    result = cv.try_as_int()
    assert result == 42


def test_try_as_scalar_failure(int_value):
    """try_as<T>() returns None when type doesn't match."""
    cv = int_value.const_view()
    result = cv.try_as_double()
    assert result is None


def test_checked_as_scalar_success(int_value):
    """as_int() returns value when type matches (checked access)."""
    cv = int_value.const_view()
    assert cv.as_int() == 42


def test_checked_as_scalar_throws(int_value):
    """as_double() throws when type doesn't match (checked access)."""
    cv = int_value.const_view()
    with pytest.raises(RuntimeError):
        cv.as_double()


# =============================================================================
# ConstValueView Operations
# =============================================================================

def test_equals_same_value():
    """equals() returns True for same values."""
    a = Value(42)
    b = Value(42)
    assert a.const_view().equals(b.const_view())


def test_equals_different_value():
    """equals() returns False for different values."""
    a = Value(42)
    b = Value(100)
    assert not a.const_view().equals(b.const_view())


def test_hash_returns_int(int_value):
    """hash() returns an integer."""
    cv = int_value.const_view()
    h = cv.hash()
    assert isinstance(h, int)


def test_to_string_returns_str(int_value):
    """to_string() returns a string representation."""
    cv = int_value.const_view()
    s = cv.to_string()
    assert isinstance(s, str)
    assert s == "42"


def test_clone_creates_owning_copy(int_value):
    """clone() creates an owning Value copy."""
    cv = int_value.const_view()
    cloned = cv.clone()
    assert cloned.valid()
    assert cloned.const_view().as_int() == 42


# =============================================================================
# Safe View Conversions (try_as_* methods)
# =============================================================================

def test_try_as_bundle_returns_none_for_scalar(int_value):
    """try_as_bundle() returns None for scalar values."""
    cv = int_value.const_view()
    result = cv.try_as_bundle()
    assert result is None


def test_try_as_list_returns_none_for_scalar(int_value):
    """try_as_list() returns None for scalar values."""
    cv = int_value.const_view()
    result = cv.try_as_list()
    assert result is None


def test_try_as_bundle_returns_view_for_bundle(bundle_schema):
    """try_as_bundle() returns ConstBundleView for bundle values."""
    v = Value(bundle_schema)
    cv = v.const_view()
    result = cv.try_as_bundle()
    assert result is not None


def test_try_as_tuple_returns_view_for_tuple(tuple_schema):
    """try_as_tuple() returns ConstTupleView for tuple values."""
    v = Value(tuple_schema)
    cv = v.const_view()
    result = cv.try_as_tuple()
    assert result is not None


# =============================================================================
# Throwing View Conversions (as_* methods)
# =============================================================================

def test_as_bundle_throws_for_scalar(int_value):
    """as_bundle() throws for scalar values."""
    cv = int_value.const_view()
    with pytest.raises(RuntimeError):
        cv.as_bundle()


def test_as_list_throws_for_scalar(int_value):
    """as_list() throws for scalar values."""
    cv = int_value.const_view()
    with pytest.raises(RuntimeError):
        cv.as_list()


def test_as_bundle_succeeds_for_bundle(bundle_schema):
    """as_bundle() succeeds for bundle values."""
    v = Value(bundle_schema)
    cv = v.const_view()
    bv = cv.as_bundle()
    assert bv is not None


# =============================================================================
# Section 6.4: ValueView Tests
# =============================================================================

def test_view_from_value(int_value):
    """ValueView can be created from Value."""
    v = int_value.view()
    assert v.valid()


def test_view_validity(int_value):
    """ValueView reports valid() correctly."""
    v = int_value.view()
    assert v.valid() is True


def test_view_schema_access(int_value):
    """ValueView provides access to schema."""
    v = int_value.view()
    schema = v.schema  # schema is a property
    assert schema is not None


# =============================================================================
# ValueView Mutable Access
# =============================================================================

def test_as_mutable_access():
    """as<T>() provides mutable access."""
    v = Value(42)
    view = v.view()
    view.set_int(100)
    assert v.const_view().as_int() == 100


def test_mutable_data_access(int_value):
    """data() provides raw data pointer (as integer for FFI)."""
    view = int_value.view()
    assert view.data() is not None


# =============================================================================
# ValueView copy_from
# =============================================================================

def test_copy_from_same_type():
    """copy_from() copies value from another view."""
    a = Value(42)
    b = Value(100)
    a.view().copy_from(b.const_view())
    assert a.const_view().as_int() == 100


def test_copy_from_preserves_source():
    """copy_from() doesn't modify source."""
    a = Value(42)
    b = Value(100)
    a.view().copy_from(b.const_view())
    assert b.const_view().as_int() == 100


# =============================================================================
# ValueView from_python
# =============================================================================

def test_from_python_int():
    """from_python() can update value from Python object."""
    v = Value(0)
    view = v.view()
    view.from_python(123)
    assert v.const_view().as_int() == 123


def test_from_python_string():
    """from_python() can update string value."""
    v = Value("")
    view = v.view()
    view.from_python("updated")
    assert v.const_view().as_string() == "updated"


# =============================================================================
# ValueView Mutable Conversions (skipped - composite not implemented)
# =============================================================================

def test_as_bundle_mutable(bundle_schema):
    """as_bundle() returns mutable BundleView."""
    v = Value(bundle_schema)
    view = v.view()
    bv = view.as_bundle()
    assert bv is not None


def test_try_as_bundle_mutable(bundle_schema):
    """try_as_bundle() returns mutable BundleView."""
    v = Value(bundle_schema)
    view = v.view()
    bv = view.try_as_bundle()
    assert bv is not None


# =============================================================================
# View Inheritance
# =============================================================================

def test_value_view_has_const_methods(int_value):
    """ValueView has all ConstValueView methods."""
    view = int_value.view()
    assert hasattr(view, 'valid')
    assert hasattr(view, 'schema')
    assert hasattr(view, 'is_scalar')
    assert hasattr(view, 'as_int')
    assert hasattr(view, 'equals')
    assert hasattr(view, 'hash')
    assert hasattr(view, 'to_string')


def test_value_view_has_mutable_methods(int_value):
    """ValueView has mutable-only methods."""
    view = int_value.view()
    assert hasattr(view, 'copy_from')
    assert hasattr(view, 'from_python')


# =============================================================================
# Type Checking Through Views
# =============================================================================

def test_is_type_with_same_schema(int_value):
    """is_type() returns True for matching schema."""
    cv = int_value.const_view()
    schema = cv.schema
    assert cv.is_type(schema) is True


def test_is_type_with_different_schema():
    """is_type() returns False for different schema."""
    int_val = Value(42)
    str_val = Value("hello")
    int_schema = int_val.const_view().schema
    assert str_val.const_view().is_type(int_schema) is False


# =============================================================================
# View Lifetime and Reference Safety
# =============================================================================

def test_view_reflects_value_changes():
    """Changes through view are reflected in value."""
    v = Value(42)
    view = v.view()
    view.set_int(100)
    assert v.const_view().as_int() == 100


def test_multiple_views_same_value():
    """Multiple views can reference same value."""
    v = Value(42)
    v1 = v.view()
    v2 = v.view()
    v1.set_int(100)
    assert v2.as_int() == 100


def test_clone_is_independent():
    """Cloned value is independent of original."""
    v = Value(42)
    cloned = v.const_view().clone()
    v.view().set_int(100)
    assert cloned.const_view().as_int() == 42


# =============================================================================
# Python Interop Through Views
# =============================================================================

def test_const_view_to_python(int_value):
    """ConstValueView.to_python() converts to Python object."""
    cv = int_value.const_view()
    py_obj = cv.to_python()
    assert py_obj == 42


def test_value_view_to_python(int_value):
    """ValueView.to_python() converts to Python object."""
    v = int_value.view()
    py_obj = v.to_python()
    assert py_obj == 42


def test_value_view_from_python():
    """ValueView.from_python() updates from Python object."""
    v = Value(0)
    v.view().from_python(99)
    assert v.const_view().as_int() == 99


# =============================================================================
# Edge Cases
# =============================================================================

def test_view_of_empty_string():
    """View works with empty string value."""
    v = Value("")
    cv = v.const_view()
    assert cv.as_string() == ""


def test_view_of_zero():
    """View works with zero value."""
    v = Value(0)
    cv = v.const_view()
    assert cv.as_int() == 0


def test_view_of_false():
    """View works with False value."""
    v = Value(False)
    cv = v.const_view()
    assert cv.as_bool() is False


def test_view_of_negative_number():
    """View works with negative numbers."""
    v = Value(-42)
    cv = v.const_view()
    assert cv.as_int() == -42


def test_view_of_large_number():
    """View works with large numbers (>256)."""
    v = Value(123456789)
    cv = v.const_view()
    assert cv.as_int() == 123456789
