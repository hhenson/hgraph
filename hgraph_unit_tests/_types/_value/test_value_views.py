"""
Test Value Type System - Views

Tests for ValueView and ConstValueView based on the Value_DESIGN.md Section 6:
Value and View Classes

These tests verify the C++ implementation once available.
The tests are designed to work with the hgraph._hgraph extension module.
"""
import pytest
from datetime import date, datetime, timedelta

# Try to import the C++ extension module
# Tests will be skipped if not available
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
# Section 6.2: ConstValueView Tests
# =============================================================================

class TestConstValueViewCreation:
    """Tests for ConstValueView creation and validity."""

    def test_const_view_from_value(self, int_value):
        """ConstValueView can be created from Value."""
        cv = int_value.const_view()
        assert cv.valid()

    def test_const_view_validity(self, int_value):
        """ConstValueView reports valid() correctly."""
        cv = int_value.const_view()
        assert cv.valid() is True

    def test_const_view_schema_access(self, int_value):
        """ConstValueView provides access to schema."""
        cv = int_value.const_view()
        schema = cv.schema  # schema is a property, not a method
        assert schema is not None


class TestConstValueViewTypeQueries:
    """Tests for ConstValueView type queries (Design Section 6.3)."""

    def test_is_scalar_for_int(self, int_value):
        """is_scalar() returns True for integer values."""
        cv = int_value.const_view()
        assert cv.is_scalar() is True

    def test_is_scalar_for_string(self, string_value):
        """is_scalar() returns True for string values."""
        cv = string_value.const_view()
        assert cv.is_scalar() is True

    def test_is_bundle_for_scalar(self, int_value):
        """is_bundle() returns False for scalar values."""
        cv = int_value.const_view()
        assert cv.is_bundle() is False

    def test_is_list_for_scalar(self, int_value):
        """is_list() returns False for scalar values."""
        cv = int_value.const_view()
        assert cv.is_list() is False

    def test_is_set_for_scalar(self, int_value):
        """is_set() returns False for scalar values."""
        cv = int_value.const_view()
        assert cv.is_set() is False

    def test_is_map_for_scalar(self, int_value):
        """is_map() returns False for scalar values."""
        cv = int_value.const_view()
        assert cv.is_map() is False

    def test_is_tuple_for_scalar(self, int_value):
        """is_tuple() returns False for scalar values."""
        cv = int_value.const_view()
        assert cv.is_tuple() is False


@pytest.mark.skip(reason="Composite TypeOps not yet implemented - ops is nullptr")
class TestConstValueViewBundleQueries:
    """Tests for ConstValueView with bundle types."""

    def test_is_bundle_for_bundle_value(self, bundle_schema):
        """is_bundle() returns True for bundle values."""
        v = Value(bundle_schema)
        cv = v.const_view()
        assert cv.is_bundle() is True

    def test_is_scalar_for_bundle(self, bundle_schema):
        """is_scalar() returns False for bundle values."""
        v = Value(bundle_schema)
        cv = v.const_view()
        assert cv.is_scalar() is False


@pytest.mark.skip(reason="Composite TypeOps not yet implemented - ops is nullptr")
class TestConstValueViewTupleQueries:
    """Tests for ConstValueView with tuple types."""

    def test_is_tuple_for_tuple_value(self, tuple_schema):
        """is_tuple() returns True for tuple values."""
        v = Value(tuple_schema)
        cv = v.const_view()
        assert cv.is_tuple() is True

    def test_is_bundle_for_tuple(self, tuple_schema):
        """is_bundle() returns False for tuple values (tuples are unnamed)."""
        v = Value(tuple_schema)
        cv = v.const_view()
        assert cv.is_bundle() is False


@pytest.mark.skip(reason="Composite TypeOps not yet implemented - ops is nullptr")
class TestConstValueViewListQueries:
    """Tests for ConstValueView with list types."""

    def test_is_list_for_list_value(self, list_schema):
        """is_list() returns True for list values."""
        v = Value(list_schema)
        cv = v.const_view()
        assert cv.is_list() is True


class TestConstValueViewScalarAccess:
    """Tests for ConstValueView scalar type access."""

    def test_is_scalar_type_correct(self, int_value):
        """is_scalar_type<T>() returns True for correct type."""
        cv = int_value.const_view()
        # In Python this would be is_int_type() or similar
        assert cv.is_int_type() is True

    def test_is_scalar_type_incorrect(self, int_value):
        """is_scalar_type<T>() returns False for incorrect type."""
        cv = int_value.const_view()
        assert cv.is_double_type() is False

    def test_as_scalar_read(self, int_value):
        """as<T>() provides read access to scalar value."""
        cv = int_value.const_view()
        assert cv.as_int() == 42

    def test_try_as_scalar_success(self, int_value):
        """try_as<T>() returns value when type matches."""
        cv = int_value.const_view()
        result = cv.try_as_int()
        assert result == 42

    def test_try_as_scalar_failure(self, int_value):
        """try_as<T>() returns None when type doesn't match."""
        cv = int_value.const_view()
        result = cv.try_as_double()
        assert result is None

    def test_checked_as_scalar_success(self, int_value):
        """checked_as<T>() returns value when type matches."""
        cv = int_value.const_view()
        assert cv.checked_as_int() == 42

    def test_checked_as_scalar_throws(self, int_value):
        """checked_as<T>() throws when type doesn't match."""
        cv = int_value.const_view()
        with pytest.raises(RuntimeError):
            cv.checked_as_double()


class TestConstValueViewOperations:
    """Tests for ConstValueView operations."""

    def test_equals_same_value(self):
        """equals() returns True for same values."""
        a = Value(42)
        b = Value(42)
        assert a.const_view().equals(b.const_view())

    def test_equals_different_value(self):
        """equals() returns False for different values."""
        a = Value(42)
        b = Value(100)
        assert not a.const_view().equals(b.const_view())

    def test_hash_returns_int(self, int_value):
        """hash() returns an integer."""
        cv = int_value.const_view()
        h = cv.hash()
        assert isinstance(h, int)

    def test_to_string_returns_str(self, int_value):
        """to_string() returns a string representation."""
        cv = int_value.const_view()
        s = cv.to_string()
        assert isinstance(s, str)
        assert s == "42"

    def test_clone_creates_owning_copy(self, int_value):
        """clone() creates an owning Value copy."""
        cv = int_value.const_view()
        cloned = cv.clone()
        assert cloned.valid()
        assert cloned.const_view().as_int() == 42


class TestConstValueViewSafeConversions:
    """Tests for safe view conversions (try_as_* methods)."""

    def test_try_as_bundle_returns_none_for_scalar(self, int_value):
        """try_as_bundle() returns None for scalar values."""
        cv = int_value.const_view()
        result = cv.try_as_bundle()
        assert result is None

    def test_try_as_list_returns_none_for_scalar(self, int_value):
        """try_as_list() returns None for scalar values."""
        cv = int_value.const_view()
        result = cv.try_as_list()
        assert result is None

    def test_try_as_bundle_returns_view_for_bundle(self, bundle_schema):
        """try_as_bundle() returns ConstBundleView for bundle values."""
        v = Value(bundle_schema)
        cv = v.const_view()
        result = cv.try_as_bundle()
        assert result is not None

    def test_try_as_tuple_returns_view_for_tuple(self, tuple_schema):
        """try_as_tuple() returns ConstTupleView for tuple values."""
        v = Value(tuple_schema)
        cv = v.const_view()
        result = cv.try_as_tuple()
        assert result is not None


class TestConstValueViewThrowingConversions:
    """Tests for throwing view conversions (as_* methods)."""

    def test_as_bundle_throws_for_scalar(self, int_value):
        """as_bundle() throws for scalar values."""
        cv = int_value.const_view()
        with pytest.raises(RuntimeError):
            cv.as_bundle()

    def test_as_list_throws_for_scalar(self, int_value):
        """as_list() throws for scalar values."""
        cv = int_value.const_view()
        with pytest.raises(RuntimeError):
            cv.as_list()

    def test_as_bundle_succeeds_for_bundle(self, bundle_schema):
        """as_bundle() succeeds for bundle values."""
        v = Value(bundle_schema)
        cv = v.const_view()
        bv = cv.as_bundle()
        assert bv is not None


# =============================================================================
# Section 6.4: ValueView Tests
# =============================================================================

class TestValueViewCreation:
    """Tests for ValueView creation."""

    def test_view_from_value(self, int_value):
        """ValueView can be created from Value."""
        v = int_value.view()
        assert v.valid()

    def test_view_validity(self, int_value):
        """ValueView reports valid() correctly."""
        v = int_value.view()
        assert v.valid() is True

    def test_view_schema_access(self, int_value):
        """ValueView provides access to schema."""
        v = int_value.view()
        schema = v.schema  # schema is a property
        assert schema is not None


class TestValueViewMutableAccess:
    """Tests for ValueView mutable access."""

    def test_as_mutable_access(self):
        """as<T>() provides mutable access."""
        v = Value(42)
        view = v.view()
        # Modify through view
        view.set_int(100)
        # Verify change
        assert v.const_view().as_int() == 100

    def test_mutable_data_access(self, int_value):
        """data() provides raw mutable pointer."""
        view = int_value.view()
        # data() should not be None
        assert view.data() is not None


class TestValueViewCopyFrom:
    """Tests for ValueView copy_from method."""

    def test_copy_from_same_type(self):
        """copy_from() copies value from another view."""
        a = Value(42)
        b = Value(100)
        a.view().copy_from(b.const_view())
        assert a.const_view().as_int() == 100

    def test_copy_from_preserves_source(self):
        """copy_from() doesn't modify source."""
        a = Value(42)
        b = Value(100)
        a.view().copy_from(b.const_view())
        assert b.const_view().as_int() == 100


class TestValueViewFromPython:
    """Tests for ValueView from_python method."""

    def test_from_python_int(self):
        """from_python() can update value from Python object."""
        v = Value(0)
        view = v.view()
        view.from_python(123)
        assert v.const_view().as_int() == 123

    def test_from_python_string(self):
        """from_python() can update string value."""
        v = Value("")
        view = v.view()
        view.from_python("updated")
        assert v.const_view().as_string() == "updated"


class TestValueViewMutableConversions:
    """Tests for ValueView mutable conversions."""

    def test_as_bundle_mutable(self, bundle_schema):
        """as_bundle() returns mutable BundleView."""
        v = Value(bundle_schema)
        view = v.view()
        bv = view.as_bundle()
        assert bv is not None

    def test_try_as_bundle_mutable(self, bundle_schema):
        """try_as_bundle() returns mutable BundleView."""
        v = Value(bundle_schema)
        view = v.view()
        bv = view.try_as_bundle()
        assert bv is not None


# =============================================================================
# View Inheritance and Type Query Tests
# =============================================================================

class TestViewInheritance:
    """Tests for view inheritance (ValueView extends ConstValueView)."""

    def test_value_view_has_const_methods(self, int_value):
        """ValueView has all ConstValueView methods."""
        view = int_value.view()
        # All const methods should be available
        assert hasattr(view, 'valid')
        assert hasattr(view, 'schema')
        assert hasattr(view, 'is_scalar')
        assert hasattr(view, 'as_int')
        assert hasattr(view, 'equals')
        assert hasattr(view, 'hash')
        assert hasattr(view, 'to_string')
        assert hasattr(view, 'clone')

    def test_value_view_has_mutable_methods(self, int_value):
        """ValueView has mutable-only methods."""
        view = int_value.view()
        assert hasattr(view, 'copy_from')
        assert hasattr(view, 'from_python')


# =============================================================================
# Type Checking Through Views
# =============================================================================

class TestTypeCheckingThroughViews:
    """Tests for type checking through views."""

    def test_is_type_with_same_schema(self, int_value):
        """is_type() returns True for matching schema."""
        cv = int_value.const_view()
        schema = cv.schema  # schema is a property
        assert cv.is_type(schema) is True

    def test_is_type_with_different_schema(self):
        """is_type() returns False for different schema."""
        int_val = Value(42)
        str_val = Value("hello")
        int_schema = int_val.const_view().schema  # schema is a property
        assert str_val.const_view().is_type(int_schema) is False


# =============================================================================
# View Lifetime and Reference Safety
# =============================================================================

class TestViewLifetimeSafety:
    """Tests for view lifetime and reference safety."""

    def test_view_reflects_value_changes(self):
        """Changes through view are reflected in value."""
        v = Value(42)
        view = v.view()
        view.set_int(100)
        # Changes should be visible through const_view
        assert v.const_view().as_int() == 100

    def test_multiple_views_same_value(self):
        """Multiple views can reference same value."""
        v = Value(42)
        v1 = v.view()
        v2 = v.view()
        v1.set_int(100)
        # Both views see the change
        assert v2.as_int() == 100

    def test_clone_is_independent(self):
        """Cloned value is independent of original."""
        v = Value(42)
        cloned = v.const_view().clone()
        v.view().set_int(100)
        # Clone should be unchanged
        assert cloned.const_view().as_int() == 42


# =============================================================================
# Python Interop Through Views
# =============================================================================

class TestViewPythonInterop:
    """Tests for Python interop through views."""

    def test_const_view_to_python(self, int_value):
        """ConstValueView.to_python() converts to Python object."""
        cv = int_value.const_view()
        py_obj = cv.to_python()
        assert py_obj == 42

    def test_value_view_to_python(self, int_value):
        """ValueView.to_python() converts to Python object."""
        v = int_value.view()
        py_obj = v.to_python()
        assert py_obj == 42

    def test_value_view_from_python(self):
        """ValueView.from_python() updates from Python object."""
        v = Value(0)
        v.view().from_python(99)
        assert v.const_view().as_int() == 99


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================

class TestViewEdgeCases:
    """Tests for edge cases in view handling."""

    def test_view_of_empty_string(self):
        """View works with empty string value."""
        v = Value("")
        cv = v.const_view()
        assert cv.as_string() == ""

    def test_view_of_zero(self):
        """View works with zero value."""
        v = Value(0)
        cv = v.const_view()
        assert cv.as_int() == 0

    def test_view_of_false(self):
        """View works with False value."""
        v = Value(False)
        cv = v.const_view()
        assert cv.as_bool() is False

    def test_view_of_negative_number(self):
        """View works with negative numbers."""
        v = Value(-42)
        cv = v.const_view()
        assert cv.as_int() == -42

    def test_view_of_large_number(self):
        """View works with large numbers (>256)."""
        v = Value(123456789)
        cv = v.const_view()
        assert cv.as_int() == 123456789
