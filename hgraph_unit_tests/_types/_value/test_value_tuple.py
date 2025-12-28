"""
Tests for Value Tuple types.

Tests the Tuple type from the Value type system (Section 4 of User Guide).
Tuples are heterogeneous indexed collections - each position can hold a different type.
Unlike bundles, tuples have NO named fields and access is by index position only.

Reference: ts_design_docs/Value_USER_GUIDE.md Section 4
"""

import pytest

# Skip all tests if C++ extension is not available
_hgraph = pytest.importorskip("hgraph._hgraph")
value = _hgraph.value  # Value types are in the value submodule

# Convenience aliases to avoid variable shadowing
PlainValue = value.PlainValue
TypeRegistry = value.TypeRegistry
TypeKind = value.TypeKind


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def type_registry():
    """Get the TypeRegistry instance."""
    return TypeRegistry.instance()


@pytest.fixture
def int_schema(type_registry):
    """Schema for int64_t scalar type."""
    return value.scalar_type_meta_int64()


@pytest.fixture
def double_schema(type_registry):
    """Schema for double scalar type."""
    return value.scalar_type_meta_double()


@pytest.fixture
def string_schema(type_registry):
    """Schema for string scalar type."""
    return value.scalar_type_meta_string()


@pytest.fixture
def bool_schema(type_registry):
    """Schema for bool scalar type."""
    return value.scalar_type_meta_bool()


@pytest.fixture
def simple_tuple_schema(type_registry, int_schema, string_schema, double_schema):
    """Create a simple tuple schema with (int64_t, string, double)."""
    return type_registry.tuple() \
        .element(int_schema) \
        .element(string_schema) \
        .element(double_schema) \
        .build()


@pytest.fixture
def homogeneous_tuple_schema(type_registry, int_schema):
    """Create a tuple of all same type (3 int64_t elements)."""
    return type_registry.tuple() \
        .element(int_schema) \
        .element(int_schema) \
        .element(int_schema) \
        .build()


@pytest.fixture
def single_element_tuple_schema(type_registry, int_schema):
    """Create a tuple with single element."""
    return type_registry.tuple() \
        .element(int_schema) \
        .build()


# =============================================================================
# Section 4.1: Creating Tuple Schemas
# =============================================================================

class TestTupleSchemaCreation:
    """Tests for tuple schema creation (Section 4.1)."""

    def test_create_heterogeneous_tuple_schema(self, simple_tuple_schema):
        """Tuple schema can be created with different element types."""
        assert simple_tuple_schema is not None
        # Tuple should have 3 fields
        assert simple_tuple_schema.field_count == 3

    def test_tuple_schema_element_types(self, simple_tuple_schema, int_schema, string_schema, double_schema):
        """Each position in tuple can have different type."""
        # Access element types by index
        assert simple_tuple_schema.fields[0].type == int_schema
        assert simple_tuple_schema.fields[1].type == string_schema
        assert simple_tuple_schema.fields[2].type == double_schema

    def test_tuple_schema_is_kind_tuple(self, simple_tuple_schema):
        """Tuple schema has TypeKind.Tuple."""
        assert simple_tuple_schema.kind == TypeKind.Tuple

    def test_tuple_schema_has_no_names(self, simple_tuple_schema):
        """Tuple fields should not have meaningful names (anonymous)."""
        # Tuples are unnamed - fields should have no name or empty/None name
        for i in range(simple_tuple_schema.field_count):
            field_info = simple_tuple_schema.fields[i]
            # Name should be empty, None, or not meaningful
            assert field_info.name is None or field_info.name == ""

    def test_empty_tuple_schema_not_allowed(self, type_registry):
        """Creating an empty tuple should fail or produce invalid schema."""
        with pytest.raises((RuntimeError, ValueError)):
            type_registry.tuple().build()

    def test_single_element_tuple_schema(self, single_element_tuple_schema):
        """Tuple with single element is valid."""
        assert single_element_tuple_schema is not None
        assert single_element_tuple_schema.field_count == 1


# =============================================================================
# Section 4.2: Creating and Accessing Tuple Values
# =============================================================================

class TestTupleValueCreation:
    """Tests for creating tuple values (Section 4.2)."""

    def test_create_tuple_value_from_schema(self, simple_tuple_schema):
        """Value can be created from tuple schema."""
        v = PlainValue(simple_tuple_schema)
        assert v.valid()

    def test_tuple_value_has_correct_schema(self, simple_tuple_schema):
        """Tuple value reports correct schema."""
        v = PlainValue(simple_tuple_schema)
        assert v.schema == simple_tuple_schema  # schema is a property


class TestTupleViewAccess:
    """Tests for accessing tuple values via TupleView (Section 4.2)."""

    def test_get_tuple_view(self, simple_tuple_schema):
        """Can get TupleView from tuple Value."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        assert tv is not None

    def test_tuple_view_size(self, simple_tuple_schema):
        """TupleView reports correct size."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        assert tv.size() == 3

    def test_tuple_view_set_by_index(self, simple_tuple_schema):
        """TupleView allows setting elements by index."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()

        # Set values - templated set auto-wraps native types
        tv.set(0, 42)
        tv.set(1, "hello")
        tv.set(2, 3.14)

        # Verify
        assert tv[0].as_int() == 42
        assert tv[1].as_string() == "hello"
        assert abs(tv[2].as_double() - 3.14) < 1e-10

    def test_tuple_view_set_with_value(self, simple_tuple_schema):
        """TupleView allows setting elements with explicit Value wrapping."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()

        tv.set(0, PlainValue(100))
        assert tv[0].as_int() == 100

    def test_tuple_view_operator_bracket_read(self, simple_tuple_schema):
        """TupleView operator[] provides read access."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        tv.set(0, 42)

        # Read via operator[]
        elem = tv[0]
        assert elem.as_int() == 42

    def test_tuple_view_operator_bracket_write(self, simple_tuple_schema):
        """TupleView operator[] provides write access via set()."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()

        # Write via set()
        tv.set(0, 200)
        tv.set(1, "world")

        assert tv[0].as_int() == 200
        assert tv[1].as_string() == "world"

    def test_tuple_view_at_method(self, simple_tuple_schema):
        """TupleView at() method provides element access."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        tv.set(0, 42)

        elem = tv.at(0)
        assert elem.as_int() == 42

    def test_const_tuple_view_read_access(self, simple_tuple_schema):
        """ConstTupleView provides read-only access."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        tv.set(0, 42)
        tv.set(1, "hello")
        tv.set(2, 3.14)

        # Get const view
        ctv = v.const_view().as_tuple()

        assert ctv.at(0).as_int() == 42
        assert ctv[1].as_string() == "hello"
        assert abs(ctv[2].as_double() - 3.14) < 1e-10

    def test_tuple_element_type_access(self, simple_tuple_schema, int_schema, string_schema, double_schema):
        """TupleView element_type() returns type at position."""
        v = PlainValue(simple_tuple_schema)
        ctv = v.const_view().as_tuple()

        assert ctv.element_type(0) == int_schema
        assert ctv.element_type(1) == string_schema
        assert ctv.element_type(2) == double_schema


class TestTupleIteration:
    """Tests for iterating over tuple elements (Section 4.2)."""

    def test_tuple_iteration_by_index(self, simple_tuple_schema):
        """Tuple elements can be iterated by index."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        tv.set(0, 1)
        tv.set(1, "two")
        tv.set(2, 3.0)

        ctv = v.const_view().as_tuple()

        # Iterate using size()
        values_read = []
        for i in range(ctv.size()):
            values_read.append(ctv[i].to_string())

        assert len(values_read) == 3

    def test_tuple_range_based_iteration(self, homogeneous_tuple_schema):
        """Tuple supports range-based iteration."""
        v = PlainValue(homogeneous_tuple_schema)
        tv = v.as_tuple()
        tv.set(0, 10)
        tv.set(1, 20)
        tv.set(2, 30)

        ctv = v.const_view().as_tuple()

        # Range-based for loop
        elements = list(ctv)
        assert len(elements) == 3


# =============================================================================
# Error Conditions and Boundary Cases
# =============================================================================

class TestTupleErrorConditions:
    """Tests for tuple error conditions and boundary cases."""

    def test_tuple_index_out_of_bounds(self, simple_tuple_schema):
        """Accessing index beyond tuple size raises error."""
        v = PlainValue(simple_tuple_schema)
        ctv = v.const_view().as_tuple()

        with pytest.raises((IndexError, RuntimeError)):
            _ = ctv.at(10)

    def test_tuple_negative_index_raises(self, simple_tuple_schema):
        """Negative index access raises error."""
        v = PlainValue(simple_tuple_schema)
        ctv = v.const_view().as_tuple()

        with pytest.raises((IndexError, RuntimeError, OverflowError)):
            _ = ctv.at(-1)

    def test_tuple_set_wrong_type_raises(self, simple_tuple_schema):
        """Setting element with wrong type raises error."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()

        # Index 0 expects int64_t, not string
        with pytest.raises((TypeError, RuntimeError)):
            tv.set(0, "not an int")

    def test_tuple_as_wrong_view_type_raises(self, simple_tuple_schema):
        """Getting wrong view type raises error."""
        v = PlainValue(simple_tuple_schema)

        # Tuple should not be convertible to bundle view directly
        with pytest.raises((TypeError, RuntimeError)):
            _ = v.as_list()

    def test_non_tuple_value_as_tuple_raises(self, int_schema):
        """Getting tuple view from non-tuple value raises error."""
        # Create a scalar value
        v = PlainValue(42)

        with pytest.raises((TypeError, RuntimeError)):
            _ = v.as_tuple()


class TestTupleViewQueries:
    """Tests for tuple type queries."""

    def test_is_tuple_on_tuple_value(self, simple_tuple_schema):
        """is_tuple() returns True for tuple values."""
        v = PlainValue(simple_tuple_schema)
        assert v.const_view().is_tuple()

    def test_is_tuple_on_scalar_value(self):
        """is_tuple() returns False for scalar values."""
        v = PlainValue(42)
        assert not v.const_view().is_tuple()

    def test_is_bundle_on_tuple_value(self, simple_tuple_schema):
        """is_bundle() returns False for tuple values."""
        v = PlainValue(simple_tuple_schema)
        assert not v.const_view().is_bundle()

    def test_try_as_tuple_on_tuple_value(self, simple_tuple_schema):
        """try_as_tuple() returns view for tuple values."""
        v = PlainValue(simple_tuple_schema)
        result = v.const_view().try_as_tuple()
        assert result is not None

    def test_try_as_tuple_on_non_tuple_value(self):
        """try_as_tuple() returns None for non-tuple values."""
        v = PlainValue(42)
        result = v.const_view().try_as_tuple()
        assert result is None


# =============================================================================
# Section 4.3: Tuple vs Bundle Comparison
# =============================================================================

class TestTupleVsBundleComparison:
    """Tests demonstrating differences between Tuple and Bundle (Section 4.3)."""

    def test_tuple_has_no_named_access(self, simple_tuple_schema):
        """Tuple does not support named field access."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()

        # Tuple should NOT have at(name) method
        assert not hasattr(tv, 'at') or \
               not callable(getattr(tv.at, '__call__', None)) or \
               'name' not in str(type(tv.at))

        # Or if at() exists, it should not accept string
        with pytest.raises((TypeError, AttributeError)):
            tv.at("x")

    def test_tuple_access_by_index_only(self, simple_tuple_schema):
        """Tuple only supports index-based access."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        tv.set(0, 42)

        # Index access works
        assert tv[0].as_int() == 42
        assert tv.at(0).as_int() == 42


class TestTupleCloning:
    """Tests for cloning tuple values."""

    def test_clone_tuple_value(self, simple_tuple_schema):
        """Tuple values can be cloned."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        tv.set(0, 42)
        tv.set(1, "hello")
        tv.set(2, 3.14)

        # Clone
        cloned = v.const_view().clone()

        # Verify clone has same values
        ctv = cloned.const_view().as_tuple()
        assert ctv[0].as_int() == 42
        assert ctv[1].as_string() == "hello"
        assert abs(ctv[2].as_double() - 3.14) < 1e-10

    def test_cloned_tuple_is_independent(self, simple_tuple_schema):
        """Cloned tuple is independent of original."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        tv.set(0, 42)

        # Clone
        cloned = v.const_view().clone()

        # Modify original
        tv.set(0, 100)

        # Clone should be unchanged
        ctv = cloned.const_view().as_tuple()
        assert ctv[0].as_int() == 42


class TestTupleEquality:
    """Tests for tuple comparison operations."""

    def test_tuple_equals_same_values(self, simple_tuple_schema):
        """Tuples with same values are equal."""
        v1 = value.PlainValue(simple_tuple_schema)
        tv1 = v1.as_tuple()
        tv1.set(0, 42)
        tv1.set(1, "hello")
        tv1.set(2, 3.14)

        v2 = value.PlainValue(simple_tuple_schema)
        tv2 = v2.as_tuple()
        tv2.set(0, 42)
        tv2.set(1, "hello")
        tv2.set(2, 3.14)

        assert v1.equals(v2)

    def test_tuple_not_equals_different_values(self, simple_tuple_schema):
        """Tuples with different values are not equal."""
        v1 = value.PlainValue(simple_tuple_schema)
        tv1 = v1.as_tuple()
        tv1.set(0, 42)
        tv1.set(1, "hello")
        tv1.set(2, 3.14)

        v2 = value.PlainValue(simple_tuple_schema)
        tv2 = v2.as_tuple()
        tv2.set(0, 100)  # Different value
        tv2.set(1, "hello")
        tv2.set(2, 3.14)

        assert not v1.equals(v2)


class TestTuplePythonInterop:
    """Tests for tuple Python interoperability."""

    def test_tuple_to_python(self, simple_tuple_schema):
        """Tuple can be converted to Python object."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        tv.set(0, 42)
        tv.set(1, "hello")
        tv.set(2, 3.14)

        py_obj = v.to_python()

        # Should be a tuple or list
        assert isinstance(py_obj, (tuple, list))
        assert len(py_obj) == 3
        assert py_obj[0] == 42
        assert py_obj[1] == "hello"
        assert abs(py_obj[2] - 3.14) < 1e-10

    def test_tuple_from_python(self, simple_tuple_schema):
        """Tuple can be populated from Python object."""
        v = PlainValue(simple_tuple_schema)

        py_tuple = (42, "hello", 3.14)
        v.from_python(py_tuple)

        ctv = v.const_view().as_tuple()
        assert ctv[0].as_int() == 42
        assert ctv[1].as_string() == "hello"
        assert abs(ctv[2].as_double() - 3.14) < 1e-10


class TestTupleToString:
    """Tests for tuple string representation."""

    def test_tuple_to_string(self, simple_tuple_schema):
        """Tuple can be converted to string representation."""
        v = PlainValue(simple_tuple_schema)
        tv = v.as_tuple()
        tv.set(0, 42)
        tv.set(1, "hello")
        tv.set(2, 3.14)

        s = v.to_string()

        # Should contain the values in some format
        assert "42" in s
        assert "hello" in s
