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

def test_create_heterogeneous_tuple_schema(simple_tuple_schema):
    """Tuple schema can be created with different element types."""
    assert simple_tuple_schema is not None
    # Tuple should have 3 fields
    assert simple_tuple_schema.field_count == 3


def test_tuple_schema_element_types(simple_tuple_schema, int_schema, string_schema, double_schema):
    """Each position in tuple can have different type."""
    # Access element types by index
    assert simple_tuple_schema.fields[0].type == int_schema
    assert simple_tuple_schema.fields[1].type == string_schema
    assert simple_tuple_schema.fields[2].type == double_schema


def test_tuple_schema_is_kind_tuple(simple_tuple_schema):
    """Tuple schema has TypeKind.Tuple."""
    assert simple_tuple_schema.kind == TypeKind.Tuple


def test_tuple_schema_has_no_names(simple_tuple_schema):
    """Tuple fields should not have meaningful names (anonymous)."""
    # Tuples are unnamed - fields should have no name or empty/None name
    for i in range(simple_tuple_schema.field_count):
        field_info = simple_tuple_schema.fields[i]
        # Name should be empty, None, or not meaningful
        assert field_info.name is None or field_info.name == ""


def test_empty_tuple_schema_allowed(type_registry):
    """Creating an empty tuple should produce a valid schema."""
    # Note: Unlike bundles, empty tuples may or may not be allowed
    # If not allowed, this test should be changed to expect an exception
    empty_tuple = type_registry.tuple().build()
    assert empty_tuple is not None
    assert empty_tuple.field_count == 0


def test_single_element_tuple_schema(single_element_tuple_schema):
    """Tuple with single element is valid."""
    assert single_element_tuple_schema is not None
    assert single_element_tuple_schema.field_count == 1


# =============================================================================
# Section 4.2: Creating and Accessing Tuple Values
# (Skipped - Tuple TypeOps not yet implemented)
# =============================================================================

# Tuple TypeOps now implemented
def test_create_tuple_value_from_schema(simple_tuple_schema):
    """Value can be created from tuple schema."""
    v = PlainValue(simple_tuple_schema)
    assert v.valid()


# Tuple TypeOps now implemented
def test_tuple_value_has_correct_schema(simple_tuple_schema):
    """Tuple value reports correct schema."""
    v = PlainValue(simple_tuple_schema)
    assert v.schema == simple_tuple_schema


# Tuple TypeOps now implemented
def test_get_tuple_view(simple_tuple_schema):
    """Can get TupleView from tuple Value."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    assert tv is not None


# Tuple TypeOps now implemented
def test_tuple_view_size(simple_tuple_schema):
    """TupleView reports correct size."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    assert tv.size() == 3


# Tuple TypeOps now implemented
def test_tuple_view_set_by_index(simple_tuple_schema):
    """TupleView allows setting elements by index."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()

    # Set values using typed setters via at()
    tv.at(0).set_int(42)
    tv.at(1).set_string("hello")
    tv.at(2).set_double(3.14)

    # Verify
    assert tv[0].as_int() == 42
    assert tv[1].as_string() == "hello"
    assert abs(tv[2].as_double() - 3.14) < 1e-10


def test_tuple_view_set_with_value(simple_tuple_schema):
    """TupleView allows setting elements with PlainValue."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()

    tv.set(0, PlainValue(100))
    assert tv[0].as_int() == 100


# Tuple TypeOps now implemented
def test_tuple_view_operator_bracket_read(simple_tuple_schema):
    """TupleView operator[] provides read access."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    tv.at(0).set_int(42)

    # Read via operator[]
    elem = tv[0]
    assert elem.as_int() == 42


# Tuple TypeOps now implemented
def test_tuple_view_operator_bracket_write(simple_tuple_schema):
    """TupleView operator[] provides write access via at().set_*()."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()

    # Write via at().set_*()
    tv.at(0).set_int(200)
    tv.at(1).set_string("world")

    assert tv[0].as_int() == 200
    assert tv[1].as_string() == "world"


# Tuple TypeOps now implemented
def test_tuple_view_at_method(simple_tuple_schema):
    """TupleView at() method provides element access."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    tv.at(0).set_int(42)

    elem = tv.at(0)
    assert elem.as_int() == 42


# Tuple TypeOps now implemented
def test_const_tuple_view_read_access(simple_tuple_schema):
    """ConstTupleView provides read-only access."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    tv.at(0).set_int(42)
    tv.at(1).set_string("hello")
    tv.at(2).set_double(3.14)

    # Get const view
    ctv = v.const_view().as_tuple()

    assert ctv.at(0).as_int() == 42
    assert ctv[1].as_string() == "hello"
    assert abs(ctv[2].as_double() - 3.14) < 1e-10


# Tuple TypeOps now implemented
def test_tuple_element_type_access(simple_tuple_schema, int_schema, string_schema, double_schema):
    """TupleView element_type() returns type at position."""
    v = PlainValue(simple_tuple_schema)
    ctv = v.const_view().as_tuple()

    assert ctv.element_type(0) == int_schema
    assert ctv.element_type(1) == string_schema
    assert ctv.element_type(2) == double_schema


# =============================================================================
# Tuple Iteration
# =============================================================================

# Tuple TypeOps now implemented
def test_tuple_iteration_by_index(simple_tuple_schema):
    """Tuple elements can be iterated by index."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    tv.at(0).set_int(1)
    tv.at(1).set_string("two")
    tv.at(2).set_double(3.0)

    ctv = v.const_view().as_tuple()

    # Iterate using size()
    values_read = []
    for i in range(ctv.size()):
        values_read.append(ctv[i].to_string())

    assert len(values_read) == 3


# Tuple TypeOps now implemented
def test_tuple_range_based_iteration(homogeneous_tuple_schema):
    """Tuple supports range-based iteration."""
    v = PlainValue(homogeneous_tuple_schema)
    tv = v.as_tuple()
    tv.at(0).set_int(10)
    tv.at(1).set_int(20)
    tv.at(2).set_int(30)

    ctv = v.const_view().as_tuple()

    # Range-based for loop
    elements = list(ctv)
    assert len(elements) == 3


# =============================================================================
# Error Conditions and Boundary Cases
# =============================================================================

# Tuple TypeOps now implemented
def test_tuple_index_out_of_bounds(simple_tuple_schema):
    """Accessing index beyond tuple size raises error."""
    v = PlainValue(simple_tuple_schema)
    ctv = v.const_view().as_tuple()

    with pytest.raises((IndexError, RuntimeError)):
        _ = ctv.at(10)


def test_tuple_negative_index_raises(simple_tuple_schema):
    """Negative index access via at() raises error (at() doesn't support negative indices)."""
    v = PlainValue(simple_tuple_schema)
    ctv = v.const_view().as_tuple()

    # at() method doesn't support negative indices (TypeError from nanobind)
    with pytest.raises((IndexError, RuntimeError, OverflowError, TypeError)):
        _ = ctv.at(-1)


def test_tuple_set_wrong_type_raises(simple_tuple_schema):
    """Setting element with wrong type raises error."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()

    # Index 0 expects int64_t, not string - from_python will fail
    with pytest.raises((TypeError, RuntimeError)):
        tv.set(0, "not an int")


# Tuple TypeOps now implemented
def test_tuple_as_wrong_view_type_raises(simple_tuple_schema):
    """Getting wrong view type raises error."""
    v = PlainValue(simple_tuple_schema)

    # Tuple should not be convertible to list view directly
    with pytest.raises((TypeError, RuntimeError)):
        _ = v.as_list()


# Tuple TypeOps now implemented
def test_non_tuple_value_as_tuple_raises():
    """Getting tuple view from non-tuple value raises error."""
    # Create a scalar value
    v = PlainValue(42)

    with pytest.raises((TypeError, RuntimeError)):
        _ = v.as_tuple()


# =============================================================================
# Tuple View Queries
# =============================================================================

# Tuple TypeOps now implemented
def test_is_tuple_on_tuple_value(simple_tuple_schema):
    """is_tuple() returns True for tuple values."""
    v = PlainValue(simple_tuple_schema)
    assert v.const_view().is_tuple()


def test_is_tuple_on_scalar_value():
    """is_tuple() returns False for scalar values."""
    v = PlainValue(42)
    assert not v.const_view().is_tuple()


# Tuple TypeOps now implemented
def test_is_bundle_on_tuple_value(simple_tuple_schema):
    """is_bundle() returns False for tuple values."""
    v = PlainValue(simple_tuple_schema)
    assert not v.const_view().is_bundle()


def test_try_as_tuple_on_tuple_value(simple_tuple_schema):
    """try_as_tuple() returns view for tuple values."""
    v = PlainValue(simple_tuple_schema)
    result = v.const_view().try_as_tuple()
    assert result is not None


def test_try_as_tuple_on_non_tuple_value():
    """try_as_tuple() returns None for non-tuple values."""
    v = PlainValue(42)
    result = v.const_view().try_as_tuple()
    assert result is None


# =============================================================================
# Section 4.3: Tuple vs Bundle Comparison
# =============================================================================

# Tuple TypeOps now implemented
def test_tuple_has_no_named_access(simple_tuple_schema):
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


# Tuple TypeOps now implemented
def test_tuple_access_by_index_only(simple_tuple_schema):
    """Tuple only supports index-based access."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    tv.at(0).set_int(42)

    # Index access works
    assert tv[0].as_int() == 42
    assert tv.at(0).as_int() == 42


# =============================================================================
# Tuple Cloning
# =============================================================================

def test_clone_tuple_value(simple_tuple_schema):
    """Tuple values can be cloned."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    tv.at(0).set_int(42)
    tv.at(1).set_string("hello")
    tv.at(2).set_double(3.14)

    # Clone
    cloned = v.const_view().clone()

    # Verify clone has same values
    ctv = cloned.const_view().as_tuple()
    assert ctv[0].as_int() == 42
    assert ctv[1].as_string() == "hello"
    assert abs(ctv[2].as_double() - 3.14) < 1e-10


def test_cloned_tuple_is_independent(simple_tuple_schema):
    """Cloned tuple is independent of original."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    tv.at(0).set_int(42)

    # Clone
    cloned = v.const_view().clone()

    # Modify original
    tv.at(0).set_int(100)

    # Clone should be unchanged
    ctv = cloned.const_view().as_tuple()
    assert ctv[0].as_int() == 42


# =============================================================================
# Tuple Equality
# =============================================================================

# Tuple TypeOps now implemented
def test_tuple_equals_same_values(simple_tuple_schema):
    """Tuples with same values are equal."""
    v1 = PlainValue(simple_tuple_schema)
    tv1 = v1.as_tuple()
    tv1.at(0).set_int(42)
    tv1.at(1).set_string("hello")
    tv1.at(2).set_double(3.14)

    v2 = PlainValue(simple_tuple_schema)
    tv2 = v2.as_tuple()
    tv2.at(0).set_int(42)
    tv2.at(1).set_string("hello")
    tv2.at(2).set_double(3.14)

    assert v1.equals(v2.const_view())


# Tuple TypeOps now implemented
def test_tuple_not_equals_different_values(simple_tuple_schema):
    """Tuples with different values are not equal."""
    v1 = PlainValue(simple_tuple_schema)
    tv1 = v1.as_tuple()
    tv1.at(0).set_int(42)
    tv1.at(1).set_string("hello")
    tv1.at(2).set_double(3.14)

    v2 = PlainValue(simple_tuple_schema)
    tv2 = v2.as_tuple()
    tv2.at(0).set_int(100)  # Different value
    tv2.at(1).set_string("hello")
    tv2.at(2).set_double(3.14)

    assert not v1.equals(v2.const_view())


# =============================================================================
# Tuple Python Interop
# =============================================================================

# Tuple TypeOps now implemented
def test_tuple_to_python(simple_tuple_schema):
    """Tuple can be converted to Python object."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    tv.at(0).set_int(42)
    tv.at(1).set_string("hello")
    tv.at(2).set_double(3.14)

    py_obj = v.to_python()

    # Should be a tuple or list
    assert isinstance(py_obj, (tuple, list))
    assert len(py_obj) == 3
    assert py_obj[0] == 42
    assert py_obj[1] == "hello"
    assert abs(py_obj[2] - 3.14) < 1e-10


# Tuple TypeOps now implemented
def test_tuple_from_python(simple_tuple_schema):
    """Tuple can be populated from Python object."""
    v = PlainValue(simple_tuple_schema)

    py_tuple = (42, "hello", 3.14)
    v.from_python(py_tuple)

    ctv = v.const_view().as_tuple()
    assert ctv[0].as_int() == 42
    assert ctv[1].as_string() == "hello"
    assert abs(ctv[2].as_double() - 3.14) < 1e-10


# =============================================================================
# Tuple To String
# =============================================================================

# Tuple TypeOps now implemented
def test_tuple_to_string(simple_tuple_schema):
    """Tuple can be converted to string representation."""
    v = PlainValue(simple_tuple_schema)
    tv = v.as_tuple()
    tv.at(0).set_int(42)
    tv.at(1).set_string("hello")
    tv.at(2).set_double(3.14)

    s = v.to_string()

    # Should contain the values in some format
    assert "42" in s
    assert "hello" in s
