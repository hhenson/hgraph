"""
Tests for Value List types.

Tests the List type from the Value type system (Section 6 of User Guide).
Lists are homogeneous indexed collections. They come in two variants:
- Dynamic lists: Can grow/shrink at runtime (default)
- Fixed-size lists: Pre-allocated with a fixed capacity

Reference: ts_design_docs/Value_USER_GUIDE.md Section 6
"""

import pytest
import math

from hgraph._feature_switch import is_feature_enabled
if not is_feature_enabled("use_cpp"):
    pytest.skip("Buffer protocol tests require C++ extension", allow_module_level=True)


# Skip all tests if C++ extension is not available
_hgraph = pytest.importorskip("hgraph._hgraph")
value = _hgraph.value  # Value types are in the value submodule

# Convenience aliases to avoid variable shadowing
Value = value.Value
TypeRegistry = value.TypeRegistry
TypeKind = value.TypeKind


# =============================================================================
# Helpers for current API (View-based)
# =============================================================================

def make_int_value(val):
    """Create a Value containing an int."""
    int_schema = value.scalar_type_meta_int64()
    v = Value(int_schema)

    v.emplace()
    v.set_int(val)
    return v


def make_double_value(val):
    """Create a Value containing a double."""
    double_schema = value.scalar_type_meta_double()
    v = Value(double_schema)

    v.emplace()
    v.set_double(val)
    return v


def make_string_value(val):
    """Create a Value containing a string."""
    string_schema = value.scalar_type_meta_string()
    v = Value(string_schema)

    v.emplace()
    v.set_string(val)
    return v


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
def dynamic_int_list_schema(type_registry, int_schema):
    """Create a dynamic list schema for int64_t elements."""
    return type_registry.list(int_schema).build()


@pytest.fixture
def dynamic_double_list_schema(type_registry, double_schema):
    """Create a dynamic list schema for double elements."""
    return type_registry.list(double_schema).build()


@pytest.fixture
def dynamic_string_list_schema(type_registry, string_schema):
    """Create a dynamic list schema for string elements."""
    return type_registry.list(string_schema).build()


@pytest.fixture
def fixed_double_list_schema(type_registry, double_schema):
    """Create a fixed-size list schema for 10 double elements."""
    return type_registry.fixed_list(double_schema, 10).build()


@pytest.fixture
def fixed_int_list_schema(type_registry, int_schema):
    """Create a fixed-size list schema for 5 int64_t elements."""
    return type_registry.fixed_list(int_schema, 5).build()


# =============================================================================
# Section 6.1: Dynamic Lists - Schema Creation
# =============================================================================

def test_create_dynamic_list_schema(dynamic_int_list_schema):
    """Dynamic list schema can be created."""
    assert dynamic_int_list_schema is not None


def test_dynamic_list_schema_is_kind_list(dynamic_int_list_schema):
    """Dynamic list schema has TypeKind.List."""
    assert dynamic_int_list_schema.kind == TypeKind.List


def test_dynamic_list_schema_element_type(dynamic_int_list_schema, int_schema):
    """Dynamic list schema has correct element type."""
    assert dynamic_int_list_schema.element_type == int_schema


def test_dynamic_list_schema_not_fixed_size(dynamic_int_list_schema):
    """Dynamic list schema is not fixed size."""
    assert not dynamic_int_list_schema.is_fixed_size()


# =============================================================================
# Section 6.1: Dynamic Lists - Value Creation and Manipulation
# (Skipped - List TypeOps not yet implemented)
# =============================================================================

# List TypeOps now implemented
def test_create_dynamic_list_value(dynamic_int_list_schema):
    """Dynamic list value can be created from schema."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    assert v.valid()


# List TypeOps now implemented
def test_dynamic_list_initially_empty(dynamic_int_list_schema):
    """Dynamic list is initially empty."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()

    assert lv.size() == 0
    assert lv.empty()


# List TypeOps now implemented
def test_list_push_back_native_type(dynamic_int_list_schema):
    """ListView.push_back() with View."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()

    v1 = make_int_value(10)
    v2 = make_int_value(20)
    v3 = make_int_value(30)
    lv.push_back(v1.view())
    lv.push_back(v2.view())
    lv.push_back(v3.view())

    assert lv.size() == 3
    assert lv[0].as_int() == 10
    assert lv[1].as_int() == 20
    assert lv[2].as_int() == 30


def test_list_push_back_with_value(dynamic_int_list_schema):
    """ListView.push_back(View) works with explicit wrapping."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()

    elem = make_int_value(40)
    lv.push_back(elem.view())

    assert lv.size() == 1
    assert lv[0].as_int() == 40


def test_list_push_back_strings(dynamic_string_list_schema):
    """Dynamic list of strings can be populated."""
    v = Value(dynamic_string_list_schema)

    v.emplace()
    lv = v.as_list()

    s1 = make_string_value("hello")
    s2 = make_string_value("world")
    lv.push_back(s1.view())
    lv.push_back(s2.view())

    assert lv.size() == 2
    assert lv[0].as_string() == "hello"
    assert lv[1].as_string() == "world"


def test_list_access_by_index(dynamic_int_list_schema):
    """List elements can be accessed by index."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    v1 = make_int_value(10)
    v2 = make_int_value(20)
    lv.push_back(v1.view())
    lv.push_back(v2.view())

    assert lv[0].as_int() == 10
    assert lv[1].as_int() == 20


def test_list_at_method(dynamic_int_list_schema):
    """List at() method provides element access."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    elem = make_int_value(10)
    lv.push_back(elem.view())

    assert lv.at(0).as_int() == 10


def test_list_modify_element(dynamic_int_list_schema):
    """List elements can be modified via at().set_int()."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    elem = make_int_value(10)
    lv.push_back(elem.view())

    lv.at(0).set_int(25)

    assert lv[0].as_int() == 25


def test_list_set_element(dynamic_int_list_schema):
    """List set() assigns to specific index."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    v1 = make_int_value(10)
    v2 = make_int_value(20)
    v3 = make_int_value(30)
    lv.push_back(v1.view())
    lv.push_back(v2.view())
    lv.push_back(v3.view())

    new_val = make_int_value(35)
    lv.set(2, new_val.view())

    assert lv[2].as_int() == 35


def test_list_front_and_back(dynamic_int_list_schema):
    """List front() and back() access first/last elements."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    v1 = make_int_value(10)
    v2 = make_int_value(20)
    v3 = make_int_value(30)
    lv.push_back(v1.view())
    lv.push_back(v2.view())
    lv.push_back(v3.view())

    assert lv.front().as_int() == 10
    assert lv.back().as_int() == 30


def test_list_pop_back(dynamic_int_list_schema):
    """List pop_back() removes last element."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    v1 = make_int_value(10)
    v2 = make_int_value(20)
    v3 = make_int_value(30)
    lv.push_back(v1.view())
    lv.push_back(v2.view())
    lv.push_back(v3.view())

    lv.pop_back()

    assert lv.size() == 2
    assert lv.back().as_int() == 20


def test_list_clear(dynamic_int_list_schema):
    """List clear() removes all elements."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    v1 = make_int_value(10)
    v2 = make_int_value(20)
    lv.push_back(v1.view())
    lv.push_back(v2.view())

    lv.clear()

    assert lv.size() == 0
    assert lv.empty()


def test_list_resize_grow(dynamic_int_list_schema):
    """List resize() grows list with default values."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    elem = make_int_value(10)
    lv.push_back(elem.view())

    lv.resize(5)

    assert lv.size() == 5
    assert lv[0].as_int() == 10


def test_list_resize_shrink(dynamic_int_list_schema):
    """List resize() shrinks list."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    v1 = make_int_value(10)
    v2 = make_int_value(20)
    v3 = make_int_value(30)
    lv.push_back(v1.view())
    lv.push_back(v2.view())
    lv.push_back(v3.view())

    lv.resize(1)

    assert lv.size() == 1
    assert lv[0].as_int() == 10


def test_list_empty_property(dynamic_int_list_schema):
    """List empty() returns correct value."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()

    assert lv.empty()

    elem = make_int_value(10)
    lv.push_back(elem.view())

    assert not lv.empty()


# List TypeOps now implemented
def test_list_iteration_by_index(dynamic_int_list_schema):
    """List elements can be iterated by index."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    v1 = make_int_value(10)
    v2 = make_int_value(20)
    v3 = make_int_value(30)
    lv.push_back(v1.view())
    lv.push_back(v2.view())
    lv.push_back(v3.view())

    clv = v.view().as_list()

    values = []
    for i in range(clv.size()):
        values.append(clv[i].as_int())

    assert values == [10, 20, 30]


def test_list_range_based_iteration(dynamic_int_list_schema):
    """List supports range-based iteration."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    v1 = make_int_value(10)
    v2 = make_int_value(20)
    v3 = make_int_value(30)
    lv.push_back(v1.view())
    lv.push_back(v2.view())
    lv.push_back(v3.view())

    clv = v.view().as_list()

    elements = list(clv)
    assert len(elements) == 3


# =============================================================================
# Section 6.2: Fixed-Size Lists - Schema Creation
# =============================================================================

def test_create_fixed_list_schema(fixed_double_list_schema):
    """Fixed-size list schema can be created."""
    assert fixed_double_list_schema is not None


def test_fixed_list_schema_is_kind_list(fixed_double_list_schema):
    """Fixed-size list schema has TypeKind.List."""
    assert fixed_double_list_schema.kind == TypeKind.List


def test_fixed_list_schema_is_fixed_size(fixed_double_list_schema):
    """Fixed-size list schema reports is_fixed_size() true."""
    assert fixed_double_list_schema.is_fixed_size()


def test_fixed_list_schema_size(fixed_double_list_schema):
    """Fixed-size list schema has correct fixed_size."""
    assert fixed_double_list_schema.fixed_size == 10


# =============================================================================
# Section 6.2: Fixed-Size Lists - Value Creation
# (Skipped - List TypeOps not yet implemented)
# =============================================================================

# List TypeOps now implemented
def test_create_fixed_list_value(fixed_double_list_schema):
    """Fixed-size list value can be created."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    assert v.valid()


# List TypeOps now implemented
def test_fixed_list_initial_size(fixed_double_list_schema):
    """Fixed-size list has size equal to fixed_size."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    lv = v.as_list()

    assert lv.size() == 10


# List TypeOps now implemented
def test_fixed_list_is_fixed_query(fixed_double_list_schema):
    """is_fixed_list() returns True for fixed-size list."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    assert v.view().is_fixed_list()


# List TypeOps now implemented
def test_fixed_list_access_by_index(fixed_double_list_schema):
    """Fixed-size list elements can be accessed by index."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()

    v1 = make_double_value(1.0)
    v2 = make_double_value(2.0)
    flv.set(0, v1.view())
    flv.set(1, v2.view())

    assert abs(flv[0].as_double() - 1.0) < 1e-10
    assert abs(flv[1].as_double() - 2.0) < 1e-10


# List TypeOps now implemented
def test_fixed_list_set_element(fixed_double_list_schema):
    """Fixed-size list set() assigns to specific index."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()

    v5 = make_double_value(5.0)
    flv.set(5, v5.view())

    assert abs(flv[5].as_double() - 5.0) < 1e-10


def test_fixed_list_reset_with_value(fixed_double_list_schema):
    """Fixed-size list reset() sets all elements to sentinel."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()

    # Set some values
    v1 = make_double_value(1.0)
    v5 = make_double_value(5.0)
    flv.set(0, v1.view())
    flv.set(5, v5.view())

    # Reset all to 0.0
    reset_val = make_double_value(0.0)
    flv.reset(reset_val.view())

    for i in range(flv.size()):
        assert abs(flv[i].as_double() - 0.0) < 1e-10


def test_fixed_list_reset_with_nan(fixed_double_list_schema):
    """Fixed-size list can be reset with NaN sentinel."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()

    nan_val = make_double_value(float('nan'))
    flv.reset(nan_val.view())

    for i in range(flv.size()):
        assert math.isnan(flv[i].as_double())


def test_fixed_list_reset_with_explicit_value(fixed_double_list_schema):
    """Fixed-size list reset(Value) works with explicit wrapping."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()

    reset_val = make_double_value(-1.0)
    flv.reset(reset_val.view())

    for i in range(flv.size()):
        assert abs(flv[i].as_double() - (-1.0)) < 1e-10


def test_dynamic_list_reset(dynamic_double_list_schema):
    """Dynamic list also supports reset()."""
    v = Value(dynamic_double_list_schema)

    v.emplace()
    lv = v.as_list()
    v1 = make_double_value(1.0)
    v2 = make_double_value(2.0)
    v3 = make_double_value(3.0)
    lv.push_back(v1.view())
    lv.push_back(v2.view())
    lv.push_back(v3.view())

    reset_val = make_double_value(0.0)
    lv.reset(reset_val.view())

    for i in range(lv.size()):
        assert abs(lv[i].as_double() - 0.0) < 1e-10


# List TypeOps now implemented
def test_fixed_list_push_back_raises(fixed_double_list_schema):
    """Fixed-size list push_back() throws."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()

    elem = make_double_value(1.0)
    with pytest.raises((RuntimeError, NotImplementedError)):
        flv.push_back(elem.view())


# List TypeOps now implemented
def test_fixed_list_pop_back_raises(fixed_double_list_schema):
    """Fixed-size list pop_back() throws."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()

    with pytest.raises((RuntimeError, NotImplementedError)):
        flv.pop_back()


# List TypeOps now implemented
def test_fixed_list_resize_raises(fixed_double_list_schema):
    """Fixed-size list resize() throws."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()

    with pytest.raises((RuntimeError, NotImplementedError)):
        flv.resize(5)


# List TypeOps now implemented
def test_fixed_list_clear_raises(fixed_double_list_schema):
    """Fixed-size list clear() throws."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()

    with pytest.raises((RuntimeError, NotImplementedError)):
        flv.clear()


# =============================================================================
# Section 6.3: Dynamic vs Fixed-Size Comparison
# =============================================================================

def test_dynamic_list_is_not_fixed(dynamic_int_list_schema):
    """Dynamic list is_fixed_size() returns False."""
    assert not dynamic_int_list_schema.is_fixed_size()


def test_fixed_list_is_fixed(fixed_double_list_schema):
    """Fixed-size list is_fixed_size() returns True."""
    assert fixed_double_list_schema.is_fixed_size()


# List TypeOps now implemented
def test_dynamic_list_size_changes(dynamic_int_list_schema):
    """Dynamic list size changes with operations."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()

    assert lv.size() == 0

    v1 = make_int_value(1)
    lv.push_back(v1.view())
    assert lv.size() == 1

    v2 = make_int_value(2)
    lv.push_back(v2.view())
    assert lv.size() == 2

    lv.pop_back()
    assert lv.size() == 1


# List TypeOps now implemented
def test_fixed_list_size_constant(fixed_int_list_schema):
    """Fixed-size list size remains constant."""
    v = Value(fixed_int_list_schema)

    v.emplace()
    flv = v.as_list()

    initial_size = flv.size()

    # Set some values
    v1 = make_int_value(100)
    v2 = make_int_value(400)
    flv.set(0, v1.view())
    flv.set(4, v2.view())

    # Size unchanged
    assert flv.size() == initial_size


# =============================================================================
# Error Conditions and Boundary Cases
# =============================================================================

# List TypeOps now implemented
def test_list_index_out_of_bounds(dynamic_int_list_schema):
    """Accessing index beyond list size raises error."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    elem = make_int_value(10)
    lv.push_back(elem.view())

    with pytest.raises((IndexError, RuntimeError)):
        _ = lv.at(10)


def test_list_negative_index_raises(dynamic_int_list_schema):
    """Negative index access via at() raises error (at() doesn't support negative indices)."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    elem = make_int_value(10)
    lv.push_back(elem.view())

    # at() method doesn't support negative indices (TypeError from nanobind)
    with pytest.raises((IndexError, RuntimeError, OverflowError, TypeError)):
        _ = lv.at(-1)


def test_list_set_wrong_type_raises(dynamic_int_list_schema):
    """Setting element with wrong type raises error."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    elem = make_int_value(10)
    lv.push_back(elem.view())

    wrong = make_string_value("not an int")
    with pytest.raises((TypeError, RuntimeError)):
        lv.set(0, wrong.view())


def test_list_push_back_wrong_type_raises(dynamic_int_list_schema):
    """push_back with wrong type raises error."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()

    wrong = make_string_value("not an int")
    with pytest.raises((TypeError, RuntimeError)):
        lv.push_back(wrong.view())


# List TypeOps now implemented
def test_non_list_value_as_list_raises():
    """Getting list view from non-list value raises error."""
    v = Value(42)

    with pytest.raises((TypeError, RuntimeError)):
        _ = v.as_list()


# List TypeOps now implemented
def test_empty_list_front_raises(dynamic_int_list_schema):
    """front() on empty list raises error."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()

    with pytest.raises((IndexError, RuntimeError)):
        _ = lv.front()


# List TypeOps now implemented
def test_empty_list_back_raises(dynamic_int_list_schema):
    """back() on empty list raises error."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()

    with pytest.raises((IndexError, RuntimeError)):
        _ = lv.back()


# List TypeOps now implemented
def test_empty_list_pop_back_raises(dynamic_int_list_schema):
    """pop_back() on empty list raises error."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()

    with pytest.raises((IndexError, RuntimeError)):
        lv.pop_back()


# =============================================================================
# List View Queries
# =============================================================================

# List TypeOps now implemented
def test_is_list_on_list_value(dynamic_int_list_schema):
    """is_list() returns True for list values."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    assert v.view().is_list()


def test_is_list_on_scalar_value():
    """is_list() returns False for scalar values."""
    v = Value(42)
    assert not v.view().is_list()


# List TypeOps now implemented
def test_is_fixed_list_on_dynamic_list(dynamic_int_list_schema):
    """is_fixed_list() returns False for dynamic lists."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    assert not v.view().is_fixed_list()


# List TypeOps now implemented
def test_is_fixed_list_on_fixed_list(fixed_int_list_schema):
    """is_fixed_list() returns True for fixed-size lists."""
    v = Value(fixed_int_list_schema)

    v.emplace()
    assert v.view().is_fixed_list()


def test_try_as_list_on_list_value(dynamic_int_list_schema):
    """try_as_list() returns view for list values."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    result = v.view().try_as_list()
    assert result is not None


def test_try_as_list_on_non_list_value():
    """try_as_list() returns None for non-list values."""
    v = Value(42)
    result = v.view().try_as_list()
    assert result is None


# =============================================================================
# Cloning Tests
# =============================================================================

def test_clone_dynamic_list(dynamic_int_list_schema):
    """Dynamic list can be cloned."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    v1 = make_int_value(10)
    v2 = make_int_value(20)
    v3 = make_int_value(30)
    lv.push_back(v1.view())
    lv.push_back(v2.view())
    lv.push_back(v3.view())

    cloned = v.view().clone()

    clv = cloned.view().as_list()
    assert clv.size() == 3
    assert clv[0].as_int() == 10
    assert clv[1].as_int() == 20
    assert clv[2].as_int() == 30


def test_clone_fixed_list(fixed_double_list_schema):
    """Fixed-size list can be cloned."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()
    d1 = make_double_value(1.0)
    d5 = make_double_value(5.0)
    flv.set(0, d1.view())
    flv.set(5, d5.view())

    cloned = v.view().clone()

    clv = cloned.view().as_list()
    assert clv.size() == 10
    assert abs(clv[0].as_double() - 1.0) < 1e-10
    assert abs(clv[5].as_double() - 5.0) < 1e-10


def test_cloned_list_is_independent(dynamic_int_list_schema):
    """Cloned list is independent of original."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    elem = make_int_value(10)
    lv.push_back(elem.view())

    cloned = v.view().clone()

    # Modify original
    new_val = make_int_value(100)
    lv.set(0, new_val.view())
    another = make_int_value(20)
    lv.push_back(another.view())

    # Clone should be unchanged
    clv = cloned.view().as_list()
    assert clv.size() == 1
    assert clv[0].as_int() == 10


# =============================================================================
# Equality Tests
# =============================================================================

# List TypeOps now implemented
def test_list_equals_same_values(dynamic_int_list_schema):
    """Lists with same values are equal."""
    v1 = Value(dynamic_int_list_schema)

    v1.emplace()
    lv1 = v1.as_list()
    e1 = make_int_value(10)
    e2 = make_int_value(20)
    lv1.push_back(e1.view())
    lv1.push_back(e2.view())

    v2 = Value(dynamic_int_list_schema)

    v2.emplace()
    lv2 = v2.as_list()
    e3 = make_int_value(10)
    e4 = make_int_value(20)
    lv2.push_back(e3.view())
    lv2.push_back(e4.view())

    assert v1.equals(v2.view())


# List TypeOps now implemented
def test_list_not_equals_different_values(dynamic_int_list_schema):
    """Lists with different values are not equal."""
    v1 = Value(dynamic_int_list_schema)

    v1.emplace()
    lv1 = v1.as_list()
    e1 = make_int_value(10)
    e2 = make_int_value(20)
    lv1.push_back(e1.view())
    lv1.push_back(e2.view())

    v2 = Value(dynamic_int_list_schema)

    v2.emplace()
    lv2 = v2.as_list()
    e3 = make_int_value(10)
    e4 = make_int_value(30)  # Different
    lv2.push_back(e3.view())
    lv2.push_back(e4.view())

    assert not v1.equals(v2.view())


# List TypeOps now implemented
def test_list_not_equals_different_lengths(dynamic_int_list_schema):
    """Lists with different lengths are not equal."""
    v1 = Value(dynamic_int_list_schema)

    v1.emplace()
    lv1 = v1.as_list()
    e1 = make_int_value(10)
    lv1.push_back(e1.view())

    v2 = Value(dynamic_int_list_schema)

    v2.emplace()
    lv2 = v2.as_list()
    e2 = make_int_value(10)
    e3 = make_int_value(20)
    lv2.push_back(e2.view())
    lv2.push_back(e3.view())

    assert not v1.equals(v2.view())


# =============================================================================
# Python Interop Tests
# =============================================================================

def test_list_to_python(dynamic_int_list_schema):
    """List can be converted to Python list."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    e1 = make_int_value(10)
    e2 = make_int_value(20)
    e3 = make_int_value(30)
    lv.push_back(e1.view())
    lv.push_back(e2.view())
    lv.push_back(e3.view())

    py_obj = v.to_python()

    assert isinstance(py_obj, list)
    assert py_obj == [10, 20, 30]


def test_list_from_python(dynamic_int_list_schema):
    """List can be populated from Python list."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    py_list = [10, 20, 30]
    v.from_python(py_list)

    clv = v.view().as_list()
    assert clv.size() == 3
    assert clv[0].as_int() == 10
    assert clv[1].as_int() == 20
    assert clv[2].as_int() == 30


def test_list_from_python_none_elements_round_trip_as_null(dynamic_int_list_schema):
    """Dynamic list supports None element states with typed schema."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    v.from_python([10, None, 30])

    lv = v.view().as_list()
    assert lv.size() == 3
    assert lv[0].as_int() == 10
    assert not lv[1].valid()
    assert lv[2].as_int() == 30
    assert v.to_python() == [10, None, 30]


def test_fixed_list_to_python(fixed_double_list_schema):
    """Fixed-size list can be converted to Python list."""
    v = Value(fixed_double_list_schema)

    v.emplace()
    flv = v.as_list()
    d1 = make_double_value(1.0)
    d2 = make_double_value(2.0)
    flv.set(0, d1.view())
    flv.set(1, d2.view())

    py_obj = v.to_python()

    assert isinstance(py_obj, list)
    assert len(py_obj) == 10


def test_fixed_list_from_python_none_elements_set_null_state(fixed_int_list_schema):
    """Fixed-size list supports None element states by index."""
    v = Value(fixed_int_list_schema)

    v.emplace()
    v.from_python([11, None, 33])

    lv = v.view().as_list()
    assert lv.size() == 5
    assert lv[0].as_int() == 11
    assert not lv[1].valid()
    assert lv[2].as_int() == 33
    assert v.to_python()[1] is None


def test_list_to_string(dynamic_int_list_schema):
    """List can be converted to string representation."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    e1 = make_int_value(10)
    e2 = make_int_value(20)
    e3 = make_int_value(30)
    lv.push_back(e1.view())
    lv.push_back(e2.view())
    lv.push_back(e3.view())

    s = v.to_string()

    assert "10" in s
    assert "20" in s
    assert "30" in s


# =============================================================================
# ListView Read-Only Tests
# =============================================================================

# List TypeOps now implemented
def test_const_list_view_read_only(dynamic_int_list_schema):
    """View.as_list() provides read access."""
    v = Value(dynamic_int_list_schema)

    v.emplace()
    lv = v.as_list()
    e1 = make_int_value(10)
    e2 = make_int_value(20)
    lv.push_back(e1.view())
    lv.push_back(e2.view())

    clv = v.view().as_list()

    assert clv.size() == 2
    assert clv[0].as_int() == 10
    assert clv.at(1).as_int() == 20
    assert clv.front().as_int() == 10
    assert clv.back().as_int() == 20
    assert not clv.empty()
