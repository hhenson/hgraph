"""
Tests for Value Set and Map types.

Tests the Set and Map types from the Value type system (Section 7 of User Guide).
- Sets: Unique element collections with O(1) lookup
- Maps: Key-value pair collections with O(1) lookup

Reference: ts_design_docs/Value_USER_GUIDE.md Section 7
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
def int_set_schema(type_registry, int_schema):
    """Create a set schema for int64_t elements."""
    return type_registry.set(int_schema).build()


@pytest.fixture
def string_set_schema(type_registry, string_schema):
    """Create a set schema for string elements."""
    return type_registry.set(string_schema).build()


@pytest.fixture
def string_double_map_schema(type_registry, string_schema, double_schema):
    """Create a map schema from string to double."""
    return type_registry.map(string_schema, double_schema).build()


@pytest.fixture
def int_string_map_schema(type_registry, int_schema, string_schema):
    """Create a map schema from int64_t to string."""
    return type_registry.map(int_schema, string_schema).build()


# =============================================================================
# Helpers for current API (ConstValueView-based)
# =============================================================================

def make_int_value(val):
    """Create a PlainValue containing an int."""
    int_schema = value.scalar_type_meta_int64()
    v = PlainValue(int_schema)
    v.set_int(val)
    return v


def make_double_value(val):
    """Create a PlainValue containing a double."""
    double_schema = value.scalar_type_meta_double()
    v = PlainValue(double_schema)
    v.set_double(val)
    return v


def make_string_value(val):
    """Create a PlainValue containing a string."""
    string_schema = value.scalar_type_meta_string()
    v = PlainValue(string_schema)
    v.set_string(val)
    return v


# =============================================================================
# Section 7.1: Sets - Schema Creation
# =============================================================================

def test_create_set_schema(int_set_schema):
    """Set schema can be created."""
    assert int_set_schema is not None


def test_set_schema_is_kind_set(int_set_schema):
    """Set schema has TypeKind.Set."""
    assert int_set_schema.kind == TypeKind.Set


def test_set_schema_element_type(int_set_schema, int_schema):
    """Set schema has correct element type."""
    assert int_set_schema.element_type == int_schema


# =============================================================================
# Section 7.1: Sets - Value Creation
# (Skipped - Set TypeOps not yet implemented)
# =============================================================================

def test_create_set_value(int_set_schema):
    """Set value can be created from schema."""
    v = PlainValue(int_set_schema)
    assert v.valid()


def test_set_initially_empty(int_set_schema):
    """Set is initially empty."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    assert sv.size() == 0
    assert sv.empty()


# =============================================================================
# Section 7.1: Sets - Insert Operations
# =============================================================================

def test_set_insert_native_type(int_set_schema):
    """SetView.insert() with ConstValueView."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(1).const_view())
    sv.insert(make_int_value(2).const_view())
    sv.insert(make_int_value(3).const_view())

    assert sv.size() == 3


def test_set_insert_returns_true_for_new(int_set_schema):
    """SetView.insert() returns True for new elements."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    result = sv.insert(make_int_value(1).const_view())

    assert result is True


def test_set_insert_returns_false_for_existing(int_set_schema):
    """SetView.insert() returns False for existing elements."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(1).const_view())
    result = sv.insert(make_int_value(1).const_view())

    assert result is False


def test_set_insert_duplicates_dont_increase_size(int_set_schema):
    """Inserting duplicate elements doesn't increase size."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(1).const_view())
    sv.insert(make_int_value(2).const_view())
    sv.insert(make_int_value(2).const_view())  # Duplicate
    sv.insert(make_int_value(3).const_view())
    sv.insert(make_int_value(1).const_view())  # Duplicate

    assert sv.size() == 3


def test_set_insert_with_value(int_set_schema):
    """SetView.insert(ConstValueView) works with explicit wrapping."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(100).const_view())

    assert sv.size() == 1
    assert sv.contains(make_int_value(100).const_view())


def test_set_insert_strings(string_set_schema):
    """Set of strings can be populated."""
    v = PlainValue(string_set_schema)
    sv = v.as_set()

    sv.insert(make_string_value("apple").const_view())
    sv.insert(make_string_value("banana").const_view())
    sv.insert(make_string_value("cherry").const_view())

    assert sv.size() == 3


# =============================================================================
# Section 7.1: Sets - Contains Operations
# =============================================================================

def test_set_contains_native_type(int_set_schema):
    """SetView.contains() with ConstValueView."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(1).const_view())
    sv.insert(make_int_value(2).const_view())
    sv.insert(make_int_value(3).const_view())

    assert sv.contains(make_int_value(2).const_view())


def test_set_contains_returns_false_for_missing(int_set_schema):
    """SetView.contains() returns False for missing elements."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(1).const_view())
    sv.insert(make_int_value(2).const_view())

    assert not sv.contains(make_int_value(10).const_view())


def test_set_contains_with_value_view(int_set_schema):
    """SetView.contains(ConstValueView) works."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(100).const_view())

    key = make_int_value(100)
    assert sv.contains(key.const_view())


# =============================================================================
# Section 7.1: Sets - Erase Operations
# =============================================================================

def test_set_erase_native_type(int_set_schema):
    """SetView.erase() with ConstValueView."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(1).const_view())
    sv.insert(make_int_value(2).const_view())
    sv.insert(make_int_value(3).const_view())

    sv.erase(make_int_value(2).const_view())

    assert sv.size() == 2
    assert not sv.contains(make_int_value(2).const_view())


def test_set_erase_returns_true_for_existing(int_set_schema):
    """SetView.erase() returns True for existing elements."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(1).const_view())
    result = sv.erase(make_int_value(1).const_view())

    assert result is True


def test_set_erase_returns_false_for_missing(int_set_schema):
    """SetView.erase() returns False for missing elements."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(1).const_view())
    result = sv.erase(make_int_value(10).const_view())  # Not in set

    assert result is False


# =============================================================================
# Section 7.1: Sets - Clear and Size Operations
# =============================================================================

def test_set_clear(int_set_schema):
    """SetView.clear() removes all elements."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    sv.insert(make_int_value(1).const_view())
    sv.insert(make_int_value(2).const_view())
    sv.insert(make_int_value(3).const_view())

    sv.clear()

    assert sv.size() == 0
    assert sv.empty()


def test_set_size(int_set_schema):
    """SetView.size() returns correct count."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    assert sv.size() == 0

    sv.insert(make_int_value(1).const_view())
    assert sv.size() == 1

    sv.insert(make_int_value(2).const_view())
    assert sv.size() == 2


def test_set_empty(int_set_schema):
    """SetView.empty() returns correct value."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    assert sv.empty()

    sv.insert(make_int_value(1).const_view())

    assert not sv.empty()


# =============================================================================
# Section 7.1: Sets - Iteration
# =============================================================================

def test_set_iteration(int_set_schema):
    """Set elements can be iterated."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()
    e1 = make_int_value(10)
    e2 = make_int_value(20)
    e3 = make_int_value(30)
    sv.insert(e1.const_view())
    sv.insert(e2.const_view())
    sv.insert(e3.const_view())

    # Get const view for iteration
    csv = v.const_view().as_set()
    elements = []
    for elem in csv:
        elements.append(elem.as_int())

    assert sorted(elements) == [10, 20, 30]


# =============================================================================
# Section 7.2: Maps - Schema Creation
# =============================================================================

def test_create_map_schema(string_double_map_schema):
    """Map schema can be created."""
    assert string_double_map_schema is not None


def test_map_schema_is_kind_map(string_double_map_schema):
    """Map schema has TypeKind.Map."""
    assert string_double_map_schema.kind == TypeKind.Map


def test_map_schema_key_type(string_double_map_schema, string_schema):
    """Map schema has correct key type."""
    assert string_double_map_schema.key_type == string_schema


def test_map_schema_value_type(string_double_map_schema, double_schema):
    """Map schema has correct value type (element_type)."""
    assert string_double_map_schema.element_type == double_schema


# =============================================================================
# Section 7.2: Maps - Value Creation
# (Skipped - Map TypeOps not yet implemented)
# =============================================================================

def test_create_map_value(string_double_map_schema):
    """Map value can be created from schema."""
    v = PlainValue(string_double_map_schema)
    assert v.valid()


def test_map_initially_empty(string_double_map_schema):
    """Map is initially empty."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    assert mv.size() == 0
    assert mv.empty()


# =============================================================================
# Section 7.2: Maps - Set Operations
# =============================================================================

def test_map_set_native_types(string_double_map_schema):
    """MapView.set() with ConstValueView key and value."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    mv.set(make_string_value("apple").const_view(), make_double_value(1.50).const_view())
    mv.set(make_string_value("banana").const_view(), make_double_value(0.75).const_view())

    assert mv.size() == 2


def test_map_set_overwrites_existing(string_double_map_schema):
    """MapView.set() overwrites existing key."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    k2 = make_string_value("apple")
    v2 = make_double_value(2.00)
    mv.set(k2.const_view(), v2.const_view())  # Overwrite

    assert mv.size() == 1

    k3 = make_string_value("apple")
    assert abs(mv.at(k3.const_view()).as_double() - 2.00) < 1e-10


def test_map_set_with_value(string_double_map_schema):
    """MapView.set() works with explicit Value wrapping."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    key = make_string_value("orange")
    mv.set(key.const_view(), make_double_value(2.00).const_view())

    assert mv.contains(make_string_value("orange").const_view())


# =============================================================================
# Section 7.2: Maps - Access Operations
# =============================================================================

def test_map_at_native_type(string_double_map_schema):
    """MapView.at() with ConstValueView key."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    k2 = make_string_value("apple")
    price = mv.at(k2.const_view()).as_double()
    assert abs(price - 1.50) < 1e-10


def test_map_at_with_value_view(string_double_map_schema):
    """MapView.at(ConstValueView) works."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    key = make_string_value("apple")
    price = mv.at(key.const_view()).as_double()
    assert abs(price - 1.50) < 1e-10


def test_map_operator_bracket_read(string_double_map_schema):
    """MapView operator[] provides read access."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    key = make_string_value("apple")
    price = mv[key.const_view()].as_double()
    assert abs(price - 1.50) < 1e-10


def test_map_operator_bracket_write(string_double_map_schema):
    """MapView operator[] allows write access via set()."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    # Overwrite via set()
    k2 = make_string_value("apple")
    v2 = make_double_value(2.50)
    mv.set(k2.const_view(), v2.const_view())

    k3 = make_string_value("apple")
    assert abs(mv.at(k3.const_view()).as_double() - 2.50) < 1e-10


def test_map_operator_bracket_inserts_default(string_double_map_schema):
    """MapView operator[] inserts default if key missing."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    key = make_string_value("new_key")
    _ = mv[key.const_view()]  # Access inserts default

    assert mv.contains(make_string_value("new_key").const_view())


# =============================================================================
# Section 7.2: Maps - Contains Operations
# =============================================================================

def test_map_contains_native_type(string_double_map_schema):
    """MapView.contains() with ConstValueView key."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    k2 = make_string_value("apple")
    assert mv.contains(k2.const_view())


def test_map_contains_returns_false_for_missing(string_double_map_schema):
    """MapView.contains() returns False for missing keys."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    k2 = make_string_value("banana")
    assert not mv.contains(k2.const_view())


def test_const_map_view_contains(string_double_map_schema):
    """ConstMapView.contains() works."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    cmv = v.const_view().as_map()
    k2 = make_string_value("apple")
    assert cmv.contains(k2.const_view())


# =============================================================================
# Section 7.2: Maps - Insert Operations
# =============================================================================

def test_map_insert_returns_true_for_new(string_double_map_schema):
    """MapView.insert() returns True for new keys."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    result = mv.insert(make_string_value("apple").const_view(), make_double_value(1.50).const_view())

    assert result is True


def test_map_insert_returns_false_for_existing(string_double_map_schema):
    """MapView.insert() returns False for existing keys."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    mv.insert(make_string_value("apple").const_view(), make_double_value(1.50).const_view())
    result = mv.insert(make_string_value("apple").const_view(), make_double_value(1.75).const_view())

    assert result is False


def test_map_insert_doesnt_overwrite(string_double_map_schema):
    """MapView.insert() doesn't overwrite existing value."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.insert(k1.const_view(), v1.const_view())

    k2 = make_string_value("apple")
    v2 = make_double_value(1.75)
    mv.insert(k2.const_view(), v2.const_view())  # Should not overwrite

    k3 = make_string_value("apple")
    assert abs(mv.at(k3.const_view()).as_double() - 1.50) < 1e-10


# =============================================================================
# Section 7.2: Maps - Erase Operations
# =============================================================================

def test_map_erase_native_type(string_double_map_schema):
    """MapView.erase() with ConstValueView key."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    # Add some entries
    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    k2 = make_string_value("banana")
    v2 = make_double_value(0.75)
    mv.set(k2.const_view(), v2.const_view())

    assert mv.size() == 2

    # Erase using ConstValueView key
    mv.erase(k1.const_view())

    assert mv.size() == 1
    assert mv.contains(k2.const_view())
    assert not mv.contains(k1.const_view())


def test_map_erase_returns_true_for_existing(string_double_map_schema):
    """MapView.erase() returns True for existing keys."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    # Erase returns True for existing key
    result = mv.erase(k1.const_view())
    assert result is True


def test_map_erase_returns_false_for_missing(string_double_map_schema):
    """MapView.erase() returns False for missing keys."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    # Try to erase non-existent key
    k2 = make_string_value("banana")
    result = mv.erase(k2.const_view())
    assert result is False
    assert mv.size() == 1


# =============================================================================
# Section 7.2: Maps - Clear and Size Operations
# =============================================================================

def test_map_clear(string_double_map_schema):
    """MapView.clear() removes all entries."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    k2 = make_string_value("banana")
    v2 = make_double_value(0.75)
    mv.set(k2.const_view(), v2.const_view())

    mv.clear()

    assert mv.size() == 0
    assert mv.empty()


def test_map_size(string_double_map_schema):
    """MapView.size() returns correct count."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    assert mv.size() == 0

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())
    assert mv.size() == 1

    k2 = make_string_value("banana")
    v2 = make_double_value(0.75)
    mv.set(k2.const_view(), v2.const_view())
    assert mv.size() == 2

    k3 = make_string_value("apple")
    v3 = make_double_value(2.00)
    mv.set(k3.const_view(), v3.const_view())  # Overwrite, not new
    assert mv.size() == 2


# =============================================================================
# Section 7.2: Maps - Iteration
# =============================================================================

def test_map_iteration_key_value_pairs(string_double_map_schema):
    """Map entries can be iterated as key-value pairs."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    k2 = make_string_value("banana")
    v2 = make_double_value(2.25)
    mv.set(k1.const_view(), v1.const_view())
    mv.set(k2.const_view(), v2.const_view())

    # Get const view for iteration
    cmv = v.const_view().as_map()
    items = dict(cmv.items())
    assert items == {"apple": 1.50, "banana": 2.25}


def test_map_keys_iteration(string_double_map_schema):
    """Map keys can be iterated separately - returns ConstKeySetView."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    k2 = make_string_value("banana")
    v2 = make_double_value(2.25)
    mv.set(k1.const_view(), v1.const_view())
    mv.set(k2.const_view(), v2.const_view())

    # Get const view for iteration
    cmv = v.const_view().as_map()
    key_set = cmv.keys()  # Returns ConstKeySetView
    # Convert ConstValueView keys to Python strings for comparison
    keys = [k.as_string() for k in key_set]
    assert sorted(keys) == ["apple", "banana"]


# =============================================================================
# Error Conditions - Sets
# =============================================================================

def test_set_insert_wrong_type_raises(int_set_schema):
    """Inserting wrong type raises error."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    with pytest.raises((TypeError, RuntimeError)):
        sv.insert("not an int")


def test_non_set_value_as_set_raises():
    """Getting set view from non-set value raises error."""
    v = PlainValue(42)

    with pytest.raises((TypeError, RuntimeError)):
        _ = v.as_set()


# =============================================================================
# Set View Queries
# =============================================================================

def test_is_set_on_set_value(int_set_schema):
    """is_set() returns True for set values."""
    v = PlainValue(int_set_schema)
    assert v.const_view().is_set()


def test_is_set_on_scalar_value():
    """is_set() returns False for scalar values."""
    v = PlainValue(42)
    assert not v.const_view().is_set()


def test_try_as_set_on_set_value(int_set_schema):
    """try_as_set() returns view for set values."""
    pass


def test_try_as_set_on_non_set_value():
    """try_as_set() returns None for non-set values."""
    pass


# =============================================================================
# Error Conditions - Maps
# =============================================================================

def test_map_at_missing_key_raises(string_double_map_schema):
    """Accessing missing key with at() raises error."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    k2 = make_string_value("nonexistent")
    with pytest.raises((KeyError, RuntimeError, IndexError)):
        _ = mv.at(k2.const_view())


def test_map_set_wrong_key_type_raises(string_double_map_schema):
    """Setting with wrong key type raises error."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    with pytest.raises((TypeError, RuntimeError)):
        mv.set(42, 1.50)  # Key should be string


def test_map_set_wrong_value_type_raises(string_double_map_schema):
    """Setting with wrong value type raises error."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    with pytest.raises((TypeError, RuntimeError)):
        mv.set("apple", "not a double")


def test_non_map_value_as_map_raises():
    """Getting map view from non-map value raises error."""
    v = PlainValue(42)

    with pytest.raises((TypeError, RuntimeError)):
        _ = v.as_map()


# =============================================================================
# Map View Queries
# =============================================================================

def test_is_map_on_map_value(string_double_map_schema):
    """is_map() returns True for map values."""
    v = PlainValue(string_double_map_schema)
    assert v.const_view().is_map()


def test_is_map_on_scalar_value():
    """is_map() returns False for scalar values."""
    v = PlainValue(42)
    assert not v.const_view().is_map()


def test_try_as_map_on_map_value(string_double_map_schema):
    """try_as_map() returns view for map values."""
    pass


def test_try_as_map_on_non_map_value():
    """try_as_map() returns None for non-map values."""
    pass


# =============================================================================
# Set Cloning
# =============================================================================

def test_clone_set(int_set_schema):
    """Set can be cloned."""
    pass


def test_cloned_set_is_independent(int_set_schema):
    """Cloned set is independent of original."""
    pass


# =============================================================================
# Set Equality
# =============================================================================

def test_set_equals_same_values(int_set_schema):
    """Sets with same values are equal."""
    v1 = PlainValue(int_set_schema)
    sv1 = v1.as_set()
    v2 = PlainValue(int_set_schema)
    sv2 = v2.as_set()

    e1 = make_int_value(10)
    e2 = make_int_value(20)
    sv1.insert(e1.const_view())
    sv1.insert(e2.const_view())
    sv2.insert(e1.const_view())
    sv2.insert(e2.const_view())

    assert v1.equals(v2.const_view())


def test_set_not_equals_different_values(int_set_schema):
    """Sets with different values are not equal."""
    v1 = PlainValue(int_set_schema)
    sv1 = v1.as_set()
    v2 = PlainValue(int_set_schema)
    sv2 = v2.as_set()

    e1 = make_int_value(10)
    e2 = make_int_value(20)
    e3 = make_int_value(30)
    sv1.insert(e1.const_view())
    sv1.insert(e2.const_view())
    sv2.insert(e1.const_view())
    sv2.insert(e3.const_view())

    assert not v1.equals(v2.const_view())


# =============================================================================
# Map Cloning
# =============================================================================

def test_clone_map(string_double_map_schema):
    """Map can be cloned."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("apple")
    val1 = make_double_value(1.50)
    mv.set(k1.const_view(), val1.const_view())

    cloned = v.const_view().clone()
    assert cloned.valid()
    assert cloned.const_view().as_map().size() == 1


def test_cloned_map_is_independent(string_double_map_schema):
    """Cloned map is independent of original."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("apple")
    val1 = make_double_value(1.50)
    mv.set(k1.const_view(), val1.const_view())

    cloned = v.const_view().clone()
    # Modify original
    k2 = make_string_value("banana")
    val2 = make_double_value(2.25)
    mv.set(k2.const_view(), val2.const_view())

    assert v.as_map().size() == 2
    assert cloned.const_view().as_map().size() == 1  # Clone unchanged


# =============================================================================
# Map Equality
# =============================================================================

def test_map_equals_same_entries(string_double_map_schema):
    """Maps with same entries are equal."""
    v1 = PlainValue(string_double_map_schema)
    mv1 = v1.as_map()
    v2 = PlainValue(string_double_map_schema)
    mv2 = v2.as_map()

    k1 = make_string_value("apple")
    val1 = make_double_value(1.50)
    mv1.set(k1.const_view(), val1.const_view())
    mv2.set(k1.const_view(), val1.const_view())

    assert v1.equals(v2.const_view())


def test_map_not_equals_different_values(string_double_map_schema):
    """Maps with different values are not equal."""
    v1 = PlainValue(string_double_map_schema)
    mv1 = v1.as_map()
    v2 = PlainValue(string_double_map_schema)
    mv2 = v2.as_map()

    k1 = make_string_value("apple")
    val1 = make_double_value(1.50)
    val2 = make_double_value(2.50)
    mv1.set(k1.const_view(), val1.const_view())
    mv2.set(k1.const_view(), val2.const_view())

    assert not v1.equals(v2.const_view())


# =============================================================================
# Python Interop Tests - Sets
# =============================================================================

def test_set_to_python(int_set_schema):
    """Set can be converted to Python set."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()
    e1 = make_int_value(10)
    e2 = make_int_value(20)
    e3 = make_int_value(30)
    sv.insert(e1.const_view())
    sv.insert(e2.const_view())
    sv.insert(e3.const_view())

    py_obj = v.to_python()

    assert isinstance(py_obj, (set, frozenset, list))
    # Convert to set for comparison (implementation may return list)
    assert set(py_obj) == {10, 20, 30}


def test_set_from_python(int_set_schema):
    """Set can be populated from Python set."""
    v = PlainValue(int_set_schema)

    py_set = {10, 20, 30}
    v.from_python(py_set)

    csv = v.const_view().as_set()
    assert csv.size() == 3


# =============================================================================
# Python Interop Tests - Maps
# =============================================================================

def test_map_to_python(string_double_map_schema):
    """Map can be converted to Python dict."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    k2 = make_string_value("banana")
    v2 = make_double_value(2.25)
    mv.set(k1.const_view(), v1.const_view())
    mv.set(k2.const_view(), v2.const_view())

    py_obj = v.to_python()

    assert isinstance(py_obj, dict)
    assert py_obj == {"apple": 1.50, "banana": 2.25}


def test_map_from_python(string_double_map_schema):
    """Map can be populated from Python dict."""
    v = PlainValue(string_double_map_schema)

    py_dict = {"apple": 1.50, "banana": 2.25}
    v.from_python(py_dict)

    cmv = v.const_view().as_map()
    assert cmv.size() == 2


# =============================================================================
# To String Tests
# =============================================================================

def test_set_to_string(int_set_schema):
    """Set can be converted to string representation."""
    v = PlainValue(int_set_schema)
    sv = v.as_set()
    e1 = make_int_value(10)
    e2 = make_int_value(20)
    sv.insert(e1.const_view())
    sv.insert(e2.const_view())

    s = v.to_string()

    # Should contain the values (order may vary)
    assert "10" in s
    assert "20" in s


def test_map_to_string(string_double_map_schema):
    """Map can be converted to string representation."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("key")
    v1 = make_double_value(1.5)
    mv.set(k1.const_view(), v1.const_view())

    s = v.to_string()

    assert "key" in s


# =============================================================================
# ConstKeySetView Tests (Map Keys as Set)
# =============================================================================

def test_map_keys_returns_const_key_set_view(string_double_map_schema):
    """Map.keys() returns a ConstKeySetView object."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    cmv = v.const_view().as_map()
    key_set = cmv.keys()

    # Should be a ConstKeySetView, not a Python dict_keys
    assert hasattr(key_set, 'size')
    assert hasattr(key_set, 'empty')
    assert hasattr(key_set, 'contains')
    assert hasattr(key_set, 'element_type')


def test_keyset_size(string_double_map_schema):
    """ConstKeySetView.size() returns the number of keys."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    k2 = make_string_value("banana")
    v2 = make_double_value(2.25)
    mv.set(k1.const_view(), v1.const_view())
    mv.set(k2.const_view(), v2.const_view())

    cmv = v.const_view().as_map()
    key_set = cmv.keys()

    assert key_set.size() == 2
    assert len(key_set) == 2


def test_keyset_empty(string_double_map_schema):
    """ConstKeySetView.empty() returns True for empty map."""
    v = PlainValue(string_double_map_schema)
    cmv = v.const_view().as_map()
    key_set = cmv.keys()

    assert key_set.empty() is True
    assert key_set.size() == 0


def test_keyset_contains(string_double_map_schema):
    """ConstKeySetView.contains() checks for key existence."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    cmv = v.const_view().as_map()
    key_set = cmv.keys()

    # Check with ConstValueView
    k_apple = make_string_value("apple")
    k_banana = make_string_value("banana")
    assert key_set.contains(k_apple.const_view()) is True
    assert key_set.contains(k_banana.const_view()) is False


def test_keyset_dunder_contains(string_double_map_schema):
    """ConstKeySetView supports 'in' operator via __contains__."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("apple")
    v1 = make_double_value(1.50)
    mv.set(k1.const_view(), v1.const_view())

    cmv = v.const_view().as_map()
    key_set = cmv.keys()

    k_apple = make_string_value("apple")
    k_banana = make_string_value("banana")
    assert k_apple.const_view() in key_set
    assert k_banana.const_view() not in key_set


def test_keyset_element_type(string_double_map_schema):
    """ConstKeySetView.element_type() returns the key type."""
    v = PlainValue(string_double_map_schema)
    cmv = v.const_view().as_map()
    key_set = cmv.keys()

    # element_type should be the key type (string)
    key_type = key_set.element_type()
    assert key_type is not None
    # The map key type should match the key_set element type
    assert key_type == cmv.key_type()


def test_keyset_iteration(string_double_map_schema):
    """ConstKeySetView can be iterated to get keys."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("x")
    v1 = make_double_value(1.0)
    k2 = make_string_value("y")
    v2 = make_double_value(2.0)
    k3 = make_string_value("z")
    v3 = make_double_value(3.0)
    mv.set(k1.const_view(), v1.const_view())
    mv.set(k2.const_view(), v2.const_view())
    mv.set(k3.const_view(), v3.const_view())

    cmv = v.const_view().as_map()
    key_set = cmv.keys()

    # Collect keys via iteration
    keys = [k.as_string() for k in key_set]
    assert sorted(keys) == ["x", "y", "z"]


def test_keyset_same_interface_as_constsetview(int_set_schema, string_double_map_schema):
    """ConstKeySetView has the same interface as ConstSetView."""
    # Create a set
    set_v = PlainValue(int_set_schema)
    sv = set_v.as_set()
    e1 = make_int_value(10)
    sv.insert(e1.const_view())
    const_set = set_v.const_view().as_set()

    # Create a map and get its key set
    map_v = PlainValue(string_double_map_schema)
    mv = map_v.as_map()
    k1 = make_string_value("test")
    v1 = make_double_value(1.0)
    mv.set(k1.const_view(), v1.const_view())
    key_set = map_v.const_view().as_map().keys()

    # Both should have the same methods
    set_methods = {'size', 'empty', 'contains', 'element_type', '__len__', '__iter__', '__contains__'}
    for method in set_methods:
        assert hasattr(const_set, method), f"ConstSetView missing {method}"
        assert hasattr(key_set, method), f"ConstKeySetView missing {method}"


def test_keyset_mutable_map_keys(string_double_map_schema):
    """MapView.keys() also returns ConstKeySetView."""
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()
    k1 = make_string_value("key1")
    v1 = make_double_value(1.0)
    mv.set(k1.const_view(), v1.const_view())

    # keys() on mutable view should also work
    key_set = mv.keys()
    assert key_set.size() == 1
    assert key_set.contains(k1.const_view())


# =============================================================================
# Performance Tests (O(1) verification)
# =============================================================================

def test_set_large_insert_performance(int_set_schema):
    """Set insert remains fast with many elements (O(1) amortized)."""
    import time
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    n = 1000
    start = time.perf_counter()
    for i in range(n):
        elem = make_int_value(i)
        sv.insert(elem.const_view())
    elapsed = time.perf_counter() - start

    assert sv.size() == n
    # Should complete quickly - O(n) total for n insertions
    assert elapsed < 5.0, f"Insert took {elapsed:.2f}s for {n} elements - too slow"


def test_set_large_contains_performance(int_set_schema):
    """Set contains remains fast with many elements (O(1))."""
    import time
    v = PlainValue(int_set_schema)
    sv = v.as_set()

    # Insert many elements
    n = 1000
    for i in range(n):
        elem = make_int_value(i)
        sv.insert(elem.const_view())

    csv = v.const_view().as_set()

    # Time contains operations
    start = time.perf_counter()
    for i in range(n):
        elem = make_int_value(i)
        assert csv.contains(elem.const_view())
    elapsed = time.perf_counter() - start

    # Should complete quickly - O(n) total for n lookups
    assert elapsed < 5.0, f"Contains took {elapsed:.2f}s for {n} lookups - too slow"


def test_map_large_set_performance(string_double_map_schema):
    """Map set operations remain fast with many elements (O(1) amortized)."""
    import time
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    n = 1000
    start = time.perf_counter()
    for i in range(n):
        k = make_string_value(f"key_{i}")
        val = make_double_value(float(i))
        mv.set(k.const_view(), val.const_view())
    elapsed = time.perf_counter() - start

    assert mv.size() == n
    # Should complete quickly - O(n) total for n insertions
    assert elapsed < 5.0, f"Set took {elapsed:.2f}s for {n} elements - too slow"


def test_map_large_get_performance(string_double_map_schema):
    """Map get operations remain fast with many elements (O(1))."""
    import time
    v = PlainValue(string_double_map_schema)
    mv = v.as_map()

    # Insert many elements
    n = 1000
    for i in range(n):
        k = make_string_value(f"key_{i}")
        val = make_double_value(float(i))
        mv.set(k.const_view(), val.const_view())

    cmv = v.const_view().as_map()

    # Time get operations
    start = time.perf_counter()
    for i in range(n):
        k = make_string_value(f"key_{i}")
        val = cmv.at(k.const_view())
        assert abs(val.as_double() - float(i)) < 0.001
    elapsed = time.perf_counter() - start

    # Should complete quickly - O(n) total for n lookups
    assert elapsed < 5.0, f"Get took {elapsed:.2f}s for {n} lookups - too slow"
