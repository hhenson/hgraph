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
# Section 7.1: Sets - Schema Creation
# =============================================================================

class TestSetSchemaCreation:
    """Tests for set schema creation (Section 7.1)."""

    def test_create_set_schema(self, int_set_schema):
        """Set schema can be created."""
        assert int_set_schema is not None

    def test_set_schema_is_kind_set(self, int_set_schema):
        """Set schema has TypeKind.Set."""
        assert int_set_schema.kind == TypeKind.Set

    def test_set_schema_element_type(self, int_set_schema, int_schema):
        """Set schema has correct element type."""
        assert int_set_schema.element_type == int_schema


# =============================================================================
# Section 7.1: Sets - Value Creation
# =============================================================================

class TestSetValueCreation:
    """Tests for creating set values (Section 7.1)."""

    def test_create_set_value(self, int_set_schema):
        """Set value can be created from schema."""
        v = PlainValue(int_set_schema)
        assert v.valid()

    def test_set_initially_empty(self, int_set_schema):
        """Set is initially empty."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        assert sv.size() == 0
        assert sv.empty()


# =============================================================================
# Section 7.1: Sets - Insert Operations
# =============================================================================

class TestSetInsert:
    """Tests for set insert operations (Section 7.1)."""

    def test_set_insert_native_type(self, int_set_schema):
        """SetView.insert() auto-wraps native types."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(1)
        sv.insert(2)
        sv.insert(3)

        assert sv.size() == 3

    def test_set_insert_returns_true_for_new(self, int_set_schema):
        """SetView.insert() returns True for new elements."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        result = sv.insert(1)

        assert result is True

    def test_set_insert_returns_false_for_existing(self, int_set_schema):
        """SetView.insert() returns False for existing elements."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(1)
        result = sv.insert(1)

        assert result is False

    def test_set_insert_duplicates_dont_increase_size(self, int_set_schema):
        """Inserting duplicate elements doesn't increase size."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(1)
        sv.insert(2)
        sv.insert(2)  # Duplicate
        sv.insert(3)
        sv.insert(1)  # Duplicate

        assert sv.size() == 3

    def test_set_insert_with_value(self, int_set_schema):
        """SetView.insert(Value) works with explicit wrapping."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(PlainValue(100))

        assert sv.size() == 1
        assert sv.contains(100)

    def test_set_insert_strings(self, string_set_schema):
        """Set of strings can be populated."""
        v = PlainValue(string_set_schema)
        sv = v.as_set()

        sv.insert("apple")
        sv.insert("banana")
        sv.insert("cherry")

        assert sv.size() == 3


# =============================================================================
# Section 7.1: Sets - Contains Operations
# =============================================================================

class TestSetContains:
    """Tests for set membership operations (Section 7.1)."""

    def test_set_contains_native_type(self, int_set_schema):
        """SetView.contains() auto-wraps native types."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(1)
        sv.insert(2)
        sv.insert(3)

        assert sv.contains(2)

    def test_set_contains_returns_false_for_missing(self, int_set_schema):
        """SetView.contains() returns False for missing elements."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(1)
        sv.insert(2)

        assert not sv.contains(10)

    def test_set_contains_with_value_view(self, int_set_schema):
        """SetView.contains(ConstValueView) works."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(100)

        key = PlainValue(100)
        assert sv.contains(key.const_view())


# =============================================================================
# Section 7.1: Sets - Erase Operations
# =============================================================================

class TestSetErase:
    """Tests for set erase operations (Section 7.1)."""

    def test_set_erase_native_type(self, int_set_schema):
        """SetView.erase() auto-wraps native types."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(1)
        sv.insert(2)
        sv.insert(3)

        sv.erase(2)

        assert sv.size() == 2
        assert not sv.contains(2)

    def test_set_erase_returns_true_for_existing(self, int_set_schema):
        """SetView.erase() returns True for existing elements."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(1)
        result = sv.erase(1)

        assert result is True

    def test_set_erase_returns_false_for_missing(self, int_set_schema):
        """SetView.erase() returns False for missing elements."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(1)
        result = sv.erase(10)  # Not in set

        assert result is False


# =============================================================================
# Section 7.1: Sets - Clear and Size Operations
# =============================================================================

class TestSetClearAndSize:
    """Tests for set clear and size operations (Section 7.1)."""

    def test_set_clear(self, int_set_schema):
        """SetView.clear() removes all elements."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(1)
        sv.insert(2)
        sv.insert(3)

        sv.clear()

        assert sv.size() == 0
        assert sv.empty()

    def test_set_size(self, int_set_schema):
        """SetView.size() returns correct count."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        assert sv.size() == 0

        sv.insert(1)
        assert sv.size() == 1

        sv.insert(2)
        assert sv.size() == 2

    def test_set_empty(self, int_set_schema):
        """SetView.empty() returns correct value."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        assert sv.empty()

        sv.insert(1)

        assert not sv.empty()


# =============================================================================
# Section 7.1: Sets - Iteration
# =============================================================================

class TestSetIteration:
    """Tests for iterating sets (Section 7.1)."""

    def test_set_iteration(self, int_set_schema):
        """Set elements can be iterated."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        sv.insert(1)
        sv.insert(2)
        sv.insert(3)

        csv = v.const_view().as_set()

        elements = set()
        for elem in csv:
            elements.add(elem.as_int())

        assert elements == {1, 2, 3}


# =============================================================================
# Section 7.2: Maps - Schema Creation
# =============================================================================

class TestMapSchemaCreation:
    """Tests for map schema creation (Section 7.2)."""

    def test_create_map_schema(self, string_double_map_schema):
        """Map schema can be created."""
        assert string_double_map_schema is not None

    def test_map_schema_is_kind_map(self, string_double_map_schema):
        """Map schema has TypeKind.Map."""
        assert string_double_map_schema.kind == TypeKind.Map

    def test_map_schema_key_type(self, string_double_map_schema, string_schema):
        """Map schema has correct key type."""
        assert string_double_map_schema.key_type == string_schema

    def test_map_schema_value_type(self, string_double_map_schema, double_schema):
        """Map schema has correct value type (element_type)."""
        assert string_double_map_schema.element_type == double_schema


# =============================================================================
# Section 7.2: Maps - Value Creation
# =============================================================================

class TestMapValueCreation:
    """Tests for creating map values (Section 7.2)."""

    def test_create_map_value(self, string_double_map_schema):
        """Map value can be created from schema."""
        v = PlainValue(string_double_map_schema)
        assert v.valid()

    def test_map_initially_empty(self, string_double_map_schema):
        """Map is initially empty."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        assert mv.size() == 0
        assert mv.empty()


# =============================================================================
# Section 7.2: Maps - Set Operations
# =============================================================================

class TestMapSet:
    """Tests for map set operations (Section 7.2)."""

    def test_map_set_native_types(self, string_double_map_schema):
        """MapView.set() auto-wraps key and value."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)
        mv.set("banana", 0.75)

        assert mv.size() == 2

    def test_map_set_overwrites_existing(self, string_double_map_schema):
        """MapView.set() overwrites existing key."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)
        mv.set("apple", 2.00)  # Overwrite

        assert mv.size() == 1
        assert abs(mv.at("apple").as_double() - 2.00) < 1e-10

    def test_map_set_with_value(self, string_double_map_schema):
        """MapView.set() works with explicit Value wrapping."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        key = PlainValue("orange")
        mv.set(key.const_view(), PlainValue(2.00))

        assert mv.contains("orange")


# =============================================================================
# Section 7.2: Maps - Access Operations
# =============================================================================

class TestMapAccess:
    """Tests for map access operations (Section 7.2)."""

    def test_map_at_native_type(self, string_double_map_schema):
        """MapView.at() with native type key."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)

        price = mv.at("apple").as_double()
        assert abs(price - 1.50) < 1e-10

    def test_map_at_with_value_view(self, string_double_map_schema):
        """MapView.at(ConstValueView) works."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)

        key = PlainValue("apple")
        price = mv.at(key.const_view()).as_double()
        assert abs(price - 1.50) < 1e-10

    def test_map_operator_bracket_read(self, string_double_map_schema):
        """MapView operator[] provides read access."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)

        key = PlainValue("apple")
        price = mv[key.const_view()].as_double()
        assert abs(price - 1.50) < 1e-10

    def test_map_operator_bracket_write(self, string_double_map_schema):
        """MapView operator[] allows write access via set()."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)

        # Overwrite via set()
        mv.set("apple", 2.50)

        assert abs(mv.at("apple").as_double() - 2.50) < 1e-10

    def test_map_operator_bracket_inserts_default(self, string_double_map_schema):
        """MapView operator[] inserts default if key missing."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        key = PlainValue("new_key")
        _ = mv[key.const_view()]  # Access inserts default

        assert mv.contains("new_key")


# =============================================================================
# Section 7.2: Maps - Contains Operations
# =============================================================================

class TestMapContains:
    """Tests for map membership operations (Section 7.2)."""

    def test_map_contains_native_type(self, string_double_map_schema):
        """MapView.contains() auto-wraps key."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)

        assert mv.contains("apple")

    def test_map_contains_returns_false_for_missing(self, string_double_map_schema):
        """MapView.contains() returns False for missing keys."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)

        assert not mv.contains("banana")

    def test_const_map_view_contains(self, string_double_map_schema):
        """ConstMapView.contains() works."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)

        cmv = v.const_view().as_map()
        assert cmv.contains("apple")


# =============================================================================
# Section 7.2: Maps - Insert Operations
# =============================================================================

class TestMapInsert:
    """Tests for map insert operations (Section 7.2)."""

    def test_map_insert_returns_true_for_new(self, string_double_map_schema):
        """MapView.insert() returns True for new keys."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        result = mv.insert("apple", 1.50)

        assert result is True

    def test_map_insert_returns_false_for_existing(self, string_double_map_schema):
        """MapView.insert() returns False for existing keys."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.insert("apple", 1.50)
        result = mv.insert("apple", 1.75)

        assert result is False

    def test_map_insert_doesnt_overwrite(self, string_double_map_schema):
        """MapView.insert() doesn't overwrite existing value."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.insert("apple", 1.50)
        mv.insert("apple", 1.75)  # Should not overwrite

        assert abs(mv.at("apple").as_double() - 1.50) < 1e-10


# =============================================================================
# Section 7.2: Maps - Erase Operations
# =============================================================================

class TestMapErase:
    """Tests for map erase operations (Section 7.2)."""

    def test_map_erase_native_type(self, string_double_map_schema):
        """MapView.erase() auto-wraps key."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)
        mv.set("banana", 0.75)

        mv.erase("apple")

        assert mv.size() == 1
        assert not mv.contains("apple")

    def test_map_erase_returns_true_for_existing(self, string_double_map_schema):
        """MapView.erase() returns True for existing keys."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)
        result = mv.erase("apple")

        assert result is True

    def test_map_erase_returns_false_for_missing(self, string_double_map_schema):
        """MapView.erase() returns False for missing keys."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)
        result = mv.erase("banana")

        assert result is False


# =============================================================================
# Section 7.2: Maps - Clear and Size Operations
# =============================================================================

class TestMapClearAndSize:
    """Tests for map clear and size operations (Section 7.2)."""

    def test_map_clear(self, string_double_map_schema):
        """MapView.clear() removes all entries."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)
        mv.set("banana", 0.75)

        mv.clear()

        assert mv.size() == 0
        assert mv.empty()

    def test_map_size(self, string_double_map_schema):
        """MapView.size() returns correct count."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        assert mv.size() == 0

        mv.set("apple", 1.50)
        assert mv.size() == 1

        mv.set("banana", 0.75)
        assert mv.size() == 2

        mv.set("apple", 2.00)  # Overwrite, not new
        assert mv.size() == 2


# =============================================================================
# Section 7.2: Maps - Iteration
# =============================================================================

class TestMapIteration:
    """Tests for iterating maps (Section 7.2)."""

    def test_map_iteration_key_value_pairs(self, string_double_map_schema):
        """Map entries can be iterated as key-value pairs."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)
        mv.set("banana", 0.75)
        mv.set("cherry", 2.00)

        cmv = v.const_view().as_map()

        entries = {}
        for key, val in cmv:
            entries[key.as_string()] = val.as_double()

        assert len(entries) == 3
        assert abs(entries["apple"] - 1.50) < 1e-10
        assert abs(entries["banana"] - 0.75) < 1e-10
        assert abs(entries["cherry"] - 2.00) < 1e-10

    def test_map_keys_iteration(self, string_double_map_schema):
        """Map keys can be iterated separately."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)
        mv.set("banana", 0.75)

        cmv = v.const_view().as_map()

        keys = set()
        for key in cmv.keys():
            keys.add(key.as_string())

        assert keys == {"apple", "banana"}


# =============================================================================
# Error Conditions - Sets
# =============================================================================

class TestSetErrorConditions:
    """Tests for set error conditions."""

    def test_set_insert_wrong_type_raises(self, int_set_schema):
        """Inserting wrong type raises error."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()

        with pytest.raises((TypeError, RuntimeError)):
            sv.insert("not an int")

    def test_non_set_value_as_set_raises(self):
        """Getting set view from non-set value raises error."""
        v = PlainValue(42)

        with pytest.raises((TypeError, RuntimeError)):
            _ = v.as_set()


class TestSetViewQueries:
    """Tests for set type queries."""

    def test_is_set_on_set_value(self, int_set_schema):
        """is_set() returns True for set values."""
        v = PlainValue(int_set_schema)
        assert v.const_view().is_set()

    def test_is_set_on_scalar_value(self):
        """is_set() returns False for scalar values."""
        v = PlainValue(42)
        assert not v.const_view().is_set()

    def test_try_as_set_on_set_value(self, int_set_schema):
        """try_as_set() returns view for set values."""
        v = PlainValue(int_set_schema)
        result = v.const_view().try_as_set()
        assert result is not None

    def test_try_as_set_on_non_set_value(self):
        """try_as_set() returns None for non-set values."""
        v = PlainValue(42)
        result = v.const_view().try_as_set()
        assert result is None


# =============================================================================
# Error Conditions - Maps
# =============================================================================

class TestMapErrorConditions:
    """Tests for map error conditions."""

    def test_map_at_missing_key_raises(self, string_double_map_schema):
        """Accessing missing key with at() raises error."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        mv.set("apple", 1.50)

        with pytest.raises((KeyError, RuntimeError)):
            _ = mv.at("nonexistent")

    def test_map_set_wrong_key_type_raises(self, string_double_map_schema):
        """Setting with wrong key type raises error."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        with pytest.raises((TypeError, RuntimeError)):
            mv.set(42, 1.50)  # Key should be string

    def test_map_set_wrong_value_type_raises(self, string_double_map_schema):
        """Setting with wrong value type raises error."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()

        with pytest.raises((TypeError, RuntimeError)):
            mv.set("apple", "not a double")

    def test_non_map_value_as_map_raises(self):
        """Getting map view from non-map value raises error."""
        v = PlainValue(42)

        with pytest.raises((TypeError, RuntimeError)):
            _ = v.as_map()


class TestMapViewQueries:
    """Tests for map type queries."""

    def test_is_map_on_map_value(self, string_double_map_schema):
        """is_map() returns True for map values."""
        v = PlainValue(string_double_map_schema)
        assert v.const_view().is_map()

    def test_is_map_on_scalar_value(self):
        """is_map() returns False for scalar values."""
        v = PlainValue(42)
        assert not v.const_view().is_map()

    def test_try_as_map_on_map_value(self, string_double_map_schema):
        """try_as_map() returns view for map values."""
        v = PlainValue(string_double_map_schema)
        result = v.const_view().try_as_map()
        assert result is not None

    def test_try_as_map_on_non_map_value(self):
        """try_as_map() returns None for non-map values."""
        v = PlainValue(42)
        result = v.const_view().try_as_map()
        assert result is None


# =============================================================================
# Comparison and Cloning Tests - Sets
# =============================================================================

class TestSetCloning:
    """Tests for cloning set values."""

    def test_clone_set(self, int_set_schema):
        """Set can be cloned."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()
        sv.insert(1)
        sv.insert(2)
        sv.insert(3)

        cloned = v.const_view().clone()

        csv = cloned.const_view().as_set()
        assert csv.size() == 3
        assert csv.contains(1)
        assert csv.contains(2)
        assert csv.contains(3)

    def test_cloned_set_is_independent(self, int_set_schema):
        """Cloned set is independent of original."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()
        sv.insert(1)

        cloned = v.const_view().clone()

        # Modify original
        sv.insert(2)
        sv.erase(1)

        # Clone should be unchanged
        csv = cloned.const_view().as_set()
        assert csv.size() == 1
        assert csv.contains(1)
        assert not csv.contains(2)


class TestSetEquality:
    """Tests for set comparison operations."""

    def test_set_equals_same_values(self, int_set_schema):
        """Sets with same values are equal."""
        v1 = PlainValue(int_set_schema)
        sv1 = v1.as_set()
        sv1.insert(1)
        sv1.insert(2)
        sv1.insert(3)

        v2 = PlainValue(int_set_schema)
        sv2 = v2.as_set()
        sv2.insert(3)  # Insert in different order
        sv2.insert(1)
        sv2.insert(2)

        assert v1.equals(v2)

    def test_set_not_equals_different_values(self, int_set_schema):
        """Sets with different values are not equal."""
        v1 = PlainValue(int_set_schema)
        sv1 = v1.as_set()
        sv1.insert(1)
        sv1.insert(2)

        v2 = PlainValue(int_set_schema)
        sv2 = v2.as_set()
        sv2.insert(1)
        sv2.insert(3)  # Different

        assert not v1.equals(v2)


# =============================================================================
# Comparison and Cloning Tests - Maps
# =============================================================================

class TestMapCloning:
    """Tests for cloning map values."""

    def test_clone_map(self, string_double_map_schema):
        """Map can be cloned."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()
        mv.set("apple", 1.50)
        mv.set("banana", 0.75)

        cloned = v.const_view().clone()

        cmv = cloned.const_view().as_map()
        assert cmv.size() == 2
        assert cmv.contains("apple")
        assert abs(cmv.at("apple").as_double() - 1.50) < 1e-10

    def test_cloned_map_is_independent(self, string_double_map_schema):
        """Cloned map is independent of original."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()
        mv.set("apple", 1.50)

        cloned = v.const_view().clone()

        # Modify original
        mv.set("apple", 2.00)
        mv.set("banana", 0.75)

        # Clone should be unchanged
        cmv = cloned.const_view().as_map()
        assert cmv.size() == 1
        assert abs(cmv.at("apple").as_double() - 1.50) < 1e-10


class TestMapEquality:
    """Tests for map comparison operations."""

    def test_map_equals_same_entries(self, string_double_map_schema):
        """Maps with same entries are equal."""
        v1 = PlainValue(string_double_map_schema)
        mv1 = v1.as_map()
        mv1.set("apple", 1.50)
        mv1.set("banana", 0.75)

        v2 = PlainValue(string_double_map_schema)
        mv2 = v2.as_map()
        mv2.set("banana", 0.75)  # Insert in different order
        mv2.set("apple", 1.50)

        assert v1.equals(v2)

    def test_map_not_equals_different_values(self, string_double_map_schema):
        """Maps with different values are not equal."""
        v1 = PlainValue(string_double_map_schema)
        mv1 = v1.as_map()
        mv1.set("apple", 1.50)

        v2 = PlainValue(string_double_map_schema)
        mv2 = v2.as_map()
        mv2.set("apple", 2.00)  # Different value

        assert not v1.equals(v2)


# =============================================================================
# Python Interop Tests
# =============================================================================

class TestSetPythonInterop:
    """Tests for set Python interoperability."""

    def test_set_to_python(self, int_set_schema):
        """Set can be converted to Python set."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()
        sv.insert(1)
        sv.insert(2)
        sv.insert(3)

        py_obj = v.to_python()

        assert isinstance(py_obj, (set, frozenset))
        assert py_obj == {1, 2, 3}

    def test_set_from_python(self, int_set_schema):
        """Set can be populated from Python set."""
        v = PlainValue(int_set_schema)

        py_set = {1, 2, 3}
        v.from_python(py_set)

        csv = v.const_view().as_set()
        assert csv.size() == 3
        assert csv.contains(1)
        assert csv.contains(2)
        assert csv.contains(3)


class TestMapPythonInterop:
    """Tests for map Python interoperability."""

    def test_map_to_python(self, string_double_map_schema):
        """Map can be converted to Python dict."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()
        mv.set("apple", 1.50)
        mv.set("banana", 0.75)

        py_obj = v.to_python()

        assert isinstance(py_obj, dict)
        assert abs(py_obj["apple"] - 1.50) < 1e-10
        assert abs(py_obj["banana"] - 0.75) < 1e-10

    def test_map_from_python(self, string_double_map_schema):
        """Map can be populated from Python dict."""
        v = PlainValue(string_double_map_schema)

        py_dict = {"apple": 1.50, "banana": 0.75}
        v.from_python(py_dict)

        cmv = v.const_view().as_map()
        assert cmv.size() == 2
        assert abs(cmv.at("apple").as_double() - 1.50) < 1e-10
        assert abs(cmv.at("banana").as_double() - 0.75) < 1e-10


class TestSetMapToString:
    """Tests for set and map string representations."""

    def test_set_to_string(self, int_set_schema):
        """Set can be converted to string representation."""
        v = PlainValue(int_set_schema)
        sv = v.as_set()
        sv.insert(1)
        sv.insert(2)

        s = v.to_string()

        assert "1" in s
        assert "2" in s

    def test_map_to_string(self, string_double_map_schema):
        """Map can be converted to string representation."""
        v = PlainValue(string_double_map_schema)
        mv = v.as_map()
        mv.set("apple", 1.50)

        s = v.to_string()

        assert "apple" in s
