"""
Tests for Value Path-Based Access.

Tests the Path-Based Access feature from the Value type system (Section 10 of User Guide).
Path-based access enables navigation through nested structures using path expressions like:
- value.navigate("user.address.city")
- value.navigate("items[0].name")

Reference: ts_design_docs/Value_USER_GUIDE.md Section 10
"""

import pytest

# Skip all tests if C++ extension is not available
_hgraph = pytest.importorskip("hgraph._hgraph")
value = _hgraph.value  # Value types are in the value submodule

# Convenience aliases
PlainValue = value.PlainValue
TypeRegistry = value.TypeRegistry
TypeKind = value.TypeKind

# Path-related types (to be exposed in C++ extension)
# These will be imported once implemented:
# PathElement = value.PathElement
# parse_path = value.parse_path


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
def simple_bundle_schema(type_registry, int_schema, double_schema, string_schema):
    """Create a simple bundle schema with x, y, name fields."""
    return type_registry.bundle() \
        .field("x", int_schema) \
        .field("y", double_schema) \
        .field("name", string_schema) \
        .build()


@pytest.fixture
def address_schema(type_registry, string_schema, int_schema):
    """Create an address bundle schema."""
    return type_registry.bundle("Address") \
        .field("street", string_schema) \
        .field("city", string_schema) \
        .field("zip", int_schema) \
        .build()


@pytest.fixture
def person_schema(type_registry, string_schema, int_schema, address_schema):
    """Create a person bundle schema with nested address."""
    return type_registry.bundle("Person") \
        .field("name", string_schema) \
        .field("age", int_schema) \
        .field("address", address_schema) \
        .build()


@pytest.fixture
def deeply_nested_schema(type_registry, string_schema, int_schema, person_schema):
    """Create a deeply nested bundle schema."""
    return type_registry.bundle("Company") \
        .field("company_name", string_schema) \
        .field("employee_count", int_schema) \
        .field("ceo", person_schema) \
        .build()


@pytest.fixture
def list_of_ints_schema(type_registry, int_schema):
    """Create a dynamic list schema for int64_t elements."""
    return type_registry.list(int_schema).build()


@pytest.fixture
def list_of_strings_schema(type_registry, string_schema):
    """Create a dynamic list schema for string elements."""
    return type_registry.list(string_schema).build()


@pytest.fixture
def list_of_addresses_schema(type_registry, address_schema):
    """Create a dynamic list schema for Address bundles."""
    return type_registry.list(address_schema).build()


@pytest.fixture
def person_with_addresses_schema(type_registry, string_schema, int_schema, list_of_addresses_schema):
    """Create a person bundle with a list of addresses."""
    return type_registry.bundle("PersonWithAddresses") \
        .field("name", string_schema) \
        .field("age", int_schema) \
        .field("addresses", list_of_addresses_schema) \
        .build()


@pytest.fixture
def tuple_schema(type_registry, int_schema, string_schema, double_schema):
    """Create a simple tuple schema with (int64_t, string, double)."""
    return type_registry.tuple() \
        .element(int_schema) \
        .element(string_schema) \
        .element(double_schema) \
        .build()


@pytest.fixture
def bundle_with_tuple_schema(type_registry, string_schema, tuple_schema):
    """Create a bundle containing a tuple."""
    return type_registry.bundle() \
        .field("label", string_schema) \
        .field("data", tuple_schema) \
        .build()


@pytest.fixture
def list_of_tuples_schema(type_registry, tuple_schema):
    """Create a list of tuples schema."""
    return type_registry.list(tuple_schema).build()


# =============================================================================
# Helper Functions
# =============================================================================

def make_int_value(val):
    """Create a PlainValue containing an int."""
    int_schema = value.scalar_type_meta_int64()
    v = PlainValue(int_schema)
    v.set_int(val)
    return v


def make_string_value(val):
    """Create a PlainValue containing a string."""
    string_schema = value.scalar_type_meta_string()
    v = PlainValue(string_schema)
    v.set_string(val)
    return v


def make_double_value(val):
    """Create a PlainValue containing a double."""
    double_schema = value.scalar_type_meta_double()
    v = PlainValue(double_schema)
    v.set_double(val)
    return v


# =============================================================================
# Section 10.1: PathElement Tests
# =============================================================================

def test_path_element_field_creation():
    """PathElement can be created for field access."""
    PathElement = value.PathElement
    elem = PathElement.field("name")
    assert elem is not None
    assert elem.name == "name"


def test_path_element_index_creation():
    """PathElement can be created for index access."""
    PathElement = value.PathElement
    elem = PathElement.index(0)
    assert elem is not None
    assert elem.get_index() == 0


def test_path_element_is_field():
    """PathElement.is_field() returns True for field elements."""
    PathElement = value.PathElement
    field_elem = PathElement.field("name")
    index_elem = PathElement.index(0)

    assert field_elem.is_field() is True
    assert index_elem.is_field() is False


def test_path_element_is_index():
    """PathElement.is_index() returns True for index elements."""
    PathElement = value.PathElement
    field_elem = PathElement.field("name")
    index_elem = PathElement.index(0)

    assert field_elem.is_index() is False
    assert index_elem.is_index() is True


def test_path_element_field_with_empty_name():
    """PathElement.field() with empty name raises or creates valid element."""
    PathElement = value.PathElement
    # Depending on design, empty field name might be valid or throw
    # This test documents expected behavior
    try:
        elem = PathElement.field("")
        assert elem.name == ""
    except (ValueError, RuntimeError):
        pass  # Empty name is not allowed


def test_path_element_negative_index():
    """PathElement.index() with negative index raises (size_t cannot be negative)."""
    PathElement = value.PathElement
    # Negative index causes type conversion error in nanobind (size_t cannot be negative)
    with pytest.raises((ValueError, RuntimeError, OverflowError, TypeError)):
        PathElement.index(-1)


# =============================================================================
# Section 10.2: Path Parsing Tests
# =============================================================================

def test_parse_simple_field_path():
    """parse_path() parses single field name."""
    parse_path = value.parse_path
    path = parse_path("name")

    assert len(path) == 1
    assert path[0].is_field()
    assert path[0].name == "name"


def test_parse_dotted_path():
    """parse_path() parses dotted field path like 'user.name'."""
    parse_path = value.parse_path
    path = parse_path("user.name")

    assert len(path) == 2
    assert path[0].is_field()
    assert path[0].name == "user"
    assert path[1].is_field()
    assert path[1].name == "name"


def test_parse_indexed_path():
    """parse_path() parses indexed path like 'items[0]'."""
    parse_path = value.parse_path
    path = parse_path("items[0]")

    assert len(path) == 2
    assert path[0].is_field()
    assert path[0].name == "items"
    assert path[1].is_index()
    assert path[1].get_index() == 0


def test_parse_mixed_path():
    """parse_path() parses mixed path like 'users[0].addresses[1].city'."""
    parse_path = value.parse_path
    path = parse_path("users[0].addresses[1].city")

    assert len(path) == 5
    assert path[0].is_field() and path[0].name == "users"
    assert path[1].is_index() and path[1].get_index() == 0
    assert path[2].is_field() and path[2].name == "addresses"
    assert path[3].is_index() and path[3].get_index() == 1
    assert path[4].is_field() and path[4].name == "city"


def test_parse_empty_path():
    """parse_path() with empty string returns empty path."""
    parse_path = value.parse_path
    path = parse_path("")

    assert len(path) == 0


def test_parse_invalid_path_throws():
    """parse_path() with invalid syntax throws."""
    parse_path = value.parse_path

    # Various invalid path patterns
    invalid_paths = [
        "items[",      # Unclosed bracket
        "items[]",     # Empty index
        "items[abc]",  # Non-numeric index
        ".name",       # Leading dot
        "name.",       # Trailing dot
        "items[0",     # Missing closing bracket
        "items0]",     # Missing opening bracket
        "items[[0]]",  # Double brackets
        "name..field", # Double dots
    ]

    for invalid_path in invalid_paths:
        with pytest.raises((ValueError, RuntimeError)):
            parse_path(invalid_path)


def test_parse_deeply_nested_path():
    """parse_path() handles deeply nested paths."""
    parse_path = value.parse_path
    path = parse_path("a.b.c.d.e.f.g")

    assert len(path) == 7
    for i, name in enumerate(['a', 'b', 'c', 'd', 'e', 'f', 'g']):
        assert path[i].is_field()
        assert path[i].name == name


def test_parse_multiple_consecutive_indices():
    """parse_path() handles consecutive indices like 'matrix[0][1]'."""
    parse_path = value.parse_path
    path = parse_path("matrix[0][1]")

    assert len(path) == 3
    assert path[0].is_field() and path[0].name == "matrix"
    assert path[1].is_index() and path[1].get_index() == 0
    assert path[2].is_index() and path[2].get_index() == 1


def test_parse_large_index():
    """parse_path() handles large indices."""
    parse_path = value.parse_path
    path = parse_path("items[999999]")

    assert len(path) == 2
    assert path[1].is_index()
    assert path[1].get_index() == 999999


# =============================================================================
# Section 10.3: Navigation Tests - Single Field
# =============================================================================

def test_navigate_single_field(simple_bundle_schema):
    """navigate() to single bundle field."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 42)
    bv.set("y", 3.14)
    bv.set("name", "test")

    # Navigate to single field
    result = v.navigate("x")

    assert result is not None
    assert result.as_int() == 42


def test_navigate_single_field_string(simple_bundle_schema):
    """navigate() to string field."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("name", "hello world")

    result = v.navigate("name")

    assert result is not None
    assert result.as_string() == "hello world"


def test_navigate_single_field_double(simple_bundle_schema):
    """navigate() to double field."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("y", 3.14159)

    result = v.navigate("y")

    assert result is not None
    assert abs(result.as_double() - 3.14159) < 1e-10


# =============================================================================
# Section 10.3: Navigation Tests - Nested Fields
# =============================================================================

def test_navigate_nested_fields(person_schema, address_schema):
    """navigate() through nested bundles."""
    v = PlainValue(person_schema)
    bv = v.as_bundle()
    bv.set("name", "Alice")
    bv.set("age", 30)

    addr = bv["address"].as_bundle()
    addr.set("street", "123 Main St")
    addr.set("city", "Boston")
    addr.set("zip", 12345)

    # Navigate to nested field
    result = v.navigate("address.city")

    assert result is not None
    assert result.as_string() == "Boston"


def test_navigate_deeply_nested_fields(deeply_nested_schema, person_schema, address_schema):
    """navigate() through deeply nested bundles."""
    v = PlainValue(deeply_nested_schema)
    bv = v.as_bundle()
    bv.set("company_name", "Acme Corp")
    bv.set("employee_count", 100)

    ceo = bv["ceo"].as_bundle()
    ceo.set("name", "Bob")
    ceo.set("age", 45)

    ceo_addr = ceo["address"].as_bundle()
    ceo_addr.set("street", "456 Oak Ave")
    ceo_addr.set("city", "New York")
    ceo_addr.set("zip", 10001)

    # Navigate through three levels
    result = v.navigate("ceo.address.street")

    assert result is not None
    assert result.as_string() == "456 Oak Ave"


# =============================================================================
# Section 10.3: Navigation Tests - List Index
# =============================================================================

def test_navigate_list_index(list_of_ints_schema):
    """navigate() to list element by index."""
    v = PlainValue(list_of_ints_schema)
    lv = v.as_list()

    lv.push_back(make_int_value(10).const_view())
    lv.push_back(make_int_value(20).const_view())
    lv.push_back(make_int_value(30).const_view())

    result = v.navigate("[1]")

    assert result is not None
    assert result.as_int() == 20


def test_navigate_list_first_element(list_of_strings_schema):
    """navigate() to first list element."""
    v = PlainValue(list_of_strings_schema)
    lv = v.as_list()

    lv.push_back(make_string_value("first").const_view())
    lv.push_back(make_string_value("second").const_view())

    result = v.navigate("[0]")

    assert result is not None
    assert result.as_string() == "first"


def test_navigate_list_last_element(list_of_ints_schema):
    """navigate() to last list element."""
    v = PlainValue(list_of_ints_schema)
    lv = v.as_list()

    lv.push_back(make_int_value(10).const_view())
    lv.push_back(make_int_value(20).const_view())
    lv.push_back(make_int_value(30).const_view())

    result = v.navigate("[2]")

    assert result is not None
    assert result.as_int() == 30


# =============================================================================
# Section 10.3: Navigation Tests - Tuple Index
# =============================================================================

def test_navigate_tuple_index(tuple_schema):
    """navigate() to tuple element by index."""
    v = PlainValue(tuple_schema)
    tv = v.as_tuple()

    tv.at(0).set_int(42)
    tv.at(1).set_string("hello")
    tv.at(2).set_double(3.14)

    result = v.navigate("[1]")

    assert result is not None
    assert result.as_string() == "hello"


def test_navigate_tuple_first_element(tuple_schema):
    """navigate() to first tuple element."""
    v = PlainValue(tuple_schema)
    tv = v.as_tuple()

    tv.at(0).set_int(100)

    result = v.navigate("[0]")

    assert result is not None
    assert result.as_int() == 100


def test_navigate_tuple_last_element(tuple_schema):
    """navigate() to last tuple element."""
    v = PlainValue(tuple_schema)
    tv = v.as_tuple()

    tv.at(2).set_double(9.99)

    result = v.navigate("[2]")

    assert result is not None
    assert abs(result.as_double() - 9.99) < 1e-10


# =============================================================================
# Section 10.3: Navigation Tests - Mixed Paths
# =============================================================================

def test_navigate_mixed(person_with_addresses_schema, address_schema):
    """navigate() with mixed field and index access."""
    v = PlainValue(person_with_addresses_schema)
    bv = v.as_bundle()
    bv.set("name", "Charlie")
    bv.set("age", 35)

    addresses = bv["addresses"].as_list()

    # Add first address
    addr1 = PlainValue(address_schema)
    addr1_bv = addr1.as_bundle()
    addr1_bv.set("street", "100 First St")
    addr1_bv.set("city", "Chicago")
    addr1_bv.set("zip", 60601)
    addresses.push_back(addr1.const_view())

    # Add second address
    addr2 = PlainValue(address_schema)
    addr2_bv = addr2.as_bundle()
    addr2_bv.set("street", "200 Second St")
    addr2_bv.set("city", "Los Angeles")
    addr2_bv.set("zip", 90001)
    addresses.push_back(addr2.const_view())

    # Navigate: person.addresses[0].city
    result = v.navigate("addresses[0].city")

    assert result is not None
    assert result.as_string() == "Chicago"


def test_navigate_mixed_second_element(person_with_addresses_schema, address_schema):
    """navigate() with mixed access to second list element."""
    v = PlainValue(person_with_addresses_schema)
    bv = v.as_bundle()
    bv.set("name", "Dave")
    bv.set("age", 40)

    addresses = bv["addresses"].as_list()

    # Add two addresses
    for i, city in enumerate(["Denver", "Seattle"]):
        addr = PlainValue(address_schema)
        addr_bv = addr.as_bundle()
        addr_bv.set("street", f"{i * 100} Main St")
        addr_bv.set("city", city)
        addr_bv.set("zip", 80000 + i)
        addresses.push_back(addr.const_view())

    # Navigate: person.addresses[1].city
    result = v.navigate("addresses[1].city")

    assert result is not None
    assert result.as_string() == "Seattle"


def test_navigate_bundle_with_tuple(bundle_with_tuple_schema):
    """navigate() through bundle containing tuple."""
    v = PlainValue(bundle_with_tuple_schema)
    bv = v.as_bundle()
    bv.set("label", "test data")

    data = bv["data"].as_tuple()
    data.at(0).set_int(42)
    data.at(1).set_string("value")
    data.at(2).set_double(2.718)

    # Navigate: bundle.data[1]
    result = v.navigate("data[1]")

    assert result is not None
    assert result.as_string() == "value"


def test_navigate_list_of_tuples(list_of_tuples_schema, tuple_schema):
    """navigate() through list of tuples."""
    v = PlainValue(list_of_tuples_schema)
    lv = v.as_list()

    # Add first tuple
    t1 = PlainValue(tuple_schema)
    t1.as_tuple().at(0).set_int(1)
    t1.as_tuple().at(1).set_string("one")
    t1.as_tuple().at(2).set_double(1.0)
    lv.push_back(t1.const_view())

    # Add second tuple
    t2 = PlainValue(tuple_schema)
    t2.as_tuple().at(0).set_int(2)
    t2.as_tuple().at(1).set_string("two")
    t2.as_tuple().at(2).set_double(2.0)
    lv.push_back(t2.const_view())

    # Navigate: [1][1] - second tuple, second element
    result = v.navigate("[1][1]")

    assert result is not None
    assert result.as_string() == "two"


# =============================================================================
# Section 10.3: Navigation Tests - Error Conditions
# =============================================================================

def test_navigate_invalid_field_throws(simple_bundle_schema):
    """navigate() to non-existent field throws."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 42)

    with pytest.raises((KeyError, RuntimeError)):
        v.navigate("nonexistent")


def test_navigate_invalid_index_throws(list_of_ints_schema):
    """navigate() to out-of-range index throws."""
    v = PlainValue(list_of_ints_schema)
    lv = v.as_list()
    lv.push_back(make_int_value(10).const_view())

    with pytest.raises((IndexError, RuntimeError)):
        v.navigate("[5]")


def test_navigate_negative_index_throws(list_of_ints_schema):
    """navigate() with negative index throws."""
    v = PlainValue(list_of_ints_schema)
    lv = v.as_list()
    lv.push_back(make_int_value(10).const_view())

    # Negative indices are not supported
    with pytest.raises((IndexError, ValueError, RuntimeError)):
        v.navigate("[-1]")


def test_navigate_type_mismatch_field_on_scalar_throws():
    """navigate() with field access on scalar throws."""
    v = PlainValue(42)  # Scalar value

    with pytest.raises((TypeError, RuntimeError)):
        v.navigate("field")


def test_navigate_type_mismatch_index_on_scalar_throws():
    """navigate() with index access on scalar throws."""
    v = PlainValue(42)  # Scalar value

    with pytest.raises((TypeError, RuntimeError)):
        v.navigate("[0]")


def test_navigate_index_on_bundle_succeeds(simple_bundle_schema):
    """navigate() with index access on bundle succeeds (bundles support positional access)."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 42)
    bv.set("y", 3.14)
    bv.set("name", "test")

    # Bundles support index access by field position
    result = v.navigate("[0]")  # First field (x)
    assert result.as_int() == 42

    result = v.navigate("[1]")  # Second field (y)
    assert abs(result.as_double() - 3.14) < 1e-10


def test_navigate_type_mismatch_field_on_list_throws(list_of_ints_schema):
    """navigate() with field access on list throws."""
    v = PlainValue(list_of_ints_schema)
    lv = v.as_list()
    lv.push_back(make_int_value(10).const_view())

    # Lists don't support field access
    with pytest.raises((TypeError, RuntimeError)):
        v.navigate("field")


def test_navigate_empty_path_returns_self(simple_bundle_schema):
    """navigate() with empty path returns view of self."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 42)

    result = v.navigate("")

    # Empty path should return view of the value itself
    assert result is not None
    assert result.is_bundle()


def test_navigate_nested_invalid_field_throws(person_schema, address_schema):
    """navigate() to non-existent nested field throws."""
    v = PlainValue(person_schema)
    bv = v.as_bundle()
    bv.set("name", "Test")

    # First part exists, second doesn't
    with pytest.raises((KeyError, RuntimeError)):
        v.navigate("address.country")  # country doesn't exist


def test_navigate_partial_path_valid_then_invalid(person_with_addresses_schema, address_schema):
    """navigate() fails partway through when path becomes invalid."""
    v = PlainValue(person_with_addresses_schema)
    bv = v.as_bundle()
    bv.set("name", "Test")
    # addresses list is empty

    # addresses exists but [0] doesn't
    with pytest.raises((IndexError, RuntimeError)):
        v.navigate("addresses[0].city")


# =============================================================================
# Section 10.4: try_navigate Tests
# =============================================================================

def test_try_navigate_success(simple_bundle_schema):
    """try_navigate() returns view on success."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 42)

    result = v.try_navigate("x")

    assert result is not None
    assert result.as_int() == 42


def test_try_navigate_failure_invalid_field(simple_bundle_schema):
    """try_navigate() returns None on invalid field."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 42)

    result = v.try_navigate("nonexistent")

    assert result is None


def test_try_navigate_failure_invalid_index(list_of_ints_schema):
    """try_navigate() returns None on invalid index."""
    v = PlainValue(list_of_ints_schema)
    lv = v.as_list()
    lv.push_back(make_int_value(10).const_view())

    result = v.try_navigate("[5]")

    assert result is None


def test_try_navigate_failure_type_mismatch():
    """try_navigate() returns None on type mismatch."""
    v = PlainValue(42)  # Scalar

    result = v.try_navigate("field")

    assert result is None


def test_try_navigate_nested_success(person_schema, address_schema):
    """try_navigate() succeeds for valid nested path."""
    v = PlainValue(person_schema)
    bv = v.as_bundle()
    bv.set("name", "Alice")

    addr = bv["address"].as_bundle()
    addr.set("city", "Boston")

    result = v.try_navigate("address.city")

    assert result is not None
    assert result.as_string() == "Boston"


def test_try_navigate_nested_failure(person_schema):
    """try_navigate() returns None for invalid nested path."""
    v = PlainValue(person_schema)
    bv = v.as_bundle()
    bv.set("name", "Alice")

    result = v.try_navigate("address.nonexistent")

    assert result is None


def test_try_navigate_empty_path(simple_bundle_schema):
    """try_navigate() with empty path returns view of self."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 42)

    result = v.try_navigate("")

    assert result is not None
    assert result.is_bundle()


# =============================================================================
# Section 10.5: Mutable Navigation Tests
# =============================================================================

def test_navigate_mut_set_value(simple_bundle_schema):
    """navigate_mut() returns mutable view that can be modified."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 0)

    # Get mutable view and modify
    mut_view = v.navigate_mut("x")
    mut_view.set_int(100)

    # Verify modification
    assert v.as_bundle()["x"].as_int() == 100


def test_navigate_mut_nested(person_schema, address_schema):
    """navigate_mut() through nested path."""
    v = PlainValue(person_schema)
    bv = v.as_bundle()
    bv.set("name", "Test")
    bv["address"].as_bundle().set("city", "Old City")

    # Modify nested value
    mut_view = v.navigate_mut("address.city")
    mut_view.set_string("New City")

    # Verify modification
    assert v.as_bundle()["address"].as_bundle()["city"].as_string() == "New City"


def test_navigate_mut_list_element(list_of_ints_schema):
    """navigate_mut() to list element for modification."""
    v = PlainValue(list_of_ints_schema)
    lv = v.as_list()
    lv.push_back(make_int_value(10).const_view())
    lv.push_back(make_int_value(20).const_view())

    # Modify list element
    mut_view = v.navigate_mut("[1]")
    mut_view.set_int(200)

    # Verify modification
    assert v.as_list()[1].as_int() == 200


# =============================================================================
# Section 10.6: Edge Cases
# =============================================================================

def test_navigate_with_numeric_field_name(type_registry, int_schema):
    """navigate() handles field names that look like numbers."""
    # Create bundle with numeric-looking field name
    schema = type_registry.bundle() \
        .field("123", int_schema) \
        .field("value", int_schema) \
        .build()

    v = PlainValue(schema)
    bv = v.as_bundle()
    bv.set("123", 42)

    # Navigate to field named "123" (not index)
    result = v.navigate("123")

    assert result is not None
    assert result.as_int() == 42


def test_navigate_whitespace_in_path_invalid():
    """navigate() rejects paths with whitespace."""
    v = PlainValue(42)

    with pytest.raises((ValueError, RuntimeError)):
        v.navigate("name .field")


def test_navigate_special_characters_in_field_name(type_registry, int_schema):
    """navigate() handles special characters in field names if allowed."""
    # Some implementations may allow underscores
    schema = type_registry.bundle() \
        .field("field_name", int_schema) \
        .field("field2", int_schema) \
        .build()

    v = PlainValue(schema)
    bv = v.as_bundle()
    bv.set("field_name", 42)

    result = v.navigate("field_name")

    assert result is not None
    assert result.as_int() == 42


# =============================================================================
# Section 10.7: ConstValueView and ValueView navigate methods
# =============================================================================

def test_const_view_navigate(simple_bundle_schema):
    """ConstValueView.navigate() works correctly."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 42)

    cv = v.const_view()
    result = cv.navigate("x")

    assert result is not None
    assert result.as_int() == 42


def test_value_view_navigate(simple_bundle_schema):
    """ValueView.navigate() works correctly."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 42)

    view = v.view()
    result = view.navigate("x")

    assert result is not None
    assert result.as_int() == 42


def test_value_view_navigate_mut(simple_bundle_schema):
    """ValueView.navigate_mut() returns mutable view."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 0)

    view = v.view()
    mut_result = view.navigate_mut("x")
    mut_result.set_int(999)

    assert v.as_bundle()["x"].as_int() == 999


# =============================================================================
# Integration with to_python/from_python
# =============================================================================

def test_navigate_result_to_python(simple_bundle_schema):
    """navigated view can be converted to Python."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("name", "test value")

    result = v.navigate("name")
    py_obj = result.to_python()

    assert py_obj == "test value"


def test_navigate_mut_from_python(simple_bundle_schema):
    """navigated mutable view can be updated from Python."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.set("x", 0)

    mut_view = v.navigate_mut("x")
    mut_view.from_python(12345)

    assert v.as_bundle()["x"].as_int() == 12345


# =============================================================================
# Section 10.8: Map Key Access Tests
# =============================================================================

@pytest.fixture
def string_to_int_map_schema(type_registry, string_schema, int_schema):
    """Create a Map[str, int] schema."""
    return type_registry.map(string_schema, int_schema).build()


@pytest.fixture
def int_to_string_map_schema(type_registry, int_schema, string_schema):
    """Create a Map[int, str] schema."""
    return type_registry.map(int_schema, string_schema).build()


@pytest.fixture
def nested_map_schema(type_registry, string_schema, address_schema):
    """Create a Map[str, Address] schema for nested access."""
    return type_registry.map(string_schema, address_schema).build()


def test_parse_quoted_string_key_double_quotes():
    """parse_path() parses quoted string key with double quotes."""
    parse_path = value.parse_path
    path = parse_path('map["key"]')

    assert len(path) == 2
    assert path[0].is_field()
    assert path[0].name == "map"
    assert path[1].is_field()  # String keys are treated as fields
    assert path[1].name == "key"


def test_parse_quoted_string_key_single_quotes():
    """parse_path() parses quoted string key with single quotes."""
    parse_path = value.parse_path
    path = parse_path("data['mykey']")

    assert len(path) == 2
    assert path[0].is_field()
    assert path[0].name == "data"
    assert path[1].is_field()
    assert path[1].name == "mykey"


def test_parse_mixed_index_and_string_key():
    """parse_path() handles mix of numeric index and string key."""
    parse_path = value.parse_path
    # items[0]["name"] - first element of list, then string key
    path = parse_path('items[0]["name"]')

    assert len(path) == 3
    assert path[0].is_field() and path[0].name == "items"
    assert path[1].is_index() and path[1].get_index() == 0
    assert path[2].is_field() and path[2].name == "name"


def test_parse_string_key_with_special_chars():
    """parse_path() parses string key with special characters."""
    parse_path = value.parse_path
    path = parse_path('map["key.with.dots"]')

    assert len(path) == 2
    assert path[1].name == "key.with.dots"


def test_parse_consecutive_string_keys():
    """parse_path() handles consecutive string keys like map["a"]["b"]."""
    parse_path = value.parse_path
    path = parse_path('nested["level1"]["level2"]')

    assert len(path) == 3
    assert path[0].is_field() and path[0].name == "nested"
    assert path[1].is_field() and path[1].name == "level1"
    assert path[2].is_field() and path[2].name == "level2"


def test_navigate_map_string_key(string_to_int_map_schema):
    """navigate() through map with string key using quoted syntax."""
    v = PlainValue(string_to_int_map_schema)
    mv = v.as_map()

    # Insert key-value pairs
    key1 = make_string_value("alpha")
    val1 = make_int_value(100)
    mv.insert(key1.const_view(), val1.const_view())

    key2 = make_string_value("beta")
    val2 = make_int_value(200)
    mv.insert(key2.const_view(), val2.const_view())

    # Navigate using string key
    result = v.navigate('["alpha"]')

    assert result is not None
    assert result.as_int() == 100


def test_navigate_map_string_key_single_quotes(string_to_int_map_schema):
    """navigate() through map with string key using single quotes."""
    v = PlainValue(string_to_int_map_schema)
    mv = v.as_map()

    key = make_string_value("mykey")
    val = make_int_value(42)
    mv.insert(key.const_view(), val.const_view())

    result = v.navigate("['mykey']")

    assert result is not None
    assert result.as_int() == 42


def test_navigate_map_int_key(int_to_string_map_schema):
    """navigate() through map with integer key."""
    v = PlainValue(int_to_string_map_schema)
    mv = v.as_map()

    key1 = make_int_value(1)
    val1 = make_string_value("one")
    mv.insert(key1.const_view(), val1.const_view())

    key2 = make_int_value(2)
    val2 = make_string_value("two")
    mv.insert(key2.const_view(), val2.const_view())

    # Navigate using integer key
    result = v.navigate("[2]")

    assert result is not None
    assert result.as_string() == "two"


def test_navigate_nested_map_value(nested_map_schema, address_schema):
    """navigate() through map to nested bundle field."""
    v = PlainValue(nested_map_schema)
    mv = v.as_map()

    # Insert an address with key "home"
    addr = PlainValue(address_schema)
    addr_bv = addr.as_bundle()
    addr_bv.set("street", "123 Main St")
    addr_bv.set("city", "Boston")
    addr_bv.set("zip", 12345)

    key = make_string_value("home")
    mv.insert(key.const_view(), addr.const_view())

    # Navigate: map["home"].city
    result = v.navigate('["home"].city')

    assert result is not None
    assert result.as_string() == "Boston"


def test_navigate_map_invalid_key_throws(string_to_int_map_schema):
    """navigate() with non-existent map key throws."""
    v = PlainValue(string_to_int_map_schema)
    mv = v.as_map()

    key = make_string_value("exists")
    val = make_int_value(42)
    mv.insert(key.const_view(), val.const_view())

    with pytest.raises((KeyError, IndexError, RuntimeError)):
        v.navigate('["nonexistent"]')


def test_try_navigate_map_key_success(string_to_int_map_schema):
    """try_navigate() returns view for valid map key."""
    v = PlainValue(string_to_int_map_schema)
    mv = v.as_map()

    key = make_string_value("key")
    val = make_int_value(999)
    mv.insert(key.const_view(), val.const_view())

    result = v.try_navigate('["key"]')

    assert result is not None
    assert result.as_int() == 999


def test_try_navigate_map_key_failure(string_to_int_map_schema):
    """try_navigate() returns None for invalid map key."""
    v = PlainValue(string_to_int_map_schema)
    # Empty map

    result = v.try_navigate('["missing"]')

    assert result is None


def test_navigate_mut_map_value(string_to_int_map_schema):
    """navigate_mut() through map key for modification."""
    v = PlainValue(string_to_int_map_schema)
    mv = v.as_map()

    key = make_string_value("count")
    val = make_int_value(0)
    mv.insert(key.const_view(), val.const_view())

    # Modify via navigate_mut
    mut_view = v.navigate_mut('["count"]')
    mut_view.set_int(42)

    # Verify
    key_lookup = make_string_value("count")
    assert mv.at(key_lookup.const_view()).as_int() == 42


# =============================================================================
# Section 10.9: Value Key Path Tests (Arbitrary Map Key Types)
# =============================================================================

@pytest.fixture
def tuple_key_map_schema(type_registry, tuple_schema, string_schema):
    """Create a Map[Tuple[int, str, double], str] schema."""
    return type_registry.map(tuple_schema, string_schema).build()


def test_path_element_value_key():
    """PathElement.key() creates a value key element."""
    PathElement = value.PathElement
    key_value = make_int_value(42)

    elem = PathElement.key(key_value.const_view())

    assert elem is not None
    assert elem.is_value()
    assert not elem.is_field()
    assert not elem.is_index()


def test_path_element_value_key_string():
    """PathElement.key() works with string key values."""
    PathElement = value.PathElement
    key_value = make_string_value("mykey")

    elem = PathElement.key(key_value.const_view())

    assert elem.is_value()
    # The stored key should match
    assert elem.get_value().as_string() == "mykey"


def test_navigate_with_value_key(string_to_int_map_schema):
    """navigate() using PathElement.key() with value key."""
    PathElement = value.PathElement
    v = PlainValue(string_to_int_map_schema)
    mv = v.as_map()

    # Insert value
    key = make_string_value("target")
    val = make_int_value(999)
    mv.insert(key.const_view(), val.const_view())

    # Build path with value key
    path = [PathElement.key(make_string_value("target").const_view())]

    result = value.navigate(v.const_view(), path)

    assert result is not None
    assert result.as_int() == 999


def test_navigate_tuple_key_map(tuple_key_map_schema, tuple_schema):
    """navigate() through map with tuple keys."""
    PathElement = value.PathElement
    v = PlainValue(tuple_key_map_schema)
    mv = v.as_map()

    # Create tuple key
    tuple_key = PlainValue(tuple_schema)
    tuple_key.as_tuple().at(0).set_int(1)
    tuple_key.as_tuple().at(1).set_string("x")
    tuple_key.as_tuple().at(2).set_double(1.0)

    # Insert value
    val = make_string_value("found")
    mv.insert(tuple_key.const_view(), val.const_view())

    # Navigate with tuple key
    path = [PathElement.key(tuple_key.const_view())]
    result = value.navigate(v.const_view(), path)

    assert result is not None
    assert result.as_string() == "found"


def test_navigate_value_key_type_mismatch(string_to_int_map_schema):
    """navigate() with wrong value key type throws."""
    PathElement = value.PathElement
    v = PlainValue(string_to_int_map_schema)  # Has string keys
    mv = v.as_map()

    key = make_string_value("test")
    val = make_int_value(42)
    mv.insert(key.const_view(), val.const_view())

    # Try to navigate with int key on string-keyed map
    int_key = make_int_value(123)
    path = [PathElement.key(int_key.const_view())]

    with pytest.raises((TypeError, RuntimeError)):
        value.navigate(v.const_view(), path)


def test_navigate_mixed_path_with_value_key(type_registry, string_schema, int_schema, address_schema):
    """navigate() with mixed field and value key access."""
    PathElement = value.PathElement

    # Create: Bundle{data: Map[str, Address]}
    map_schema = type_registry.map(string_schema, address_schema).build()
    bundle_schema = type_registry.bundle() \
        .field("data", map_schema) \
        .build()

    v = PlainValue(bundle_schema)
    bv = v.as_bundle()

    # Insert address into the map
    addr = PlainValue(address_schema)
    addr.as_bundle().set("street", "123 Main")
    addr.as_bundle().set("city", "Boston")
    addr.as_bundle().set("zip", 12345)

    map_view = bv["data"].as_map()
    key = make_string_value("home")
    map_view.insert(key.const_view(), addr.const_view())

    # Navigate: data[<value key>].city
    path = [
        PathElement.field("data"),
        PathElement.key(make_string_value("home").const_view()),
        PathElement.field("city")
    ]

    result = value.navigate(v.const_view(), path)

    assert result is not None
    assert result.as_string() == "Boston"
