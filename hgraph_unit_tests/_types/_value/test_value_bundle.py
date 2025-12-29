"""
Tests for Value Bundle types.

Tests the Bundle type from the Value type system (Section 5 of User Guide).
Bundles are struct-like containers with ordered, named fields.
Fields can be accessed by BOTH index position AND name.

Reference: ts_design_docs/Value_USER_GUIDE.md Section 5
"""

import pytest

# Skip all tests if C++ extension is not available
_hgraph = pytest.importorskip("hgraph._hgraph")
value = _hgraph.value  # Value types are in the value submodule

# Convenience aliases
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
def simple_bundle_schema(type_registry, int_schema, double_schema, string_schema):
    """Create a simple anonymous bundle schema with x, y, name fields."""
    return type_registry.bundle() \
        .field("x", int_schema) \
        .field("y", double_schema) \
        .field("name", string_schema) \
        .build()


@pytest.fixture
def point_schema(type_registry, int_schema):
    """Create a named bundle schema called 'Point' with x, y fields."""
    return type_registry.bundle("Point") \
        .field("x", int_schema) \
        .field("y", int_schema) \
        .build()


@pytest.fixture
def single_field_bundle_schema(type_registry, int_schema):
    """Create a bundle with a single field."""
    return type_registry.bundle() \
        .field("value", int_schema) \
        .build()


@pytest.fixture
def nested_bundle_schema(type_registry, int_schema, point_schema):
    """Create a bundle containing another bundle."""
    return type_registry.bundle() \
        .field("id", int_schema) \
        .field("location", point_schema) \
        .build()


# =============================================================================
# Section 5.1: Creating Bundle Schemas
# =============================================================================

def test_create_anonymous_bundle_schema(simple_bundle_schema):
    """Anonymous bundle schema can be created with named fields."""
    assert simple_bundle_schema is not None
    assert simple_bundle_schema.field_count == 3


def test_create_named_bundle_schema(point_schema):
    """Named bundle schema can be created and has expected fields."""
    assert point_schema is not None
    assert point_schema.field_count == 2


def test_retrieve_named_bundle_by_name(type_registry, point_schema):
    """Named bundle can be retrieved by name from registry."""
    retrieved = type_registry.get_bundle_by_name("Point")
    assert retrieved == point_schema


def test_bundle_schema_is_kind_bundle(simple_bundle_schema):
    """Bundle schema has TypeKind.Bundle."""
    assert simple_bundle_schema.kind == TypeKind.Bundle


def test_bundle_field_names(simple_bundle_schema):
    """Bundle fields have proper names."""
    field_names = [simple_bundle_schema.fields[i].name
                   for i in range(simple_bundle_schema.field_count)]
    assert field_names == ["x", "y", "name"]


def test_bundle_field_types(simple_bundle_schema, int_schema, double_schema, string_schema):
    """Bundle fields have correct types."""
    assert simple_bundle_schema.fields[0].type == int_schema
    assert simple_bundle_schema.fields[1].type == double_schema
    assert simple_bundle_schema.fields[2].type == string_schema


def test_bundle_field_indices(simple_bundle_schema):
    """Bundle fields have correct indices matching order added."""
    assert simple_bundle_schema.fields[0].index == 0
    assert simple_bundle_schema.fields[1].index == 1
    assert simple_bundle_schema.fields[2].index == 2


def test_single_field_bundle(single_field_bundle_schema):
    """Bundle with single field is valid."""
    assert single_field_bundle_schema is not None
    assert single_field_bundle_schema.field_count == 1


def test_empty_bundle_allowed(type_registry):
    """Empty bundles are allowed (field_count = 0)."""
    empty_bundle = type_registry.bundle().build()
    assert empty_bundle is not None
    assert empty_bundle.field_count == 0


# =============================================================================
# Section 5.2: Creating and Accessing Bundle Values
# =============================================================================

def test_create_bundle_value_from_schema(simple_bundle_schema):
    """Value can be created from bundle schema."""
    v = PlainValue(simple_bundle_schema)
    assert v.valid()


def test_bundle_value_has_correct_schema(simple_bundle_schema):
    """Bundle value reports correct schema."""
    v = PlainValue(simple_bundle_schema)
    assert v.schema == simple_bundle_schema


def test_bundle_view_set_by_name_with_native_types(simple_bundle_schema):
    """BundleView.set(name, value) auto-wraps native types."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()

    bv.set("x", 10)
    bv.set("y", 20.5)
    bv.set("name", "origin")

    assert bv["x"].as_int() == 10
    assert abs(bv["y"].as_double() - 20.5) < 1e-10
    assert bv["name"].as_string() == "origin"


def test_bundle_view_set_via_at_name_mut(simple_bundle_schema):
    """BundleView.at_name_mut() returns mutable view for setting values."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()

    bv.at_name_mut("x").set_int(10)
    bv.at_name_mut("y").set_double(20.5)
    bv.at_name_mut("name").set_string("origin")

    assert bv["x"].as_int() == 10
    assert abs(bv["y"].as_double() - 20.5) < 1e-10
    assert bv["name"].as_string() == "origin"


def test_bundle_view_set_by_index(simple_bundle_schema):
    """BundleView field access by index."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()

    bv.at(0).set_int(100)
    bv.at(1).set_double(25.5)

    assert bv.at(0).as_int() == 100
    assert abs(bv.at(1).as_double() - 25.5) < 1e-10


def test_const_bundle_view_at_by_name(simple_bundle_schema):
    """ConstBundleView.at_name(name) provides read access."""
    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.at_name_mut("x").set_int(42)
    bv.at_name_mut("y").set_double(3.14)
    bv.at_name_mut("name").set_string("test")

    cbv = v.const_view().as_bundle()

    assert cbv.at_name("x").as_int() == 42
    assert abs(cbv["y"].as_double() - 3.14) < 1e-10
    assert cbv["name"].as_string() == "test"


def test_bundle_has_field(simple_bundle_schema):
    """has_field() returns True for existing fields."""
    v = PlainValue(simple_bundle_schema)
    cbv = v.const_view().as_bundle()

    assert cbv.has_field("x")
    assert cbv.has_field("y")
    assert cbv.has_field("name")
    assert not cbv.has_field("nonexistent")


def test_bundle_field_index_lookup(simple_bundle_schema):
    """field_index() returns correct index for field name."""
    v = PlainValue(simple_bundle_schema)
    cbv = v.const_view().as_bundle()

    assert cbv.field_index("x") == 0
    assert cbv.field_index("y") == 1
    assert cbv.field_index("name") == 2


def test_nested_bundle_access(nested_bundle_schema):
    """Nested bundle fields can be accessed."""
    v = PlainValue(nested_bundle_schema)
    bv = v.as_bundle()

    bv.at_name_mut("id").set_int(42)

    location = bv["location"].as_bundle()
    location.at_name_mut("x").set_int(10)
    location.at_name_mut("y").set_int(20)

    assert bv["id"].as_int() == 42
    assert bv["location"].as_bundle()["x"].as_int() == 10
    assert bv["location"].as_bundle()["y"].as_int() == 20


def test_bundle_access_nonexistent_field_raises(simple_bundle_schema):
    """Accessing non-existent field raises error."""
    v = PlainValue(simple_bundle_schema)
    cbv = v.const_view().as_bundle()

    with pytest.raises((KeyError, RuntimeError)):
        _ = cbv.at_name("nonexistent")


def test_bundle_equals_same_values(simple_bundle_schema):
    """Bundles with same values are equal."""
    v1 = PlainValue(simple_bundle_schema)
    bv1 = v1.as_bundle()
    bv1.at_name_mut("x").set_int(42)
    bv1.at_name_mut("y").set_double(3.14)
    bv1.at_name_mut("name").set_string("test")

    v2 = PlainValue(simple_bundle_schema)
    bv2 = v2.as_bundle()
    bv2.at_name_mut("x").set_int(42)
    bv2.at_name_mut("y").set_double(3.14)
    bv2.at_name_mut("name").set_string("test")

    assert v1.equals(v2.const_view())


def test_bundle_not_equals_different_values(simple_bundle_schema):
    """Bundles with different values are not equal."""
    v1 = PlainValue(simple_bundle_schema)
    bv1 = v1.as_bundle()
    bv1.at_name_mut("x").set_int(42)

    v2 = PlainValue(simple_bundle_schema)
    bv2 = v2.as_bundle()
    bv2.at_name_mut("x").set_int(100)

    assert not v1.equals(v2.const_view())
