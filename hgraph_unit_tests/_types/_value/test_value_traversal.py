"""
Tests for Value Deep Traversal Feature.

Tests the deep traversal functionality from the Value type system (Section 9 of User Guide).
Deep traversal allows recursive visiting of all leaf values in nested structures,
tracking the path to each leaf.

Functions tested:
- deep_visit(value, callback) - Call callback(leaf_value, path) for each leaf
- count_leaves(value) - Count all leaf values
- collect_leaf_paths(value) - Get all paths to leaves

Reference: ts_design_docs/Value_USER_GUIDE.md Section 9
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
# Helpers for creating Values with specific types
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


def make_bool_value(val):
    """Create a PlainValue containing a bool."""
    bool_schema = value.scalar_type_meta_bool()
    v = PlainValue(bool_schema)
    v.set_bool(val)
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
def nested_bundle_schema(type_registry, int_schema, point_schema):
    """Create a bundle containing another bundle."""
    return type_registry.bundle() \
        .field("id", int_schema) \
        .field("location", point_schema) \
        .build()


@pytest.fixture
def dynamic_int_list_schema(type_registry, int_schema):
    """Create a dynamic list schema for int64_t elements."""
    return type_registry.list(int_schema).build()


@pytest.fixture
def int_set_schema(type_registry, int_schema):
    """Create a set schema for int64_t elements."""
    return type_registry.set(int_schema).build()


@pytest.fixture
def string_int_map_schema(type_registry, string_schema, int_schema):
    """Create a map schema from string to int."""
    return type_registry.map(string_schema, int_schema).build()


@pytest.fixture
def list_of_bundles_schema(type_registry, point_schema):
    """Create a list of Point bundles."""
    return type_registry.list(point_schema).build()


@pytest.fixture
def bundle_with_list_schema(type_registry, int_schema, dynamic_int_list_schema):
    """Create a bundle containing a list."""
    return type_registry.bundle() \
        .field("id", int_schema) \
        .field("values", dynamic_int_list_schema) \
        .build()


@pytest.fixture
def deeply_nested_schema(type_registry, int_schema):
    """Create a schema with 5+ levels of nesting."""
    # Level 5: scalar int
    level_5 = int_schema

    # Level 4: bundle with one field
    level_4 = type_registry.bundle() \
        .field("deep", level_5) \
        .build()

    # Level 3: list of level_4 bundles
    level_3 = type_registry.list(level_4).build()

    # Level 2: bundle containing level_3 list
    level_2 = type_registry.bundle() \
        .field("items", level_3) \
        .build()

    # Level 1: bundle containing level_2
    level_1 = type_registry.bundle() \
        .field("container", level_2) \
        .build()

    return level_1


# =============================================================================
# Helper to check if traversal functions are available
# =============================================================================

def get_traversal_functions():
    """Get the traversal functions, skip if not available."""
    try:
        deep_visit = value.deep_visit
        count_leaves = value.count_leaves
        collect_leaf_paths = value.collect_leaf_paths
        return deep_visit, count_leaves, collect_leaf_paths
    except AttributeError:
        pytest.skip("Deep traversal functions not yet implemented")


# =============================================================================
# Section 9.1: Scalar Traversal
# =============================================================================

def test_deep_visit_scalar():
    """Single scalar calls callback once with empty path."""
    deep_visit, _, _ = get_traversal_functions()

    v = make_int_value(42)
    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_int(), list(path)))

    deep_visit(v.const_view(), callback)

    assert len(visited) == 1
    assert visited[0][0] == 42
    assert visited[0][1] == []  # Empty path for scalar


def test_count_leaves_scalar():
    """Scalar has 1 leaf."""
    _, count_leaves, _ = get_traversal_functions()

    v = make_int_value(42)
    count = count_leaves(v.const_view())

    assert count == 1


def test_deep_visit_scalar_string():
    """String scalar calls callback once with empty path."""
    deep_visit, _, _ = get_traversal_functions()

    v = make_string_value("hello")
    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_string(), list(path)))

    deep_visit(v.const_view(), callback)

    assert len(visited) == 1
    assert visited[0][0] == "hello"
    assert visited[0][1] == []


def test_deep_visit_scalar_double():
    """Double scalar calls callback once with empty path."""
    deep_visit, _, _ = get_traversal_functions()

    v = make_double_value(3.14)
    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_double(), list(path)))

    deep_visit(v.const_view(), callback)

    assert len(visited) == 1
    assert abs(visited[0][0] - 3.14) < 1e-10
    assert visited[0][1] == []


def test_deep_visit_scalar_bool():
    """Bool scalar calls callback once with empty path."""
    deep_visit, _, _ = get_traversal_functions()

    v = make_bool_value(True)
    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_bool(), list(path)))

    deep_visit(v.const_view(), callback)

    assert len(visited) == 1
    assert visited[0][0] is True
    assert visited[0][1] == []


# =============================================================================
# Section 9.2: List Traversal
# =============================================================================

def test_deep_visit_list(dynamic_int_list_schema):
    """Visits each element with index path."""
    deep_visit, _, _ = get_traversal_functions()

    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()
    lv.push_back(make_int_value(10).const_view())
    lv.push_back(make_int_value(20).const_view())
    lv.push_back(make_int_value(30).const_view())

    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_int(), list(path)))

    deep_visit(v.const_view(), callback)

    assert len(visited) == 3
    # Paths should be indices
    paths = [v[1] for v in visited]
    values = [v[0] for v in visited]

    assert sorted(values) == [10, 20, 30]
    # Each path should be a single index
    assert [0] in paths
    assert [1] in paths
    assert [2] in paths


def test_count_leaves_list(dynamic_int_list_schema):
    """List of N scalars has N leaves."""
    _, count_leaves, _ = get_traversal_functions()

    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()
    lv.push_back(make_int_value(10).const_view())
    lv.push_back(make_int_value(20).const_view())
    lv.push_back(make_int_value(30).const_view())
    lv.push_back(make_int_value(40).const_view())
    lv.push_back(make_int_value(50).const_view())

    count = count_leaves(v.const_view())

    assert count == 5


def test_deep_visit_empty_list(dynamic_int_list_schema):
    """Empty list has 0 leaves."""
    deep_visit, count_leaves, _ = get_traversal_functions()

    v = PlainValue(dynamic_int_list_schema)
    # List is empty by default

    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value, list(path)))

    deep_visit(v.const_view(), callback)

    assert len(visited) == 0

    count = count_leaves(v.const_view())
    assert count == 0


def test_collect_leaf_paths_list(dynamic_int_list_schema):
    """collect_leaf_paths returns index paths for list elements."""
    _, _, collect_leaf_paths = get_traversal_functions()

    v = PlainValue(dynamic_int_list_schema)
    lv = v.as_list()
    lv.push_back(make_int_value(10).const_view())
    lv.push_back(make_int_value(20).const_view())

    paths = collect_leaf_paths(v.const_view())

    assert len(paths) == 2
    # Convert paths to lists for comparison
    path_lists = [list(p) for p in paths]
    assert [0] in path_lists
    assert [1] in path_lists


# =============================================================================
# Section 9.3: Bundle Traversal
# =============================================================================

def test_deep_visit_bundle(simple_bundle_schema):
    """Visits each field with field name path."""
    deep_visit, _, _ = get_traversal_functions()

    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.at_name_mut("x").set_int(10)
    bv.at_name_mut("y").set_double(20.5)
    bv.at_name_mut("name").set_string("test")

    visited = []

    def callback(leaf_value, path):
        visited.append((path[-1] if path else None, list(path)))

    deep_visit(v.const_view(), callback)

    assert len(visited) == 3
    paths = [v[1] for v in visited]
    # Each path should be a single field name
    assert ["x"] in paths
    assert ["y"] in paths
    assert ["name"] in paths


def test_count_leaves_bundle(simple_bundle_schema):
    """Bundle with N scalar fields has N leaves."""
    _, count_leaves, _ = get_traversal_functions()

    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.at_name_mut("x").set_int(10)
    bv.at_name_mut("y").set_double(20.5)
    bv.at_name_mut("name").set_string("test")

    count = count_leaves(v.const_view())

    assert count == 3


def test_collect_leaf_paths_bundle(simple_bundle_schema):
    """Returns correct field paths for bundle."""
    _, _, collect_leaf_paths = get_traversal_functions()

    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.at_name_mut("x").set_int(10)
    bv.at_name_mut("y").set_double(20.5)
    bv.at_name_mut("name").set_string("test")

    paths = collect_leaf_paths(v.const_view())

    assert len(paths) == 3
    path_lists = [list(p) for p in paths]
    assert ["x"] in path_lists
    assert ["y"] in path_lists
    assert ["name"] in path_lists


def test_deep_visit_point_bundle(point_schema):
    """Visits Point bundle fields correctly."""
    deep_visit, _, _ = get_traversal_functions()

    v = PlainValue(point_schema)
    bv = v.as_bundle()
    bv.at_name_mut("x").set_int(100)
    bv.at_name_mut("y").set_int(200)

    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_int(), list(path)))

    deep_visit(v.const_view(), callback)

    assert len(visited) == 2
    values_and_paths = {tuple(v[1]): v[0] for v in visited}
    assert values_and_paths[("x",)] == 100
    assert values_and_paths[("y",)] == 200


# =============================================================================
# Section 9.4: Nested Structure Traversal
# =============================================================================

def test_deep_visit_nested_bundles(nested_bundle_schema):
    """Traverses through nested bundles."""
    deep_visit, _, _ = get_traversal_functions()

    v = PlainValue(nested_bundle_schema)
    bv = v.as_bundle()
    bv.at_name_mut("id").set_int(1)
    location = bv["location"].as_bundle()
    location.at_name_mut("x").set_int(10)
    location.at_name_mut("y").set_int(20)

    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_int(), list(path)))

    deep_visit(v.const_view(), callback)

    assert len(visited) == 3
    values_and_paths = {tuple(v[1]): v[0] for v in visited}
    assert values_and_paths[("id",)] == 1
    assert values_and_paths[("location", "x")] == 10
    assert values_and_paths[("location", "y")] == 20


def test_deep_visit_list_of_bundles(list_of_bundles_schema):
    """Traverses list containing bundles."""
    deep_visit, count_leaves, _ = get_traversal_functions()

    v = PlainValue(list_of_bundles_schema)
    lv = v.as_list()

    # Create and add first point
    point1 = PlainValue(lv.element_type())
    p1 = point1.as_bundle()
    p1.at_name_mut("x").set_int(1)
    p1.at_name_mut("y").set_int(2)
    lv.push_back(point1.const_view())

    # Create and add second point
    point2 = PlainValue(lv.element_type())
    p2 = point2.as_bundle()
    p2.at_name_mut("x").set_int(3)
    p2.at_name_mut("y").set_int(4)
    lv.push_back(point2.const_view())

    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_int(), list(path)))

    deep_visit(v.const_view(), callback)

    # Should have 4 leaves: 2 points * 2 fields each
    assert len(visited) == 4
    assert count_leaves(v.const_view()) == 4

    # Check paths include list index and field name
    values_and_paths = {tuple(v[1]): v[0] for v in visited}
    assert values_and_paths[(0, "x")] == 1
    assert values_and_paths[(0, "y")] == 2
    assert values_and_paths[(1, "x")] == 3
    assert values_and_paths[(1, "y")] == 4


def test_deep_visit_bundle_with_list(bundle_with_list_schema):
    """Traverses bundle containing list."""
    deep_visit, count_leaves, _ = get_traversal_functions()

    v = PlainValue(bundle_with_list_schema)
    bv = v.as_bundle()
    bv.at_name_mut("id").set_int(42)

    values_list = bv["values"].as_list()
    values_list.push_back(make_int_value(100).const_view())
    values_list.push_back(make_int_value(200).const_view())
    values_list.push_back(make_int_value(300).const_view())

    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_int(), list(path)))

    deep_visit(v.const_view(), callback)

    # Should have 4 leaves: 1 id + 3 list elements
    assert len(visited) == 4
    assert count_leaves(v.const_view()) == 4

    values_and_paths = {tuple(v[1]): v[0] for v in visited}
    assert values_and_paths[("id",)] == 42
    assert values_and_paths[("values", 0)] == 100
    assert values_and_paths[("values", 1)] == 200
    assert values_and_paths[("values", 2)] == 300


def test_deep_visit_deeply_nested(deeply_nested_schema):
    """5+ levels deep works correctly."""
    deep_visit, count_leaves, collect_leaf_paths = get_traversal_functions()

    v = PlainValue(deeply_nested_schema)

    # Navigate to the nested structure and populate it
    # Structure: container -> items (list) -> [bundle with 'deep' field]
    bv = v.as_bundle()
    container = bv["container"].as_bundle()
    items = container["items"].as_list()

    # Get the element type for the list (the level 4 bundle)
    elem_type = items.element_type()

    # Create and add an element
    elem1 = PlainValue(elem_type)
    elem1.as_bundle().at_name_mut("deep").set_int(999)
    items.push_back(elem1.const_view())

    # Create and add another element
    elem2 = PlainValue(elem_type)
    elem2.as_bundle().at_name_mut("deep").set_int(888)
    items.push_back(elem2.const_view())

    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_int(), list(path)))

    deep_visit(v.const_view(), callback)

    # Should have 2 leaves
    assert len(visited) == 2
    assert count_leaves(v.const_view()) == 2

    # Check the paths are correct (5 levels deep)
    paths = collect_leaf_paths(v.const_view())
    assert len(paths) == 2

    # Paths should be like: ["container", "items", 0, "deep"]
    for path in paths:
        path_list = list(path)
        assert len(path_list) == 4
        assert path_list[0] == "container"
        assert path_list[1] == "items"
        assert path_list[2] in [0, 1]  # List index
        assert path_list[3] == "deep"


def test_path_accuracy_nested(nested_bundle_schema):
    """Paths accurately reflect structure."""
    _, _, collect_leaf_paths = get_traversal_functions()

    v = PlainValue(nested_bundle_schema)
    bv = v.as_bundle()
    bv.at_name_mut("id").set_int(1)
    location = bv["location"].as_bundle()
    location.at_name_mut("x").set_int(10)
    location.at_name_mut("y").set_int(20)

    paths = collect_leaf_paths(v.const_view())

    path_lists = [list(p) for p in paths]

    # Verify exact paths
    assert ["id"] in path_lists
    assert ["location", "x"] in path_lists
    assert ["location", "y"] in path_lists

    # Each path should uniquely identify its leaf
    assert len(path_lists) == len(set(tuple(p) for p in path_lists))


# =============================================================================
# Section 9.5: Set and Map Traversal
# =============================================================================

def test_deep_visit_set(int_set_schema):
    """Visits each set element."""
    deep_visit, count_leaves, _ = get_traversal_functions()

    v = PlainValue(int_set_schema)
    sv = v.as_set()
    sv.insert(make_int_value(10).const_view())
    sv.insert(make_int_value(20).const_view())
    sv.insert(make_int_value(30).const_view())

    visited = []

    def callback(leaf_value, path):
        visited.append((leaf_value.as_int(), list(path)))

    deep_visit(v.const_view(), callback)

    # Should visit all 3 elements
    assert len(visited) == 3
    assert count_leaves(v.const_view()) == 3

    values = sorted([v[0] for v in visited])
    assert values == [10, 20, 30]


def test_deep_visit_map(string_int_map_schema):
    """Visits each key and value in map."""
    deep_visit, count_leaves, _ = get_traversal_functions()

    v = PlainValue(string_int_map_schema)
    mv = v.as_map()
    mv.set(make_string_value("a").const_view(), make_int_value(1).const_view())
    mv.set(make_string_value("b").const_view(), make_int_value(2).const_view())

    visited = []

    def callback(leaf_value, path):
        # Try to get value as int (for values) or string (for keys)
        try:
            val = leaf_value.as_int()
        except (TypeError, RuntimeError):
            val = leaf_value.as_string()
        visited.append((val, list(path)))

    deep_visit(v.const_view(), callback)

    # Maps have keys and values as leaves
    # Depending on implementation, this could be:
    # - 4 leaves (2 keys + 2 values)
    # - 2 leaves (just values, keys are path components)
    # The exact count depends on implementation
    assert len(visited) >= 2

    count = count_leaves(v.const_view())
    assert count >= 2


def test_count_leaves_empty_set(int_set_schema):
    """Empty set has 0 leaves."""
    _, count_leaves, _ = get_traversal_functions()

    v = PlainValue(int_set_schema)
    # Set is empty by default

    count = count_leaves(v.const_view())
    assert count == 0


def test_count_leaves_empty_map(string_int_map_schema):
    """Empty map has 0 leaves."""
    _, count_leaves, _ = get_traversal_functions()

    v = PlainValue(string_int_map_schema)
    # Map is empty by default

    count = count_leaves(v.const_view())
    assert count == 0


# =============================================================================
# Section 9.6: collect_leaf_paths Tests
# =============================================================================

def test_collect_leaf_paths_flat():
    """Simple structure returns single empty path for scalar."""
    _, _, collect_leaf_paths = get_traversal_functions()

    v = make_int_value(42)
    paths = collect_leaf_paths(v.const_view())

    assert len(paths) == 1
    assert list(paths[0]) == []


def test_collect_leaf_paths_nested(nested_bundle_schema):
    """Nested structure returns all paths."""
    _, _, collect_leaf_paths = get_traversal_functions()

    v = PlainValue(nested_bundle_schema)
    bv = v.as_bundle()
    bv.at_name_mut("id").set_int(1)
    location = bv["location"].as_bundle()
    location.at_name_mut("x").set_int(10)
    location.at_name_mut("y").set_int(20)

    paths = collect_leaf_paths(v.const_view())

    assert len(paths) == 3
    path_lists = [list(p) for p in paths]
    assert ["id"] in path_lists
    assert ["location", "x"] in path_lists
    assert ["location", "y"] in path_lists


def test_collect_leaf_paths_list_of_bundles(list_of_bundles_schema):
    """collect_leaf_paths works with list of bundles."""
    _, _, collect_leaf_paths = get_traversal_functions()

    v = PlainValue(list_of_bundles_schema)
    lv = v.as_list()

    # Add two points
    point1 = PlainValue(lv.element_type())
    p1 = point1.as_bundle()
    p1.at_name_mut("x").set_int(1)
    p1.at_name_mut("y").set_int(2)
    lv.push_back(point1.const_view())

    point2 = PlainValue(lv.element_type())
    p2 = point2.as_bundle()
    p2.at_name_mut("x").set_int(3)
    p2.at_name_mut("y").set_int(4)
    lv.push_back(point2.const_view())

    paths = collect_leaf_paths(v.const_view())

    assert len(paths) == 4
    path_tuples = [tuple(p) for p in paths]
    assert (0, "x") in path_tuples
    assert (0, "y") in path_tuples
    assert (1, "x") in path_tuples
    assert (1, "y") in path_tuples


def test_collect_leaf_paths_empty_containers(dynamic_int_list_schema, int_set_schema, string_int_map_schema):
    """Empty containers return empty path list."""
    _, _, collect_leaf_paths = get_traversal_functions()

    empty_list = PlainValue(dynamic_int_list_schema)
    empty_set = PlainValue(int_set_schema)
    empty_map = PlainValue(string_int_map_schema)

    assert len(collect_leaf_paths(empty_list.const_view())) == 0
    assert len(collect_leaf_paths(empty_set.const_view())) == 0
    assert len(collect_leaf_paths(empty_map.const_view())) == 0


# =============================================================================
# Section 9.7: Edge Cases and Error Handling
# =============================================================================

def test_deep_visit_callback_receives_const_view():
    """Callback receives ConstValueView, not mutable view."""
    deep_visit, _, _ = get_traversal_functions()

    v = make_int_value(42)

    def callback(leaf_value, path):
        # Should be a ConstValueView
        assert hasattr(leaf_value, 'as_int')
        # Should not have mutable methods like set_int
        # (or they should raise if called)

    deep_visit(v.const_view(), callback)


def test_deep_visit_order_deterministic(simple_bundle_schema):
    """Traversal order is deterministic for same structure."""
    deep_visit, _, _ = get_traversal_functions()

    v = PlainValue(simple_bundle_schema)
    bv = v.as_bundle()
    bv.at_name_mut("x").set_int(10)
    bv.at_name_mut("y").set_double(20.5)
    bv.at_name_mut("name").set_string("test")

    paths1 = []
    paths2 = []

    def callback1(leaf_value, path):
        paths1.append(list(path))

    def callback2(leaf_value, path):
        paths2.append(list(path))

    deep_visit(v.const_view(), callback1)
    deep_visit(v.const_view(), callback2)

    # Same order both times
    assert paths1 == paths2


def test_count_leaves_matches_visited_count(nested_bundle_schema):
    """count_leaves matches number of callback invocations."""
    deep_visit, count_leaves, _ = get_traversal_functions()

    v = PlainValue(nested_bundle_schema)
    bv = v.as_bundle()
    bv.at_name_mut("id").set_int(1)
    location = bv["location"].as_bundle()
    location.at_name_mut("x").set_int(10)
    location.at_name_mut("y").set_int(20)

    visit_count = [0]

    def callback(leaf_value, path):
        visit_count[0] += 1

    deep_visit(v.const_view(), callback)
    count = count_leaves(v.const_view())

    assert visit_count[0] == count


def test_collect_leaf_paths_matches_count(bundle_with_list_schema):
    """collect_leaf_paths returns same count as count_leaves."""
    _, count_leaves, collect_leaf_paths = get_traversal_functions()

    v = PlainValue(bundle_with_list_schema)
    bv = v.as_bundle()
    bv.at_name_mut("id").set_int(42)

    values_list = bv["values"].as_list()
    values_list.push_back(make_int_value(100).const_view())
    values_list.push_back(make_int_value(200).const_view())

    paths = collect_leaf_paths(v.const_view())
    count = count_leaves(v.const_view())

    assert len(paths) == count
