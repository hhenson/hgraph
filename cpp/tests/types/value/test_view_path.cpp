/**
 * @file test_view_path.cpp
 * @brief Unit tests for view path tracking and ViewRange iterators.
 *
 * Phase 2 tests for:
 * - View owner() and path() methods
 * - Path tracking through navigation
 * - Small-path-optimization
 * - ViewRange iteration
 * - ViewPairRange iteration
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/types/value/path.h>
#include <hgraph/types/value/type_registry.h>

using namespace hgraph::value;

// ============================================================================
// Owner and Path Method Existence Tests
// ============================================================================

TEST_CASE("View - has owner() method", "[value][phase2][view_path]") {
    SKIP("Awaiting Phase 2 implementation - View::owner()");

    PlainValue v(42);
    View view = v.const_view();

    // FUTURE: const void* owner = view.owner();
    // Owner should point to the Value for root views
}

TEST_CASE("View - has path() method", "[value][phase2][view_path]") {
    SKIP("Awaiting Phase 2 implementation - View::path()");

    PlainValue v(42);
    View view = v.const_view();

    // FUTURE: const ValuePath& path = view.path();
    // Path should be empty for root views
    // CHECK(path.empty());
}

// ============================================================================
// Path Tracking Through Navigation
// ============================================================================

TEST_CASE("View path - root view has empty path", "[value][phase2][view_path]") {
    SKIP("Awaiting Phase 2 implementation - path tracking");

    auto& registry = TypeRegistry::instance();
    auto* bundle_type = registry.bundle("TestBundle")
        .field("x", registry.register_scalar<int64_t>())
        .build();
    PlainValue bundle(bundle_type);

    View root = bundle.const_view();
    // FUTURE: CHECK(root.path().empty());
}

TEST_CASE("View path - field access adds field element to path", "[value][phase2][view_path]") {
    SKIP("Awaiting Phase 2 implementation - path tracking");

    auto& registry = TypeRegistry::instance();
    auto* bundle_type = registry.bundle("TestBundle2")
        .field("name", registry.register_scalar<std::string>())
        .build();
    PlainValue bundle(bundle_type);

    ConstBundleView bv = bundle.const_view().as_bundle();
    // FUTURE: View name_view = bv.at("name");
    // FUTURE: CHECK(name_view.path().size() == 1);
    // FUTURE: CHECK(name_view.path()[0].is_field());
    // FUTURE: CHECK(name_view.path()[0].name() == "name");
}

TEST_CASE("View path - index access adds index element to path", "[value][phase2][view_path]") {
    SKIP("Awaiting Phase 2 implementation - path tracking");

    auto& registry = TypeRegistry::instance();
    auto* list_type = registry.list(registry.register_scalar<int64_t>()).build();
    PlainValue list(list_type);

    list.as_list().push_back(int64_t{1});
    list.as_list().push_back(int64_t{2});

    ConstListView lv = list.const_view().as_list();
    // FUTURE: View elem = lv[0];
    // FUTURE: CHECK(elem.path().size() == 1);
    // FUTURE: CHECK(elem.path()[0].is_index());
    // FUTURE: CHECK(elem.path()[0].get_index() == 0);
}

TEST_CASE("View path - nested access accumulates path elements", "[value][phase2][view_path]") {
    SKIP("Awaiting Phase 2 implementation - path tracking");

    // Create nested structure: outer.inner.value
    auto& registry = TypeRegistry::instance();
    auto* inner = registry.bundle("Inner")
        .field("value", registry.register_scalar<int64_t>())
        .build();
    auto* outer = registry.bundle("Outer")
        .field("inner", inner)
        .build();

    PlainValue v(outer);

    // Navigate: outer.inner.value
    // FUTURE:
    // View view = v.const_view().as_bundle().at("inner").as_bundle().at("value");
    // CHECK(view.path().size() == 2);
    // CHECK(view.path()[0].name() == "inner");
    // CHECK(view.path()[1].name() == "value");
}

TEST_CASE("View path - owner pointer points to root value", "[value][phase2][view_path]") {
    SKIP("Awaiting Phase 2 implementation - owner tracking");

    auto& registry = TypeRegistry::instance();
    auto* bundle_type = registry.bundle("TestBundle3")
        .field("x", registry.register_scalar<int64_t>())
        .build();
    PlainValue bundle(bundle_type);

    ConstBundleView bv = bundle.const_view().as_bundle();
    // FUTURE: View x_view = bv.at("x");
    // FUTURE: CHECK(x_view.owner() == bundle.data());
}

// ============================================================================
// Small-Path-Optimization Tests
// ============================================================================

TEST_CASE("ValuePath - inline storage for paths <= 3 levels", "[value][phase2][path_spo]") {
    SKIP("Awaiting Phase 2 implementation - small-path-optimization");

    ValuePath path;
    path.push_back(PathElement::field("a"));
    path.push_back(PathElement::field("b"));
    path.push_back(PathElement::field("c"));

    CHECK(path.size() == 3);
    // Should use inline storage (performance optimization)
}

TEST_CASE("ValuePath - overflow to heap for paths > 3 levels", "[value][phase2][path_spo]") {
    SKIP("Awaiting Phase 2 implementation - small-path-optimization");

    ValuePath path;
    path.push_back(PathElement::field("a"));
    path.push_back(PathElement::field("b"));
    path.push_back(PathElement::field("c"));
    path.push_back(PathElement::field("d"));
    path.push_back(PathElement::field("e"));

    CHECK(path.size() == 5);
    CHECK(path[0].name() == "a");
    CHECK(path[4].name() == "e");
}

// ============================================================================
// ViewRange Tests
// ============================================================================

TEST_CASE("ViewRange - default construction creates empty range", "[value][phase2][view_range]") {
    SKIP("Awaiting Phase 2 implementation - ViewRange");

    // FUTURE: ViewRange range;
    // CHECK(range.empty());
    // CHECK(range.size() == 0);
}

TEST_CASE("ViewRange - iteration yields View for each element", "[value][phase2][view_range]") {
    SKIP("Awaiting Phase 2 implementation - ViewRange");

    auto& registry = TypeRegistry::instance();
    auto* list_type = registry.list(registry.register_scalar<int64_t>()).build();
    PlainValue list(list_type);

    list.as_list().push_back(int64_t{10});
    list.as_list().push_back(int64_t{20});
    list.as_list().push_back(int64_t{30});

    // FUTURE: ViewRange range = list.const_view().as_list().elements();
    // CHECK(range.size() == 3);

    // size_t idx = 0;
    // for (View elem : range) {
    //     CHECK(elem.valid());
    //     ++idx;
    // }
    // CHECK(idx == 3);
}

TEST_CASE("ViewRange - operator[] provides random access", "[value][phase2][view_range]") {
    SKIP("Awaiting Phase 2 implementation - ViewRange");

    // FUTURE: Create range and test range[0], range[1], etc.
}

// ============================================================================
// ViewPairRange Tests
// ============================================================================

TEST_CASE("ViewPairRange - default construction creates empty range", "[value][phase2][view_pair_range]") {
    SKIP("Awaiting Phase 2 implementation - ViewPairRange");

    // FUTURE: ViewPairRange range;
    // CHECK(range.empty());
    // CHECK(range.size() == 0);
}

TEST_CASE("ViewPairRange - iteration yields key-value pairs", "[value][phase2][view_pair_range]") {
    SKIP("Awaiting Phase 2 implementation - ViewPairRange");

    auto& registry = TypeRegistry::instance();
    auto* map_type = registry.map(
        registry.register_scalar<std::string>(),
        registry.register_scalar<int64_t>()
    ).build();
    PlainValue map(map_type);

    map.as_map().set(std::string("a"), int64_t{1});
    map.as_map().set(std::string("b"), int64_t{2});

    // FUTURE: ViewPairRange range = map.const_view().as_map().items();
    // CHECK(range.size() == 2);

    // for (auto [key, value] : range) {
    //     CHECK(key.valid());
    //     CHECK(value.valid());
    // }
}

// ============================================================================
// Path String Conversion Tests
// ============================================================================

TEST_CASE("path_to_string - empty path returns empty string", "[value][phase2][path_string]") {
    ValuePath empty_path;
    CHECK(path_to_string(empty_path) == "");
}

TEST_CASE("path_to_string - field elements use dot notation", "[value][phase2][path_string]") {
    ValuePath path;
    path.push_back(PathElement::field("user"));
    path.push_back(PathElement::field("name"));
    CHECK(path_to_string(path) == "user.name");
}

TEST_CASE("path_to_string - index elements use bracket notation", "[value][phase2][path_string]") {
    ValuePath path;
    path.push_back(PathElement::field("items"));
    path.push_back(PathElement::index(0));
    CHECK(path_to_string(path) == "items[0]");
}

TEST_CASE("path_to_string - mixed path elements", "[value][phase2][path_string]") {
    ValuePath path;
    path.push_back(PathElement::field("users"));
    path.push_back(PathElement::index(0));
    path.push_back(PathElement::field("name"));
    CHECK(path_to_string(path) == "users[0].name");
}
