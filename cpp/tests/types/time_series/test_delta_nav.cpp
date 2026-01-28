/**
 * @file test_delta_nav.cpp
 * @brief Unit tests for BundleDeltaNav and ListDeltaNav.
 *
 * Tests navigation delta structures for TSB and TSL.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/delta_nav.h>
#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/util/date_time.h>

using namespace hgraph;

// ============================================================================
// Helper
// ============================================================================

namespace {

bool contains(const SlotSet& s, size_t val) {
    return s.contains(val);
}

}  // namespace

// ============================================================================
// BundleDeltaNav Tests
// ============================================================================

TEST_CASE("BundleDeltaNav - default construction", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    CHECK(nav.last_cleared_time == MIN_ST);
    CHECK(nav.children.empty());
}

TEST_CASE("BundleDeltaNav - children can be resized", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    nav.children.resize(5);

    CHECK(nav.children.size() == 5);
    for (const auto& child : nav.children) {
        CHECK(std::holds_alternative<std::monostate>(child));
    }
}

TEST_CASE("BundleDeltaNav - children can hold SetDelta", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    nav.children.resize(3);

    SetDelta child;
    child.on_insert(0);
    nav.children[0] = &child;

    CHECK(std::holds_alternative<SetDelta*>(nav.children[0]));
    auto* retrieved = std::get<SetDelta*>(nav.children[0]);
    REQUIRE(retrieved != nullptr);
    CHECK(contains(retrieved->added(), 0));
}

TEST_CASE("BundleDeltaNav - children can hold MapDelta", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    nav.children.resize(3);

    MapDelta child;
    child.on_insert(42);
    nav.children[1] = &child;

    CHECK(std::holds_alternative<MapDelta*>(nav.children[1]));
    auto* retrieved = std::get<MapDelta*>(nav.children[1]);
    REQUIRE(retrieved != nullptr);
    CHECK(contains(retrieved->added(), 42));
}

TEST_CASE("BundleDeltaNav - children can hold nested BundleDeltaNav", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    nav.children.resize(3);

    BundleDeltaNav nested;
    nested.children.resize(2);
    nav.children[2] = &nested;

    CHECK(std::holds_alternative<BundleDeltaNav*>(nav.children[2]));
    auto* retrieved = std::get<BundleDeltaNav*>(nav.children[2]);
    REQUIRE(retrieved != nullptr);
    CHECK(retrieved->children.size() == 2);
}

TEST_CASE("BundleDeltaNav - clear resets children to monostate", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    nav.children.resize(3);

    SetDelta child1;
    MapDelta child2;
    nav.children[0] = &child1;
    nav.children[1] = &child2;

    nav.clear();

    // Children should be reset to monostate
    for (const auto& child : nav.children) {
        CHECK(std::holds_alternative<std::monostate>(child));
    }
}

TEST_CASE("BundleDeltaNav - clear preserves children capacity", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    nav.children.resize(5);

    nav.clear();

    CHECK(nav.children.size() == 5);
}

TEST_CASE("BundleDeltaNav - clear does not reset last_cleared_time", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    nav.last_cleared_time = MIN_ST + std::chrono::microseconds(1000);

    nav.clear();

    // last_cleared_time is managed by the caller, not clear()
    CHECK(nav.last_cleared_time == MIN_ST + std::chrono::microseconds(1000));
}

TEST_CASE("BundleDeltaNav - last_cleared_time can be set", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    const auto t = MIN_ST + std::chrono::microseconds(5000);
    nav.last_cleared_time = t;

    CHECK(nav.last_cleared_time == t);
}

// ============================================================================
// ListDeltaNav Tests
// ============================================================================

TEST_CASE("ListDeltaNav - default construction", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    CHECK(nav.last_cleared_time == MIN_ST);
    CHECK(nav.children.empty());
}

TEST_CASE("ListDeltaNav - children can be resized", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    nav.children.resize(10);

    CHECK(nav.children.size() == 10);
    for (const auto& child : nav.children) {
        CHECK(std::holds_alternative<std::monostate>(child));
    }
}

TEST_CASE("ListDeltaNav - children can hold SetDelta", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    nav.children.resize(5);

    SetDelta child;
    child.on_insert(7);
    nav.children[3] = &child;

    CHECK(std::holds_alternative<SetDelta*>(nav.children[3]));
    auto* retrieved = std::get<SetDelta*>(nav.children[3]);
    REQUIRE(retrieved != nullptr);
    CHECK(contains(retrieved->added(), 7));
}

TEST_CASE("ListDeltaNav - children can hold MapDelta", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    nav.children.resize(5);

    MapDelta child;
    child.on_update(99);
    nav.children[0] = &child;

    CHECK(std::holds_alternative<MapDelta*>(nav.children[0]));
    auto* retrieved = std::get<MapDelta*>(nav.children[0]);
    REQUIRE(retrieved != nullptr);
    CHECK(contains(retrieved->updated(), 99));
}

TEST_CASE("ListDeltaNav - children can hold nested ListDeltaNav", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    nav.children.resize(5);

    ListDeltaNav nested;
    nested.children.resize(3);
    nav.children[4] = &nested;

    CHECK(std::holds_alternative<ListDeltaNav*>(nav.children[4]));
    auto* retrieved = std::get<ListDeltaNav*>(nav.children[4]);
    REQUIRE(retrieved != nullptr);
    CHECK(retrieved->children.size() == 3);
}

TEST_CASE("ListDeltaNav - clear resets children to monostate", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    nav.children.resize(3);

    SetDelta child1;
    MapDelta child2;
    BundleDeltaNav child3;
    nav.children[0] = &child1;
    nav.children[1] = &child2;
    nav.children[2] = &child3;

    nav.clear();

    for (const auto& child : nav.children) {
        CHECK(std::holds_alternative<std::monostate>(child));
    }
}

TEST_CASE("ListDeltaNav - clear preserves children capacity", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    nav.children.resize(7);

    nav.clear();

    CHECK(nav.children.size() == 7);
}

TEST_CASE("ListDeltaNav - last_cleared_time can be set", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    const auto t = MIN_ST + std::chrono::microseconds(12345);
    nav.last_cleared_time = t;

    CHECK(nav.last_cleared_time == t);
}

// ============================================================================
// DeltaVariant Tests
// ============================================================================

TEST_CASE("DeltaVariant - monostate by default", "[time_series][phase2][delta_nav]") {
    DeltaVariant v;
    CHECK(std::holds_alternative<std::monostate>(v));
}

TEST_CASE("DeltaVariant - can hold SetDelta", "[time_series][phase2][delta_nav]") {
    SetDelta sd;
    DeltaVariant v = &sd;

    CHECK(std::holds_alternative<SetDelta*>(v));
    CHECK(std::get<SetDelta*>(v) == &sd);
}

TEST_CASE("DeltaVariant - can hold MapDelta", "[time_series][phase2][delta_nav]") {
    MapDelta md;
    DeltaVariant v = &md;

    CHECK(std::holds_alternative<MapDelta*>(v));
    CHECK(std::get<MapDelta*>(v) == &md);
}

TEST_CASE("DeltaVariant - can hold BundleDeltaNav", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    DeltaVariant v = &nav;

    CHECK(std::holds_alternative<BundleDeltaNav*>(v));
    CHECK(std::get<BundleDeltaNav*>(v) == &nav);
}

TEST_CASE("DeltaVariant - can hold ListDeltaNav", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    DeltaVariant v = &nav;

    CHECK(std::holds_alternative<ListDeltaNav*>(v));
    CHECK(std::get<ListDeltaNav*>(v) == &nav);
}

TEST_CASE("DeltaVariant - can be reassigned", "[time_series][phase2][delta_nav]") {
    SetDelta sd;
    MapDelta md;
    DeltaVariant v;

    // Start as monostate
    CHECK(std::holds_alternative<std::monostate>(v));

    // Assign SetDelta
    v = &sd;
    CHECK(std::holds_alternative<SetDelta*>(v));

    // Reassign to MapDelta
    v = &md;
    CHECK(std::holds_alternative<MapDelta*>(v));

    // Reset to monostate
    v = std::monostate{};
    CHECK(std::holds_alternative<std::monostate>(v));
}

TEST_CASE("DeltaVariant - index method", "[time_series][phase2][delta_nav]") {
    DeltaVariant v;

    v = std::monostate{};
    CHECK(v.index() == 0);

    SetDelta sd;
    v = &sd;
    CHECK(v.index() == 1);

    MapDelta md;
    v = &md;
    CHECK(v.index() == 2);

    BundleDeltaNav bundle;
    v = &bundle;
    CHECK(v.index() == 3);

    ListDeltaNav list;
    v = &list;
    CHECK(v.index() == 4);
}

// ============================================================================
// Cross-Type Navigation Tests
// ============================================================================

TEST_CASE("BundleDeltaNav with ListDeltaNav child", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav bundle;
    bundle.children.resize(2);

    ListDeltaNav list;
    list.children.resize(3);

    SetDelta set_delta;
    set_delta.on_insert(100);
    list.children[0] = &set_delta;

    bundle.children[1] = &list;

    // Navigate: bundle -> list -> set_delta
    auto* list_ptr = std::get<ListDeltaNav*>(bundle.children[1]);
    REQUIRE(list_ptr != nullptr);

    auto* set_ptr = std::get<SetDelta*>(list_ptr->children[0]);
    REQUIRE(set_ptr != nullptr);

    CHECK(contains(set_ptr->added(), 100));
}

TEST_CASE("ListDeltaNav with BundleDeltaNav child", "[time_series][phase2][delta_nav]") {
    ListDeltaNav list;
    list.children.resize(2);

    BundleDeltaNav bundle;
    bundle.children.resize(2);

    MapDelta map_delta;
    map_delta.on_update(200);
    bundle.children[0] = &map_delta;

    list.children[1] = &bundle;

    // Navigate: list -> bundle -> map_delta
    auto* bundle_ptr = std::get<BundleDeltaNav*>(list.children[1]);
    REQUIRE(bundle_ptr != nullptr);

    auto* map_ptr = std::get<MapDelta*>(bundle_ptr->children[0]);
    REQUIRE(map_ptr != nullptr);

    CHECK(contains(map_ptr->updated(), 200));
}
