/**
 * @file test_map_delta.cpp
 * @brief Unit tests for slot-based MapDelta.
 *
 * Tests add/remove/update tracking with child deltas.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/map_delta.h>
#include <hgraph/types/time_series/set_delta.h>

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
// Construction Tests
// ============================================================================

TEST_CASE("MapDelta - default construction", "[time_series][phase2][delta]") {
    MapDelta md;
    CHECK(md.empty());
    CHECK(md.added().empty());
    CHECK(md.removed().empty());
    CHECK(md.updated().empty());
    CHECK(md.children().empty());
    CHECK(!md.was_cleared());
}

// ============================================================================
// SlotObserver Protocol Tests
// ============================================================================

TEST_CASE("MapDelta - on_capacity resizes children vector", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);

    CHECK(md.children().size() == 10);
    // All should be monostate
    for (const auto& child : md.children()) {
        CHECK(std::holds_alternative<std::monostate>(child));
    }
}

TEST_CASE("MapDelta - on_capacity grows children", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 5);
    CHECK(md.children().size() == 5);

    md.on_capacity(5, 10);
    CHECK(md.children().size() == 10);
}

TEST_CASE("MapDelta - on_insert adds to added list", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_insert(5);

    CHECK(contains(md.added(), 5));
    CHECK(!md.empty());
}

TEST_CASE("MapDelta - on_erase adds to removed list", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_erase(5);

    CHECK(contains(md.removed(), 5));
    CHECK(!md.empty());
}

TEST_CASE("MapDelta - on_update adds to updated list", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_update(5);

    CHECK(contains(md.updated(), 5));
    CHECK(!md.empty());
}

TEST_CASE("MapDelta - on_clear sets was_cleared flag", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_insert(1);
    md.on_clear();

    CHECK(md.was_cleared());
}

// ============================================================================
// Cancellation Tests
// ============================================================================

TEST_CASE("MapDelta - insert then erase cancels out", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_insert(5);
    md.on_erase(5);

    CHECK(!contains(md.added(), 5));
    CHECK(!contains(md.removed(), 5));
    CHECK(md.empty());
}

TEST_CASE("MapDelta - erase then insert creates both entries", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_erase(5);
    md.on_insert(5);

    CHECK(contains(md.removed(), 5));
    CHECK(contains(md.added(), 5));
}

TEST_CASE("MapDelta - insert update erase cancels all", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_insert(5);
    md.on_update(5);  // Should be ignored for newly added slot
    md.on_erase(5);   // Cancels with insert

    CHECK(!contains(md.added(), 5));
    CHECK(!contains(md.updated(), 5));
    CHECK(!contains(md.removed(), 5));
    CHECK(md.empty());
}

TEST_CASE("MapDelta - update then erase for existing slot", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_update(5);  // Update existing slot
    md.on_erase(5);   // Then erase it

    CHECK(!contains(md.updated(), 5));  // Update removed
    CHECK(contains(md.removed(), 5));   // Removal recorded
}

// ============================================================================
// Update Tracking Tests
// ============================================================================

TEST_CASE("MapDelta - on_update ignores newly added slots", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_insert(5);
    md.on_update(5);

    CHECK(contains(md.added(), 5));
    CHECK(!contains(md.updated(), 5));  // Not in updated (was just added)
}

TEST_CASE("MapDelta - on_update deduplicates", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_update(5);
    md.on_update(5);
    md.on_update(5);

    CHECK(md.updated().size() == 1);
    CHECK(contains(md.updated(), 5));
}

TEST_CASE("MapDelta - multiple updates to different slots", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_update(1);
    md.on_update(2);
    md.on_update(3);

    CHECK(md.updated().size() == 3);
    CHECK(contains(md.updated(), 1));
    CHECK(contains(md.updated(), 2));
    CHECK(contains(md.updated(), 3));
}

// ============================================================================
// Children Tests
// ============================================================================

TEST_CASE("MapDelta - children default to monostate", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);

    CHECK(std::holds_alternative<std::monostate>(md.children()[0]));
    CHECK(std::holds_alternative<std::monostate>(md.children()[5]));
    CHECK(std::holds_alternative<std::monostate>(md.children()[9]));
}

TEST_CASE("MapDelta - children can hold SetDelta", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);

    SetDelta child;
    child.on_insert(42);

    md.children()[0] = &child;

    CHECK(std::holds_alternative<SetDelta*>(md.children()[0]));
    auto* retrieved = std::get<SetDelta*>(md.children()[0]);
    REQUIRE(retrieved != nullptr);
    CHECK(contains(retrieved->added(), 42));
}

TEST_CASE("MapDelta - children can hold MapDelta", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);

    MapDelta child;
    child.on_insert(99);

    md.children()[5] = &child;

    CHECK(std::holds_alternative<MapDelta*>(md.children()[5]));
    auto* retrieved = std::get<MapDelta*>(md.children()[5]);
    REQUIRE(retrieved != nullptr);
    CHECK(contains(retrieved->added(), 99));
}

TEST_CASE("MapDelta - multiple children of different types", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);

    SetDelta set_child;
    MapDelta map_child;

    md.children()[0] = &set_child;
    md.children()[1] = &map_child;
    // Slot 2 stays as monostate

    CHECK(std::holds_alternative<SetDelta*>(md.children()[0]));
    CHECK(std::holds_alternative<MapDelta*>(md.children()[1]));
    CHECK(std::holds_alternative<std::monostate>(md.children()[2]));
}

// ============================================================================
// Clear Tests
// ============================================================================

TEST_CASE("MapDelta - clear resets all state", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);
    md.on_insert(1);
    md.on_erase(2);
    md.on_update(3);
    md.on_clear();

    SetDelta child;
    md.children()[0] = &child;

    md.clear();

    CHECK(md.empty());
    CHECK(md.added().empty());
    CHECK(md.removed().empty());
    CHECK(md.updated().empty());
    CHECK(!md.was_cleared());
    // Children reset to monostate
    CHECK(std::holds_alternative<std::monostate>(md.children()[0]));
}

TEST_CASE("MapDelta - clear preserves children capacity", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);

    md.clear();

    CHECK(md.children().size() == 10);
}

TEST_CASE("MapDelta - clear then reuse", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);

    // First tick
    md.on_insert(1);
    md.on_update(2);
    CHECK(md.added().size() == 1);
    CHECK(md.updated().size() == 1);

    // New tick
    md.clear();
    CHECK(md.empty());

    // Second tick operations
    md.on_erase(1);
    md.on_insert(3);
    md.on_update(4);
    CHECK(md.added().size() == 1);
    CHECK(contains(md.added(), 3));
    CHECK(md.removed().size() == 1);
    CHECK(contains(md.removed(), 1));
    CHECK(md.updated().size() == 1);
    CHECK(contains(md.updated(), 4));
}

// ============================================================================
// Complex Scenarios
// ============================================================================

TEST_CASE("MapDelta - mixed operations complex", "[time_series][phase2][delta]") {
    MapDelta md;

    // Insert 0-4
    for (size_t i = 0; i < 5; ++i) {
        md.on_insert(i);
    }

    // Update 5, 6 (existing slots)
    md.on_update(5);
    md.on_update(6);

    // Erase 2, 3 (cancels with prior inserts)
    md.on_erase(2);
    md.on_erase(3);

    // Erase 5 (removes from updated, adds to removed)
    md.on_erase(5);

    // Insert 7
    md.on_insert(7);

    // Added: 0, 1, 4, 7 (2, 3 cancelled)
    CHECK(md.added().size() == 4);
    CHECK(contains(md.added(), 0));
    CHECK(contains(md.added(), 1));
    CHECK(!contains(md.added(), 2));
    CHECK(!contains(md.added(), 3));
    CHECK(contains(md.added(), 4));
    CHECK(contains(md.added(), 7));

    // Removed: 5
    CHECK(md.removed().size() == 1);
    CHECK(contains(md.removed(), 5));

    // Updated: 6 (5 was removed so it's not in updated)
    CHECK(md.updated().size() == 1);
    CHECK(contains(md.updated(), 6));
}

TEST_CASE("MapDelta - empty check considers all fields", "[time_series][phase2][delta]") {
    MapDelta md;

    // Empty by default
    CHECK(md.empty());

    // Not empty with added
    md.on_insert(1);
    CHECK(!md.empty());
    md.clear();

    // Not empty with removed
    md.on_erase(1);
    CHECK(!md.empty());
    md.clear();

    // Not empty with updated
    md.on_update(1);
    CHECK(!md.empty());
    md.clear();

    // Not empty when cleared
    md.on_clear();
    CHECK(!md.empty());
}
