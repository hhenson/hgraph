/**
 * @file test_set_delta.cpp
 * @brief Unit tests for slot-based SetDelta.
 *
 * Tests add/remove tracking using slot indices with cancellation logic.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/set_delta.h>

#include <algorithm>

using namespace hgraph;

// ============================================================================
// Helper
// ============================================================================

namespace {

bool contains(const std::vector<size_t>& v, size_t val) {
    return std::find(v.begin(), v.end(), val) != v.end();
}

}  // namespace

// ============================================================================
// Construction Tests
// ============================================================================

TEST_CASE("SetDelta - default construction", "[time_series][phase2][delta]") {
    SetDelta sd;
    CHECK(sd.empty());
    CHECK(sd.added().empty());
    CHECK(sd.removed().empty());
    CHECK(!sd.was_cleared());
}

// ============================================================================
// SlotObserver Protocol Tests
// ============================================================================

TEST_CASE("SetDelta - on_capacity is no-op", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_capacity(0, 100);
    CHECK(sd.empty());
}

TEST_CASE("SetDelta - on_insert adds to added list", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_insert(5);

    CHECK(contains(sd.added(), 5));
    CHECK(!sd.empty());
}

TEST_CASE("SetDelta - on_erase adds to removed list", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_erase(5);

    CHECK(contains(sd.removed(), 5));
    CHECK(!sd.empty());
}

TEST_CASE("SetDelta - on_update is no-op for sets", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_update(5);

    // Sets don't track updates, only add/remove
    CHECK(sd.empty());
}

TEST_CASE("SetDelta - on_clear sets was_cleared flag", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_insert(1);
    sd.on_insert(2);
    sd.on_clear();

    CHECK(sd.was_cleared());
    // on_clear does not clear the operation lists
    CHECK(!sd.added().empty());
}

// ============================================================================
// Cancellation Tests
// ============================================================================

TEST_CASE("SetDelta - insert then erase cancels out", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_insert(5);  // Add slot 5
    sd.on_erase(5);   // Remove same slot

    // Should cancel out
    CHECK(!contains(sd.added(), 5));
    CHECK(!contains(sd.removed(), 5));
    CHECK(sd.empty());
}

TEST_CASE("SetDelta - erase then insert creates both entries", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_erase(5);   // Remove pre-existing element
    sd.on_insert(5);  // Add new element to same slot

    // Both operations recorded (slot reuse scenario)
    CHECK(contains(sd.removed(), 5));
    CHECK(contains(sd.added(), 5));
}

TEST_CASE("SetDelta - multiple insert then erase cycles", "[time_series][phase2][delta]") {
    SetDelta sd;

    // First cycle: insert then erase (cancels)
    sd.on_insert(1);
    sd.on_erase(1);

    // Second cycle on same slot: insert then erase (cancels)
    sd.on_insert(1);
    sd.on_erase(1);

    CHECK(!contains(sd.added(), 1));
    CHECK(!contains(sd.removed(), 1));
}

TEST_CASE("SetDelta - mixed cancellation scenarios", "[time_series][phase2][delta]") {
    SetDelta sd;

    // Slot 1: insert (stays)
    sd.on_insert(1);

    // Slot 2: insert then erase (cancels)
    sd.on_insert(2);
    sd.on_erase(2);

    // Slot 3: erase then insert (both stay)
    sd.on_erase(3);
    sd.on_insert(3);

    // Slot 4: erase (stays)
    sd.on_erase(4);

    CHECK(contains(sd.added(), 1));
    CHECK(!contains(sd.added(), 2));
    CHECK(!contains(sd.removed(), 2));
    CHECK(contains(sd.added(), 3));
    CHECK(contains(sd.removed(), 3));
    CHECK(contains(sd.removed(), 4));
}

// ============================================================================
// Multiple Operations Tests
// ============================================================================

TEST_CASE("SetDelta - multiple inserts", "[time_series][phase2][delta]") {
    SetDelta sd;

    for (size_t i = 0; i < 10; ++i) {
        sd.on_insert(i);
    }

    CHECK(sd.added().size() == 10);
    for (size_t i = 0; i < 10; ++i) {
        CHECK(contains(sd.added(), i));
    }
}

TEST_CASE("SetDelta - multiple erases", "[time_series][phase2][delta]") {
    SetDelta sd;

    for (size_t i = 0; i < 10; ++i) {
        sd.on_erase(i);
    }

    CHECK(sd.removed().size() == 10);
    for (size_t i = 0; i < 10; ++i) {
        CHECK(contains(sd.removed(), i));
    }
}

TEST_CASE("SetDelta - mixed operations", "[time_series][phase2][delta]") {
    SetDelta sd;

    // Insert 0-4
    for (size_t i = 0; i < 5; ++i) {
        sd.on_insert(i);
    }

    // Erase 2, 3 (cancels with prior inserts)
    sd.on_erase(2);
    sd.on_erase(3);

    // Insert 5, 6
    sd.on_insert(5);
    sd.on_insert(6);

    // Added: 0, 1, 4, 5, 6 (2, 3 cancelled)
    CHECK(sd.added().size() == 5);
    CHECK(contains(sd.added(), 0));
    CHECK(contains(sd.added(), 1));
    CHECK(!contains(sd.added(), 2));  // Cancelled
    CHECK(!contains(sd.added(), 3));  // Cancelled
    CHECK(contains(sd.added(), 4));
    CHECK(contains(sd.added(), 5));
    CHECK(contains(sd.added(), 6));

    // No removals (all were cancellations with inserts)
    CHECK(sd.removed().empty());
}

// ============================================================================
// Clear Tests
// ============================================================================

TEST_CASE("SetDelta - clear resets all state", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_insert(1);
    sd.on_erase(2);
    sd.on_clear();

    CHECK(sd.was_cleared());
    CHECK(!sd.added().empty());
    CHECK(!sd.removed().empty());

    sd.clear();

    CHECK(sd.empty());
    CHECK(sd.added().empty());
    CHECK(sd.removed().empty());
    CHECK(!sd.was_cleared());
}

TEST_CASE("SetDelta - empty after clear", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_insert(1);
    sd.on_insert(2);
    sd.on_insert(3);

    CHECK(!sd.empty());

    sd.clear();

    CHECK(sd.empty());
}

TEST_CASE("SetDelta - clear then reuse", "[time_series][phase2][delta]") {
    SetDelta sd;

    // First tick
    sd.on_insert(1);
    sd.on_insert(2);
    CHECK(sd.added().size() == 2);

    // New tick
    sd.clear();
    CHECK(sd.empty());

    // Second tick operations
    sd.on_erase(1);
    sd.on_insert(3);
    CHECK(sd.added().size() == 1);
    CHECK(contains(sd.added(), 3));
    CHECK(sd.removed().size() == 1);
    CHECK(contains(sd.removed(), 1));
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_CASE("SetDelta - same slot inserted multiple times", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_insert(5);
    sd.on_insert(5);
    sd.on_insert(5);

    // Implementation allows duplicates in added list
    // This reflects multiple insert calls for the same slot
    CHECK(sd.added().size() == 3);
}

TEST_CASE("SetDelta - cleared flag independent of operations", "[time_series][phase2][delta]") {
    SetDelta sd;

    // Cleared with no operations
    sd.on_clear();
    CHECK(sd.was_cleared());
    CHECK(sd.added().empty());
    CHECK(sd.removed().empty());
    CHECK(!sd.empty());  // Not empty because was_cleared is true
}

TEST_CASE("SetDelta - operations after on_clear", "[time_series][phase2][delta]") {
    SetDelta sd;

    sd.on_clear();
    sd.on_insert(1);
    sd.on_erase(2);

    CHECK(sd.was_cleared());
    CHECK(contains(sd.added(), 1));
    CHECK(contains(sd.removed(), 2));
}
