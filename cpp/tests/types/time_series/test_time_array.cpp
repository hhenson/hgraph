/**
 * @file test_time_array.cpp
 * @brief Unit tests for TimeArray.
 *
 * Tests parallel timestamp storage synchronized with KeySet.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/time_array.h>
#include <hgraph/util/date_time.h>

using namespace hgraph;

// ============================================================================
// Construction Tests
// ============================================================================

TEST_CASE("TimeArray - default construction", "[time_series][phase1][time]") {
    TimeArray ta;
    CHECK(ta.size() == 0);
    CHECK(ta.capacity() == 0);
}

// ============================================================================
// SlotObserver Protocol Tests
// ============================================================================

TEST_CASE("TimeArray - on_capacity resizes storage", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    CHECK(ta.capacity() == 10);
    CHECK(ta.data() != nullptr);
}

TEST_CASE("TimeArray - on_capacity initializes new slots to MIN_DT", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 5);

    for (size_t i = 0; i < 5; ++i) {
        CHECK(ta.at(i) == MIN_DT);
    }
}

TEST_CASE("TimeArray - on_insert initializes to MIN_DT", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    CHECK(ta.at(0) == MIN_DT);
    CHECK(!ta.valid(0));
    CHECK(ta.size() == 1);
}

TEST_CASE("TimeArray - on_insert multiple slots", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);

    for (size_t i = 0; i < 5; ++i) {
        ta.on_insert(i);
    }

    CHECK(ta.size() == 5);
    for (size_t i = 0; i < 5; ++i) {
        CHECK(ta.at(i) == MIN_DT);
    }
}

TEST_CASE("TimeArray - on_erase preserves timestamp", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    const auto t = MIN_DT + std::chrono::microseconds(1000);
    ta.set(0, t);

    ta.on_erase(0);

    // Timestamp preserved for delta queries
    CHECK(ta.at(0) == t);
    CHECK(ta.size() == 0);
}

TEST_CASE("TimeArray - on_update is no-op", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    const auto t = MIN_DT + std::chrono::microseconds(1000);
    ta.set(0, t);

    ta.on_update(0);

    // Timestamp unchanged
    CHECK(ta.at(0) == t);
}

TEST_CASE("TimeArray - on_clear resets all slots", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);

    for (size_t i = 0; i < 5; ++i) {
        ta.on_insert(i);
        ta.set(i, MIN_DT + std::chrono::microseconds(1000 + static_cast<int64_t>(i)));
    }

    ta.on_clear();

    CHECK(ta.size() == 0);
    for (size_t i = 0; i < 5; ++i) {
        CHECK(ta.at(i) == MIN_DT);
    }
}

// ============================================================================
// Time Access Tests
// ============================================================================

TEST_CASE("TimeArray - set and at", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    const auto t = MIN_DT + std::chrono::microseconds(1000);
    ta.set(0, t);

    CHECK(ta.at(0) == t);
}

TEST_CASE("TimeArray - valid returns false for MIN_DT", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    CHECK(!ta.valid(0));

    ta.set(0, MIN_DT + std::chrono::microseconds(1000));
    CHECK(ta.valid(0));
}

TEST_CASE("TimeArray - modified uses >= comparison", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    const auto t1000 = MIN_DT + std::chrono::microseconds(1000);
    const auto t999 = MIN_DT + std::chrono::microseconds(999);
    const auto t1001 = MIN_DT + std::chrono::microseconds(1001);

    ta.set(0, t1000);

    // Modified at same time (>=)
    CHECK(ta.modified(0, t1000));

    // Modified at earlier query time (1000 >= 999)
    CHECK(ta.modified(0, t999));

    // Not modified at later query time (1000 >= 1001 is false)
    CHECK(!ta.modified(0, t1001));
}

TEST_CASE("TimeArray - data returns pointer to storage", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    const auto t = MIN_DT + std::chrono::microseconds(42);
    ta.set(0, t);

    engine_time_t* ptr = ta.data();
    REQUIRE(ptr != nullptr);
    CHECK(ptr[0] == t);
}

TEST_CASE("TimeArray - const data access", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    const auto t = MIN_DT + std::chrono::microseconds(42);
    ta.set(0, t);

    const TimeArray& const_ta = ta;
    const engine_time_t* ptr = const_ta.data();
    REQUIRE(ptr != nullptr);
    CHECK(ptr[0] == t);
}

TEST_CASE("TimeArray - size returns active slot count", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);

    CHECK(ta.size() == 0);

    ta.on_insert(0);
    CHECK(ta.size() == 1);

    ta.on_insert(1);
    CHECK(ta.size() == 2);

    ta.on_erase(0);
    CHECK(ta.size() == 1);
}

TEST_CASE("TimeArray - capacity returns total slots", "[time_series][phase1][time]") {
    TimeArray ta;
    CHECK(ta.capacity() == 0);

    ta.on_capacity(0, 10);
    CHECK(ta.capacity() == 10);

    ta.on_capacity(10, 20);
    CHECK(ta.capacity() == 20);
}
