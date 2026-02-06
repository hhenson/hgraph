/**
 * @file test_observer_array.cpp
 * @brief Unit tests for ObserverArray.
 *
 * Tests parallel observer lists synchronized with KeySet.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/observer_array.h>
#include <hgraph/util/date_time.h>

using namespace hgraph;

// ============================================================================
// Mock Observer (same as in test_observer_list.cpp)
// ============================================================================

namespace {

class MockObserver : public Notifiable {
public:
    int notify_count = 0;
    engine_time_t last_time = MIN_DT;

    void notify(engine_time_t t) override {
        ++notify_count;
        last_time = t;
    }
};

}  // namespace

// ============================================================================
// Construction Tests
// ============================================================================

TEST_CASE("ObserverArray - default construction", "[time_series][phase1][observer]") {
    ObserverArray oa;
    CHECK(oa.size() == 0);
    CHECK(oa.capacity() == 0);
}

// ============================================================================
// SlotObserver Protocol Tests
// ============================================================================

TEST_CASE("ObserverArray - on_capacity resizes storage", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    CHECK(oa.capacity() == 10);
}

TEST_CASE("ObserverArray - on_capacity creates empty ObserverLists", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 5);

    for (size_t i = 0; i < 5; ++i) {
        CHECK(oa.at(i).empty());
    }
}

TEST_CASE("ObserverArray - on_insert creates empty ObserverList", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);

    CHECK(oa.at(0).empty());
    CHECK(oa.size() == 1);
}

TEST_CASE("ObserverArray - on_insert clears existing ObserverList", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);

    MockObserver obs;
    oa.at(0).add_observer(&obs);
    CHECK(oa.at(0).size() == 1);

    // Simulating slot reuse
    oa.on_erase(0);
    oa.on_insert(0);

    CHECK(oa.at(0).empty());
}

TEST_CASE("ObserverArray - on_erase clears observer list", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);

    MockObserver obs;
    oa.at(0).add_observer(&obs);

    oa.on_erase(0);

    // ObserverList should be cleared
    CHECK(oa.at(0).empty());
    CHECK(oa.size() == 0);
}

TEST_CASE("ObserverArray - on_update is no-op", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);

    MockObserver obs;
    oa.at(0).add_observer(&obs);

    oa.on_update(0);

    // Observer should not have been notified
    CHECK(obs.notify_count == 0);
    // ObserverList should be unchanged
    CHECK(oa.at(0).size() == 1);
}

TEST_CASE("ObserverArray - on_clear clears all observer lists", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);

    MockObserver obs1, obs2, obs3;
    oa.on_insert(0);
    oa.on_insert(1);
    oa.on_insert(2);

    oa.at(0).add_observer(&obs1);
    oa.at(1).add_observer(&obs2);
    oa.at(2).add_observer(&obs3);

    oa.on_clear();

    // All ObserverLists should be cleared
    CHECK(oa.at(0).empty());
    CHECK(oa.at(1).empty());
    CHECK(oa.at(2).empty());
    CHECK(oa.size() == 0);
}

// ============================================================================
// Access Tests
// ============================================================================

TEST_CASE("ObserverArray - at returns modifiable ObserverList", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);

    MockObserver obs;
    oa.at(0).add_observer(&obs);

    const auto t = MIN_DT + std::chrono::microseconds(1000);
    oa.at(0).notify_modified(t);

    CHECK(obs.notify_count == 1);
    CHECK(obs.last_time == t);
}

TEST_CASE("ObserverArray - const at returns const ObserverList", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);

    MockObserver obs;
    oa.at(0).add_observer(&obs);

    const ObserverArray& const_oa = oa;
    CHECK(const_oa.at(0).size() == 1);
}

TEST_CASE("ObserverArray - slots are independent", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);
    oa.on_insert(1);

    MockObserver obs1, obs2;
    oa.at(0).add_observer(&obs1);
    oa.at(1).add_observer(&obs2);

    // Notify only slot 0
    oa.at(0).notify_modified(MIN_DT);

    CHECK(obs1.notify_count == 1);
    CHECK(obs2.notify_count == 0);
}

TEST_CASE("ObserverArray - size returns active slot count", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);

    CHECK(oa.size() == 0);

    oa.on_insert(0);
    CHECK(oa.size() == 1);

    oa.on_insert(1);
    oa.on_insert(2);
    CHECK(oa.size() == 3);

    oa.on_erase(1);
    CHECK(oa.size() == 2);
}

TEST_CASE("ObserverArray - capacity returns total slots", "[time_series][phase1][observer]") {
    ObserverArray oa;
    CHECK(oa.capacity() == 0);

    oa.on_capacity(0, 10);
    CHECK(oa.capacity() == 10);

    oa.on_capacity(10, 20);
    CHECK(oa.capacity() == 20);
}
