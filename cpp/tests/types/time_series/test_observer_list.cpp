/**
 * @file test_observer_list.cpp
 * @brief Unit tests for ObserverList.
 *
 * Tests observer list management and notification.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/util/date_time.h>

using namespace hgraph;

// ============================================================================
// Mock TSObserver
// ============================================================================

namespace {

class MockTSObserver : public TSObserver {
public:
    int modified_count = 0;
    int removed_count = 0;
    engine_time_t last_time = MIN_ST;

    void notify_modified(engine_time_t t) override {
        ++modified_count;
        last_time = t;
    }

    void notify_removed() override {
        ++removed_count;
    }
};

}  // namespace

// ============================================================================
// Construction Tests
// ============================================================================

TEST_CASE("ObserverList - default construction creates empty list", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    CHECK(obs_list.empty());
    CHECK(obs_list.size() == 0);
}

TEST_CASE("ObserverList - copy construction", "[time_series][phase1][observer]") {
    ObserverList obs_list1;
    MockTSObserver obs;
    obs_list1.add_observer(&obs);

    ObserverList obs_list2(obs_list1);
    CHECK(obs_list2.size() == 1);
}

TEST_CASE("ObserverList - move construction", "[time_series][phase1][observer]") {
    ObserverList obs_list1;
    MockTSObserver obs;
    obs_list1.add_observer(&obs);

    ObserverList obs_list2(std::move(obs_list1));
    CHECK(obs_list2.size() == 1);
}

// ============================================================================
// Observer Management Tests
// ============================================================================

TEST_CASE("ObserverList - add_observer increases size", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockTSObserver obs;

    obs_list.add_observer(&obs);

    CHECK(!obs_list.empty());
    CHECK(obs_list.size() == 1);
}

TEST_CASE("ObserverList - remove_observer decreases size", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockTSObserver obs;

    obs_list.add_observer(&obs);
    obs_list.remove_observer(&obs);

    CHECK(obs_list.empty());
    CHECK(obs_list.size() == 0);
}

TEST_CASE("ObserverList - remove non-existent observer is safe", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockTSObserver obs1, obs2;

    obs_list.add_observer(&obs1);
    obs_list.remove_observer(&obs2);  // Not in list

    CHECK(obs_list.size() == 1);
}

TEST_CASE("ObserverList - clear removes all observers", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockTSObserver obs1, obs2, obs3;

    obs_list.add_observer(&obs1);
    obs_list.add_observer(&obs2);
    obs_list.add_observer(&obs3);

    obs_list.clear();

    CHECK(obs_list.empty());
}

TEST_CASE("ObserverList - add null observer is safe", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    obs_list.add_observer(nullptr);
    CHECK(obs_list.empty());
}

// ============================================================================
// Notification Tests
// ============================================================================

TEST_CASE("ObserverList - notify_modified calls all observers", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockTSObserver obs1, obs2;

    obs_list.add_observer(&obs1);
    obs_list.add_observer(&obs2);

    const engine_time_t t = MIN_ST + std::chrono::microseconds(1000);
    obs_list.notify_modified(t);

    CHECK(obs1.modified_count == 1);
    CHECK(obs1.last_time == t);
    CHECK(obs2.modified_count == 1);
    CHECK(obs2.last_time == t);
}

TEST_CASE("ObserverList - notify_removed calls all observers", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockTSObserver obs1, obs2;

    obs_list.add_observer(&obs1);
    obs_list.add_observer(&obs2);

    obs_list.notify_removed();

    CHECK(obs1.removed_count == 1);
    CHECK(obs2.removed_count == 1);
}

TEST_CASE("ObserverList - notify on empty list is safe", "[time_series][phase1][observer]") {
    ObserverList obs_list;

    // Should not crash
    obs_list.notify_modified(MIN_ST + std::chrono::microseconds(1000));
    obs_list.notify_removed();

    CHECK(obs_list.empty());
}

TEST_CASE("ObserverList - multiple notifications accumulate", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockTSObserver obs;

    obs_list.add_observer(&obs);

    const auto t1 = MIN_ST + std::chrono::microseconds(100);
    const auto t2 = MIN_ST + std::chrono::microseconds(200);
    const auto t3 = MIN_ST + std::chrono::microseconds(300);

    obs_list.notify_modified(t1);
    obs_list.notify_modified(t2);
    obs_list.notify_modified(t3);

    CHECK(obs.modified_count == 3);
    CHECK(obs.last_time == t3);
}

TEST_CASE("ObserverList - same observer added multiple times gets multiple notifications", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockTSObserver obs;

    obs_list.add_observer(&obs);
    obs_list.add_observer(&obs);  // Added twice

    obs_list.notify_modified(MIN_ST);

    CHECK(obs.modified_count == 2);
}
