# C++ Test Specifications: TSValue Infrastructure

**Version:** 1.0
**Date:** 2026-01-23
**Reference:** `ts_value_v2601/implementation/ts_value/01_IMPLEMENTATION_PLAN_v2.md`

---

## Section 1: Overview

### 1.1 Testing Framework

All C++ tests use Catch2. Test files are organized under `cpp/tests/types/time_series/` to mirror the source structure.

### 1.2 Test File Organization

| Test File | Phase | Components Tested |
|-----------|-------|-------------------|
| `test_observer_list.cpp` | 1 | ObserverList |
| `test_time_array.cpp` | 1 | TimeArray |
| `test_observer_array.cpp` | 1 | ObserverArray |
| `test_set_delta.cpp` | 2 | SetDelta |
| `test_map_delta.cpp` | 2 | MapDelta |
| `test_delta_nav.cpp` | 2 | BundleDeltaNav, ListDeltaNav |
| `test_ts_meta_schema.cpp` | 3 | Schema generation functions |
| `test_ts_value.cpp` | 4 | TSValue class |
| `test_ts_view.cpp` | 5 | TSView and kind-specific views |

### 1.3 Common Test Patterns

```cpp
// Standard includes for all test files
#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/util/date_time.h>

using namespace hgraph;
using namespace hgraph::value;

// Test tags:
// [time_series] - All TSValue tests
// [phase1] - Foundation types
// [phase2] - Delta structures
// [phase3] - Schema generation
// [phase4] - TSValue
// [phase5] - TSView
// [observer] - Observer tests
// [delta] - Delta tracking tests
// [schema] - Schema generation tests
```

---

## Section 2: Phase 1 - Foundation Types

### 2.1 test_observer_list.cpp

**File:** `cpp/tests/types/time_series/test_observer_list.cpp`

```cpp
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
// Mock Notifiable
// ============================================================================

namespace {

class MockNotifiable : public Notifiable {
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
    MockNotifiable obs;
    obs_list1.add_observer(&obs);

    ObserverList obs_list2(obs_list1);
    CHECK(obs_list2.size() == 1);
}

TEST_CASE("ObserverList - move construction", "[time_series][phase1][observer]") {
    ObserverList obs_list1;
    MockNotifiable obs;
    obs_list1.add_observer(&obs);

    ObserverList obs_list2(std::move(obs_list1));
    CHECK(obs_list2.size() == 1);
}

// ============================================================================
// Observer Management Tests
// ============================================================================

TEST_CASE("ObserverList - add_observer increases size", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockNotifiable obs;

    obs_list.add_observer(&obs);

    CHECK(!obs_list.empty());
    CHECK(obs_list.size() == 1);
}

TEST_CASE("ObserverList - remove_observer decreases size", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockNotifiable obs;

    obs_list.add_observer(&obs);
    obs_list.remove_observer(&obs);

    CHECK(obs_list.empty());
    CHECK(obs_list.size() == 0);
}

TEST_CASE("ObserverList - remove non-existent observer is safe", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockNotifiable obs1, obs2;

    obs_list.add_observer(&obs1);
    obs_list.remove_observer(&obs2);  // Not in list

    CHECK(obs_list.size() == 1);
}

TEST_CASE("ObserverList - clear removes all observers", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockNotifiable obs1, obs2, obs3;

    obs_list.add_observer(&obs1);
    obs_list.add_observer(&obs2);
    obs_list.add_observer(&obs3);

    obs_list.clear();

    CHECK(obs_list.empty());
}

// ============================================================================
// Notification Tests
// ============================================================================

TEST_CASE("ObserverList - notify_modified calls all observers", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockNotifiable obs1, obs2;

    obs_list.add_observer(&obs1);
    obs_list.add_observer(&obs2);

    const engine_time_t t = engine_time_t{1000};
    obs_list.notify_modified(t);

    CHECK(obs1.modified_count == 1);
    CHECK(obs1.last_time == t);
    CHECK(obs2.modified_count == 1);
    CHECK(obs2.last_time == t);
}

TEST_CASE("ObserverList - notify_removed calls all observers", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockNotifiable obs1, obs2;

    obs_list.add_observer(&obs1);
    obs_list.add_observer(&obs2);

    obs_list.notify_removed();

    CHECK(obs1.removed_count == 1);
    CHECK(obs2.removed_count == 1);
}

TEST_CASE("ObserverList - notify on empty list is safe", "[time_series][phase1][observer]") {
    ObserverList obs_list;

    // Should not crash
    obs_list.notify_modified(engine_time_t{1000});
    obs_list.notify_removed();

    CHECK(obs_list.empty());
}

TEST_CASE("ObserverList - multiple notifications accumulate", "[time_series][phase1][observer]") {
    ObserverList obs_list;
    MockNotifiable obs;

    obs_list.add_observer(&obs);

    obs_list.notify_modified(engine_time_t{100});
    obs_list.notify_modified(engine_time_t{200});
    obs_list.notify_modified(engine_time_t{300});

    CHECK(obs.modified_count == 3);
    CHECK(obs.last_time == engine_time_t{300});
}
```

### 2.2 test_time_array.cpp

**File:** `cpp/tests/types/time_series/test_time_array.cpp`

```cpp
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
}

// ============================================================================
// SlotObserver Protocol Tests
// ============================================================================

TEST_CASE("TimeArray - on_capacity resizes storage", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    // Capacity should be at least 10
    CHECK(ta.data() != nullptr);
}

TEST_CASE("TimeArray - on_insert initializes to MIN_ST", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    CHECK(ta.at(0) == MIN_ST);
    CHECK(!ta.valid(0));
}

TEST_CASE("TimeArray - on_insert multiple slots", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);

    for (size_t i = 0; i < 5; ++i) {
        ta.on_insert(i);
    }

    for (size_t i = 0; i < 5; ++i) {
        CHECK(ta.at(i) == MIN_ST);
    }
}

TEST_CASE("TimeArray - on_erase handles removal", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);
    ta.set(0, engine_time_t{1000});

    ta.on_erase(0);
    // Behavior after erase is implementation-defined
    // Document expected behavior
}

TEST_CASE("TimeArray - on_clear resets all slots", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);

    for (size_t i = 0; i < 5; ++i) {
        ta.on_insert(i);
        ta.set(i, engine_time_t{1000 + static_cast<int64_t>(i)});
    }

    ta.on_clear();
    // All slots should be invalidated
}

// ============================================================================
// Time Access Tests
// ============================================================================

TEST_CASE("TimeArray - set and at", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    const engine_time_t t{1000};
    ta.set(0, t);

    CHECK(ta.at(0) == t);
}

TEST_CASE("TimeArray - valid returns false for MIN_ST", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);

    CHECK(!ta.valid(0));

    ta.set(0, engine_time_t{1000});
    CHECK(ta.valid(0));
}

TEST_CASE("TimeArray - modified uses >= comparison", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);
    ta.set(0, engine_time_t{1000});

    // Modified at same time (>=)
    CHECK(ta.modified(0, engine_time_t{1000}));

    // Modified at earlier query time (1000 >= 999)
    CHECK(ta.modified(0, engine_time_t{999}));

    // Not modified at later query time (1000 >= 1001 is false)
    CHECK(!ta.modified(0, engine_time_t{1001}));
}

TEST_CASE("TimeArray - data returns pointer to storage", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);
    ta.on_insert(0);
    ta.set(0, engine_time_t{42});

    engine_time_t* ptr = ta.data();
    REQUIRE(ptr != nullptr);
    CHECK(ptr[0] == engine_time_t{42});
}

TEST_CASE("TimeArray - size returns slot count", "[time_series][phase1][time]") {
    TimeArray ta;
    ta.on_capacity(0, 10);

    CHECK(ta.size() == 0);

    ta.on_insert(0);
    CHECK(ta.size() == 1);

    ta.on_insert(1);
    CHECK(ta.size() == 2);
}
```

### 2.3 test_observer_array.cpp

**File:** `cpp/tests/types/time_series/test_observer_array.cpp`

```cpp
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
// Mock Notifiable (same as in test_observer_list.cpp)
// ============================================================================

namespace {

class MockNotifiable : public Notifiable {
public:
    int modified_count = 0;
    void notify_modified(engine_time_t) override { ++modified_count; }
    void notify_removed() override {}
};

}  // namespace

// ============================================================================
// Construction Tests
// ============================================================================

TEST_CASE("ObserverArray - default construction", "[time_series][phase1][observer]") {
    ObserverArray oa;
    CHECK(oa.size() == 0);
}

// ============================================================================
// SlotObserver Protocol Tests
// ============================================================================

TEST_CASE("ObserverArray - on_capacity resizes storage", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    // Should have capacity for 10 slots
}

TEST_CASE("ObserverArray - on_insert creates empty ObserverList", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);

    CHECK(oa.at(0).empty());
}

TEST_CASE("ObserverArray - on_erase handles removal", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);

    MockNotifiable obs;
    oa.at(0).add_observer(&obs);

    oa.on_erase(0);
    // ObserverList at slot 0 should be cleaned up
}

TEST_CASE("ObserverArray - on_clear clears all lists", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);

    MockNotifiable obs1, obs2;
    oa.on_insert(0);
    oa.on_insert(1);
    oa.at(0).add_observer(&obs1);
    oa.at(1).add_observer(&obs2);

    oa.on_clear();
    // All observer lists should be cleared
}

// ============================================================================
// Access Tests
// ============================================================================

TEST_CASE("ObserverArray - at returns modifiable ObserverList", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);

    MockNotifiable obs;
    oa.at(0).add_observer(&obs);
    oa.at(0).notify_modified(engine_time_t{1000});

    CHECK(obs.modified_count == 1);
}

TEST_CASE("ObserverArray - slots are independent", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);
    oa.on_insert(0);
    oa.on_insert(1);

    MockNotifiable obs1, obs2;
    oa.at(0).add_observer(&obs1);
    oa.at(1).add_observer(&obs2);

    // Notify only slot 0
    oa.at(0).notify_modified(engine_time_t{1000});

    CHECK(obs1.modified_count == 1);
    CHECK(obs2.modified_count == 0);
}

TEST_CASE("ObserverArray - size returns slot count", "[time_series][phase1][observer]") {
    ObserverArray oa;
    oa.on_capacity(0, 10);

    CHECK(oa.size() == 0);

    oa.on_insert(0);
    CHECK(oa.size() == 1);

    oa.on_insert(1);
    oa.on_insert(2);
    CHECK(oa.size() == 3);
}
```

---

## Section 3: Phase 2 - Delta Structures

### 3.1 test_set_delta.cpp

**File:** `cpp/tests/types/time_series/test_set_delta.cpp`

```cpp
/**
 * @file test_set_delta.cpp
 * @brief Unit tests for slot-based SetDelta.
 *
 * Tests add/remove tracking using slot indices.
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
}

TEST_CASE("SetDelta - erase then insert creates both entries", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_erase(5);   // Remove pre-existing element
    sd.on_insert(5);  // Add new element to same slot

    // Both operations recorded (slot reuse scenario)
    CHECK(contains(sd.removed(), 5));
    CHECK(contains(sd.added(), 5));
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

TEST_CASE("SetDelta - mixed operations", "[time_series][phase2][delta]") {
    SetDelta sd;

    // Insert 0-4
    for (size_t i = 0; i < 5; ++i) {
        sd.on_insert(i);
    }

    // Erase 2, 3
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
}

// ============================================================================
// Clear Tests
// ============================================================================

TEST_CASE("SetDelta - clear resets all state", "[time_series][phase2][delta]") {
    SetDelta sd;
    sd.on_insert(1);
    sd.on_erase(2);
    sd.on_clear();

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
```

### 3.2 test_map_delta.cpp

**File:** `cpp/tests/types/time_series/test_map_delta.cpp`

```cpp
/**
 * @file test_map_delta.cpp
 * @brief Unit tests for slot-based MapDelta.
 *
 * Tests add/remove/update tracking with child deltas.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/map_delta.h>
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

TEST_CASE("MapDelta - default construction", "[time_series][phase2][delta]") {
    MapDelta md;
    CHECK(md.empty());
    CHECK(md.added().empty());
    CHECK(md.removed().empty());
    CHECK(md.updated().empty());
    CHECK(!md.was_cleared());
}

// ============================================================================
// SlotObserver Protocol Tests
// ============================================================================

TEST_CASE("MapDelta - on_insert adds to added list", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_insert(5);

    CHECK(contains(md.added(), 5));
}

TEST_CASE("MapDelta - on_erase adds to removed list", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_erase(5);

    CHECK(contains(md.removed(), 5));
}

TEST_CASE("MapDelta - on_update adds to updated list", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_update(5);

    CHECK(contains(md.updated(), 5));
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
}

// ============================================================================
// Children Tests
// ============================================================================

TEST_CASE("MapDelta - on_capacity creates children vector", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);

    CHECK(md.children().size() == 10);
}

TEST_CASE("MapDelta - children can be assigned", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);

    SetDelta child;
    child.on_insert(42);

    md.children()[0] = &child;

    auto* retrieved = std::get<SetDelta*>(md.children()[0]);
    REQUIRE(retrieved != nullptr);
    CHECK(contains(retrieved->added(), 42));
}

TEST_CASE("MapDelta - children default to monostate", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_capacity(0, 10);

    CHECK(std::holds_alternative<std::monostate>(md.children()[0]));
}

// ============================================================================
// Clear Tests
// ============================================================================

TEST_CASE("MapDelta - clear resets all state", "[time_series][phase2][delta]") {
    MapDelta md;
    md.on_insert(1);
    md.on_erase(2);
    md.on_update(3);
    md.on_clear();

    md.clear();

    CHECK(md.empty());
    CHECK(md.added().empty());
    CHECK(md.removed().empty());
    CHECK(md.updated().empty());
    CHECK(!md.was_cleared());
}
```

### 3.3 test_delta_nav.cpp

**File:** `cpp/tests/types/time_series/test_delta_nav.cpp`

```cpp
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
// BundleDeltaNav Tests
// ============================================================================

TEST_CASE("BundleDeltaNav - default construction", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    CHECK(nav.last_cleared_time == MIN_ST);
    CHECK(nav.children.empty());
}

TEST_CASE("BundleDeltaNav - children can be populated", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    nav.children.resize(3);

    SetDelta child;
    child.on_insert(0);
    nav.children[0] = &child;

    CHECK(std::holds_alternative<SetDelta*>(nav.children[0]));
}

TEST_CASE("BundleDeltaNav - clear resets children", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    nav.children.resize(3);

    SetDelta child1, child2;
    child1.on_insert(0);
    child2.on_insert(1);
    nav.children[0] = &child1;
    nav.children[1] = &child2;

    nav.clear();

    // Children should be cleared (implementation-dependent)
}

TEST_CASE("BundleDeltaNav - last_cleared_time can be set", "[time_series][phase2][delta_nav]") {
    BundleDeltaNav nav;
    nav.last_cleared_time = engine_time_t{1000};

    CHECK(nav.last_cleared_time == engine_time_t{1000});
}

// ============================================================================
// ListDeltaNav Tests
// ============================================================================

TEST_CASE("ListDeltaNav - default construction", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    CHECK(nav.last_cleared_time == MIN_ST);
    CHECK(nav.children.empty());
}

TEST_CASE("ListDeltaNav - children can be populated", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    nav.children.resize(5);

    for (size_t i = 0; i < 5; ++i) {
        SetDelta* child = new SetDelta();
        child->on_insert(i);
        nav.children[i] = child;
    }

    for (size_t i = 0; i < 5; ++i) {
        auto* child = std::get<SetDelta*>(nav.children[i]);
        REQUIRE(child != nullptr);
    }

    // Cleanup (in real code, ownership would be managed)
    for (size_t i = 0; i < 5; ++i) {
        delete std::get<SetDelta*>(nav.children[i]);
    }
}

TEST_CASE("ListDeltaNav - clear resets all children", "[time_series][phase2][delta_nav]") {
    ListDeltaNav nav;
    nav.children.resize(3);

    nav.clear();
    // Children should be reset
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
```

---

## Section 4: Phase 3 - Schema Generation

### 4.1 test_ts_meta_schema.cpp

**File:** `cpp/tests/types/time_series/test_ts_meta_schema.cpp`

```cpp
/**
 * @file test_ts_meta_schema.cpp
 * @brief Unit tests for TSMeta schema generation.
 *
 * Tests has_delta(), generate_time_schema(), generate_observer_schema(),
 * and generate_delta_value_schema().
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/value/type_registry.h>

using namespace hgraph;
using namespace hgraph::value;

// ============================================================================
// Helpers
// ============================================================================

namespace {

const TSMeta* make_ts_meta(TSKind kind, const TypeMeta* value_type) {
    // Implementation depends on TSMeta factory
    return nullptr;  // TODO: Implement
}

const TSMeta* make_tsb_meta(std::initializer_list<std::pair<std::string, TSKind>> fields) {
    // Implementation depends on TSMeta factory
    return nullptr;  // TODO: Implement
}

}  // namespace

// ============================================================================
// has_delta() Tests
// ============================================================================

TEST_CASE("has_delta - TS has no delta", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
    // auto* meta = make_ts_meta(TSKind::TS, TypeRegistry::instance().register_scalar<int64_t>());
    // CHECK(!has_delta(meta->kind(), meta));
}

TEST_CASE("has_delta - TSS has delta", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
    // auto* meta = make_ts_meta(TSKind::TSS, TypeRegistry::instance().register_scalar<int64_t>());
    // CHECK(has_delta(meta->kind(), meta));
}

TEST_CASE("has_delta - TSD has delta", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
    // auto* meta = make_ts_meta(TSKind::TSD, ...);
    // CHECK(has_delta(meta->kind(), meta));
}

TEST_CASE("has_delta - TSB with only TS fields has no delta", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
    // auto* meta = make_tsb_meta({{"a", TSKind::TS}, {"b", TSKind::TS}});
    // CHECK(!has_delta(meta->kind(), meta));
}

TEST_CASE("has_delta - TSB with TSS field has delta", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
    // auto* meta = make_tsb_meta({{"a", TSKind::TS}, {"b", TSKind::TSS}});
    // CHECK(has_delta(meta->kind(), meta));
}

TEST_CASE("has_delta - TSL with TS element has no delta", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("has_delta - TSL with TSS element has delta", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("has_delta - SIGNAL has no delta", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

// ============================================================================
// generate_time_schema() Tests
// ============================================================================

TEST_CASE("time_schema - TS is engine_time_t", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
    // auto* meta = make_ts_meta(TSKind::TS, ...);
    // auto* time_meta = generate_time_schema(meta->kind(), meta);
    // CHECK(time_meta->type_id() == type_id<engine_time_t>());
}

TEST_CASE("time_schema - TSS is engine_time_t", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("time_schema - TSD is tuple[engine_time_t, var_list]", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
    // auto* meta = make_ts_meta(TSKind::TSD, ...);
    // auto* time_meta = generate_time_schema(meta->kind(), meta);
    // CHECK(time_meta->kind() == TypeKind::Tuple);
    // auto fields = time_meta->tuple_fields();
    // CHECK(fields.size() == 2);
    // CHECK(fields[0]->type_id() == type_id<engine_time_t>());
    // CHECK(fields[1]->kind() == TypeKind::VarList);
}

TEST_CASE("time_schema - TSB is tuple[engine_time_t, fixed_list]", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("time_schema - TSL is tuple[engine_time_t, fixed_list]", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

// ============================================================================
// generate_observer_schema() Tests
// ============================================================================

TEST_CASE("observer_schema - TS is ObserverList", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("observer_schema - TSD is tuple[ObserverList, var_list]", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("observer_schema - TSB is tuple[ObserverList, fixed_list]", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

// ============================================================================
// generate_delta_value_schema() Tests
// ============================================================================

TEST_CASE("delta_schema - TS is void", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("delta_schema - TSS is SetDelta", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("delta_schema - TSD is MapDelta", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("delta_schema - TSB with delta field is BundleDeltaNav", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("delta_schema - TSB without delta field is void", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("delta_schema - TSL with delta element is ListDeltaNav", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("delta_schema - TSL without delta element is void", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}

TEST_CASE("delta_schema - SIGNAL is void", "[time_series][phase3][schema]") {
    SKIP("Awaiting TSMeta implementation");
}
```

---

## Section 5: Phase 4 - TSValue

### 5.1 test_ts_value.cpp

**File:** `cpp/tests/types/time_series/test_ts_value.cpp`

```cpp
/**
 * @file test_ts_value.cpp
 * @brief Unit tests for TSValue owning container.
 *
 * Tests construction, view access, time semantics, and lazy delta clearing.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/util/date_time.h>

using namespace hgraph;

// ============================================================================
// Construction Tests
// ============================================================================

TEST_CASE("TSValue - construction for TS[int]", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
    // auto* meta = create_ts_meta(TSKind::TS, int_type());
    // TSValue ts_val(meta);
    // CHECK(ts_val.meta() == meta);
}

TEST_CASE("TSValue - construction for TSS[int]", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
    // auto* meta = create_ts_meta(TSKind::TSS, int_type());
    // TSValue ts_val(meta);
    // CHECK(ts_val.has_delta());
}

TEST_CASE("TSValue - construction for TSD[str, int]", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
}

TEST_CASE("TSValue - construction for TSB", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
}

// ============================================================================
// View Access Tests
// ============================================================================

TEST_CASE("TSValue - value_view returns valid View", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
}

TEST_CASE("TSValue - time_view returns valid View", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
}

TEST_CASE("TSValue - observer_view returns valid View", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
}

TEST_CASE("TSValue - delta_value_view for TS returns null/empty", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
}

TEST_CASE("TSValue - delta_value_view for TSS returns SetDelta view", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
}

// ============================================================================
// Time Semantics Tests
// ============================================================================

TEST_CASE("TSValue - initial not valid", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
    // TSValue ts_val(meta);
    // CHECK(!ts_val.valid());
}

TEST_CASE("TSValue - initial not modified", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
    // TSValue ts_val(meta);
    // CHECK(!ts_val.modified(engine_time_t{1000}));
}

TEST_CASE("TSValue - last_modified_time initial is MIN_ST", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
    // TSValue ts_val(meta);
    // CHECK(ts_val.last_modified_time() == MIN_ST);
}

TEST_CASE("TSValue - modified uses >= comparison", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
    // TSValue ts_val(meta);
    // Set time to 1000
    // CHECK(ts_val.modified(engine_time_t{1000}));  // >=
    // CHECK(ts_val.modified(engine_time_t{999}));   // >= (earlier query)
    // CHECK(!ts_val.modified(engine_time_t{1001})); // not >=
}

// ============================================================================
// Lazy Delta Clearing Tests
// ============================================================================

TEST_CASE("TSValue - delta cleared on tick advance", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
    // TSValue ts_val(tss_meta);
    // Get delta at t=1000, make changes
    // auto delta1 = ts_val.delta_value_view(engine_time_t{1000});
    // Get delta at t=1001 - should be cleared
    // auto delta2 = ts_val.delta_value_view(engine_time_t{1001});
    // CHECK(delta2.as<SetDelta>().empty());
}

TEST_CASE("TSValue - delta not cleared same tick", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
    // Multiple delta accesses at same time don't clear
}

TEST_CASE("TSValue - delta clear uses > comparison", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
    // current_time > last_delta_clear_time triggers clear
    // current_time == last_delta_clear_time does NOT clear
}

// ============================================================================
// ts_view() Tests
// ============================================================================

TEST_CASE("TSValue - ts_view returns coordinated TSView", "[time_series][phase4][ts_value]") {
    SKIP("Awaiting TSValue implementation");
    // TSValue ts_val(meta);
    // TSView view = ts_val.ts_view(engine_time_t{1000});
    // CHECK(view.meta() == meta);
    // CHECK(view.current_time() == engine_time_t{1000});
}
```

---

## Section 6: Phase 5 - TSView

### 6.1 test_ts_view.cpp

**File:** `cpp/tests/types/time_series/test_ts_view.cpp`

```cpp
/**
 * @file test_ts_view.cpp
 * @brief Unit tests for TSView and kind-specific wrappers.
 *
 * Tests TSView, TSScalarView, TSBView, TSLView, TSDView, TSSView.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/util/date_time.h>

using namespace hgraph;

// ============================================================================
// TSView Base Tests
// ============================================================================

TEST_CASE("TSView - construction from TSValue", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSView implementation");
}

TEST_CASE("TSView - modified uses >= comparison", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSView implementation");
}

TEST_CASE("TSView - valid check", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSView implementation");
}

TEST_CASE("TSView - value access", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSView implementation");
}

TEST_CASE("TSView - delta_value access", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSView implementation");
}

TEST_CASE("TSView - has_delta matches meta", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSView implementation");
}

// ============================================================================
// TSScalarView Tests
// ============================================================================

TEST_CASE("TSScalarView - value_as typed access", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSScalarView implementation");
}

// ============================================================================
// TSBView Tests
// ============================================================================

TEST_CASE("TSBView - field by name", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSBView implementation");
}

TEST_CASE("TSBView - field by index", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSBView implementation");
}

TEST_CASE("TSBView - modified per field", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSBView implementation");
}

TEST_CASE("TSBView - valid per field", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSBView implementation");
}

// ============================================================================
// TSLView Tests
// ============================================================================

TEST_CASE("TSLView - at index", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSLView implementation");
}

TEST_CASE("TSLView - size", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSLView implementation");
}

TEST_CASE("TSLView - element_modified", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSLView implementation");
}

// ============================================================================
// TSDView Tests
// ============================================================================

TEST_CASE("TSDView - at key", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSDView implementation");
}

TEST_CASE("TSDView - contains key", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSDView implementation");
}

TEST_CASE("TSDView - added_slots", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSDView implementation");
}

TEST_CASE("TSDView - removed_slots", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSDView implementation");
}

TEST_CASE("TSDView - updated_slots", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSDView implementation");
}

// ============================================================================
// TSSView Tests
// ============================================================================

TEST_CASE("TSSView - contains element", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSSView implementation");
}

TEST_CASE("TSSView - size", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSSView implementation");
}

TEST_CASE("TSSView - added_slots", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSSView implementation");
}

TEST_CASE("TSSView - removed_slots", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSSView implementation");
}

TEST_CASE("TSSView - was_cleared", "[time_series][phase5][ts_view]") {
    SKIP("Awaiting TSSView implementation");
}
```

---

## Section 7: CMake Integration

### 7.1 CMakeLists.txt Addition

Add to `cpp/tests/CMakeLists.txt`:

```cmake
# TSValue tests
add_executable(test_ts_value
    types/time_series/test_observer_list.cpp
    types/time_series/test_time_array.cpp
    types/time_series/test_observer_array.cpp
    types/time_series/test_set_delta.cpp
    types/time_series/test_map_delta.cpp
    types/time_series/test_delta_nav.cpp
    types/time_series/test_ts_meta_schema.cpp
    types/time_series/test_ts_value.cpp
    types/time_series/test_ts_view.cpp
)

target_link_libraries(test_ts_value
    PRIVATE
        hgraph
        Catch2::Catch2WithMain
)

catch_discover_tests(test_ts_value)
```

---

## Section 8: Running Tests

### 8.1 Commands

```bash
# Build tests
cmake --build cmake-build-debug --target test_ts_value

# Run all TSValue tests
./cmake-build-debug/cpp/tests/test_ts_value

# Run specific phase
./cmake-build-debug/cpp/tests/test_ts_value "[phase1]"

# Run specific component
./cmake-build-debug/cpp/tests/test_ts_value "[observer]"
./cmake-build-debug/cpp/tests/test_ts_value "[delta]"

# Run with verbose output
./cmake-build-debug/cpp/tests/test_ts_value -s

# List all tests
./cmake-build-debug/cpp/tests/test_ts_value --list-tests
```

### 8.2 Expected Test Counts

| Phase | Tag | Expected Tests |
|-------|-----|----------------|
| 1 | [phase1] | ~25 |
| 2 | [phase2] | ~30 |
| 3 | [phase3] | ~20 |
| 4 | [phase4] | ~15 |
| 5 | [phase5] | ~20 |
| **Total** | | **~110** |

---

## Section 9: Test Dependencies

```
Foundation (must pass first):
├── test_observer_list.cpp
├── test_time_array.cpp
└── test_observer_array.cpp

Delta (depends on foundation):
├── test_set_delta.cpp
├── test_map_delta.cpp
└── test_delta_nav.cpp

Schema (depends on delta):
└── test_ts_meta_schema.cpp

Core (depends on schema):
├── test_ts_value.cpp
└── test_ts_view.cpp
```
