/**
 * @file test_ts_view_range.cpp
 * @brief Unit tests for TSView iteration ranges.
 *
 * Tests the iteration functionality for:
 * - TSViewRange (list elements)
 * - TSFieldRange (bundle fields)
 * - TSDictRange (dict entries)
 * - FilteredTSViewRange, FilteredTSFieldRange, FilteredTSDictRange
 * - SlotElementRange (set element delta iteration)
 * - SlotKeyRange (dict key delta iteration)
 * - TSDictSlotRange (dict entry delta iteration)
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/ts_view_range.h>
#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/types/time_series/map_delta.h>
#include <hgraph/types/time_series/slot_set.h>
#include <hgraph/util/date_time.h>

using namespace hgraph;

// ============================================================================
// SlotSet Tests (Foundation for delta iteration)
// ============================================================================

TEST_CASE("SlotSet - basic iteration", "[time_series][view_range][slot_set]") {
    SlotSet slots;
    slots.insert(0);
    slots.insert(5);
    slots.insert(10);

    std::vector<size_t> collected;
    for (auto it = slots.begin(); it != slots.end(); ++it) {
        collected.push_back(*it);
    }

    CHECK(collected.size() == 3);
    // SlotSet iteration order may vary; just check all elements are present
    CHECK(std::find(collected.begin(), collected.end(), 0) != collected.end());
    CHECK(std::find(collected.begin(), collected.end(), 5) != collected.end());
    CHECK(std::find(collected.begin(), collected.end(), 10) != collected.end());
}

TEST_CASE("SlotSet - empty iteration", "[time_series][view_range][slot_set]") {
    SlotSet slots;

    int count = 0;
    for (auto it = slots.begin(); it != slots.end(); ++it) {
        ++count;
    }

    CHECK(count == 0);
}

TEST_CASE("SlotSet - range-based for", "[time_series][view_range][slot_set]") {
    SlotSet slots;
    slots.insert(1);
    slots.insert(2);
    slots.insert(3);

    std::vector<size_t> collected;
    for (size_t slot : slots) {
        collected.push_back(slot);
    }

    CHECK(collected.size() == 3);
}

// ============================================================================
// SetDelta Tests (for TSSView iteration)
// ============================================================================

TEST_CASE("SetDelta - added slots iteration", "[time_series][view_range][set_delta]") {
    SetDelta delta;
    delta.on_insert(0);
    delta.on_insert(5);
    delta.on_insert(10);

    const SlotSet& added = delta.added();
    CHECK(added.size() == 3);
    CHECK(added.contains(0));
    CHECK(added.contains(5));
    CHECK(added.contains(10));
}

TEST_CASE("SetDelta - removed slots iteration", "[time_series][view_range][set_delta]") {
    SetDelta delta;

    // Test slot-based removal tracking
    delta.on_erase(0);
    delta.on_erase(7);

    const SlotSet& removed = delta.removed();
    CHECK(removed.size() == 2);
    CHECK(removed.contains(0));
    CHECK(removed.contains(7));
}

TEST_CASE("SetDelta - was_slot_added check", "[time_series][view_range][set_delta]") {
    SetDelta delta;
    delta.on_insert(42);

    CHECK(delta.was_slot_added(42));
    CHECK_FALSE(delta.was_slot_added(0));
    CHECK_FALSE(delta.was_slot_added(100));
}

TEST_CASE("SetDelta - was_slot_removed check", "[time_series][view_range][set_delta]") {
    SetDelta delta;
    delta.on_erase(99);

    CHECK(delta.was_slot_removed(99));
    CHECK_FALSE(delta.was_slot_removed(0));
    CHECK_FALSE(delta.was_slot_removed(42));
}

TEST_CASE("SetDelta - clear resets all tracking", "[time_series][view_range][set_delta]") {
    SetDelta delta;
    delta.on_insert(1);
    delta.on_insert(2);
    delta.on_erase(3);

    delta.clear();

    CHECK(delta.added().empty());
    CHECK(delta.removed().empty());
}

// ============================================================================
// MapDelta Tests (for TSDView iteration)
// ============================================================================

TEST_CASE("MapDelta - added slots iteration", "[time_series][view_range][map_delta]") {
    MapDelta delta;
    delta.on_insert(0);
    delta.on_insert(10);
    delta.on_insert(20);

    const SlotSet& added = delta.added();
    CHECK(added.size() == 3);
    CHECK(added.contains(0));
    CHECK(added.contains(10));
    CHECK(added.contains(20));
}

TEST_CASE("MapDelta - updated slots iteration", "[time_series][view_range][map_delta]") {
    MapDelta delta;
    delta.on_update(5);
    delta.on_update(15);

    const SlotSet& updated = delta.updated();
    CHECK(updated.size() == 2);
    CHECK(updated.contains(5));
    CHECK(updated.contains(15));
}

TEST_CASE("MapDelta - modified slots combines added and updated", "[time_series][view_range][map_delta]") {
    MapDelta delta;
    delta.on_insert(1);  // added
    delta.on_update(2);  // updated
    delta.on_insert(3);  // added

    const SlotSet& modified = delta.modified();
    CHECK(modified.size() == 3);
    CHECK(modified.contains(1));
    CHECK(modified.contains(2));
    CHECK(modified.contains(3));
}

TEST_CASE("MapDelta - removed slots iteration", "[time_series][view_range][map_delta]") {
    MapDelta delta;

    // Test slot-based removal tracking
    delta.on_erase(100);
    delta.on_erase(200);

    const SlotSet& removed = delta.removed();
    CHECK(removed.size() == 2);
    CHECK(removed.contains(100));
    CHECK(removed.contains(200));
}

TEST_CASE("MapDelta - key_delta returns SetDelta reference", "[time_series][view_range][map_delta]") {
    MapDelta delta;
    delta.on_insert(42);

    SetDelta& key_delta = delta.key_delta();

    // The key_delta should have the same added slots
    CHECK(key_delta.added().contains(42));
}

TEST_CASE("MapDelta - clear resets all tracking", "[time_series][view_range][map_delta]") {
    MapDelta delta;
    delta.on_insert(1);
    delta.on_update(2);
    delta.on_erase(3);

    delta.clear();

    CHECK(delta.added().empty());
    CHECK(delta.updated().empty());
    CHECK(delta.removed().empty());
    CHECK(delta.modified().empty());
}

// ============================================================================
// SlotKeyIterator / SlotKeyRange Tests
// ============================================================================

TEST_CASE("SlotKeyRange - empty range", "[time_series][view_range][slot_key]") {
    SlotKeyRange range;

    CHECK(range.empty());
    CHECK(range.size() == 0);
    CHECK(range.begin() == range.end());
}

TEST_CASE("SlotKeyRange - null slots returns empty", "[time_series][view_range][slot_key]") {
    SlotKeyRange range(nullptr, nullptr, nullptr);

    CHECK(range.empty());
    CHECK(range.size() == 0);
}

// ============================================================================
// SlotElementIterator / SlotElementRange Tests
// ============================================================================

TEST_CASE("SlotElementRange - empty range", "[time_series][view_range][slot_element]") {
    SlotElementRange range;

    CHECK(range.empty());
    CHECK(range.size() == 0);
    CHECK(range.begin() == range.end());
}

TEST_CASE("SlotElementRange - null slots returns empty", "[time_series][view_range][slot_element]") {
    SlotElementRange range(nullptr, nullptr, nullptr);

    CHECK(range.empty());
    CHECK(range.size() == 0);
}

// ============================================================================
// TSViewRange Tests
// ============================================================================

TEST_CASE("TSViewRange - default construction", "[time_series][view_range][ts_view]") {
    TSViewRange range;

    CHECK(range.empty());
    CHECK(range.size() == 0);
    CHECK(range.begin() == range.end());
}

TEST_CASE("TSViewIterator - default construction", "[time_series][view_range][ts_view]") {
    TSViewIterator it;
    TSViewIterator end;

    CHECK(it == end);
}

TEST_CASE("TSViewIterator - index accessor", "[time_series][view_range][ts_view]") {
    ViewData vd;
    TSViewIterator it(&vd, 5, 10, MIN_DT);

    CHECK(it.index() == 5);
}

TEST_CASE("TSViewIterator - increment", "[time_series][view_range][ts_view]") {
    ViewData vd;
    TSViewIterator it(&vd, 0, 3, MIN_DT);

    CHECK(it.index() == 0);
    ++it;
    CHECK(it.index() == 1);
    ++it;
    CHECK(it.index() == 2);
}

TEST_CASE("TSViewIterator - equality comparison", "[time_series][view_range][ts_view]") {
    ViewData vd;
    TSViewIterator it1(&vd, 5, 10, MIN_DT);
    TSViewIterator it2(&vd, 5, 10, MIN_DT);
    TSViewIterator it3(&vd, 6, 10, MIN_DT);

    CHECK(it1 == it2);
    CHECK(it1 != it3);
}

// ============================================================================
// TSFieldRange Tests
// ============================================================================

TEST_CASE("TSFieldRange - default construction", "[time_series][view_range][ts_field]") {
    TSFieldRange range;

    CHECK(range.empty());
    CHECK(range.size() == 0);
    CHECK(range.begin() == range.end());
}

TEST_CASE("TSFieldIterator - default construction", "[time_series][view_range][ts_field]") {
    TSFieldIterator it;
    TSFieldIterator end;

    CHECK(it == end);
}

TEST_CASE("TSFieldIterator - index accessor", "[time_series][view_range][ts_field]") {
    ViewData vd;
    TSFieldIterator it(&vd, nullptr, 3, 10, MIN_DT);

    CHECK(it.index() == 3);
}

TEST_CASE("TSFieldIterator - name accessor with null meta", "[time_series][view_range][ts_field]") {
    ViewData vd;
    TSFieldIterator it(&vd, nullptr, 0, 1, MIN_DT);

    // With null meta, name() should return empty string
    CHECK(std::string(it.name()) == "");
}

// ============================================================================
// TSFieldNameRange Tests
// ============================================================================

TEST_CASE("TSFieldNameRange - default construction", "[time_series][view_range][ts_field_name]") {
    TSFieldNameRange range;

    CHECK(range.empty());
    CHECK(range.size() == 0);
    CHECK(range.begin() == range.end());
}

TEST_CASE("TSFieldNameIterator - default construction", "[time_series][view_range][ts_field_name]") {
    TSFieldNameIterator it;
    TSFieldNameIterator end;

    CHECK(it == end);
}

TEST_CASE("TSFieldNameIterator - index accessor", "[time_series][view_range][ts_field_name]") {
    TSFieldNameIterator it(nullptr, 3, 10);

    CHECK(it.index() == 3);
}

TEST_CASE("TSFieldNameIterator - name with null meta returns empty string", "[time_series][view_range][ts_field_name]") {
    TSFieldNameIterator it(nullptr, 0, 1);

    CHECK(std::string(*it) == "");
}

TEST_CASE("TSFieldNameIterator - increment", "[time_series][view_range][ts_field_name]") {
    TSFieldNameIterator it(nullptr, 0, 3);

    CHECK(it.index() == 0);
    ++it;
    CHECK(it.index() == 1);
    ++it;
    CHECK(it.index() == 2);
}

TEST_CASE("TSFieldNameIterator - equality comparison", "[time_series][view_range][ts_field_name]") {
    TSFieldNameIterator it1(nullptr, 5, 10);
    TSFieldNameIterator it2(nullptr, 5, 10);
    TSFieldNameIterator it3(nullptr, 6, 10);

    CHECK(it1 == it2);
    CHECK(it1 != it3);
}

TEST_CASE("TSFieldNameIterator - is forward iterator", "[time_series][view_range][ts_field_name]") {
    static_assert(std::is_same_v<TSFieldNameIterator::iterator_category, std::forward_iterator_tag>);
}

// ============================================================================
// TSDictRange Tests
// ============================================================================

TEST_CASE("TSDictRange - default construction", "[time_series][view_range][ts_dict]") {
    TSDictRange range;

    CHECK(range.empty());
    CHECK(range.size() == 0);
    CHECK(range.begin() == range.end());
}

TEST_CASE("TSDictIterator - default construction", "[time_series][view_range][ts_dict]") {
    TSDictIterator it;
    TSDictIterator end;

    CHECK(it == end);
}

TEST_CASE("TSDictIterator - index accessor", "[time_series][view_range][ts_dict]") {
    ViewData vd;
    TSDictIterator it(&vd, nullptr, 7, 10, MIN_DT);

    CHECK(it.index() == 7);
}

// ============================================================================
// TSDictSlotRange Tests
// ============================================================================

TEST_CASE("TSDictSlotRange - default construction", "[time_series][view_range][ts_dict_slot]") {
    TSDictSlotRange range;

    CHECK(range.empty());
    CHECK(range.size() == 0);
    CHECK(range.begin() == range.end());
}

TEST_CASE("TSDictSlotRange - with valid slots", "[time_series][view_range][ts_dict_slot]") {
    SlotSet slots;
    slots.insert(0);
    slots.insert(5);
    slots.insert(10);

    ViewData vd;
    TSDictSlotRange range(vd, nullptr, &slots, MIN_DT);

    CHECK_FALSE(range.empty());
    CHECK(range.size() == 3);
}

TEST_CASE("TSDictSlotIterator - slot accessor", "[time_series][view_range][ts_dict_slot]") {
    SlotSet slots;
    slots.insert(42);

    ViewData vd;
    TSDictSlotIterator it(&vd, nullptr, slots.begin(), slots.end(), MIN_DT);

    CHECK(it.slot() == 42);
}

// ============================================================================
// Filtered Iterator/Range Tests - Compile-Time Only
// ============================================================================
// Note: Runtime tests for filtered iterators require linking to _hgraph
// due to the dependency on TSView. These are covered by Python integration tests.
// The compile-time checks below verify the type system is correct.

// ============================================================================
// Type Alias Tests - Compile-Time Only
// ============================================================================

TEST_CASE("Type aliases are correctly defined", "[time_series][view_range][aliases]") {
    // These static_asserts verify the type aliases at compile time
    static_assert(std::is_same_v<ValidTSViewRange, FilteredTSViewRange<TSFilter::VALID>>);
    static_assert(std::is_same_v<ModifiedTSViewRange, FilteredTSViewRange<TSFilter::MODIFIED>>);
    static_assert(std::is_same_v<ValidTSFieldRange, FilteredTSFieldRange<TSFilter::VALID>>);
    static_assert(std::is_same_v<ModifiedTSFieldRange, FilteredTSFieldRange<TSFilter::MODIFIED>>);
    static_assert(std::is_same_v<ValidTSDictRange, FilteredTSDictRange<TSFilter::VALID>>);
    static_assert(std::is_same_v<ModifiedTSDictRange, FilteredTSDictRange<TSFilter::MODIFIED>>);

    // If we reach here, all compile-time checks passed
    CHECK(true);
}

// ============================================================================
// Iterator Category Tests
// ============================================================================

TEST_CASE("TSViewIterator is forward iterator", "[time_series][view_range][iterator_traits]") {
    static_assert(std::is_same_v<TSViewIterator::iterator_category, std::forward_iterator_tag>);
}

TEST_CASE("TSFieldIterator is forward iterator", "[time_series][view_range][iterator_traits]") {
    static_assert(std::is_same_v<TSFieldIterator::iterator_category, std::forward_iterator_tag>);
}

TEST_CASE("TSDictIterator is forward iterator", "[time_series][view_range][iterator_traits]") {
    static_assert(std::is_same_v<TSDictIterator::iterator_category, std::forward_iterator_tag>);
}

TEST_CASE("TSDictSlotIterator is forward iterator", "[time_series][view_range][iterator_traits]") {
    static_assert(std::is_same_v<TSDictSlotIterator::iterator_category, std::forward_iterator_tag>);
}

TEST_CASE("SlotKeyIterator is forward iterator", "[time_series][view_range][iterator_traits]") {
    static_assert(std::is_same_v<SlotKeyIterator::iterator_category, std::forward_iterator_tag>);
}

TEST_CASE("SlotElementIterator is forward iterator", "[time_series][view_range][iterator_traits]") {
    static_assert(std::is_same_v<SlotElementIterator::iterator_category, std::forward_iterator_tag>);
}

TEST_CASE("FilteredTSViewIterator is forward iterator", "[time_series][view_range][iterator_traits]") {
    static_assert(std::is_same_v<FilteredTSViewIterator<TSFilter::VALID>::iterator_category, std::forward_iterator_tag>);
    static_assert(std::is_same_v<FilteredTSViewIterator<TSFilter::MODIFIED>::iterator_category, std::forward_iterator_tag>);
}

TEST_CASE("FilteredTSFieldIterator is forward iterator", "[time_series][view_range][iterator_traits]") {
    static_assert(std::is_same_v<FilteredTSFieldIterator<TSFilter::VALID>::iterator_category, std::forward_iterator_tag>);
    static_assert(std::is_same_v<FilteredTSFieldIterator<TSFilter::MODIFIED>::iterator_category, std::forward_iterator_tag>);
}

TEST_CASE("FilteredTSDictIterator is forward iterator", "[time_series][view_range][iterator_traits]") {
    static_assert(std::is_same_v<FilteredTSDictIterator<TSFilter::VALID>::iterator_category, std::forward_iterator_tag>);
    static_assert(std::is_same_v<FilteredTSDictIterator<TSFilter::MODIFIED>::iterator_category, std::forward_iterator_tag>);
}
