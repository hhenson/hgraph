#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/value/type_registry.h>

#include <catch2/catch_test_macros.hpp>

namespace hgraph::test {

TEST_CASE("ScalarTSOverlay tracks modification time", "[ts_overlay][scalar]") {
    ScalarTSOverlay overlay;

    // Initially invalid
    REQUIRE_FALSE(overlay.valid());
    REQUIRE(overlay.last_modified_time() == MIN_DT);

    // Mark as modified
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);

    REQUIRE(overlay.valid());
    REQUIRE(overlay.last_modified_time() == t1);
    REQUIRE(overlay.modified_at(t1));

    // Mark invalid again
    overlay.mark_invalid();
    REQUIRE_FALSE(overlay.valid());
    REQUIRE(overlay.last_modified_time() == MIN_DT);
}

TEST_CASE("ScalarTSOverlay propagates modification to parent", "[ts_overlay][scalar]") {
    ScalarTSOverlay parent;
    ScalarTSOverlay child;

    child.set_parent(&parent);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    child.mark_modified(t1);

    // Child should be modified
    REQUIRE(child.modified_at(t1));

    // Parent should also be modified
    REQUIRE(parent.modified_at(t1));
}

TEST_CASE("ListTSOverlay can be created empty", "[ts_overlay][list]") {
    // Create a TSL[TS[int], 0] meta (dynamic list)
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);

    // Create a list value schema for storage
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);

    ListTSOverlay overlay(&list_ts_meta);

    REQUIRE(overlay.child_count() == 0);
    REQUIRE_FALSE(overlay.valid());
}

TEST_CASE("ListTSOverlay push_back creates new children", "[ts_overlay][list]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);
    ListTSOverlay overlay(&list_ts_meta);

    // Add first element
    auto* child0 = overlay.push_back();
    REQUIRE(child0 != nullptr);
    REQUIRE(overlay.child_count() == 1);
    REQUIRE(overlay.child(0) == child0);

    // Verify parent pointer is set
    REQUIRE(child0->parent() == &overlay);

    // Add second element
    auto* child1 = overlay.push_back();
    REQUIRE(child1 != nullptr);
    REQUIRE(overlay.child_count() == 2);
    REQUIRE(overlay.child(1) == child1);
    REQUIRE(child1->parent() == &overlay);
}

TEST_CASE("ListTSOverlay pop_back removes last child", "[ts_overlay][list]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);
    ListTSOverlay overlay(&list_ts_meta);

    // Add three elements
    overlay.push_back();
    overlay.push_back();
    overlay.push_back();
    REQUIRE(overlay.child_count() == 3);

    // Remove one
    overlay.pop_back();
    REQUIRE(overlay.child_count() == 2);

    // Remove another
    overlay.pop_back();
    REQUIRE(overlay.child_count() == 1);

    // Remove last
    overlay.pop_back();
    REQUIRE(overlay.child_count() == 0);

    // pop_back on empty list should be safe
    overlay.pop_back();
    REQUIRE(overlay.child_count() == 0);
}

TEST_CASE("ListTSOverlay resize grows and shrinks list", "[ts_overlay][list]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);
    ListTSOverlay overlay(&list_ts_meta);

    // Resize to 5
    overlay.resize(5);
    REQUIRE(overlay.child_count() == 5);

    // Verify all children have parent pointers
    for (size_t i = 0; i < 5; ++i) {
        auto* child = overlay.child(i);
        REQUIRE(child != nullptr);
        REQUIRE(child->parent() == &overlay);
    }

    // Resize down to 2
    overlay.resize(2);
    REQUIRE(overlay.child_count() == 2);

    // Resize to same size (no-op)
    overlay.resize(2);
    REQUIRE(overlay.child_count() == 2);

    // Resize back up to 4
    overlay.resize(4);
    REQUIRE(overlay.child_count() == 4);
}

TEST_CASE("ListTSOverlay clear removes all children", "[ts_overlay][list]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);
    ListTSOverlay overlay(&list_ts_meta);

    // Add some elements
    overlay.resize(10);
    REQUIRE(overlay.child_count() == 10);

    // Clear
    overlay.clear();
    REQUIRE(overlay.child_count() == 0);
}

TEST_CASE("ListTSOverlay child modification propagates to parent", "[ts_overlay][list]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);
    ListTSOverlay overlay(&list_ts_meta);

    // Add a child
    auto* child = overlay.push_back();
    REQUIRE(child != nullptr);

    // Parent starts invalid
    REQUIRE_FALSE(overlay.valid());

    // Modify child
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    child->mark_modified(t1);

    // Child should be modified
    REQUIRE(child->modified_at(t1));

    // Parent should also be modified
    REQUIRE(overlay.modified_at(t1));
    REQUIRE(overlay.valid());
}

TEST_CASE("ListTSOverlay child bounds checking", "[ts_overlay][list]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);
    ListTSOverlay overlay(&list_ts_meta);

    overlay.resize(3);

    // Valid indices
    REQUIRE(overlay.child(0) != nullptr);
    REQUIRE(overlay.child(1) != nullptr);
    REQUIRE(overlay.child(2) != nullptr);

    // Out of bounds
    REQUIRE(overlay.child(3) == nullptr);
    REQUIRE(overlay.child(100) == nullptr);
}

// ============================================================================
// SetTSOverlay Tests (simplified with added/removed buffers)
// ============================================================================

TEST_CASE("SetTSOverlay can be created empty", "[ts_overlay][set]") {
    // Create a minimal TSMeta for testing (we don't have TSSTypeMeta yet)
    // Just pass nullptr for now - SetTSOverlay should handle it gracefully
    SetTSOverlay overlay(nullptr);

    REQUIRE_FALSE(overlay.has_added());
    REQUIRE_FALSE(overlay.has_removed());
    REQUIRE_FALSE(overlay.valid());
    REQUIRE(overlay.last_modified_time() == MIN_DT);
}

TEST_CASE("SetTSOverlay record_added tracks indices in buffer", "[ts_overlay][set]") {
    SetTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Record element added at index 0
    overlay.record_added(0, t1);

    REQUIRE(overlay.has_added());
    REQUIRE(overlay.added_indices().size() == 1);
    REQUIRE(overlay.added_indices()[0] == 0);
    REQUIRE(overlay.last_modified_time() == t1);
    REQUIRE(overlay.valid());
}

TEST_CASE("SetTSOverlay record_added accumulates multiple adds", "[ts_overlay][set]") {
    SetTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Add multiple elements in same tick
    overlay.record_added(5, t1);
    overlay.record_added(2, t1);
    overlay.record_added(10, t1);

    REQUIRE(overlay.added_indices().size() == 3);
    REQUIRE(overlay.added_indices()[0] == 5);
    REQUIRE(overlay.added_indices()[1] == 2);
    REQUIRE(overlay.added_indices()[2] == 10);
}

TEST_CASE("SetTSOverlay record_removed tracks indices and values in buffer", "[ts_overlay][set]") {
    SetTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Create a value to remove
    value::PlainValue removed_val(int64_t{42});

    // Record element removed at index 3
    overlay.record_removed(3, t1, std::move(removed_val));

    REQUIRE(overlay.has_removed());
    REQUIRE(overlay.removed_indices().size() == 1);
    REQUIRE(overlay.removed_indices()[0] == 3);
    REQUIRE(overlay.removed_values().size() == 1);
    REQUIRE(overlay.removed_values()[0].as<int64_t>() == 42);
    REQUIRE(overlay.last_modified_time() == t1);
}

TEST_CASE("SetTSOverlay has_delta_at with time check clears buffers lazily", "[ts_overlay][set]") {
    SetTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Add and remove some elements
    overlay.record_added(0, t1);
    overlay.record_added(1, t1);
    overlay.record_removed(5, t1, value::PlainValue(int64_t{999}));

    REQUIRE(overlay.added_indices().size() == 2);
    REQUIRE(overlay.removed_indices().size() == 1);
    REQUIRE(overlay.removed_values().size() == 1);

    // Query delta at the correct time - should return true
    REQUIRE(overlay.has_delta_at(t1));
    // Buffers still intact after query
    REQUIRE(overlay.added_indices().size() == 2);
    REQUIRE(overlay.removed_indices().size() == 1);

    // Query delta at a different time - should clear buffers and return false
    REQUIRE_FALSE(overlay.has_delta_at(t2));

    REQUIRE_FALSE(overlay.has_added());
    REQUIRE_FALSE(overlay.has_removed());
    REQUIRE(overlay.added_indices().empty());
    REQUIRE(overlay.removed_indices().empty());

    // Modification time preserved (has_delta_at doesn't change it)
    REQUIRE(overlay.last_modified_time() == t1);
}

TEST_CASE("SetTSOverlay lazy cleanup on record_added", "[ts_overlay][set]") {
    SetTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Add element at t1
    overlay.record_added(0, t1);
    REQUIRE(overlay.added_indices().size() == 1);
    REQUIRE(overlay.added_indices()[0] == 0);

    // Add element at t2 - should auto-clear buffers from t1
    overlay.record_added(5, t2);
    REQUIRE(overlay.added_indices().size() == 1);  // Only the new one
    REQUIRE(overlay.added_indices()[0] == 5);
    REQUIRE(overlay.last_modified_time() == t2);
}

TEST_CASE("SetTSOverlay hook_on_swap updates indices in buffers", "[ts_overlay][set][hooks]") {
    SetTSOverlay overlay(nullptr);
    auto hooks = overlay.make_hooks();

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Set up elements with some in buffers
    overlay.record_added(2, t1);
    overlay.record_added(5, t1);

    REQUIRE(overlay.added_indices()[0] == 2);
    REQUIRE(overlay.added_indices()[1] == 5);

    // Simulate swap (happens during erase of non-last element)
    hooks.swap(2, 5);

    // Buffer indices should be updated
    REQUIRE(overlay.added_indices()[0] == 5);  // Was 2, now 5
    REQUIRE(overlay.added_indices()[1] == 2);  // Was 5, now 2
}

TEST_CASE("SetTSOverlay hook integration simulates insert-erase cycle", "[ts_overlay][set][hooks]") {
    SetTSOverlay overlay(nullptr);
    auto hooks = overlay.make_hooks();

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Simulate inserting 3 elements at indices 0, 1, 2
    hooks.insert(0);
    overlay.record_added(0, t1);

    hooks.insert(1);
    overlay.record_added(1, t1);

    hooks.insert(2);
    overlay.record_added(2, t1);

    REQUIRE(overlay.added_indices().size() == 3);
    REQUIRE(overlay.added_indices()[0] == 0);
    REQUIRE(overlay.added_indices()[1] == 1);
    REQUIRE(overlay.added_indices()[2] == 2);

    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Simulate erase of element at index 1 (swap-with-last, then remove)
    // Record removal first (this also triggers lazy cleanup since t2 != t1)
    overlay.record_removed(1, t2, value::PlainValue(int64_t{100}));
    // Then swap+erase
    hooks.swap(1, 2);
    hooks.erase(2);

    // Buffers were cleared by lazy cleanup, so only the new removal
    REQUIRE(overlay.removed_indices().size() == 1);
    REQUIRE(overlay.removed_values().size() == 1);
    // The removed index was 1, but after swap it points to old index 2
    REQUIRE(overlay.removed_indices()[0] == 2);  // Updated by swap
}

TEST_CASE("SetTSOverlay mark_modified updates parent", "[ts_overlay][set]") {
    SetTSOverlay parent(nullptr);
    SetTSOverlay child(nullptr);

    child.set_parent(&parent);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Record element added on child
    child.record_added(0, t1);

    // Child should be modified
    REQUIRE(child.modified_at(t1));

    // Parent should also be modified (via propagation)
    REQUIRE(parent.modified_at(t1));
}

TEST_CASE("SetTSOverlay mark_invalid resets timestamp", "[ts_overlay][set]") {
    SetTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Record some elements
    overlay.record_added(0, t1);
    overlay.record_added(1, t1);

    REQUIRE(overlay.valid());
    REQUIRE(overlay.last_modified_time() == t1);

    // Mark invalid
    overlay.mark_invalid();

    REQUIRE_FALSE(overlay.valid());
    REQUIRE(overlay.last_modified_time() == MIN_DT);

    // Buffers should still be preserved (until clear_delta)
    REQUIRE(overlay.added_indices().size() == 2);
}

TEST_CASE("SetTSOverlay multi-tick tracking with lazy cleanup", "[ts_overlay][set]") {
    SetTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Tick 1: Add elements
    overlay.record_added(0, t1);
    overlay.record_added(1, t1);
    overlay.record_added(2, t1);

    REQUIRE(overlay.added_indices().size() == 3);
    REQUIRE_FALSE(overlay.has_removed());
    REQUIRE(overlay.has_delta_at(t1));

    // Tick 2: Add one, remove one (lazy cleanup happens automatically)
    overlay.record_added(3, t2);  // This triggers lazy cleanup since t2 != t1
    overlay.record_removed(1, t2, value::PlainValue(int64_t{200}));

    // Only tick 2's changes in buffers
    REQUIRE(overlay.added_indices().size() == 1);
    REQUIRE(overlay.added_indices()[0] == 3);
    REQUIRE(overlay.removed_indices().size() == 1);
    REQUIRE(overlay.removed_indices()[0] == 1);
    REQUIRE(overlay.removed_values().size() == 1);
    REQUIRE(overlay.removed_values()[0].as<int64_t>() == 200);

    // Container modified at latest time
    REQUIRE(overlay.last_modified_time() == t2);
    REQUIRE(overlay.has_delta_at(t2));
}

// ============================================================================
// MapTSOverlay Tests (with added/removed key buffers and child overlays for values)
// ============================================================================

TEST_CASE("MapTSOverlay can be created empty", "[ts_overlay][map]") {
    // Create a minimal TSMeta for testing (we don't have TSDTypeMeta yet)
    // Just pass nullptr for now - MapTSOverlay should handle it gracefully
    MapTSOverlay overlay(nullptr);

    REQUIRE(overlay.entry_count() == 0);
    REQUIRE_FALSE(overlay.has_added_keys());
    REQUIRE_FALSE(overlay.has_removed_keys());
    REQUIRE_FALSE(overlay.valid());
    REQUIRE(overlay.last_modified_time() == MIN_DT);
}

TEST_CASE("MapTSOverlay value overlay queries return null for non-existent slots", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    // Value overlays should be null for non-existent slots
    REQUIRE(overlay.value_overlay(0) == nullptr);
    REQUIRE(overlay.value_overlay(5) == nullptr);
    REQUIRE(overlay.value_overlay(100) == nullptr);
}

TEST_CASE("MapTSOverlay record_key_added creates child overlay for value", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Record key added at index 0
    overlay.record_key_added(0, t1);

    REQUIRE(overlay.has_added_keys());
    REQUIRE(overlay.added_key_indices().size() == 1);
    REQUIRE(overlay.added_key_indices()[0] == 0);
    // Should have created a value overlay (ScalarTSOverlay since no type info)
    REQUIRE(overlay.value_overlay(0) != nullptr);
    REQUIRE(overlay.last_modified_time() == t1);
    REQUIRE(overlay.valid());
}

TEST_CASE("MapTSOverlay value overlay can track modifications independently", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // First, add a key
    overlay.record_key_added(0, t1);
    REQUIRE(overlay.added_key_indices()[0] == 0);

    // Get the value overlay and mark it modified
    auto* value_ov = overlay.value_overlay(0);
    REQUIRE(value_ov != nullptr);
    value_ov->mark_modified(t2);

    // Value overlay tracks its own modification
    REQUIRE(value_ov->last_modified_time() == t2);
    // Container updated via parent propagation
    REQUIRE(overlay.last_modified_time() == t2);
}

TEST_CASE("MapTSOverlay distinguishes between added keys and modified values", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Add three keys at t1
    overlay.record_key_added(0, t1);
    overlay.record_key_added(1, t1);
    overlay.record_key_added(2, t1);

    REQUIRE(overlay.added_key_indices().size() == 3);

    // Modify value at index 0 at t2
    overlay.value_overlay(0)->mark_modified(t2);

    // At t2: only one value was modified
    std::vector<size_t> modified_at_t2;
    for (size_t i = 0; i < overlay.entry_count(); ++i) {
        auto* vo = overlay.value_overlay(i);
        if (vo && vo->modified_at(t2)) {
            modified_at_t2.push_back(i);
        }
    }
    REQUIRE(modified_at_t2.size() == 1);
    REQUIRE(modified_at_t2[0] == 0);
}

TEST_CASE("MapTSOverlay record_key_added grows vector as needed", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Add keys at non-sequential indices
    overlay.record_key_added(5, t1);
    REQUIRE(overlay.entry_count() >= 6);
    REQUIRE(overlay.value_overlay(5) != nullptr);

    overlay.record_key_added(2, t1);
    REQUIRE(overlay.value_overlay(2) != nullptr);
    REQUIRE(overlay.value_overlay(5) != nullptr);  // Still preserved

    overlay.record_key_added(10, t1);
    REQUIRE(overlay.entry_count() >= 11);
    REQUIRE(overlay.value_overlay(10) != nullptr);

    // Added buffer has all three
    REQUIRE(overlay.added_key_indices().size() == 3);
    REQUIRE(overlay.added_key_indices()[0] == 5);
    REQUIRE(overlay.added_key_indices()[1] == 2);
    REQUIRE(overlay.added_key_indices()[2] == 10);
}

TEST_CASE("MapTSOverlay record_key_removed tracks indices, key values, and buffers value overlay", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Add a key first
    overlay.record_key_added(3, t1);
    REQUIRE(overlay.value_overlay(3) != nullptr);

    // Verify has_delta_at at t1
    REQUIRE(overlay.has_delta_at(t1));

    // Record key removed (lazy cleanup happens since t2 != t1)
    overlay.record_key_removed(3, t2, value::PlainValue(std::string("key_3")));

    REQUIRE(overlay.has_removed_keys());
    REQUIRE(overlay.removed_key_indices().size() == 1);
    REQUIRE(overlay.removed_key_indices()[0] == 3);
    REQUIRE(overlay.removed_key_values().size() == 1);
    REQUIRE(overlay.removed_key_values()[0].as<std::string>() == "key_3");
    REQUIRE(overlay.last_modified_time() == t2);

    // Value overlay moved to removed buffer (no longer at index 3)
    REQUIRE(overlay.value_overlay(3) == nullptr);
    REQUIRE(overlay.removed_value_overlays().size() == 1);
}

TEST_CASE("MapTSOverlay has_delta_at with time check clears buffers lazily", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Add and remove some keys
    overlay.record_key_added(0, t1);
    overlay.record_key_added(1, t1);
    overlay.record_key_removed(5, t1, value::PlainValue(std::string("key_5")));

    REQUIRE(overlay.added_key_indices().size() == 2);
    REQUIRE(overlay.removed_key_indices().size() == 1);
    REQUIRE(overlay.removed_key_values().size() == 1);
    REQUIRE(overlay.removed_value_overlays().size() == 0);  // No value overlay existed at index 5

    // Query delta at the correct time - should return true
    REQUIRE(overlay.has_delta_at(t1));
    // Buffers still intact
    REQUIRE(overlay.added_key_indices().size() == 2);

    // Query delta at a different time - should clear buffers
    REQUIRE_FALSE(overlay.has_delta_at(t2));

    REQUIRE_FALSE(overlay.has_added_keys());
    REQUIRE_FALSE(overlay.has_removed_keys());
    REQUIRE(overlay.added_key_indices().empty());
    REQUIRE(overlay.removed_key_indices().empty());
    REQUIRE(overlay.removed_key_values().empty());
    REQUIRE(overlay.removed_value_overlays().empty());

    // Value overlays preserved
    REQUIRE(overlay.value_overlay(0) != nullptr);
    REQUIRE(overlay.value_overlay(1) != nullptr);
}

TEST_CASE("MapTSOverlay ensure_value_overlay creates overlay on demand", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    // No overlay exists yet
    REQUIRE(overlay.value_overlay(0) == nullptr);

    // Ensure creates it
    auto* ov = overlay.ensure_value_overlay(0);
    REQUIRE(ov != nullptr);
    REQUIRE(overlay.value_overlay(0) == ov);

    // Calling again returns same overlay
    auto* ov2 = overlay.ensure_value_overlay(0);
    REQUIRE(ov2 == ov);
}

TEST_CASE("MapTSOverlay reserve pre-allocates capacity", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    overlay.reserve(100);

    // entry_count doesn't grow vector size, just reserves
    REQUIRE(overlay.entry_count() == 0);

    // But adding entries should not cause reallocations up to reserved size
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_key_added(50, t1);

    REQUIRE(overlay.entry_count() >= 51);
    REQUIRE(overlay.value_overlay(50) != nullptr);
}

TEST_CASE("MapTSOverlay hook_on_swap exchanges overlays and updates buffers", "[ts_overlay][map][hooks]") {
    MapTSOverlay overlay(nullptr);
    auto hooks = overlay.make_hooks();

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t1_val = MIN_DT + std::chrono::microseconds(150);
    engine_time_t t2_val = MIN_DT + std::chrono::microseconds(250);

    // Set up two entries with different value overlays
    overlay.record_key_added(2, t1);
    overlay.value_overlay(2)->mark_modified(t1_val);

    overlay.record_key_added(5, t1);
    overlay.value_overlay(5)->mark_modified(t2_val);

    REQUIRE(overlay.value_overlay(2)->last_modified_time() == t1_val);
    REQUIRE(overlay.value_overlay(5)->last_modified_time() == t2_val);
    REQUIRE(overlay.added_key_indices()[0] == 2);
    REQUIRE(overlay.added_key_indices()[1] == 5);

    // Simulate swap
    hooks.swap(2, 5);

    // Value overlays should be swapped
    REQUIRE(overlay.value_overlay(2)->last_modified_time() == t2_val);
    REQUIRE(overlay.value_overlay(5)->last_modified_time() == t1_val);

    // Buffer indices should be updated
    REQUIRE(overlay.added_key_indices()[0] == 5);  // Was 2, now 5
    REQUIRE(overlay.added_key_indices()[1] == 2);  // Was 5, now 2
}

TEST_CASE("MapTSOverlay hook_on_erase is no-op - use record_key_removed to buffer overlay", "[ts_overlay][map][hooks]") {
    MapTSOverlay overlay(nullptr);
    auto hooks = overlay.make_hooks();

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Set up an entry
    overlay.record_key_added(3, t1);
    REQUIRE(overlay.value_overlay(3) != nullptr);

    // The proper way: record_key_removed moves overlay to buffer, then erase
    overlay.record_key_removed(3, t2, value::PlainValue(std::string("key_3")));
    REQUIRE(overlay.value_overlay(3) == nullptr);  // Moved to removed buffer
    REQUIRE(overlay.removed_key_values().size() == 1);
    REQUIRE(overlay.removed_value_overlays().size() == 1);

    // Now erase is a no-op
    hooks.erase(3);
    REQUIRE(overlay.removed_value_overlays().size() == 1);  // Still there
}

TEST_CASE("MapTSOverlay hook integration simulates insert-erase cycle", "[ts_overlay][map][hooks]") {
    MapTSOverlay overlay(nullptr);
    auto hooks = overlay.make_hooks();

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Simulate inserting 3 entries at indices 0, 1, 2
    hooks.insert(0);
    overlay.record_key_added(0, t1);

    hooks.insert(1);
    overlay.record_key_added(1, t1);

    hooks.insert(2);
    overlay.record_key_added(2, t1);

    REQUIRE(overlay.added_key_indices().size() == 3);
    REQUIRE(overlay.value_overlay(0) != nullptr);
    REQUIRE(overlay.value_overlay(1) != nullptr);
    REQUIRE(overlay.value_overlay(2) != nullptr);

    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Simulate erase of entry at index 1 (swap-with-last, then remove)
    // Record removal first (this also triggers lazy cleanup since t2 != t1)
    overlay.record_key_removed(1, t2, value::PlainValue(std::string("key_1")));
    // The value overlay at index 1 is now in _removed_value_overlays
    REQUIRE(overlay.value_overlay(1) == nullptr);
    REQUIRE(overlay.removed_key_values().size() == 1);
    REQUIRE(overlay.removed_value_overlays().size() == 1);

    // Then swap+erase
    hooks.swap(1, 2);
    hooks.erase(2);

    REQUIRE(overlay.removed_key_indices().size() == 1);
    // The removed index was 1, but after swap it points to old index 2
    REQUIRE(overlay.removed_key_indices()[0] == 2);  // Updated by swap

    // Value overlays: index 1 now has old index 2's overlay (from swap), index 2 is null
    REQUIRE(overlay.value_overlay(1) != nullptr);
    REQUIRE(overlay.value_overlay(2) == nullptr);
}

TEST_CASE("MapTSOverlay child overlay propagates to parent on modification", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Record key added
    overlay.record_key_added(0, t1);
    REQUIRE(overlay.modified_at(t1));

    // Modify value via child overlay
    overlay.value_overlay(0)->mark_modified(t2);

    // Container should be updated via parent propagation
    REQUIRE(overlay.modified_at(t2));
}

TEST_CASE("MapTSOverlay mark_invalid resets container timestamp", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Add some entries
    overlay.record_key_added(0, t1);
    overlay.record_key_added(1, t1);
    overlay.value_overlay(0)->mark_modified(t2);

    REQUIRE(overlay.valid());
    REQUIRE(overlay.last_modified_time() == t2);

    // Mark invalid
    overlay.mark_invalid();

    REQUIRE_FALSE(overlay.valid());
    REQUIRE(overlay.last_modified_time() == MIN_DT);

    // Buffers and child overlays should still be preserved
    REQUIRE(overlay.added_key_indices().size() == 2);
    REQUIRE(overlay.value_overlay(0) != nullptr);
    REQUIRE(overlay.value_overlay(1) != nullptr);
}

TEST_CASE("MapTSOverlay multi-tick tracking with lazy cleanup", "[ts_overlay][map]") {
    MapTSOverlay overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Tick 1: Add keys
    overlay.record_key_added(0, t1);
    overlay.record_key_added(1, t1);
    overlay.record_key_added(2, t1);

    REQUIRE(overlay.added_key_indices().size() == 3);
    REQUIRE_FALSE(overlay.has_removed_keys());
    REQUIRE(overlay.has_delta_at(t1));

    // Tick 2: Add one, remove one, modify one value
    // Lazy cleanup happens automatically on first operation with new time
    overlay.record_key_added(3, t2);  // This clears tick 1's buffers
    overlay.record_key_removed(1, t2, value::PlainValue(std::string("key_1")));
    overlay.value_overlay(0)->mark_modified(t2);

    // Only tick 2's changes in buffers
    REQUIRE(overlay.added_key_indices().size() == 1);
    REQUIRE(overlay.added_key_indices()[0] == 3);
    REQUIRE(overlay.removed_key_indices().size() == 1);
    REQUIRE(overlay.removed_key_indices()[0] == 1);
    REQUIRE(overlay.removed_key_values().size() == 1);
    REQUIRE(overlay.removed_key_values()[0].as<std::string>() == "key_1");

    // Removed value overlay is buffered
    REQUIRE(overlay.value_overlay(1) == nullptr);  // Was moved to removed buffer
    REQUIRE(overlay.removed_value_overlays().size() == 1);

    // Container modified at latest time
    REQUIRE(overlay.last_modified_time() == t2);
    REQUIRE(overlay.has_delta_at(t2));
}

// ============================================================================
// KeySetOverlayView Tests
// ============================================================================

TEST_CASE("KeySetOverlayView provides SetTSOverlay-compatible interface", "[ts_overlay][map][keyset]") {
    MapTSOverlay map_overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Add some keys
    map_overlay.record_key_added(0, t1);
    map_overlay.record_key_added(1, t1);
    map_overlay.record_key_removed(5, t1, value::PlainValue(std::string("removed_key")));

    // Get the key set view
    auto key_view = map_overlay.key_set_view();

    // Verify SetTSOverlay-compatible interface
    REQUIRE(key_view.has_added());
    REQUIRE(key_view.has_removed());
    REQUIRE(key_view.has_delta_at(t1));

    // Verify indices match MapTSOverlay's key indices
    REQUIRE(key_view.added_indices().size() == 2);
    REQUIRE(key_view.added_indices()[0] == 0);
    REQUIRE(key_view.added_indices()[1] == 1);

    REQUIRE(key_view.removed_indices().size() == 1);
    REQUIRE(key_view.removed_indices()[0] == 5);

    // Verify removed values
    REQUIRE(key_view.removed_values().size() == 1);
    REQUIRE(key_view.removed_values()[0].as<std::string>() == "removed_key");

    // Verify underlying map access
    REQUIRE(key_view.map_overlay() == &map_overlay);
}

TEST_CASE("KeySetOverlayView reflects lazy cleanup from MapTSOverlay", "[ts_overlay][map][keyset]") {
    MapTSOverlay map_overlay(nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);

    // Add keys at t1
    map_overlay.record_key_added(0, t1);
    map_overlay.record_key_added(1, t1);

    auto key_view = map_overlay.key_set_view();
    REQUIRE(key_view.added_indices().size() == 2);

    // Query at different time triggers cleanup
    REQUIRE_FALSE(key_view.has_delta_at(t2));

    // Buffers should be cleared
    REQUIRE_FALSE(key_view.has_added());
    REQUIRE_FALSE(key_view.has_removed());
    REQUIRE(key_view.added_indices().empty());
    REQUIRE(key_view.removed_indices().empty());
    REQUIRE(key_view.removed_values().empty());
}

// ============================================================================
// Factory Function Tests (P2.T7)
// ============================================================================

TEST_CASE("make_ts_overlay handles nullptr gracefully", "[ts_overlay][factory]") {
    auto overlay = make_ts_overlay(nullptr);
    REQUIRE(overlay == nullptr);
}

TEST_CASE("make_ts_overlay creates ScalarTSOverlay for TS type", "[ts_overlay][factory]") {
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto ts_meta = std::make_shared<TSValueMeta>(int_schema);

    auto overlay = make_ts_overlay(ts_meta.get());
    REQUIRE(overlay != nullptr);

    // Verify it's a ScalarTSOverlay by checking behavior
    REQUIRE_FALSE(overlay->valid());
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay->mark_modified(t1);
    REQUIRE(overlay->modified_at(t1));

    // ScalarTSOverlay doesn't have child methods, so we can't dynamic_cast in tests
    // But we can verify it behaves like a scalar overlay
}

TEST_CASE("make_ts_overlay creates CompositeTSOverlay for TSB type", "[ts_overlay][factory]") {
    auto& reg = value::TypeRegistry::instance();

    // Create a simple bundle: TSB[field_a: TS[int], field_b: TS[float]]
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    const value::TypeMeta* float_schema = value::scalar_type_meta<double>();

    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    auto float_ts_meta = std::make_shared<TSValueMeta>(float_schema);

    std::vector<TSBFieldInfo> fields;
    fields.push_back({"field_a", 0, int_ts_meta.get()});
    fields.push_back({"field_b", 1, float_ts_meta.get()});

    // Create bundle value schema
    auto bundle_schema = reg.bundle()
        .field("field_a", int_schema)
        .field("field_b", float_schema)
        .build();

    auto bundle_ts_meta = std::make_shared<TSBTypeMeta>(fields, bundle_schema, "TestBundle");

    auto overlay = make_ts_overlay(bundle_ts_meta.get());
    REQUIRE(overlay != nullptr);

    // Cast to CompositeTSOverlay to verify structure
    auto* composite = dynamic_cast<CompositeTSOverlay*>(overlay.get());
    REQUIRE(composite != nullptr);

    // Verify it has the correct number of children
    REQUIRE(composite->child_count() == 2);

    // Verify children exist and have parent pointers
    auto* child_a = composite->child(0);
    auto* child_b = composite->child(1);
    REQUIRE(child_a != nullptr);
    REQUIRE(child_b != nullptr);
    REQUIRE(child_a->parent() == composite);
    REQUIRE(child_b->parent() == composite);

    // Verify we can access by name
    auto* named_a = composite->child("field_a");
    auto* named_b = composite->child("field_b");
    REQUIRE(named_a == child_a);
    REQUIRE(named_b == child_b);
}

TEST_CASE("make_ts_overlay creates ListTSOverlay for TSL type", "[ts_overlay][factory]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);

    auto overlay = make_ts_overlay(&list_ts_meta);
    REQUIRE(overlay != nullptr);

    // Cast to ListTSOverlay to verify structure
    auto* list_overlay = dynamic_cast<ListTSOverlay*>(overlay.get());
    REQUIRE(list_overlay != nullptr);

    // Verify it starts empty
    REQUIRE(list_overlay->child_count() == 0);

    // Verify we can add children
    auto* child = list_overlay->push_back();
    REQUIRE(child != nullptr);
    REQUIRE(list_overlay->child_count() == 1);
    REQUIRE(child->parent() == list_overlay);
}

TEST_CASE("make_ts_overlay creates SetTSOverlay for TSS type", "[ts_overlay][factory]") {
    // Create a minimal TSSTypeMeta
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* set_schema = reg.set(int_schema).build();

    TSSTypeMeta set_ts_meta(int_schema, set_schema);

    auto overlay = make_ts_overlay(&set_ts_meta);
    REQUIRE(overlay != nullptr);

    // Cast to SetTSOverlay to verify structure
    auto* set_overlay = dynamic_cast<SetTSOverlay*>(overlay.get());
    REQUIRE(set_overlay != nullptr);

    // Verify it starts empty
    REQUIRE_FALSE(set_overlay->has_added());
    REQUIRE_FALSE(set_overlay->has_removed());
    REQUIRE_FALSE(set_overlay->valid());

    // Verify we can record added elements
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    set_overlay->record_added(0, t1);
    REQUIRE(set_overlay->added_indices().size() == 1);
    REQUIRE(set_overlay->added_indices()[0] == 0);
    REQUIRE(set_overlay->valid());
}

TEST_CASE("make_ts_overlay creates MapTSOverlay for TSD type", "[ts_overlay][factory]") {
    // Create a minimal TSDTypeMeta
    const value::TypeMeta* str_schema = value::scalar_type_meta<std::string>();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);

    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* dict_schema = reg.map(str_schema, int_schema).build();

    TSDTypeMeta dict_ts_meta(str_schema, int_ts_meta.get(), dict_schema);

    auto overlay = make_ts_overlay(&dict_ts_meta);
    REQUIRE(overlay != nullptr);

    // Cast to MapTSOverlay to verify structure
    auto* map_overlay = dynamic_cast<MapTSOverlay*>(overlay.get());
    REQUIRE(map_overlay != nullptr);

    // Verify it starts empty
    REQUIRE(map_overlay->entry_count() == 0);
    REQUIRE_FALSE(map_overlay->has_added_keys());
    REQUIRE_FALSE(map_overlay->valid());

    // Verify we can record keys and get value overlays
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    map_overlay->record_key_added(0, t1);
    REQUIRE(map_overlay->added_key_indices().size() == 1);
    REQUIRE(map_overlay->added_key_indices()[0] == 0);
    REQUIRE(map_overlay->value_overlay(0) != nullptr);
    REQUIRE(map_overlay->valid());
}

TEST_CASE("make_ts_overlay creates ScalarTSOverlay for REF type", "[ts_overlay][factory]") {
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);

    REFTypeMeta ref_ts_meta(int_ts_meta.get());

    auto overlay = make_ts_overlay(&ref_ts_meta);
    REQUIRE(overlay != nullptr);

    // REF types use ScalarTSOverlay
    // Verify it behaves like a scalar
    REQUIRE_FALSE(overlay->valid());
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay->mark_modified(t1);
    REQUIRE(overlay->modified_at(t1));
}

TEST_CASE("make_ts_overlay creates ScalarTSOverlay for SIGNAL type", "[ts_overlay][factory]") {
    SignalTypeMeta signal_ts_meta;

    auto overlay = make_ts_overlay(&signal_ts_meta);
    REQUIRE(overlay != nullptr);

    // SIGNAL types use ScalarTSOverlay
    // Verify it behaves like a scalar
    REQUIRE_FALSE(overlay->valid());
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay->mark_modified(t1);
    REQUIRE(overlay->modified_at(t1));
}

TEST_CASE("make_ts_overlay creates ListTSOverlay for TSW type", "[ts_overlay][factory]") {
    // Create a size-based window: TSW[int, 10, 5]
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* window_schema = reg.list(int_schema).build();

    TSWTypeMeta window_ts_meta(int_schema, 10, 5, window_schema);

    auto overlay = make_ts_overlay(&window_ts_meta);
    REQUIRE(overlay != nullptr);

    // TSW types use ListTSOverlay for cyclic buffer behavior
    auto* list_overlay = dynamic_cast<ListTSOverlay*>(overlay.get());
    REQUIRE(list_overlay != nullptr);

    // Verify it starts empty
    REQUIRE(list_overlay->child_count() == 0);

    // Verify we can add children
    auto* child = list_overlay->push_back();
    REQUIRE(child != nullptr);
    REQUIRE(list_overlay->child_count() == 1);
}

TEST_CASE("make_ts_overlay creates nested structures recursively", "[ts_overlay][factory]") {
    auto& reg = value::TypeRegistry::instance();

    // Create TSB[field_a: TSL[TS[int], 0], field_b: TS[float]]
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    const value::TypeMeta* float_schema = value::scalar_type_meta<double>();

    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    auto float_ts_meta = std::make_shared<TSValueMeta>(float_schema);

    const value::TypeMeta* list_schema = reg.list(int_schema).build();
    auto list_ts_meta = std::make_shared<TSLTypeMeta>(int_ts_meta.get(), 0, list_schema);

    std::vector<TSBFieldInfo> fields;
    fields.push_back({"field_a", 0, list_ts_meta.get()});
    fields.push_back({"field_b", 1, float_ts_meta.get()});

    // Create bundle value schema
    auto bundle_schema = reg.bundle()
        .field("field_a", list_schema)
        .field("field_b", float_schema)
        .build();

    auto bundle_ts_meta = std::make_shared<TSBTypeMeta>(fields, bundle_schema, "NestedBundle");

    auto overlay = make_ts_overlay(bundle_ts_meta.get());
    REQUIRE(overlay != nullptr);

    // Cast to CompositeTSOverlay
    auto* composite = dynamic_cast<CompositeTSOverlay*>(overlay.get());
    REQUIRE(composite != nullptr);

    // Verify it has the correct number of children
    REQUIRE(composite->child_count() == 2);

    // Verify field_a is a ListTSOverlay
    auto* field_a = composite->child("field_a");
    REQUIRE(field_a != nullptr);
    auto* list_overlay = dynamic_cast<ListTSOverlay*>(field_a);
    REQUIRE(list_overlay != nullptr);
    REQUIRE(list_overlay->child_count() == 0);  // Starts empty

    // Verify field_b is a ScalarTSOverlay
    auto* field_b = composite->child("field_b");
    REQUIRE(field_b != nullptr);
    // We can't dynamic_cast to ScalarTSOverlay in tests, but we verified it exists

    // Verify parent pointers are set correctly
    REQUIRE(field_a->parent() == composite);
    REQUIRE(field_b->parent() == composite);

    // Add an element to the list and verify parent propagation
    auto* list_child = list_overlay->push_back();
    REQUIRE(list_child != nullptr);
    REQUIRE(list_child->parent() == list_overlay);

    // Modify the list child and verify propagation to the bundle
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    list_child->mark_modified(t1);

    REQUIRE(list_child->modified_at(t1));
    REQUIRE(list_overlay->modified_at(t1));  // List should be modified
    REQUIRE(composite->modified_at(t1));     // Bundle should be modified
}

}  // namespace hgraph::test
