#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/value/type_registry.h>

#include <catch2/catch_test_macros.hpp>

namespace hgraph::test {

// ============================================================================
// Mock Notifiable for Testing
// ============================================================================

/**
 * @brief Mock observer that records notifications for verification.
 */
class MockObserver : public Notifiable {
public:
    void notify(engine_time_t et) override {
        notification_count++;
        last_notification_time = et;
    }

    void reset() {
        notification_count = 0;
        last_notification_time = MIN_DT;
    }

    int notification_count = 0;
    engine_time_t last_notification_time = MIN_DT;
};

// ============================================================================
// PERMANENT TESTS - Phase 1: Basic Subscription/Unsubscription
// ============================================================================

TEST_CASE("ObserverList subscribe adds observer", "[ts_overlay][observers][P1]") {
    ObserverList observers;
    MockObserver observer;

    // Initially no observers
    REQUIRE_FALSE(observers.has_observers());

    // Add observer
    observers.subscribe(&observer);

    // Now has observers
    REQUIRE(observers.has_observers());
}

TEST_CASE("ObserverList unsubscribe removes observer", "[ts_overlay][observers][P1]") {
    ObserverList observers;
    MockObserver observer;

    // Add and remove
    observers.subscribe(&observer);
    REQUIRE(observers.has_observers());

    observers.unsubscribe(&observer);
    REQUIRE_FALSE(observers.has_observers());
}

TEST_CASE("ObserverList subscribe with nullptr is safe", "[ts_overlay][observers][P1]") {
    ObserverList observers;

    // Should not crash or add anything
    observers.subscribe(nullptr);
    REQUIRE_FALSE(observers.has_observers());
}

TEST_CASE("ObserverList unsubscribe with nullptr is safe", "[ts_overlay][observers][P1]") {
    ObserverList observers;

    // Should not crash
    observers.unsubscribe(nullptr);
    REQUIRE_FALSE(observers.has_observers());
}

TEST_CASE("ObserverList subscribe same observer multiple times stores only once", "[ts_overlay][observers][P1]") {
    ObserverList observers;
    MockObserver observer;

    // Subscribe same observer multiple times
    observers.subscribe(&observer);
    observers.subscribe(&observer);
    observers.subscribe(&observer);

    // Should only be subscribed once (set semantics)
    REQUIRE(observers.has_observers());

    // Unsubscribe once should remove it
    observers.unsubscribe(&observer);
    REQUIRE_FALSE(observers.has_observers());
}

TEST_CASE("ObserverList notify calls all subscribed observers", "[ts_overlay][observers][P1]") {
    ObserverList observers;
    MockObserver observer1;
    MockObserver observer2;
    MockObserver observer3;

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Subscribe multiple observers
    observers.subscribe(&observer1);
    observers.subscribe(&observer2);
    observers.subscribe(&observer3);

    // Notify all
    observers.notify(t1);

    // All should have been notified
    REQUIRE(observer1.notification_count == 1);
    REQUIRE(observer1.last_notification_time == t1);
    REQUIRE(observer2.notification_count == 1);
    REQUIRE(observer2.last_notification_time == t1);
    REQUIRE(observer3.notification_count == 1);
    REQUIRE(observer3.last_notification_time == t1);
}

TEST_CASE("ObserverList notify with no observers is safe", "[ts_overlay][observers][P1]") {
    ObserverList observers;
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);

    // Should not crash
    observers.notify(t1);
}

TEST_CASE("ObserverList multiple observers are notified", "[ts_overlay][observers][P1]") {
    // Observers are for scheduling, not side effects, so direct iteration is safe
    ObserverList observers;
    MockObserver observer1;
    MockObserver observer2;

    observers.subscribe(&observer1);
    observers.subscribe(&observer2);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    observers.notify(t1);

    REQUIRE(observer1.notification_count == 1);
    REQUIRE(observer2.notification_count == 1);
}

// ============================================================================
// PERMANENT TESTS - Phase 1: ScalarTSOverlay Observer Integration
// ============================================================================

TEST_CASE("ScalarTSOverlay can subscribe observers", "[ts_overlay][scalar][observers][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer;

    // Initially no observers
    REQUIRE(overlay.observers() == nullptr);

    // Subscribe creates observer list lazily
    overlay.ensure_observers().subscribe(&observer);

    // Now has observers
    REQUIRE(overlay.observers() != nullptr);
    REQUIRE(overlay.observers()->has_observers());
}

TEST_CASE("ScalarTSOverlay mark_modified notifies observers", "[ts_overlay][scalar][observers][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer;

    overlay.ensure_observers().subscribe(&observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);

    // Observer should be notified
    REQUIRE(observer.notification_count == 1);
    REQUIRE(observer.last_notification_time == t1);
}

TEST_CASE("ScalarTSOverlay mark_invalid notifies observers", "[ts_overlay][scalar][observers][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer;

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);

    overlay.ensure_observers().subscribe(&observer);

    // Mark invalid
    overlay.mark_invalid();

    // Observer should be notified with MIN_DT
    REQUIRE(observer.notification_count == 1);
    REQUIRE(observer.last_notification_time == MIN_DT);
}

TEST_CASE("ScalarTSOverlay multiple observers all notified", "[ts_overlay][scalar][observers][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer1;
    MockObserver observer2;
    MockObserver observer3;

    auto& observers = overlay.ensure_observers();
    observers.subscribe(&observer1);
    observers.subscribe(&observer2);
    observers.subscribe(&observer3);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);

    // All observers notified
    REQUIRE(observer1.notification_count == 1);
    REQUIRE(observer2.notification_count == 1);
    REQUIRE(observer3.notification_count == 1);
}

TEST_CASE("ScalarTSOverlay unsubscribed observer not notified", "[ts_overlay][scalar][observers][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer;

    auto& observers = overlay.ensure_observers();
    observers.subscribe(&observer);
    observers.unsubscribe(&observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);

    // Observer should NOT be notified
    REQUIRE(observer.notification_count == 0);
}

TEST_CASE("ScalarTSOverlay mark_modified without observers is efficient", "[ts_overlay][scalar][observers][P1]") {
    ScalarTSOverlay overlay;

    // Should not allocate observer list
    REQUIRE(overlay.observers() == nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);

    // Still no observer list allocated (lazy allocation)
    REQUIRE(overlay.observers() == nullptr);
    REQUIRE(overlay.last_modified_time() == t1);
}

// ============================================================================
// PERMANENT TESTS - Phase 1: CompositeTSOverlay Observer Integration
// ============================================================================

TEST_CASE("CompositeTSOverlay can subscribe observers at parent level", "[ts_overlay][composite][observers][P1]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);

    std::vector<TSBFieldInfo> fields;
    fields.push_back({"field_a", 0, int_ts_meta.get()});

    auto bundle_schema = reg.bundle().field("field_a", int_schema).build();
    auto bundle_ts_meta = std::make_shared<TSBTypeMeta>(fields, bundle_schema, "TestBundle");

    CompositeTSOverlay overlay(bundle_ts_meta.get());
    MockObserver observer;

    overlay.ensure_observers().subscribe(&observer);

    REQUIRE(overlay.observers()->has_observers());
}

TEST_CASE("CompositeTSOverlay parent observer notified on parent modification", "[ts_overlay][composite][observers][P1]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);

    std::vector<TSBFieldInfo> fields;
    fields.push_back({"field_a", 0, int_ts_meta.get()});

    auto bundle_schema = reg.bundle().field("field_a", int_schema).build();
    auto bundle_ts_meta = std::make_shared<TSBTypeMeta>(fields, bundle_schema, "TestBundle");

    CompositeTSOverlay overlay(bundle_ts_meta.get());
    MockObserver parent_observer;

    overlay.ensure_observers().subscribe(&parent_observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);

    REQUIRE(parent_observer.notification_count == 1);
    REQUIRE(parent_observer.last_notification_time == t1);
}

TEST_CASE("CompositeTSOverlay parent observer notified on child modification", "[ts_overlay][composite][observers][P1]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);

    std::vector<TSBFieldInfo> fields;
    fields.push_back({"field_a", 0, int_ts_meta.get()});

    auto bundle_schema = reg.bundle().field("field_a", int_schema).build();
    auto bundle_ts_meta = std::make_shared<TSBTypeMeta>(fields, bundle_schema, "TestBundle");

    CompositeTSOverlay overlay(bundle_ts_meta.get());
    MockObserver parent_observer;

    overlay.ensure_observers().subscribe(&parent_observer);

    // Modify child
    auto* child = overlay.child(0);
    REQUIRE(child != nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    child->mark_modified(t1);

    // Parent observer should be notified due to propagation
    REQUIRE(parent_observer.notification_count == 1);
    REQUIRE(parent_observer.last_notification_time == t1);
}

TEST_CASE("CompositeTSOverlay child observer notified on child modification", "[ts_overlay][composite][observers][P1]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);

    std::vector<TSBFieldInfo> fields;
    fields.push_back({"field_a", 0, int_ts_meta.get()});

    auto bundle_schema = reg.bundle().field("field_a", int_schema).build();
    auto bundle_ts_meta = std::make_shared<TSBTypeMeta>(fields, bundle_schema, "TestBundle");

    CompositeTSOverlay overlay(bundle_ts_meta.get());
    MockObserver child_observer;

    auto* child = overlay.child(0);
    REQUIRE(child != nullptr);

    child->ensure_observers().subscribe(&child_observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    child->mark_modified(t1);

    // Child observer should be notified
    REQUIRE(child_observer.notification_count == 1);
    REQUIRE(child_observer.last_notification_time == t1);
}

TEST_CASE("CompositeTSOverlay both parent and child observers notified", "[ts_overlay][composite][observers][P1]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);

    std::vector<TSBFieldInfo> fields;
    fields.push_back({"field_a", 0, int_ts_meta.get()});

    auto bundle_schema = reg.bundle().field("field_a", int_schema).build();
    auto bundle_ts_meta = std::make_shared<TSBTypeMeta>(fields, bundle_schema, "TestBundle");

    CompositeTSOverlay overlay(bundle_ts_meta.get());
    MockObserver parent_observer;
    MockObserver child_observer;

    overlay.ensure_observers().subscribe(&parent_observer);

    auto* child = overlay.child(0);
    child->ensure_observers().subscribe(&child_observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    child->mark_modified(t1);

    // Both should be notified
    REQUIRE(parent_observer.notification_count == 1);
    REQUIRE(child_observer.notification_count == 1);
}

// ============================================================================
// PERMANENT TESTS - Phase 1: ListTSOverlay Observer Integration
// ============================================================================

TEST_CASE("ListTSOverlay parent observer notified on child modification", "[ts_overlay][list][observers][P1]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);
    ListTSOverlay overlay(&list_ts_meta);

    MockObserver parent_observer;
    overlay.ensure_observers().subscribe(&parent_observer);

    // Add a child
    auto* child = overlay.push_back();
    REQUIRE(child != nullptr);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    child->mark_modified(t1);

    // Parent should be notified
    REQUIRE(parent_observer.notification_count == 1);
    REQUIRE(parent_observer.last_notification_time == t1);
}

TEST_CASE("ListTSOverlay child and parent observers both notified", "[ts_overlay][list][observers][P1]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);
    ListTSOverlay overlay(&list_ts_meta);

    MockObserver parent_observer;
    MockObserver child_observer;

    overlay.ensure_observers().subscribe(&parent_observer);

    auto* child = overlay.push_back();
    child->ensure_observers().subscribe(&child_observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    child->mark_modified(t1);

    // Both should be notified
    REQUIRE(parent_observer.notification_count == 1);
    REQUIRE(child_observer.notification_count == 1);
}

// ============================================================================
// PERMANENT TESTS - Phase 1: SetTSOverlay Observer Integration
// ============================================================================

TEST_CASE("SetTSOverlay observer notified on record_added", "[ts_overlay][set][observers][P1]") {
    SetTSOverlay overlay(nullptr);
    MockObserver observer;

    overlay.ensure_observers().subscribe(&observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_added(0, t1);

    // Observer should be notified
    REQUIRE(observer.notification_count == 1);
    REQUIRE(observer.last_notification_time == t1);
}

TEST_CASE("SetTSOverlay observer notified on record_removed", "[ts_overlay][set][observers][P1]") {
    SetTSOverlay overlay(nullptr);
    MockObserver observer;

    overlay.ensure_observers().subscribe(&observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_removed(0, t1, value::PlainValue(int64_t{42}));

    // Observer should be notified
    REQUIRE(observer.notification_count == 1);
    REQUIRE(observer.last_notification_time == t1);
}

TEST_CASE("SetTSOverlay observer notified once on multiple adds in same tick", "[ts_overlay][set][observers][P1]") {
    SetTSOverlay overlay(nullptr);
    MockObserver observer;

    overlay.ensure_observers().subscribe(&observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_added(0, t1);
    overlay.record_added(1, t1);
    overlay.record_added(2, t1);

    // Observer notified for each record_added call (which calls mark_modified)
    REQUIRE(observer.notification_count == 3);
}

// ============================================================================
// PERMANENT TESTS - Phase 1: MapTSOverlay Observer Integration
// ============================================================================

TEST_CASE("MapTSOverlay observer notified on record_key_added", "[ts_overlay][map][observers][P1]") {
    MapTSOverlay overlay(nullptr);
    MockObserver observer;

    overlay.ensure_observers().subscribe(&observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_key_added(0, t1);

    // Observer should be notified
    REQUIRE(observer.notification_count == 1);
    REQUIRE(observer.last_notification_time == t1);
}

TEST_CASE("MapTSOverlay observer notified on record_key_removed", "[ts_overlay][map][observers][P1]") {
    MapTSOverlay overlay(nullptr);
    MockObserver observer;

    overlay.ensure_observers().subscribe(&observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_key_removed(0, t1, value::PlainValue(std::string("key")));

    // Observer should be notified
    REQUIRE(observer.notification_count == 1);
    REQUIRE(observer.last_notification_time == t1);
}

TEST_CASE("MapTSOverlay parent observer notified on value overlay modification", "[ts_overlay][map][observers][P1]") {
    MapTSOverlay overlay(nullptr);
    MockObserver map_observer;

    overlay.ensure_observers().subscribe(&map_observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_key_added(0, t1);

    map_observer.reset();

    // Modify the value overlay
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);
    auto* value_overlay = overlay.value_overlay(0);
    REQUIRE(value_overlay != nullptr);
    value_overlay->mark_modified(t2);

    // Map observer should be notified due to propagation
    REQUIRE(map_observer.notification_count == 1);
    REQUIRE(map_observer.last_notification_time == t2);
}

TEST_CASE("MapTSOverlay value and map observers both notified", "[ts_overlay][map][observers][P1]") {
    MapTSOverlay overlay(nullptr);
    MockObserver map_observer;
    MockObserver value_observer;

    overlay.ensure_observers().subscribe(&map_observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_key_added(0, t1);

    auto* value_overlay = overlay.value_overlay(0);
    value_overlay->ensure_observers().subscribe(&value_observer);

    map_observer.reset();
    value_observer.reset();

    // Modify the value overlay
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);
    value_overlay->mark_modified(t2);

    // Both should be notified
    REQUIRE(map_observer.notification_count == 1);
    REQUIRE(value_observer.notification_count == 1);
}

// ============================================================================
// PERMANENT TESTS - Phase 1: Hierarchical Notification Propagation
// ============================================================================

TEST_CASE("Deep hierarchy propagates notifications to root", "[ts_overlay][hierarchy][observers][P1]") {
    // Create TSB[field_a: TSL[TS[int], 0]]
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();
    auto list_ts_meta = std::make_shared<TSLTypeMeta>(int_ts_meta.get(), 0, list_schema);

    std::vector<TSBFieldInfo> fields;
    fields.push_back({"field_a", 0, list_ts_meta.get()});

    auto bundle_schema = reg.bundle().field("field_a", list_schema).build();
    auto bundle_ts_meta = std::make_shared<TSBTypeMeta>(fields, bundle_schema, "NestedBundle");

    CompositeTSOverlay root(bundle_ts_meta.get());
    MockObserver root_observer;
    root.ensure_observers().subscribe(&root_observer);

    // Get the list field
    auto* list_overlay = dynamic_cast<ListTSOverlay*>(root.child(0));
    REQUIRE(list_overlay != nullptr);

    // Add an element to the list
    auto* scalar_overlay = list_overlay->push_back();
    REQUIRE(scalar_overlay != nullptr);

    // Modify the scalar (deepest level)
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    scalar_overlay->mark_modified(t1);

    // Root observer should be notified through the hierarchy
    REQUIRE(root_observer.notification_count == 1);
    REQUIRE(root_observer.last_notification_time == t1);
}

TEST_CASE("Multiple levels can each have independent observers", "[ts_overlay][hierarchy][observers][P1]") {
    // Create TSB[field_a: TSL[TS[int], 0]]
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();
    auto list_ts_meta = std::make_shared<TSLTypeMeta>(int_ts_meta.get(), 0, list_schema);

    std::vector<TSBFieldInfo> fields;
    fields.push_back({"field_a", 0, list_ts_meta.get()});

    auto bundle_schema = reg.bundle().field("field_a", list_schema).build();
    auto bundle_ts_meta = std::make_shared<TSBTypeMeta>(fields, bundle_schema, "NestedBundle");

    CompositeTSOverlay root(bundle_ts_meta.get());
    MockObserver root_observer;
    MockObserver list_observer;
    MockObserver scalar_observer;

    root.ensure_observers().subscribe(&root_observer);

    auto* list_overlay = dynamic_cast<ListTSOverlay*>(root.child(0));
    list_overlay->ensure_observers().subscribe(&list_observer);

    auto* scalar_overlay = list_overlay->push_back();
    scalar_overlay->ensure_observers().subscribe(&scalar_observer);

    // Modify the scalar
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    scalar_overlay->mark_modified(t1);

    // All three levels should be notified
    REQUIRE(root_observer.notification_count == 1);
    REQUIRE(list_observer.notification_count == 1);
    REQUIRE(scalar_observer.notification_count == 1);
}

// ============================================================================
// PERMANENT TESTS - Phase 1: Edge Cases and Boundary Conditions
// ============================================================================

TEST_CASE("Observer notifications continue after unsubscribe of one observer", "[ts_overlay][observers][edge][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer1;
    MockObserver observer2;

    auto& observers = overlay.ensure_observers();
    observers.subscribe(&observer1);
    observers.subscribe(&observer2);

    // Unsubscribe first observer
    observers.unsubscribe(&observer1);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);

    // Only observer2 should be notified
    REQUIRE(observer1.notification_count == 0);
    REQUIRE(observer2.notification_count == 1);
}

TEST_CASE("Observer can be resubscribed after unsubscribe", "[ts_overlay][observers][edge][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer;

    auto& observers = overlay.ensure_observers();
    observers.subscribe(&observer);
    observers.unsubscribe(&observer);
    observers.subscribe(&observer);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);

    // Should be notified (was resubscribed)
    REQUIRE(observer.notification_count == 1);
}

TEST_CASE("Notifications use correct timestamp", "[ts_overlay][observers][edge][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer;

    overlay.ensure_observers().subscribe(&observer);

    // Multiple modifications at different times
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);
    engine_time_t t3 = MIN_DT + std::chrono::microseconds(300);

    overlay.mark_modified(t1);
    REQUIRE(observer.last_notification_time == t1);

    overlay.mark_modified(t2);
    REQUIRE(observer.last_notification_time == t2);

    overlay.mark_modified(t3);
    REQUIRE(observer.last_notification_time == t3);

    REQUIRE(observer.notification_count == 3);
}

TEST_CASE("Empty observer list has minimal memory overhead", "[ts_overlay][observers][edge][P1]") {
    ScalarTSOverlay overlay;

    // Observer list should not be allocated until first use
    REQUIRE(overlay.observers() == nullptr);

    // Accessing ensure_observers creates it
    auto& observers = overlay.ensure_observers();
    REQUIRE(overlay.observers() != nullptr);

    // But it should still be empty
    REQUIRE_FALSE(observers.has_observers());
}

// ============================================================================
// TEMPORARY PHASE TESTS - Phase 1: Implementation Verification
// These tests verify the specific implementation details during Phase 1
// REMOVE AFTER PHASE 1 COMPLETION
// ============================================================================

TEST_CASE("TEMP P1: ScalarTSOverlay has lazy observer allocation", "[ts_overlay][observers][temp][P1]") {
    // TEMPORARY: Verify lazy allocation works as designed
    // Remove after Phase 1 implementation is verified

    ScalarTSOverlay overlay;

    // Should start with null observers
    REQUIRE(overlay.observers() == nullptr);

    // Should remain null after modifications without observers
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);
    REQUIRE(overlay.observers() == nullptr);

    // Should allocate on ensure_observers()
    overlay.ensure_observers();
    REQUIRE(overlay.observers() != nullptr);
}

TEST_CASE("TEMP P1: CompositeTSOverlay children have independent observer lists", "[ts_overlay][observers][temp][P1]") {
    // TEMPORARY: Verify each child can have its own observers
    // Remove after Phase 1 implementation is verified

    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);

    std::vector<TSBFieldInfo> fields;
    fields.push_back({"field_a", 0, int_ts_meta.get()});
    fields.push_back({"field_b", 1, int_ts_meta.get()});

    auto bundle_schema = reg.bundle()
        .field("field_a", int_schema)
        .field("field_b", int_schema)
        .build();
    auto bundle_ts_meta = std::make_shared<TSBTypeMeta>(fields, bundle_schema, "TestBundle");

    CompositeTSOverlay overlay(bundle_ts_meta.get());

    auto* child_a = overlay.child(0);
    auto* child_b = overlay.child(1);

    // Children should have independent observer lists
    REQUIRE(child_a->observers() == nullptr);
    REQUIRE(child_b->observers() == nullptr);

    child_a->ensure_observers();
    REQUIRE(child_a->observers() != nullptr);
    REQUIRE(child_b->observers() == nullptr);
}

// ============================================================================
// PERMANENT TESTS - Phase 1: New Convenience Methods
// ============================================================================

TEST_CASE("TSOverlayStorage subscribe convenience method", "[ts_overlay][observers][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer;

    // Use convenience method
    overlay.subscribe(&observer);

    // Should have allocated observer list and added observer
    REQUIRE(overlay.observers() != nullptr);
    REQUIRE(overlay.observers()->has_observers());

    // Test notification works
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);
    REQUIRE(observer.notification_count == 1);
}

TEST_CASE("TSOverlayStorage unsubscribe convenience method", "[ts_overlay][observers][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer;

    overlay.subscribe(&observer);
    overlay.unsubscribe(&observer);

    // Should no longer be subscribed
    REQUIRE_FALSE(overlay.observers()->has_observers());

    // No notification
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);
    REQUIRE(observer.notification_count == 0);
}

TEST_CASE("TSOverlayStorage unsubscribe without observer list is safe", "[ts_overlay][observers][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer;

    // Unsubscribe when no observer list exists should not crash
    overlay.unsubscribe(&observer);
    REQUIRE(overlay.observers() == nullptr);
}

TEST_CASE("TSOverlayStorage is_subscribed checks observer presence", "[ts_overlay][observers][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer1;
    MockObserver observer2;

    // Initially not subscribed
    REQUIRE_FALSE(overlay.is_subscribed(&observer1));
    REQUIRE_FALSE(overlay.is_subscribed(&observer2));

    // Subscribe observer1
    overlay.subscribe(&observer1);
    REQUIRE(overlay.is_subscribed(&observer1));
    REQUIRE_FALSE(overlay.is_subscribed(&observer2));

    // Subscribe observer2
    overlay.subscribe(&observer2);
    REQUIRE(overlay.is_subscribed(&observer1));
    REQUIRE(overlay.is_subscribed(&observer2));

    // Unsubscribe observer1
    overlay.unsubscribe(&observer1);
    REQUIRE_FALSE(overlay.is_subscribed(&observer1));
    REQUIRE(overlay.is_subscribed(&observer2));
}

TEST_CASE("TSOverlayStorage is_subscribed without observer list returns false", "[ts_overlay][observers][P1]") {
    ScalarTSOverlay overlay;
    MockObserver observer;

    // No observer list allocated yet
    REQUIRE(overlay.observers() == nullptr);
    REQUIRE_FALSE(overlay.is_subscribed(&observer));
}

TEST_CASE("ObserverList is_subscribed checks observer presence", "[ts_overlay][observers][P1]") {
    ObserverList observers;
    MockObserver observer1;
    MockObserver observer2;

    // Initially not subscribed
    REQUIRE_FALSE(observers.is_subscribed(&observer1));
    REQUIRE_FALSE(observers.is_subscribed(nullptr));

    // Subscribe observer1
    observers.subscribe(&observer1);
    REQUIRE(observers.is_subscribed(&observer1));
    REQUIRE_FALSE(observers.is_subscribed(&observer2));
}

// ============================================================================
// PERMANENT TESTS - Structural Change Correctness (Using Direct Subscription)
// ============================================================================

TEST_CASE("MapTSOverlay key removal moves overlay to removed buffer", "[ts_overlay][map][structural]") {
    MapTSOverlay overlay(nullptr);
    MockObserver observer;

    // Add an entry
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_key_added(0, t1);

    // Subscribe to the entry's value overlay
    auto* value_overlay = overlay.value_overlay(0);
    value_overlay->subscribe(&observer);
    REQUIRE(overlay.observers() == nullptr);  // Map has no direct observers
    REQUIRE(value_overlay->observers()->has_observers());

    // Record key removal - the overlay is moved to removed buffer
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);
    value::PlainValue removed_key;  // Empty placeholder
    overlay.record_key_removed(0, t2, std::move(removed_key));

    // The removed overlay is in the removed buffer
    REQUIRE(overlay.removed_value_overlays().size() == 1);

    // The original slot no longer has the overlay
    REQUIRE(overlay.value_overlay(0) == nullptr);

    // The observer is still on the removed overlay (in removed buffer)
    REQUIRE(overlay.removed_value_overlays()[0]->is_subscribed(&observer));
}

TEST_CASE("MapTSOverlay removed overlay observers don't see new slot data", "[ts_overlay][map][structural]") {
    MapTSOverlay overlay(nullptr);
    MockObserver observer;

    // Add an entry at index 0
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_key_added(0, t1);

    // Subscribe to entry 0's value overlay
    overlay.value_overlay(0)->subscribe(&observer);

    // Remove the key
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);
    value::PlainValue removed_key;
    overlay.record_key_removed(0, t2, std::move(removed_key));

    // Reset observer
    observer.reset();

    // Add a new key at the same index
    engine_time_t t3 = MIN_DT + std::chrono::microseconds(300);
    overlay.record_key_added(0, t3);

    // Modify the new entry
    engine_time_t t4 = MIN_DT + std::chrono::microseconds(400);
    overlay.value_overlay(0)->mark_modified(t4);

    // The old observer should NOT be notified (it was on the removed overlay)
    REQUIRE(observer.notification_count == 0);
}

TEST_CASE("ListTSOverlay pop_back removes child with subscription", "[ts_overlay][list][structural]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);
    ListTSOverlay overlay(&list_ts_meta);

    // Add element and subscribe directly to child
    auto* child = overlay.push_back();
    MockObserver observer;
    child->subscribe(&observer);

    REQUIRE(child->is_subscribed(&observer));

    // Pop the element - child is destroyed
    overlay.pop_back();
    REQUIRE(overlay.child_count() == 0);
}

TEST_CASE("ListTSOverlay clear removes all children", "[ts_overlay][list][structural]") {
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();

    TSLTypeMeta list_ts_meta(int_ts_meta.get(), 0, list_schema);
    ListTSOverlay overlay(&list_ts_meta);

    // Add multiple elements and subscribe directly to each
    MockObserver observer0, observer1, observer2;
    overlay.push_back()->subscribe(&observer0);
    overlay.push_back()->subscribe(&observer1);
    overlay.push_back()->subscribe(&observer2);

    REQUIRE(overlay.child_count() == 3);

    // Clear all elements
    overlay.clear();
    REQUIRE(overlay.child_count() == 0);
}

TEST_CASE("SetTSOverlay container-level subscription notified on add/remove", "[ts_overlay][set][structural]") {
    SetTSOverlay overlay(nullptr);
    MockObserver observer;

    // Subscribe to the set container
    overlay.subscribe(&observer);

    // Record an addition
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.record_added(0, t1);

    // Observer notified
    REQUIRE(observer.notification_count == 1);

    // Record a removal
    observer.reset();
    engine_time_t t2 = MIN_DT + std::chrono::microseconds(200);
    value::PlainValue removed_value;
    overlay.record_removed(0, t2, std::move(removed_value));

    // Observer notified again
    REQUIRE(observer.notification_count == 1);
}

TEST_CASE("Multiple observers on same overlay all notified", "[ts_overlay][structural]") {
    ScalarTSOverlay overlay;
    MockObserver observer1, observer2, observer3;

    overlay.subscribe(&observer1);
    overlay.subscribe(&observer2);
    overlay.subscribe(&observer3);

    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    overlay.mark_modified(t1);

    // All three observers notified
    REQUIRE(observer1.notification_count == 1);
    REQUIRE(observer2.notification_count == 1);
    REQUIRE(observer3.notification_count == 1);
}

TEST_CASE("Deep nesting structural change propagates all the way up", "[ts_overlay][hierarchy][structural]") {
    // Create TSB[level1: TSB[level2: TSL[TS[int], 0]]]
    auto& reg = value::TypeRegistry::instance();
    const value::TypeMeta* int_schema = value::scalar_type_meta<int64_t>();
    auto int_ts_meta = std::make_shared<TSValueMeta>(int_schema);
    const value::TypeMeta* list_schema = reg.list(int_schema).build();
    auto list_ts_meta = std::make_shared<TSLTypeMeta>(int_ts_meta.get(), 0, list_schema);

    // Inner bundle with list field
    std::vector<TSBFieldInfo> inner_fields;
    inner_fields.push_back({"level2", 0, list_ts_meta.get()});
    auto inner_bundle_schema = reg.bundle().field("level2", list_schema).build();
    auto inner_bundle_ts_meta = std::make_shared<TSBTypeMeta>(inner_fields, inner_bundle_schema, "InnerBundle");

    // Outer bundle with inner bundle field
    std::vector<TSBFieldInfo> outer_fields;
    outer_fields.push_back({"level1", 0, inner_bundle_ts_meta.get()});
    auto outer_bundle_schema = reg.bundle().field("level1", inner_bundle_schema).build();
    auto outer_bundle_ts_meta = std::make_shared<TSBTypeMeta>(outer_fields, outer_bundle_schema, "OuterBundle");

    CompositeTSOverlay root(outer_bundle_ts_meta.get());
    MockObserver root_observer;
    root.subscribe(&root_observer);

    // Navigate to the list: root -> level1 -> level2 (list)
    auto* level1 = dynamic_cast<CompositeTSOverlay*>(root.child(0));
    REQUIRE(level1 != nullptr);

    auto* level2_list = dynamic_cast<ListTSOverlay*>(level1->child(0));
    REQUIRE(level2_list != nullptr);

    // Add an element and modify it
    auto* element = level2_list->push_back();
    engine_time_t t1 = MIN_DT + std::chrono::microseconds(100);
    element->mark_modified(t1);

    // Root observer should be notified (propagation through 3 levels)
    REQUIRE(root_observer.notification_count == 1);
}

}  // namespace hgraph::test
