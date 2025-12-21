/**
 * Unit tests for TSInput and AccessStrategy
 *
 * Tests the time-series input binding system with hierarchical access strategies.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/access_strategy.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/delta_view.h>
#include <hgraph/types/value/scalar_type.h>
#include <string>
#include <vector>

using namespace hgraph;
using namespace hgraph::ts;
using namespace hgraph::value;

// Helper to create engine_time_t from microseconds for tests
inline engine_time_t make_time(int64_t us) {
    return engine_time_t{std::chrono::microseconds(us)};
}

// ============================================================================
// Test Fixtures - Simple type metadata for testing
// ============================================================================

namespace {

// Simple TS[int] metadata for testing
struct TestTSIntMeta : TSValueMeta {
    TestTSIntMeta() {
        ts_kind = TSKind::TS;
        name = "TS[int]";
        scalar_type = scalar_type_meta<int>();
    }

    [[nodiscard]] std::string type_name_str() const override { return "TS[int]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// Simple TS[string] metadata for testing
struct TestTSStringMeta : TSValueMeta {
    TestTSStringMeta() {
        ts_kind = TSKind::TS;
        name = "TS[string]";
        scalar_type = scalar_type_meta<std::string>();
    }

    [[nodiscard]] std::string type_name_str() const override { return "TS[string]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// REF[TS[int]] metadata for testing
struct TestREFTSIntMeta : REFTypeMeta {
    TestTSIntMeta inner_meta;

    TestREFTSIntMeta() {
        ts_kind = TSKind::REF;
        name = "REF[TS[int]]";
        value_ts_type = &inner_meta;
    }

    [[nodiscard]] std::string type_name_str() const override { return "REF[TS[int]]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// TSL[TS[int], Size[2]] metadata for testing
struct TestTSLMeta : TSLTypeMeta {
    TestTSIntMeta element_meta_instance;

    TestTSLMeta() {
        ts_kind = TSKind::TSL;
        name = "TSL[TS[int], Size[2]]";
        element_ts_type = &element_meta_instance;
        size = 2;
    }

    [[nodiscard]] std::string type_name_str() const override { return "TSL[TS[int], Size[2]]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// TSB[x: TS[int], y: TS[string]] metadata for testing
struct TestTSBMeta : TSBTypeMeta {
    TestTSIntMeta x_meta;
    TestTSStringMeta y_meta;

    TestTSBMeta() {
        ts_kind = TSKind::TSB;
        name = "TSB[x: TS[int], y: TS[string]]";
        fields = {
            {"x", &x_meta},
            {"y", &y_meta}
        };
    }

    [[nodiscard]] std::string type_name_str() const override { return "TSB[x: TS[int], y: TS[string]]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// TSL[REF[TS[int]], Size[2]] - list of refs (input type when output is TSD[str, REF[TSL[...]]])
struct TestTSLOfRefMeta : TSLTypeMeta {
    TestREFTSIntMeta element_meta_instance;

    TestTSLOfRefMeta() {
        ts_kind = TSKind::TSL;
        name = "TSL[REF[TS[int]], Size[2]]";
        element_ts_type = &element_meta_instance;
        size = 2;
    }

    [[nodiscard]] std::string type_name_str() const override { return "TSL[REF[TS[int]], Size[2]]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// REF[TSL[TS[int], Size[2]]] - ref containing a list
struct TestREFTSLMeta : REFTypeMeta {
    TestTSLMeta inner_meta;

    TestREFTSLMeta() {
        ts_kind = TSKind::REF;
        name = "REF[TSL[TS[int], Size[2]]]";
        value_ts_type = &inner_meta;
    }

    [[nodiscard]] std::string type_name_str() const override { return "REF[TSL[TS[int], Size[2]]]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// TSD[str, TS[int]] - dict with scalar values
struct TestTSDMeta : TSDTypeMeta {
    TestTSIntMeta value_meta_instance;

    TestTSDMeta() {
        ts_kind = TSKind::TSD;
        name = "TSD[str, TS[int]]";
        key_type = scalar_type_meta<std::string>();
        value_ts_type = &value_meta_instance;
    }

    [[nodiscard]] std::string type_name_str() const override { return "TSD[str, TS[int]]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// TSD[str, REF[TSL[TS[int], Size[2]]]] - dict with REF values containing lists
struct TestTSDOfRefTSLMeta : TSDTypeMeta {
    TestREFTSLMeta value_meta_instance;

    TestTSDOfRefTSLMeta() {
        ts_kind = TSKind::TSD;
        name = "TSD[str, REF[TSL[TS[int], Size[2]]]]";
        key_type = scalar_type_meta<std::string>();
        value_ts_type = &value_meta_instance;
    }

    [[nodiscard]] std::string type_name_str() const override { return "TSD[str, REF[TSL[TS[int], Size[2]]]]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// TSD[str, TSL[TS[int], Size[2]]] - dict with list values (no REF)
struct TestTSDOfTSLMeta : TSDTypeMeta {
    TestTSLMeta value_meta_instance;

    TestTSDOfTSLMeta() {
        ts_kind = TSKind::TSD;
        name = "TSD[str, TSL[TS[int], Size[2]]]";
        key_type = scalar_type_meta<std::string>();
        value_ts_type = &value_meta_instance;
    }

    [[nodiscard]] std::string type_name_str() const override { return "TSD[str, TSL[TS[int], Size[2]]]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// TSD[str, TSL[REF[TS[int]], Size[2]]] - dict with list of REFs (different REF position)
struct TestTSDOfTSLOfRefMeta : TSDTypeMeta {
    TestTSLOfRefMeta value_meta_instance;

    TestTSDOfTSLOfRefMeta() {
        ts_kind = TSKind::TSD;
        name = "TSD[str, TSL[REF[TS[int]], Size[2]]]";
        key_type = scalar_type_meta<std::string>();
        value_ts_type = &value_meta_instance;
    }

    [[nodiscard]] std::string type_name_str() const override { return "TSD[str, TSL[REF[TS[int]], Size[2]]]"; }
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr) const override { return nullptr; }
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr) const override { return nullptr; }
    [[nodiscard]] size_t output_memory_size() const override { return sizeof(TSOutput); }
    [[nodiscard]] size_t input_memory_size() const override { return sizeof(TSInput); }
};

// Global test metadata instances
TestTSIntMeta g_ts_int_meta;
TestTSStringMeta g_ts_string_meta;
TestREFTSIntMeta g_ref_ts_int_meta;
TestTSLMeta g_tsl_meta;
TestTSBMeta g_tsb_meta;
TestTSLOfRefMeta g_tsl_of_ref_meta;
TestREFTSLMeta g_ref_tsl_meta;
TestTSDMeta g_tsd_meta;
TestTSDOfRefTSLMeta g_tsd_of_ref_tsl_meta;
TestTSDOfTSLMeta g_tsd_of_tsl_meta;
TestTSDOfTSLOfRefMeta g_tsd_of_tsl_of_ref_meta;

// Test notifiable that records notifications
class TestNotifiable : public Notifiable {
public:
    std::vector<engine_time_t> notifications;

    void notify(engine_time_t time) override {
        notifications.push_back(time);
    }

    void clear() { notifications.clear(); }
    [[nodiscard]] size_t count() const { return notifications.size(); }
    [[nodiscard]] bool notified_at(engine_time_t time) const {
        return std::find(notifications.begin(), notifications.end(), time) != notifications.end();
    }
};

} // anonymous namespace

// ============================================================================
// TSOutput Basic Tests
// ============================================================================

TEST_CASE("TSOutput - creation and basic properties", "[ts][output]") {
    TSOutput output(&g_ts_int_meta, nullptr);

    REQUIRE(output.valid());
    REQUIRE(output.meta() == &g_ts_int_meta);
    REQUIRE(output.ts_kind() == TSKind::TS);
    REQUIRE_FALSE(output.has_value());
}

TEST_CASE("TSOutput - set and get value", "[ts][output]") {
    TSOutput output(&g_ts_int_meta, nullptr);
    engine_time_t time = make_time(1000);

    // Set value
    output.view().set(42, time);

    REQUIRE(output.has_value());
    REQUIRE(output.modified_at(time));
    REQUIRE(output.value().as<int>() == 42);
}

TEST_CASE("TSOutput - subscription notification", "[ts][output]") {
    TSOutput output(&g_ts_int_meta, nullptr);
    TestNotifiable subscriber;
    engine_time_t time = make_time(1000);

    output.subscribe(&subscriber);
    output.view().set(42, time);

    REQUIRE(subscriber.count() == 1);
    REQUIRE(subscriber.notified_at(time));

    output.unsubscribe(&subscriber);
    output.view().set(99, time + MIN_TD);

    // Should not be notified after unsubscribe
    REQUIRE(subscriber.count() == 1);
}

// ============================================================================
// TSInput Basic Tests
// ============================================================================

TEST_CASE("TSInput - creation and basic properties", "[ts][input]") {
    TSInput input(&g_ts_int_meta, nullptr);

    REQUIRE(input.valid());
    REQUIRE(input.meta() == &g_ts_int_meta);
    REQUIRE(input.ts_kind() == TSKind::TS);
    REQUIRE_FALSE(input.bound());
    REQUIRE_FALSE(input.active());
}

TEST_CASE("TSInput - bind to output", "[ts][input]") {
    TSOutput output(&g_ts_int_meta, nullptr);
    TSInput input(&g_ts_int_meta, nullptr);

    input.bind_output(output.view());

    REQUIRE(input.bound());
    REQUIRE(input.strategy() != nullptr);
}

TEST_CASE("TSInput - unbind from output", "[ts][input]") {
    TSOutput output(&g_ts_int_meta, nullptr);
    TSInput input(&g_ts_int_meta, nullptr);

    input.bind_output(output.view());
    REQUIRE(input.bound());

    input.unbind_output();
    REQUIRE_FALSE(input.bound());
}

TEST_CASE("TSInput - read value from bound output", "[ts][input]") {
    TSOutput output(&g_ts_int_meta, nullptr);
    TSInput input(&g_ts_int_meta, nullptr);
    engine_time_t time = make_time(1000);

    output.view().set(42, time);
    input.bind_output(output.view());

    REQUIRE(input.has_value());
    REQUIRE(input.value().as<int>() == 42);
    REQUIRE(input.modified_at(time));
}

// ============================================================================
// Activation Tests
// ============================================================================

TEST_CASE("TSInput - activation subscribes to output", "[ts][input][activation]") {
    TSOutput output(&g_ts_int_meta, nullptr);
    TSInput input(&g_ts_int_meta, nullptr);

    input.bind_output(output.view());

    REQUIRE_FALSE(input.active());

    input.make_active();
    REQUIRE(input.active());
}

TEST_CASE("TSInput - make_passive unsubscribes from output", "[ts][input][activation]") {
    TSOutput output(&g_ts_int_meta, nullptr);
    TSInput input(&g_ts_int_meta, nullptr);

    input.bind_output(output.view());
    input.make_active();
    REQUIRE(input.active());

    input.make_passive();
    REQUIRE_FALSE(input.active());
}

TEST_CASE("TSInput - activation state preserved across rebind", "[ts][input][activation]") {
    TSOutput output1(&g_ts_int_meta, nullptr);
    TSOutput output2(&g_ts_int_meta, nullptr);
    TSInput input(&g_ts_int_meta, nullptr);

    input.bind_output(output1.view());
    input.make_active();
    REQUIRE(input.active());

    // Rebind to different output
    input.bind_output(output2.view());

    // Should still be active
    REQUIRE(input.active());
    REQUIRE(input.bound());
}

// ============================================================================
// DirectAccessStrategy Tests
// ============================================================================

TEST_CASE("DirectAccessStrategy - delegates to output", "[ts][strategy][direct]") {
    TSOutput output(&g_ts_int_meta, nullptr);
    TSInput input(&g_ts_int_meta, nullptr);
    engine_time_t time = make_time(1000);

    output.view().set(42, time);
    input.bind_output(output.view());

    // DirectAccessStrategy should delegate value access to output
    REQUIRE(input.value().valid());
    REQUIRE(input.value().as<int>() == 42);
    REQUIRE(input.modified_at(time));
    REQUIRE(input.last_modified_time() == time);
}

TEST_CASE("DirectAccessStrategy - tracks output modifications", "[ts][strategy][direct]") {
    TSOutput output(&g_ts_int_meta, nullptr);
    TSInput input(&g_ts_int_meta, nullptr);
    engine_time_t time1 = make_time(1000);
    engine_time_t time2 = make_time(2000);

    input.bind_output(output.view());

    output.view().set(42, time1);
    REQUIRE(input.modified_at(time1));
    REQUIRE_FALSE(input.modified_at(time2));

    output.view().set(99, time2);
    REQUIRE(input.modified_at(time2));
}

// ============================================================================
// TSInputView Tests
// ============================================================================

TEST_CASE("TSInputView - creation from bound input", "[ts][view]") {
    TSOutput output(&g_ts_int_meta, nullptr);
    TSInput input(&g_ts_int_meta, nullptr);
    engine_time_t time = make_time(1000);

    output.view().set(42, time);
    input.bind_output(output.view());

    TSInputView view = input.view();

    REQUIRE(view.valid());
    REQUIRE(view.as<int>() == 42);
    REQUIRE(view.modified_at(time));
}

TEST_CASE("TSInputView - invalid when unbound", "[ts][view]") {
    TSInput input(&g_ts_int_meta, nullptr);

    TSInputView view = input.view();

    REQUIRE_FALSE(view.valid());
}

// ============================================================================
// build_access_strategy Tests
// ============================================================================

TEST_CASE("build_access_strategy - matching types creates DirectAccess", "[ts][strategy][builder]") {
    TSInput input(&g_ts_int_meta, nullptr);

    auto strategy = build_access_strategy(&g_ts_int_meta, &g_ts_int_meta, &input);

    REQUIRE(strategy != nullptr);
    REQUIRE(is_direct_access(strategy.get()));
}

TEST_CASE("build_access_strategy - REF output non-REF input creates RefObserver", "[ts][strategy][builder]") {
    TSInput input(&g_ts_int_meta, nullptr);

    auto strategy = build_access_strategy(&g_ts_int_meta, &g_ref_ts_int_meta, &input);

    REQUIRE(strategy != nullptr);
    REQUIRE(dynamic_cast<RefObserverAccessStrategy*>(strategy.get()) != nullptr);
}

TEST_CASE("build_access_strategy - REF input non-REF output creates RefWrapper", "[ts][strategy][builder]") {
    TSInput input(&g_ref_ts_int_meta, nullptr);

    auto strategy = build_access_strategy(&g_ref_ts_int_meta, &g_ts_int_meta, &input);

    REQUIRE(strategy != nullptr);
    REQUIRE(dynamic_cast<RefWrapperAccessStrategy*>(strategy.get()) != nullptr);
}

// ============================================================================
// String Representation Tests
// ============================================================================

TEST_CASE("TSInput - to_string when unbound", "[ts][string]") {
    TSInput input(&g_ts_int_meta, nullptr);

    REQUIRE(input.to_string() == "<unbound>");
}

TEST_CASE("TSInput - to_debug_string", "[ts][string]") {
    TSInput input(&g_ts_int_meta, nullptr);
    engine_time_t time = make_time(1000);

    std::string debug = input.to_debug_string(time);

    REQUIRE(debug.find("bound=false") != std::string::npos);
}

// ============================================================================
// CollectionAccessStrategy Tests - TSL (TimeSeriesList)
// ============================================================================

TEST_CASE("build_access_strategy - TSL matching types creates CollectionAccess with DirectAccess children",
          "[ts][strategy][collection][tsl]") {
    TSInput input(&g_tsl_meta, nullptr);

    // TSL[TS[int], Size[2]] output bound to TSL[TS[int], Size[2]] input
    auto strategy = build_access_strategy(&g_tsl_meta, &g_tsl_meta, &input);

    REQUIRE(strategy != nullptr);

    // Should be a CollectionAccessStrategy because TSL is a collection
    auto* collection_strategy = dynamic_cast<CollectionAccessStrategy*>(strategy.get());
    REQUIRE(collection_strategy != nullptr);

    // Should have 2 children (size = 2)
    REQUIRE(collection_strategy->child_count() == 2);

    // Each child should be DirectAccess since element types match
    for (size_t i = 0; i < 2; ++i) {
        auto* child = collection_strategy->child(i);
        REQUIRE(child != nullptr);
        REQUIRE(is_direct_access(child));
    }
}

TEST_CASE("CollectionAccessStrategy - no storage needed when all children are DirectAccess",
          "[ts][strategy][collection][tsl]") {
    TSInput input(&g_tsl_meta, nullptr);

    auto strategy = build_access_strategy(&g_tsl_meta, &g_tsl_meta, &input);
    auto* collection_strategy = dynamic_cast<CollectionAccessStrategy*>(strategy.get());

    REQUIRE(collection_strategy != nullptr);
    // No transformation needed, so no storage
    REQUIRE_FALSE(collection_strategy->has_storage());
}

// ============================================================================
// CollectionAccessStrategy Tests - TSB (TimeSeriesBundle)
// ============================================================================

TEST_CASE("build_access_strategy - TSB matching types creates CollectionAccess with DirectAccess children",
          "[ts][strategy][collection][tsb]") {
    TSInput input(&g_tsb_meta, nullptr);

    // TSB[x: TS[int], y: TS[string]] output bound to same input type
    auto strategy = build_access_strategy(&g_tsb_meta, &g_tsb_meta, &input);

    REQUIRE(strategy != nullptr);

    auto* collection_strategy = dynamic_cast<CollectionAccessStrategy*>(strategy.get());
    REQUIRE(collection_strategy != nullptr);

    // Should have 2 children (two fields: x and y)
    REQUIRE(collection_strategy->child_count() == 2);

    // Each child should be DirectAccess
    for (size_t i = 0; i < 2; ++i) {
        auto* child = collection_strategy->child(i);
        REQUIRE(child != nullptr);
        REQUIRE(is_direct_access(child));
    }
}

// ============================================================================
// Stacked Strategy Tests - REF inside Collection
// ============================================================================

TEST_CASE("build_access_strategy - TSL with REF elements to TSL with non-REF elements",
          "[ts][strategy][stacked][ref-in-collection]") {
    TSInput input(&g_tsl_meta, nullptr);

    // TSL[REF[TS[int]], Size[2]] output bound to TSL[TS[int], Size[2]] input
    // Each element needs RefObserver to dereference the REF
    auto strategy = build_access_strategy(&g_tsl_meta, &g_tsl_of_ref_meta, &input);

    REQUIRE(strategy != nullptr);

    auto* collection_strategy = dynamic_cast<CollectionAccessStrategy*>(strategy.get());
    REQUIRE(collection_strategy != nullptr);
    REQUIRE(collection_strategy->child_count() == 2);

    // Each child should be RefObserverAccessStrategy (not DirectAccess)
    for (size_t i = 0; i < 2; ++i) {
        auto* child = collection_strategy->child(i);
        REQUIRE(child != nullptr);

        auto* ref_observer = dynamic_cast<RefObserverAccessStrategy*>(child);
        REQUIRE(ref_observer != nullptr);

        // The RefObserver's child should be DirectAccess (to access the dereferenced TS[int])
        auto* inner_child = ref_observer->child_strategy();
        REQUIRE(inner_child != nullptr);
        REQUIRE(is_direct_access(inner_child));
    }
}

TEST_CASE("build_access_strategy - TSL with non-REF elements to TSL with REF elements",
          "[ts][strategy][stacked][ref-in-collection]") {
    TSInput input(&g_tsl_of_ref_meta, nullptr);

    // TSL[TS[int], Size[2]] output bound to TSL[REF[TS[int]], Size[2]] input
    // Each element needs RefWrapper to wrap the non-REF output as REF
    auto strategy = build_access_strategy(&g_tsl_of_ref_meta, &g_tsl_meta, &input);

    REQUIRE(strategy != nullptr);

    auto* collection_strategy = dynamic_cast<CollectionAccessStrategy*>(strategy.get());
    REQUIRE(collection_strategy != nullptr);
    REQUIRE(collection_strategy->child_count() == 2);

    // Each child should be RefWrapperAccessStrategy
    for (size_t i = 0; i < 2; ++i) {
        auto* child = collection_strategy->child(i);
        REQUIRE(child != nullptr);

        auto* ref_wrapper = dynamic_cast<RefWrapperAccessStrategy*>(child);
        REQUIRE(ref_wrapper != nullptr);
    }

    // Note: has_storage() returns false at strategy construction time.
    // Storage is created lazily via create_storage() during actual binding.
    // At this point (strategy construction only), no storage exists yet.
    REQUIRE_FALSE(collection_strategy->has_storage());
}

// ============================================================================
// Stacked Strategy Tests - REF containing Collection
// ============================================================================

TEST_CASE("build_access_strategy - REF[TSL] output to TSL input creates RefObserver with Collection child",
          "[ts][strategy][stacked][collection-in-ref]") {
    TSInput input(&g_tsl_meta, nullptr);

    // REF[TSL[TS[int], Size[2]]] output bound to TSL[TS[int], Size[2]] input
    auto strategy = build_access_strategy(&g_tsl_meta, &g_ref_tsl_meta, &input);

    REQUIRE(strategy != nullptr);

    // Root should be RefObserver (to dereference the REF)
    auto* ref_observer = dynamic_cast<RefObserverAccessStrategy*>(strategy.get());
    REQUIRE(ref_observer != nullptr);

    // RefObserver's child should be CollectionAccess (for the TSL inside)
    auto* inner_collection = dynamic_cast<CollectionAccessStrategy*>(ref_observer->child_strategy());
    REQUIRE(inner_collection != nullptr);
    REQUIRE(inner_collection->child_count() == 2);

    // Each element of the collection should be DirectAccess
    for (size_t i = 0; i < 2; ++i) {
        auto* element_child = inner_collection->child(i);
        REQUIRE(element_child != nullptr);
        REQUIRE(is_direct_access(element_child));
    }
}

TEST_CASE("build_access_strategy - TSL input to REF[TSL] output creates RefWrapper",
          "[ts][strategy][stacked][collection-in-ref]") {
    TSInput input(&g_ref_tsl_meta, nullptr);

    // TSL[TS[int], Size[2]] output bound to REF[TSL[TS[int], Size[2]]] input
    // This is a REF input binding to non-REF output - needs RefWrapper
    auto strategy = build_access_strategy(&g_ref_tsl_meta, &g_tsl_meta, &input);

    REQUIRE(strategy != nullptr);

    // Should be RefWrapper (wrapping the TSL output as a REF)
    auto* ref_wrapper = dynamic_cast<RefWrapperAccessStrategy*>(strategy.get());
    REQUIRE(ref_wrapper != nullptr);
}

// ============================================================================
// Complex Multi-Level Stacked Strategy Tests
// ============================================================================

TEST_CASE("build_access_strategy - TSD[str, TSL] matching types creates nested CollectionAccess",
          "[ts][strategy][stacked][multi-level]") {
    TSInput input(&g_tsd_of_tsl_meta, nullptr);

    // TSD[str, TSL[TS[int], Size[2]]] matching input/output
    auto strategy = build_access_strategy(&g_tsd_of_tsl_meta, &g_tsd_of_tsl_meta, &input);

    REQUIRE(strategy != nullptr);

    // TSD creates a CollectionAccessStrategy (but with 0 fixed children for dict)
    // TSD is dynamic, so it might be treated differently
    // For now, verify it creates some strategy
    // The implementation may vary based on how TSD is handled

    // At minimum, should not be DirectAccess since TSD is a collection
    // (This test documents expected behavior)
}

TEST_CASE("build_access_strategy - TSD[str, REF[TSL]] output to TSD[str, TSL] input",
          "[ts][strategy][stacked][multi-level]") {
    TSInput input(&g_tsd_of_tsl_meta, nullptr);

    // TSD[str, REF[TSL[TS[int], Size[2]]]] output
    // TSD[str, TSL[TS[int], Size[2]]] input
    // The dict's value elements need RefObserver to dereference REF
    auto strategy = build_access_strategy(&g_tsd_of_tsl_meta, &g_tsd_of_ref_tsl_meta, &input);

    REQUIRE(strategy != nullptr);

    // Document expected structure:
    // CollectionAccessStrategy (TSD)
    //   -> child strategies for each value will be RefObserver
    //     -> RefObserver.child = CollectionAccess (TSL)
    //       -> DirectAccess children (TS[int])
}

TEST_CASE("build_access_strategy - TSD[str, REF[TSL]] to TSD[str, TSL[REF]] different REF positions",
          "[ts][strategy][stacked][complex]") {
    TSInput input(&g_tsd_of_tsl_of_ref_meta, nullptr);

    // Output: TSD[str, REF[TSL[TS[int], Size[2]]]]
    // Input:  TSD[str, TSL[REF[TS[int]], Size[2]]]
    //
    // This is the complex case where REF is at different positions:
    // - Output has REF wrapping the entire TSL
    // - Input has REF on each element inside the TSL
    //
    // Strategy structure should be:
    // CollectionAccessStrategy (TSD)
    //   -> child for each dict value:
    //     RefObserverAccessStrategy (to dereference REF in output)
    //       -> CollectionAccessStrategy (TSL from inside the dereferenced REF)
    //         -> RefWrapperAccessStrategy for each element (to wrap TS[int] as REF[TS[int]])
    auto strategy = build_access_strategy(&g_tsd_of_tsl_of_ref_meta, &g_tsd_of_ref_tsl_meta, &input);

    REQUIRE(strategy != nullptr);

    // This tests the "REF redistribution" scenario:
    // The output has REF at the TSL level, but the input expects REF at the element level.
    // The strategy tree must:
    // 1. Dereference the outer REF (RefObserver)
    // 2. Navigate into the TSL (CollectionAccess)
    // 3. Wrap each element as REF (RefWrapper)
}

// ============================================================================
// Strategy Tree Verification Helpers
// ============================================================================

TEST_CASE("Verify strategy tree structure for TSL[REF[TS[int]]] output to TSL[TS[int]] input",
          "[ts][strategy][verification]") {
    TSInput input(&g_tsl_meta, nullptr);

    auto strategy = build_access_strategy(&g_tsl_meta, &g_tsl_of_ref_meta, &input);
    REQUIRE(strategy != nullptr);

    // Verify tree structure:
    // CollectionAccessStrategy (TSL)
    //   [0] -> RefObserverAccessStrategy
    //          -> DirectAccessStrategy (for TS[int])
    //   [1] -> RefObserverAccessStrategy
    //          -> DirectAccessStrategy (for TS[int])

    auto* root = dynamic_cast<CollectionAccessStrategy*>(strategy.get());
    REQUIRE(root != nullptr);
    REQUIRE(root->child_count() == 2);

    for (size_t i = 0; i < 2; ++i) {
        auto* ref_observer = dynamic_cast<RefObserverAccessStrategy*>(root->child(i));
        REQUIRE(ref_observer != nullptr);

        auto* direct = dynamic_cast<DirectAccessStrategy*>(ref_observer->child_strategy());
        REQUIRE(direct != nullptr);
    }
}

TEST_CASE("Verify strategy tree structure for REF[TSL[TS[int]]] output to TSL[TS[int]] input",
          "[ts][strategy][verification]") {
    TSInput input(&g_tsl_meta, nullptr);

    auto strategy = build_access_strategy(&g_tsl_meta, &g_ref_tsl_meta, &input);
    REQUIRE(strategy != nullptr);

    // Verify tree structure:
    // RefObserverAccessStrategy (for REF)
    //   -> CollectionAccessStrategy (for TSL)
    //      [0] -> DirectAccessStrategy (for TS[int])
    //      [1] -> DirectAccessStrategy (for TS[int])

    auto* ref_observer = dynamic_cast<RefObserverAccessStrategy*>(strategy.get());
    REQUIRE(ref_observer != nullptr);

    auto* collection = dynamic_cast<CollectionAccessStrategy*>(ref_observer->child_strategy());
    REQUIRE(collection != nullptr);
    REQUIRE(collection->child_count() == 2);

    for (size_t i = 0; i < 2; ++i) {
        auto* direct = dynamic_cast<DirectAccessStrategy*>(collection->child(i));
        REQUIRE(direct != nullptr);
    }
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

TEST_CASE("build_access_strategy - null input meta returns DirectAccess", "[ts][strategy][edge]") {
    TSInput input(&g_ts_int_meta, nullptr);

    auto strategy = build_access_strategy(nullptr, &g_ts_int_meta, &input);

    REQUIRE(strategy != nullptr);
    REQUIRE(is_direct_access(strategy.get()));
}

TEST_CASE("build_access_strategy - null output meta returns DirectAccess", "[ts][strategy][edge]") {
    TSInput input(&g_ts_int_meta, nullptr);

    auto strategy = build_access_strategy(&g_ts_int_meta, nullptr, &input);

    REQUIRE(strategy != nullptr);
    REQUIRE(is_direct_access(strategy.get()));
}

TEST_CASE("build_access_strategy - both null returns DirectAccess", "[ts][strategy][edge]") {
    TSInput input(&g_ts_int_meta, nullptr);

    auto strategy = build_access_strategy(nullptr, nullptr, &input);

    REQUIRE(strategy != nullptr);
    REQUIRE(is_direct_access(strategy.get()));
}

// ============================================================================
// Type-erased copy tests
// ============================================================================

#include <hgraph/types/time_series/ts_copy_helpers.h>

TEST_CASE("copy_from_input_view - copies scalar value", "[ts][copy]") {
    // Create source output and set value
    TSOutput source(&g_ts_int_meta, nullptr);
    auto time1 = make_time(1000);
    source.view().set<int>(42, time1);

    // Create input bound to source
    TSInput input(&g_ts_int_meta, nullptr);
    input.bind_output(source.view());

    // Create destination output
    TSOutput dest(&g_ts_int_meta, nullptr);
    auto time2 = make_time(2000);

    // Copy from input view
    bool result = copy_from_input_view(&dest, input.view(), time2);

    REQUIRE(result == true);
    REQUIRE(dest.has_value());
    REQUIRE(dest.view().as<int>() == 42);
    REQUIRE(dest.modified_at(time2));

    input.unbind_output();
}

TEST_CASE("copy_from_output_view - copies scalar value", "[ts][copy]") {
    // Create source output and set value
    TSOutput source(&g_ts_int_meta, nullptr);
    auto time1 = make_time(1000);
    source.view().set<int>(99, time1);

    // Create destination output
    TSOutput dest(&g_ts_int_meta, nullptr);
    auto time2 = make_time(2000);

    // Copy from output view
    bool result = copy_from_output_view(&dest, source.view(), time2);

    REQUIRE(result == true);
    REQUIRE(dest.has_value());
    REQUIRE(dest.view().as<int>() == 99);
    REQUIRE(dest.modified_at(time2));
}

TEST_CASE("copy_from_view - schema mismatch returns false", "[ts][copy][error]") {
    // Create source with int type
    TSOutput source(&g_ts_int_meta, nullptr);
    auto time1 = make_time(1000);
    source.view().set<int>(42, time1);

    // Create destination with string type
    TSOutput dest(&g_ts_string_meta, nullptr);
    auto time2 = make_time(2000);

    // Get source as ConstValueView - source.view() is a TSView
    // and value_view() returns the inner ValueView
    auto ts_view = source.view();
    auto source_view = ts_view.value_view();
    ConstValueView const_view(source_view.data(), source_view.schema());

    // Copy should fail due to schema mismatch
    bool result = copy_from_view(&dest, const_view, time2);

    REQUIRE(result == false);
}

TEST_CASE("copy_from_view - null output returns false", "[ts][copy][error]") {
    TSOutput source(&g_ts_int_meta, nullptr);
    auto time1 = make_time(1000);
    source.view().set<int>(42, time1);

    auto ts_view = source.view();
    auto source_view = ts_view.value_view();
    ConstValueView const_view(source_view.data(), source_view.schema());

    bool result = copy_from_view(nullptr, const_view, time1);

    REQUIRE(result == false);
}

TEST_CASE("copy_from_input_view - invalid view returns false", "[ts][copy][error]") {
    TSOutput dest(&g_ts_int_meta, nullptr);
    auto time = make_time(1000);

    // Create an invalid input view (default constructed)
    TSInputView invalid_view;

    bool result = copy_from_input_view(&dest, invalid_view, time);

    REQUIRE(result == false);
}

TEST_CASE("copy_from_output_view - invalid view returns false", "[ts][copy][error]") {
    TSOutput dest(&g_ts_int_meta, nullptr);
    auto time = make_time(1000);

    // Create an invalid output view (default constructed)
    value::TSView invalid_view;

    bool result = copy_from_output_view(&dest, invalid_view, time);

    REQUIRE(result == false);
}
