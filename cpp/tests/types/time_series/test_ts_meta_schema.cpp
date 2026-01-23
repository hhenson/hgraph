/**
 * @file test_ts_meta_schema.cpp
 * @brief Unit tests for TSMeta schema generation.
 *
 * Tests has_delta(), generate_time_schema(), generate_observer_schema(),
 * and generate_delta_value_schema().
 *
 * NOTE: These tests require linking to the _hgraph library which contains
 * the TSTypeRegistry and TSMetaSchemaCache implementations. Since _hgraph
 * is a Python MODULE_LIBRARY, these tests cannot be run directly as C++ tests.
 * They are designed for use through Python test bindings.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/util/date_time.h>

using namespace hgraph;
using namespace hgraph::value;

// ============================================================================
// Helpers
// ============================================================================

namespace {

const TypeMeta* int_type() {
    return TypeRegistry::instance().register_scalar<int64_t>();
}

const TypeMeta* double_type() {
    return TypeRegistry::instance().register_scalar<double>();
}

const TypeMeta* string_type() {
    return TypeRegistry::instance().register_scalar<std::string>();
}

}  // namespace

// ============================================================================
// has_delta() Tests
// ============================================================================

TEST_CASE("has_delta - TS has no delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());

    CHECK(!has_delta(ts_int));
}

TEST_CASE("has_delta - TSS has delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* tss_int = registry.tss(int_type());

    CHECK(has_delta(tss_int));
}

TEST_CASE("has_delta - TSD has delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* tsd = registry.tsd(string_type(), ts_int);

    CHECK(has_delta(tsd));
}

TEST_CASE("has_delta - TSW has no delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* tsw = registry.tsw(double_type(), 10, 5);

    CHECK(!has_delta(tsw));
}

TEST_CASE("has_delta - REF has no delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* ref = registry.ref(ts_int);

    CHECK(!has_delta(ref));
}

TEST_CASE("has_delta - SIGNAL has no delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* signal = registry.signal();

    CHECK(!has_delta(signal));
}

TEST_CASE("has_delta - TSB with only TS fields has no delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* ts_double = registry.ts(double_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"a", ts_int},
        {"b", ts_double}
    };

    const TSMeta* tsb = registry.tsb(fields, "TestBundleNoDeltas");

    CHECK(!has_delta(tsb));
}

TEST_CASE("has_delta - TSB with TSS field has delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* tss_int = registry.tss(int_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"a", ts_int},
        {"b", tss_int}
    };

    const TSMeta* tsb = registry.tsb(fields, "TestBundleWithTSS");

    CHECK(has_delta(tsb));
}

TEST_CASE("has_delta - TSB with TSD field has delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* tsd = registry.tsd(string_type(), ts_int);

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"a", ts_int},
        {"b", tsd}
    };

    const TSMeta* tsb = registry.tsb(fields, "TestBundleWithTSD");

    CHECK(has_delta(tsb));
}

TEST_CASE("has_delta - TSL with TS element has no delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* tsl = registry.tsl(ts_int, 5);

    CHECK(!has_delta(tsl));
}

TEST_CASE("has_delta - TSL with TSS element has delta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* tss_int = registry.tss(int_type());
    const TSMeta* tsl = registry.tsl(tss_int, 5);

    CHECK(has_delta(tsl));
}

TEST_CASE("has_delta - nullptr returns false", "[time_series][phase3][schema]") {
    CHECK(!has_delta(nullptr));
}

// ============================================================================
// generate_time_schema() Tests
// ============================================================================

TEST_CASE("time_schema - TS is engine_time_t", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());

    const TypeMeta* time_schema = generate_time_schema(ts_int);

    REQUIRE(time_schema != nullptr);
    // engine_time_t is a scalar type
    CHECK(time_schema->kind == TypeKind::Atomic);
    CHECK(time_schema->size == sizeof(engine_time_t));
}

TEST_CASE("time_schema - TSS is engine_time_t", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* tss_int = registry.tss(int_type());

    const TypeMeta* time_schema = generate_time_schema(tss_int);

    REQUIRE(time_schema != nullptr);
    CHECK(time_schema->kind == TypeKind::Atomic);
    CHECK(time_schema->size == sizeof(engine_time_t));
}

TEST_CASE("time_schema - TSD is tuple[engine_time_t, var_list]", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* tsd = registry.tsd(string_type(), ts_int);

    const TypeMeta* time_schema = generate_time_schema(tsd);

    REQUIRE(time_schema != nullptr);
    CHECK(time_schema->kind == TypeKind::Tuple);
    CHECK(time_schema->field_count == 2);

    // First element is engine_time_t
    REQUIRE(time_schema->fields != nullptr);
    CHECK(time_schema->fields[0].type->kind == TypeKind::Atomic);
    CHECK(time_schema->fields[0].type->size == sizeof(engine_time_t));

    // Second element is var_list (dynamic list)
    CHECK(time_schema->fields[1].type->kind == TypeKind::List);
    CHECK(time_schema->fields[1].type->fixed_size == 0);  // Dynamic
}

TEST_CASE("time_schema - TSB is tuple with per-field times", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* ts_double = registry.ts(double_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"a", ts_int},
        {"b", ts_double}
    };

    const TSMeta* tsb = registry.tsb(fields, "TestBundleTimeSchema");
    const TypeMeta* time_schema = generate_time_schema(tsb);

    REQUIRE(time_schema != nullptr);
    CHECK(time_schema->kind == TypeKind::Tuple);
    // 1 container time + 2 field times
    CHECK(time_schema->field_count == 3);
}

TEST_CASE("time_schema - TSL fixed is tuple with per-element times", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* tsl = registry.tsl(ts_int, 5);

    const TypeMeta* time_schema = generate_time_schema(tsl);

    REQUIRE(time_schema != nullptr);
    CHECK(time_schema->kind == TypeKind::Tuple);
    CHECK(time_schema->field_count == 2);

    // Second element is fixed_list (fixed_size > 0)
    REQUIRE(time_schema->fields != nullptr);
    CHECK(time_schema->fields[1].type->kind == TypeKind::List);
    CHECK(time_schema->fields[1].type->fixed_size == 5);
}

TEST_CASE("time_schema - caching returns same pointer", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());

    const TypeMeta* time_schema1 = generate_time_schema(ts_int);
    const TypeMeta* time_schema2 = generate_time_schema(ts_int);

    CHECK(time_schema1 == time_schema2);
}

// ============================================================================
// generate_observer_schema() Tests
// ============================================================================

TEST_CASE("observer_schema - TS is ObserverList", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());

    const TypeMeta* observer_schema = generate_observer_schema(ts_int);

    REQUIRE(observer_schema != nullptr);
    CHECK(observer_schema->kind == TypeKind::Atomic);
    CHECK(observer_schema->size == sizeof(ObserverList));
}

TEST_CASE("observer_schema - TSS is ObserverList", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* tss_int = registry.tss(int_type());

    const TypeMeta* observer_schema = generate_observer_schema(tss_int);

    REQUIRE(observer_schema != nullptr);
    CHECK(observer_schema->kind == TypeKind::Atomic);
    CHECK(observer_schema->size == sizeof(ObserverList));
}

TEST_CASE("observer_schema - TSD is tuple[ObserverList, var_list]", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* tsd = registry.tsd(string_type(), ts_int);

    const TypeMeta* observer_schema = generate_observer_schema(tsd);

    REQUIRE(observer_schema != nullptr);
    CHECK(observer_schema->kind == TypeKind::Tuple);
    CHECK(observer_schema->field_count == 2);

    // First element is ObserverList
    REQUIRE(observer_schema->fields != nullptr);
    CHECK(observer_schema->fields[0].type->size == sizeof(ObserverList));

    // Second element is var_list
    CHECK(observer_schema->fields[1].type->kind == TypeKind::List);
    CHECK(observer_schema->fields[1].type->fixed_size == 0);
}

TEST_CASE("observer_schema - TSB is tuple with per-field observers", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* ts_double = registry.ts(double_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"a", ts_int},
        {"b", ts_double}
    };

    const TSMeta* tsb = registry.tsb(fields, "TestBundleObserverSchema");
    const TypeMeta* observer_schema = generate_observer_schema(tsb);

    REQUIRE(observer_schema != nullptr);
    CHECK(observer_schema->kind == TypeKind::Tuple);
    // 1 container observer + 2 field observers
    CHECK(observer_schema->field_count == 3);
}

TEST_CASE("observer_schema - caching returns same pointer", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());

    const TypeMeta* observer_schema1 = generate_observer_schema(ts_int);
    const TypeMeta* observer_schema2 = generate_observer_schema(ts_int);

    CHECK(observer_schema1 == observer_schema2);
}

// ============================================================================
// generate_delta_value_schema() Tests
// ============================================================================

TEST_CASE("delta_schema - TS returns nullptr", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());

    const TypeMeta* delta_schema = generate_delta_value_schema(ts_int);

    CHECK(delta_schema == nullptr);
}

TEST_CASE("delta_schema - TSS returns SetDelta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* tss_int = registry.tss(int_type());

    const TypeMeta* delta_schema = generate_delta_value_schema(tss_int);

    REQUIRE(delta_schema != nullptr);
    CHECK(delta_schema->kind == TypeKind::Atomic);
    CHECK(delta_schema->size == sizeof(SetDelta));
}

TEST_CASE("delta_schema - TSD returns MapDelta", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* tsd = registry.tsd(string_type(), ts_int);

    const TypeMeta* delta_schema = generate_delta_value_schema(tsd);

    REQUIRE(delta_schema != nullptr);
    CHECK(delta_schema->kind == TypeKind::Atomic);
    CHECK(delta_schema->size == sizeof(MapDelta));
}

TEST_CASE("delta_schema - TSW returns nullptr", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* tsw = registry.tsw(double_type(), 10, 5);

    const TypeMeta* delta_schema = generate_delta_value_schema(tsw);

    CHECK(delta_schema == nullptr);
}

TEST_CASE("delta_schema - REF returns nullptr", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* ref = registry.ref(ts_int);

    const TypeMeta* delta_schema = generate_delta_value_schema(ref);

    CHECK(delta_schema == nullptr);
}

TEST_CASE("delta_schema - SIGNAL returns nullptr", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* signal = registry.signal();

    const TypeMeta* delta_schema = generate_delta_value_schema(signal);

    CHECK(delta_schema == nullptr);
}

TEST_CASE("delta_schema - TSB without delta field returns nullptr", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* ts_double = registry.ts(double_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"a", ts_int},
        {"b", ts_double}
    };

    const TSMeta* tsb = registry.tsb(fields, "TestBundleNoDeltaSchema");
    const TypeMeta* delta_schema = generate_delta_value_schema(tsb);

    CHECK(delta_schema == nullptr);
}

TEST_CASE("delta_schema - TSB with delta field returns BundleDeltaNav", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* tss_int = registry.tss(int_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"a", ts_int},
        {"b", tss_int}
    };

    const TSMeta* tsb = registry.tsb(fields, "TestBundleDeltaSchema");
    const TypeMeta* delta_schema = generate_delta_value_schema(tsb);

    REQUIRE(delta_schema != nullptr);
    CHECK(delta_schema->kind == TypeKind::Atomic);
    CHECK(delta_schema->size == sizeof(BundleDeltaNav));
}

TEST_CASE("delta_schema - TSL without delta element returns nullptr", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(int_type());
    const TSMeta* tsl = registry.tsl(ts_int, 5);

    const TypeMeta* delta_schema = generate_delta_value_schema(tsl);

    CHECK(delta_schema == nullptr);
}

TEST_CASE("delta_schema - TSL with delta element returns ListDeltaNav", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* tss_int = registry.tss(int_type());
    const TSMeta* tsl = registry.tsl(tss_int, 5);

    const TypeMeta* delta_schema = generate_delta_value_schema(tsl);

    REQUIRE(delta_schema != nullptr);
    CHECK(delta_schema->kind == TypeKind::Atomic);
    CHECK(delta_schema->size == sizeof(ListDeltaNav));
}

TEST_CASE("delta_schema - caching returns same pointer", "[time_series][phase3][schema]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* tss_int = registry.tss(int_type());

    const TypeMeta* delta_schema1 = generate_delta_value_schema(tss_int);
    const TypeMeta* delta_schema2 = generate_delta_value_schema(tss_int);

    CHECK(delta_schema1 == delta_schema2);
}

// ============================================================================
// Singleton TypeMeta Accessors Tests
// ============================================================================

TEST_CASE("TSMetaSchemaCache - engine_time_meta returns valid TypeMeta", "[time_series][phase3][schema]") {
    auto& cache = TSMetaSchemaCache::instance();
    const TypeMeta* meta = cache.engine_time_meta();

    REQUIRE(meta != nullptr);
    CHECK(meta->size == sizeof(engine_time_t));
}

TEST_CASE("TSMetaSchemaCache - observer_list_meta returns valid TypeMeta", "[time_series][phase3][schema]") {
    auto& cache = TSMetaSchemaCache::instance();
    const TypeMeta* meta = cache.observer_list_meta();

    REQUIRE(meta != nullptr);
    CHECK(meta->size == sizeof(ObserverList));
}

TEST_CASE("TSMetaSchemaCache - set_delta_meta returns valid TypeMeta", "[time_series][phase3][schema]") {
    auto& cache = TSMetaSchemaCache::instance();
    const TypeMeta* meta = cache.set_delta_meta();

    REQUIRE(meta != nullptr);
    CHECK(meta->size == sizeof(SetDelta));
}

TEST_CASE("TSMetaSchemaCache - map_delta_meta returns valid TypeMeta", "[time_series][phase3][schema]") {
    auto& cache = TSMetaSchemaCache::instance();
    const TypeMeta* meta = cache.map_delta_meta();

    REQUIRE(meta != nullptr);
    CHECK(meta->size == sizeof(MapDelta));
}

TEST_CASE("TSMetaSchemaCache - bundle_delta_nav_meta returns valid TypeMeta", "[time_series][phase3][schema]") {
    auto& cache = TSMetaSchemaCache::instance();
    const TypeMeta* meta = cache.bundle_delta_nav_meta();

    REQUIRE(meta != nullptr);
    CHECK(meta->size == sizeof(BundleDeltaNav));
}

TEST_CASE("TSMetaSchemaCache - list_delta_nav_meta returns valid TypeMeta", "[time_series][phase3][schema]") {
    auto& cache = TSMetaSchemaCache::instance();
    const TypeMeta* meta = cache.list_delta_nav_meta();

    REQUIRE(meta != nullptr);
    CHECK(meta->size == sizeof(ListDeltaNav));
}
