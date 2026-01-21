/**
 * @file test_ts_type_registry.cpp
 * @brief Unit tests for TSTypeRegistry and TSMeta schema structures.
 *
 * These tests verify the TSTypeRegistry's ability to create, cache, and manage
 * TSMeta instances for all time-series types (TS, TSS, TSD, TSL, TSW, TSB, REF, SIGNAL).
 *
 * Test Categories:
 * 1. Basic Schema Creation - verify each factory method creates valid TSMeta
 * 2. Deduplication Tests - verify same inputs return same pointer (caching)
 * 3. Field Access Tests - verify TSB field access by index
 * 4. Window Tests - verify TSW tick-based and duration-based windows
 * 5. Nested Schema Tests - verify nested/composite schemas
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_registry.h>

#include <chrono>
#include <string>
#include <vector>

using namespace hgraph;
using namespace hgraph::value;

// ============================================================================
// Test Fixtures / Helpers
// ============================================================================

/**
 * Get TypeMeta for common scalar types used in tests.
 */
class TestTypeHelper {
public:
    static const TypeMeta* int_type() {
        return TypeRegistry::instance().register_scalar<int64_t>();
    }

    static const TypeMeta* double_type() {
        return TypeRegistry::instance().register_scalar<double>();
    }

    static const TypeMeta* string_type() {
        return TypeRegistry::instance().register_scalar<std::string>();
    }

    static const TypeMeta* bool_type() {
        return TypeRegistry::instance().register_scalar<bool>();
    }
};

// ============================================================================
// Basic Schema Creation Tests
// ============================================================================

TEST_CASE("TSTypeRegistry - ts() creates valid TSMeta with TSKind::TSValue", "[ts_type_registry][ts][basic]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* int_type = TestTypeHelper::int_type();

    const TSMeta* ts_int = registry.ts(int_type);

    REQUIRE(ts_int != nullptr);
    REQUIRE(ts_int->kind == TSKind::TSValue);
    REQUIRE(ts_int->value_type == int_type);
    REQUIRE(ts_int->is_scalar_ts());
    REQUIRE_FALSE(ts_int->is_collection());
}

TEST_CASE("TSTypeRegistry - tss() creates valid TSMeta with TSKind::TSS", "[ts_type_registry][tss][basic]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* int_type = TestTypeHelper::int_type();

    const TSMeta* tss_int = registry.tss(int_type);

    REQUIRE(tss_int != nullptr);
    REQUIRE(tss_int->kind == TSKind::TSS);
    REQUIRE(tss_int->value_type == int_type);
    REQUIRE(tss_int->is_collection());
    REQUIRE_FALSE(tss_int->is_scalar_ts());
}

TEST_CASE("TSTypeRegistry - tsd() creates valid TSMeta with TSKind::TSD", "[ts_type_registry][tsd][basic]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* string_type = TestTypeHelper::string_type();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());

    const TSMeta* tsd_schema = registry.tsd(string_type, ts_int);

    REQUIRE(tsd_schema != nullptr);
    REQUIRE(tsd_schema->kind == TSKind::TSD);
    REQUIRE(tsd_schema->key_type == string_type);
    REQUIRE(tsd_schema->element_ts == ts_int);
    REQUIRE(tsd_schema->is_collection());
    REQUIRE_FALSE(tsd_schema->is_scalar_ts());
}

TEST_CASE("TSTypeRegistry - tsl() creates valid TSMeta with TSKind::TSL", "[ts_type_registry][tsl][basic]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_double = registry.ts(TestTypeHelper::double_type());

    SECTION("Dynamic size TSL (size = 0)") {
        const TSMeta* tsl_dynamic = registry.tsl(ts_double, 0);

        REQUIRE(tsl_dynamic != nullptr);
        REQUIRE(tsl_dynamic->kind == TSKind::TSL);
        REQUIRE(tsl_dynamic->element_ts == ts_double);
        REQUIRE(tsl_dynamic->fixed_size == 0);
        REQUIRE(tsl_dynamic->is_collection());
    }

    SECTION("Fixed size TSL") {
        const TSMeta* tsl_fixed = registry.tsl(ts_double, 5);

        REQUIRE(tsl_fixed != nullptr);
        REQUIRE(tsl_fixed->kind == TSKind::TSL);
        REQUIRE(tsl_fixed->element_ts == ts_double);
        REQUIRE(tsl_fixed->fixed_size == 5);
        REQUIRE(tsl_fixed->is_collection());
    }
}

TEST_CASE("TSTypeRegistry - tsw() creates valid TSMeta with TSKind::TSW (tick-based)", "[ts_type_registry][tsw][basic]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* double_type = TestTypeHelper::double_type();

    const TSMeta* tsw_ticks = registry.tsw(double_type, 10, 5);

    REQUIRE(tsw_ticks != nullptr);
    REQUIRE(tsw_ticks->kind == TSKind::TSW);
    REQUIRE(tsw_ticks->value_type == double_type);
    REQUIRE_FALSE(tsw_ticks->is_duration_based);
    REQUIRE(tsw_ticks->window.tick.period == 10);
    REQUIRE(tsw_ticks->window.tick.min_period == 5);
    REQUIRE(tsw_ticks->is_scalar_ts());
    REQUIRE_FALSE(tsw_ticks->is_collection());
}

TEST_CASE("TSTypeRegistry - tsw_duration() creates valid TSMeta with TSKind::TSW (duration-based)", "[ts_type_registry][tsw][basic]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* double_type = TestTypeHelper::double_type();

    using namespace std::chrono;
    engine_time_delta_t time_range = duration_cast<engine_time_delta_t>(hours(1));
    engine_time_delta_t min_time_range = duration_cast<engine_time_delta_t>(minutes(30));

    const TSMeta* tsw_duration = registry.tsw_duration(double_type, time_range, min_time_range);

    REQUIRE(tsw_duration != nullptr);
    REQUIRE(tsw_duration->kind == TSKind::TSW);
    REQUIRE(tsw_duration->value_type == double_type);
    REQUIRE(tsw_duration->is_duration_based);
    REQUIRE(tsw_duration->window.duration.time_range == time_range);
    REQUIRE(tsw_duration->window.duration.min_time_range == min_time_range);
    REQUIRE(tsw_duration->is_scalar_ts());
}

TEST_CASE("TSTypeRegistry - tsb() creates valid TSMeta with TSKind::TSB", "[ts_type_registry][tsb][basic]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());
    const TSMeta* ts_string = registry.ts(TestTypeHelper::string_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"count", ts_int},
        {"name", ts_string}
    };

    const TSMeta* tsb_schema = registry.tsb(fields, "TestBundle");

    REQUIRE(tsb_schema != nullptr);
    REQUIRE(tsb_schema->kind == TSKind::TSB);
    REQUIRE(tsb_schema->field_count == 2);
    REQUIRE(tsb_schema->fields != nullptr);
    REQUIRE(tsb_schema->bundle_name != nullptr);
    REQUIRE(std::string(tsb_schema->bundle_name) == "TestBundle");
    REQUIRE(tsb_schema->is_collection());
    REQUIRE_FALSE(tsb_schema->is_scalar_ts());
}

TEST_CASE("TSTypeRegistry - ref() creates valid TSMeta with TSKind::REF", "[ts_type_registry][ref][basic]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());

    const TSMeta* ref_schema = registry.ref(ts_int);

    REQUIRE(ref_schema != nullptr);
    REQUIRE(ref_schema->kind == TSKind::REF);
    REQUIRE(ref_schema->element_ts == ts_int);
    REQUIRE_FALSE(ref_schema->is_collection());
    REQUIRE_FALSE(ref_schema->is_scalar_ts());
}

TEST_CASE("TSTypeRegistry - signal() creates valid TSMeta with TSKind::SIGNAL", "[ts_type_registry][signal][basic]") {
    auto& registry = TSTypeRegistry::instance();

    const TSMeta* signal_schema = registry.signal();

    REQUIRE(signal_schema != nullptr);
    REQUIRE(signal_schema->kind == TSKind::SIGNAL);
    REQUIRE(signal_schema->is_scalar_ts());
    REQUIRE_FALSE(signal_schema->is_collection());
}

// ============================================================================
// Deduplication Tests
// ============================================================================

TEST_CASE("TSTypeRegistry - ts() returns same pointer for same value_type", "[ts_type_registry][ts][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* int_type = TestTypeHelper::int_type();

    const TSMeta* ts1 = registry.ts(int_type);
    const TSMeta* ts2 = registry.ts(int_type);

    REQUIRE(ts1 == ts2);
}

TEST_CASE("TSTypeRegistry - ts() returns different pointers for different value_types", "[ts_type_registry][ts][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* int_type = TestTypeHelper::int_type();
    const TypeMeta* double_type = TestTypeHelper::double_type();

    const TSMeta* ts_int = registry.ts(int_type);
    const TSMeta* ts_double = registry.ts(double_type);

    REQUIRE(ts_int != ts_double);
}

TEST_CASE("TSTypeRegistry - tss() returns same pointer for same element_type", "[ts_type_registry][tss][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* int_type = TestTypeHelper::int_type();

    const TSMeta* tss1 = registry.tss(int_type);
    const TSMeta* tss2 = registry.tss(int_type);

    REQUIRE(tss1 == tss2);
}

TEST_CASE("TSTypeRegistry - tsd() returns same pointer for same key/value types", "[ts_type_registry][tsd][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* string_type = TestTypeHelper::string_type();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());

    const TSMeta* tsd1 = registry.tsd(string_type, ts_int);
    const TSMeta* tsd2 = registry.tsd(string_type, ts_int);

    REQUIRE(tsd1 == tsd2);
}

TEST_CASE("TSTypeRegistry - tsd() returns different pointers for different key types", "[ts_type_registry][tsd][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* string_type = TestTypeHelper::string_type();
    const TypeMeta* int_key_type = TestTypeHelper::int_type();
    const TSMeta* ts_double = registry.ts(TestTypeHelper::double_type());

    const TSMeta* tsd_string_key = registry.tsd(string_type, ts_double);
    const TSMeta* tsd_int_key = registry.tsd(int_key_type, ts_double);

    REQUIRE(tsd_string_key != tsd_int_key);
}

TEST_CASE("TSTypeRegistry - tsd() returns different pointers for different value types", "[ts_type_registry][tsd][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* string_type = TestTypeHelper::string_type();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());
    const TSMeta* ts_double = registry.ts(TestTypeHelper::double_type());

    const TSMeta* tsd_int_value = registry.tsd(string_type, ts_int);
    const TSMeta* tsd_double_value = registry.tsd(string_type, ts_double);

    REQUIRE(tsd_int_value != tsd_double_value);
}

TEST_CASE("TSTypeRegistry - tsl() returns same pointer for same element_ts and fixed_size", "[ts_type_registry][tsl][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());

    SECTION("Dynamic size") {
        const TSMeta* tsl1 = registry.tsl(ts_int, 0);
        const TSMeta* tsl2 = registry.tsl(ts_int, 0);
        REQUIRE(tsl1 == tsl2);
    }

    SECTION("Fixed size") {
        const TSMeta* tsl1 = registry.tsl(ts_int, 10);
        const TSMeta* tsl2 = registry.tsl(ts_int, 10);
        REQUIRE(tsl1 == tsl2);
    }
}

TEST_CASE("TSTypeRegistry - tsl() returns different pointers for different fixed_sizes", "[ts_type_registry][tsl][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());

    const TSMeta* tsl_5 = registry.tsl(ts_int, 5);
    const TSMeta* tsl_10 = registry.tsl(ts_int, 10);
    const TSMeta* tsl_dynamic = registry.tsl(ts_int, 0);

    REQUIRE(tsl_5 != tsl_10);
    REQUIRE(tsl_5 != tsl_dynamic);
    REQUIRE(tsl_10 != tsl_dynamic);
}

TEST_CASE("TSTypeRegistry - tsw() returns same pointer for same tick parameters", "[ts_type_registry][tsw][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* double_type = TestTypeHelper::double_type();

    const TSMeta* tsw1 = registry.tsw(double_type, 10, 5);
    const TSMeta* tsw2 = registry.tsw(double_type, 10, 5);

    REQUIRE(tsw1 == tsw2);
}

TEST_CASE("TSTypeRegistry - tsw() returns different pointers for different tick parameters", "[ts_type_registry][tsw][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* double_type = TestTypeHelper::double_type();

    const TSMeta* tsw_10_5 = registry.tsw(double_type, 10, 5);
    const TSMeta* tsw_20_5 = registry.tsw(double_type, 20, 5);
    const TSMeta* tsw_10_3 = registry.tsw(double_type, 10, 3);

    REQUIRE(tsw_10_5 != tsw_20_5);
    REQUIRE(tsw_10_5 != tsw_10_3);
}

TEST_CASE("TSTypeRegistry - tsw_duration() returns same pointer for same duration parameters", "[ts_type_registry][tsw][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* double_type = TestTypeHelper::double_type();

    using namespace std::chrono;
    engine_time_delta_t time_range = duration_cast<engine_time_delta_t>(hours(1));
    engine_time_delta_t min_time_range = duration_cast<engine_time_delta_t>(minutes(30));

    const TSMeta* tsw1 = registry.tsw_duration(double_type, time_range, min_time_range);
    const TSMeta* tsw2 = registry.tsw_duration(double_type, time_range, min_time_range);

    REQUIRE(tsw1 == tsw2);
}

TEST_CASE("TSTypeRegistry - tsw() and tsw_duration() return different pointers", "[ts_type_registry][tsw][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* double_type = TestTypeHelper::double_type();

    using namespace std::chrono;
    const TSMeta* tsw_ticks = registry.tsw(double_type, 10, 5);
    const TSMeta* tsw_duration = registry.tsw_duration(double_type,
        duration_cast<engine_time_delta_t>(hours(1)),
        duration_cast<engine_time_delta_t>(minutes(30)));

    // They should be different schemas even if numeric values happen to match
    REQUIRE(tsw_ticks != tsw_duration);
    REQUIRE(tsw_ticks->is_duration_based != tsw_duration->is_duration_based);
}

TEST_CASE("TSTypeRegistry - tsb() returns same pointer for same fields", "[ts_type_registry][tsb][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());
    const TSMeta* ts_string = registry.ts(TestTypeHelper::string_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields1 = {
        {"x", ts_int},
        {"y", ts_string}
    };
    std::vector<std::pair<std::string, const TSMeta*>> fields2 = {
        {"x", ts_int},
        {"y", ts_string}
    };

    // Use unique name for deduplication test to avoid conflicts with other tests
    const TSMeta* tsb1 = registry.tsb(fields1, "DeduplicationTestBundle");
    const TSMeta* tsb2 = registry.tsb(fields2, "DeduplicationTestBundle");

    REQUIRE(tsb1 == tsb2);
}

TEST_CASE("TSTypeRegistry - ref() returns same pointer for same referenced_ts", "[ts_type_registry][ref][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());

    const TSMeta* ref1 = registry.ref(ts_int);
    const TSMeta* ref2 = registry.ref(ts_int);

    REQUIRE(ref1 == ref2);
}

TEST_CASE("TSTypeRegistry - ref() returns different pointers for different referenced_ts", "[ts_type_registry][ref][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());
    const TSMeta* ts_double = registry.ts(TestTypeHelper::double_type());

    const TSMeta* ref_int = registry.ref(ts_int);
    const TSMeta* ref_double = registry.ref(ts_double);

    REQUIRE(ref_int != ref_double);
}

TEST_CASE("TSTypeRegistry - signal() always returns same singleton", "[ts_type_registry][signal][deduplication]") {
    auto& registry = TSTypeRegistry::instance();

    const TSMeta* signal1 = registry.signal();
    const TSMeta* signal2 = registry.signal();
    const TSMeta* signal3 = registry.signal();

    REQUIRE(signal1 == signal2);
    REQUIRE(signal2 == signal3);
}

// ============================================================================
// Field Access Tests (TSB)
// ============================================================================

TEST_CASE("TSTypeRegistry - TSB fields accessible by index", "[ts_type_registry][tsb][fields]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());
    const TSMeta* ts_double = registry.ts(TestTypeHelper::double_type());
    const TSMeta* ts_string = registry.ts(TestTypeHelper::string_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"alpha", ts_int},
        {"beta", ts_double},
        {"gamma", ts_string}
    };

    const TSMeta* tsb = registry.tsb(fields, "FieldAccessTestBundle");

    REQUIRE(tsb->field_count == 3);
    REQUIRE(tsb->fields != nullptr);

    // Access by index
    REQUIRE(tsb->fields[0].index == 0);
    REQUIRE(tsb->fields[1].index == 1);
    REQUIRE(tsb->fields[2].index == 2);
}

TEST_CASE("TSTypeRegistry - TSB field names match input", "[ts_type_registry][tsb][fields]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());
    const TSMeta* ts_double = registry.ts(TestTypeHelper::double_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"field_one", ts_int},
        {"field_two", ts_double}
    };

    const TSMeta* tsb = registry.tsb(fields, "FieldNameTestBundle");

    REQUIRE(std::string(tsb->fields[0].name) == "field_one");
    REQUIRE(std::string(tsb->fields[1].name) == "field_two");
}

TEST_CASE("TSTypeRegistry - TSB field ts_types match input", "[ts_type_registry][tsb][fields]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());
    const TSMeta* ts_double = registry.ts(TestTypeHelper::double_type());
    const TSMeta* ts_string = registry.ts(TestTypeHelper::string_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"first", ts_int},
        {"second", ts_double},
        {"third", ts_string}
    };

    const TSMeta* tsb = registry.tsb(fields, "FieldTypeTestBundle");

    REQUIRE(tsb->fields[0].ts_type == ts_int);
    REQUIRE(tsb->fields[1].ts_type == ts_double);
    REQUIRE(tsb->fields[2].ts_type == ts_string);
}

TEST_CASE("TSTypeRegistry - TSB with empty fields", "[ts_type_registry][tsb][fields]") {
    auto& registry = TSTypeRegistry::instance();

    std::vector<std::pair<std::string, const TSMeta*>> empty_fields;

    const TSMeta* tsb = registry.tsb(empty_fields, "EmptyFieldsBundle");

    REQUIRE(tsb != nullptr);
    REQUIRE(tsb->kind == TSKind::TSB);
    REQUIRE(tsb->field_count == 0);
    // fields pointer may be nullptr or empty array depending on implementation
}

TEST_CASE("TSTypeRegistry - TSB with single field", "[ts_type_registry][tsb][fields]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_bool = registry.ts(TestTypeHelper::bool_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"only_field", ts_bool}
    };

    const TSMeta* tsb = registry.tsb(fields, "SingleFieldBundle");

    REQUIRE(tsb->field_count == 1);
    REQUIRE(std::string(tsb->fields[0].name) == "only_field");
    REQUIRE(tsb->fields[0].index == 0);
    REQUIRE(tsb->fields[0].ts_type == ts_bool);
}

// ============================================================================
// Window Tests (TSW)
// ============================================================================

TEST_CASE("TSTypeRegistry - TSW tick-based has correct period and min_period", "[ts_type_registry][tsw][window]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* int_type = TestTypeHelper::int_type();

    SECTION("period = 100, min_period = 50") {
        const TSMeta* tsw = registry.tsw(int_type, 100, 50);
        REQUIRE(tsw->window.tick.period == 100);
        REQUIRE(tsw->window.tick.min_period == 50);
    }

    SECTION("period = 1, min_period = 0 (default)") {
        const TSMeta* tsw = registry.tsw(int_type, 1, 0);
        REQUIRE(tsw->window.tick.period == 1);
        REQUIRE(tsw->window.tick.min_period == 0);
    }

    SECTION("period = 1000, min_period = 1000 (same)") {
        const TSMeta* tsw = registry.tsw(int_type, 1000, 1000);
        REQUIRE(tsw->window.tick.period == 1000);
        REQUIRE(tsw->window.tick.min_period == 1000);
    }
}

TEST_CASE("TSTypeRegistry - TSW duration-based has correct time_range and min_time_range", "[ts_type_registry][tsw][window]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* double_type = TestTypeHelper::double_type();
    using namespace std::chrono;

    SECTION("1 hour range, 30 minute min") {
        engine_time_delta_t time_range = duration_cast<engine_time_delta_t>(hours(1));
        engine_time_delta_t min_time_range = duration_cast<engine_time_delta_t>(minutes(30));

        const TSMeta* tsw = registry.tsw_duration(double_type, time_range, min_time_range);

        REQUIRE(tsw->window.duration.time_range == time_range);
        REQUIRE(tsw->window.duration.min_time_range == min_time_range);
        REQUIRE(tsw->window.duration.time_range.count() == duration_cast<microseconds>(hours(1)).count());
        REQUIRE(tsw->window.duration.min_time_range.count() == duration_cast<microseconds>(minutes(30)).count());
    }

    SECTION("1 day range, 0 min (default)") {
        engine_time_delta_t time_range = duration_cast<engine_time_delta_t>(hours(24));
        engine_time_delta_t min_time_range{0};

        const TSMeta* tsw = registry.tsw_duration(double_type, time_range, min_time_range);

        REQUIRE(tsw->window.duration.time_range == time_range);
        REQUIRE(tsw->window.duration.min_time_range == min_time_range);
    }

    SECTION("100 microseconds range") {
        engine_time_delta_t time_range{100};
        engine_time_delta_t min_time_range{10};

        const TSMeta* tsw = registry.tsw_duration(double_type, time_range, min_time_range);

        REQUIRE(tsw->window.duration.time_range.count() == 100);
        REQUIRE(tsw->window.duration.min_time_range.count() == 10);
    }
}

TEST_CASE("TSTypeRegistry - TSW is_duration_based flag is set correctly", "[ts_type_registry][tsw][window]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* int_type = TestTypeHelper::int_type();

    SECTION("tick-based window") {
        const TSMeta* tsw = registry.tsw(int_type, 10, 5);
        REQUIRE_FALSE(tsw->is_duration_based);
    }

    SECTION("duration-based window") {
        const TSMeta* tsw = registry.tsw_duration(int_type,
            engine_time_delta_t{1000000}, engine_time_delta_t{0});
        REQUIRE(tsw->is_duration_based);
    }
}

// ============================================================================
// Nested Schema Tests
// ============================================================================

TEST_CASE("TSTypeRegistry - TSD with TS value type", "[ts_type_registry][nested]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* string_key = TestTypeHelper::string_type();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());

    const TSMeta* tsd = registry.tsd(string_key, ts_int);

    REQUIRE(tsd != nullptr);
    REQUIRE(tsd->kind == TSKind::TSD);
    REQUIRE(tsd->element_ts != nullptr);
    REQUIRE(tsd->element_ts->kind == TSKind::TSValue);
}

TEST_CASE("TSTypeRegistry - TSL with TSS element type", "[ts_type_registry][nested]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* tss_int = registry.tss(TestTypeHelper::int_type());

    const TSMeta* tsl = registry.tsl(tss_int, 3);

    REQUIRE(tsl != nullptr);
    REQUIRE(tsl->kind == TSKind::TSL);
    REQUIRE(tsl->element_ts != nullptr);
    REQUIRE(tsl->element_ts->kind == TSKind::TSS);
    REQUIRE(tsl->fixed_size == 3);
}

TEST_CASE("TSTypeRegistry - REF with TSD", "[ts_type_registry][nested]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* int_key = TestTypeHelper::int_type();
    const TSMeta* ts_string = registry.ts(TestTypeHelper::string_type());
    const TSMeta* tsd = registry.tsd(int_key, ts_string);

    const TSMeta* ref = registry.ref(tsd);

    REQUIRE(ref != nullptr);
    REQUIRE(ref->kind == TSKind::REF);
    REQUIRE(ref->element_ts != nullptr);
    REQUIRE(ref->element_ts->kind == TSKind::TSD);
    REQUIRE(ref->element_ts->key_type == int_key);
    REQUIRE(ref->element_ts->element_ts == ts_string);
}

TEST_CASE("TSTypeRegistry - TSB with nested TSB field", "[ts_type_registry][nested]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());

    // Create inner bundle
    std::vector<std::pair<std::string, const TSMeta*>> inner_fields = {
        {"inner_value", ts_int}
    };
    const TSMeta* inner_tsb = registry.tsb(inner_fields, "InnerNestedBundle");

    // Create outer bundle with inner bundle as field
    std::vector<std::pair<std::string, const TSMeta*>> outer_fields = {
        {"nested", inner_tsb},
        {"value", ts_int}
    };
    const TSMeta* outer_tsb = registry.tsb(outer_fields, "OuterNestedBundle");

    REQUIRE(outer_tsb != nullptr);
    REQUIRE(outer_tsb->field_count == 2);
    REQUIRE(outer_tsb->fields[0].ts_type == inner_tsb);
    REQUIRE(outer_tsb->fields[0].ts_type->kind == TSKind::TSB);
    REQUIRE(outer_tsb->fields[1].ts_type == ts_int);
}

TEST_CASE("TSTypeRegistry - Deep nesting: TSD -> TSL -> TSS", "[ts_type_registry][nested]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* string_key = TestTypeHelper::string_type();
    const TypeMeta* int_type = TestTypeHelper::int_type();

    // Build from inside out: TSS[int] -> TSL[TSS[int]] -> TSD[str, TSL[TSS[int]]]
    const TSMeta* tss_int = registry.tss(int_type);
    const TSMeta* tsl_of_tss = registry.tsl(tss_int, 5);
    const TSMeta* tsd_deep = registry.tsd(string_key, tsl_of_tss);

    REQUIRE(tsd_deep->kind == TSKind::TSD);
    REQUIRE(tsd_deep->element_ts->kind == TSKind::TSL);
    REQUIRE(tsd_deep->element_ts->element_ts->kind == TSKind::TSS);
    REQUIRE(tsd_deep->element_ts->element_ts->value_type == int_type);
}

TEST_CASE("TSTypeRegistry - REF to TSW", "[ts_type_registry][nested]") {
    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* double_type = TestTypeHelper::double_type();
    const TSMeta* tsw = registry.tsw(double_type, 100, 50);

    const TSMeta* ref_tsw = registry.ref(tsw);

    REQUIRE(ref_tsw->kind == TSKind::REF);
    REQUIRE(ref_tsw->element_ts == tsw);
    REQUIRE(ref_tsw->element_ts->kind == TSKind::TSW);
    REQUIRE_FALSE(ref_tsw->element_ts->is_duration_based);
}

TEST_CASE("TSTypeRegistry - TSL of REFs", "[ts_type_registry][nested]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());
    const TSMeta* ref_ts_int = registry.ref(ts_int);

    const TSMeta* tsl_refs = registry.tsl(ref_ts_int, 10);

    REQUIRE(tsl_refs->kind == TSKind::TSL);
    REQUIRE(tsl_refs->element_ts->kind == TSKind::REF);
    REQUIRE(tsl_refs->element_ts->element_ts->kind == TSKind::TSValue);
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_CASE("TSTypeRegistry - Multiple registries return same singleton", "[ts_type_registry][singleton]") {
    auto& registry1 = TSTypeRegistry::instance();
    auto& registry2 = TSTypeRegistry::instance();

    REQUIRE(&registry1 == &registry2);
}

TEST_CASE("TSTypeRegistry - TSB bundle_name is correctly stored", "[ts_type_registry][tsb]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"value", ts_int}
    };

    const std::string long_name = "VeryLongBundleNameForTestingPurposes_12345";
    const TSMeta* tsb = registry.tsb(fields, long_name);

    REQUIRE(tsb->bundle_name != nullptr);
    REQUIRE(std::string(tsb->bundle_name) == long_name);
}

TEST_CASE("TSTypeRegistry - Different TSB names create different schemas", "[ts_type_registry][tsb][deduplication]") {
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* ts_int = registry.ts(TestTypeHelper::int_type());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"value", ts_int}
    };

    const TSMeta* tsb1 = registry.tsb(fields, "BundleNameA");
    const TSMeta* tsb2 = registry.tsb(fields, "BundleNameB");

    // Even with same fields, different names should create different schemas
    // (This behavior depends on the caching strategy - by name or by structure)
    // The task says TSB is cached by name, so different names = different schemas
    REQUIRE(tsb1 != tsb2);
}
