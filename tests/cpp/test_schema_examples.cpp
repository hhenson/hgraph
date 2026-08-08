// Worked examples of every schema kind the registry produces.
//
// These tests are intended to read as documentation: each TEST_CASE shows
// the canonical way to build a schema of a given kind, and confirms the
// resulting metadata's structural shape. Identity / interning behaviour
// is covered by tests/cpp/test_type_registry.cpp; this file focuses on
// "what does each kind look like in practice".

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/util/date_time.h>

#include <string>

// ============================================================================
// Value-layer schema kinds
// ============================================================================

TEST_CASE("schema example: Atomic - register a scalar C++ type")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    REQUIRE(int_meta != nullptr);
    REQUIRE(int_meta->value_kind() == ValueTypeKind::Atomic);
    REQUIRE(int_meta->is_trivially_copyable());
    REQUIRE(int_meta->is_hashable());
    REQUIRE(int_meta->is_equatable());
    REQUIRE(int_meta->is_comparable());
    REQUIRE(int_meta->is_buffer_compatible());
}

TEST_CASE("schema example: Tuple - positional fields, possibly mixed types")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *int_meta    = registry.register_scalar<std::int32_t>("int32");
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *bool_meta   = registry.register_scalar<bool>("bool");

    // (int, double, bool) — typical mixed-type tuple, addressed by index.
    const auto *tup = registry.tuple({int_meta, double_meta, bool_meta});

    REQUIRE(tup->value_kind() == ValueTypeKind::Tuple);
    REQUIRE(tup->field_count == 3);
    REQUIRE(tup->fields[0].type == int_meta);
    REQUIRE(tup->fields[1].type == double_meta);
    REQUIRE(tup->fields[2].type == bool_meta);
    // Tuple fields are unnamed.
    REQUIRE(tup->fields[0].name == nullptr);
    REQUIRE(tup->fields[1].name == nullptr);
}

TEST_CASE("schema example: Bundle - named fields with display name")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *str_meta    = registry.register_scalar<std::string>("string");

    // A simple labelled 2D point.
    const auto *point = registry.bundle(
        "ExamplePoint2D",
        {{"x", double_meta}, {"y", double_meta}, {"label", str_meta}});

    REQUIRE(point->value_kind() == ValueTypeKind::Bundle);
    REQUIRE(point->field_count == 3);
    REQUIRE(std::string(point->fields[0].name) == "x");
    REQUIRE(std::string(point->fields[1].name) == "y");
    REQUIRE(std::string(point->fields[2].name) == "label");
    REQUIRE(point->fields[0].type == double_meta);
    REQUIRE(point->fields[2].type == str_meta);
    // Bundle is reachable by display name.
    REQUIRE(registry.value_type("ExamplePoint2D") == point);
}

TEST_CASE("schema example: List - fixed-size")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    // Static list of 4 ints (e.g. for a 4-vector).
    const auto *arr4 = registry.list(int_meta, /*fixed_size=*/4);

    REQUIRE(arr4->value_kind() == ValueTypeKind::List);
    REQUIRE(arr4->element_type == int_meta);
    REQUIRE(arr4->fixed_size == 4);
    REQUIRE(arr4->is_fixed_size());
}

TEST_CASE("schema example: List - dynamic (no fixed size)")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    // Dynamic list — fixed_size == 0 means "growable".
    const auto *vec = registry.list(int_meta, /*fixed_size=*/0);

    REQUIRE(vec->value_kind() == ValueTypeKind::List);
    REQUIRE(vec->element_type == int_meta);
    REQUIRE(vec->fixed_size == 0);
    REQUIRE_FALSE(vec->is_fixed_size());
}

TEST_CASE("schema example: Set - unordered unique elements")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *str_meta = registry.register_scalar<std::string>("string");

    const auto *string_set = registry.set(str_meta);

    REQUIRE(string_set->value_kind() == ValueTypeKind::Set);
    REQUIRE(string_set->element_type == str_meta);
}

TEST_CASE("schema example: Map - keyed lookup")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *str_meta = registry.register_scalar<std::string>("string");
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    // Map<string, int> — the typical name → count pattern.
    const auto *counts = registry.map(str_meta, int_meta);

    REQUIRE(counts->value_kind() == ValueTypeKind::Map);
    REQUIRE(counts->key_type == str_meta);
    REQUIRE(counts->element_type == int_meta);
}

TEST_CASE("schema example: CyclicBuffer - fixed-capacity ring")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");

    // Rolling buffer of the last 32 doubles.
    const auto *rolling = registry.cyclic_buffer(double_meta, /*capacity=*/32);

    REQUIRE(rolling->value_kind() == ValueTypeKind::CyclicBuffer);
    REQUIRE(rolling->element_type == double_meta);
    REQUIRE(rolling->fixed_size == 32);
}

TEST_CASE("schema example: Queue - FIFO with capacity")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    // Bounded queue — at most 100 ints.
    const auto *bounded = registry.queue(int_meta, /*max_capacity=*/100);
    REQUIRE(bounded->value_kind() == ValueTypeKind::Queue);
    REQUIRE(bounded->element_type == int_meta);
    REQUIRE(bounded->fixed_size == 100);

    // Unbounded queue — max_capacity == 0 means "no cap".
    const auto *unbounded = registry.queue(int_meta, /*max_capacity=*/0);
    REQUIRE(unbounded->fixed_size == 0);
    REQUIRE(unbounded != bounded);
}

// ============================================================================
// Time-series-layer schema kinds
// ============================================================================

TEST_CASE("schema example: TS[T] - scalar time-series")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");

    // TS[double] — the simplest TS shape.
    const auto *ts_double = registry.ts(double_meta);

    REQUIRE(ts_double->kind == TSTypeKind::TS);
    REQUIRE(ts_double->value_type == double_meta);
    REQUIRE(ts_double->is_scalar_ts());
    REQUIRE_FALSE(ts_double->is_collection());
}

TEST_CASE("schema example: TSS - time-series set")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *str_meta = registry.register_scalar<std::string>("string");

    // TSS<string> — set of strings ticking over time.
    const auto *tss = registry.tss(str_meta);

    REQUIRE(tss->kind == TSTypeKind::TSS);
    REQUIRE(tss->is_collection());
    // The TSS schema points at the underlying value-layer Set schema.
    REQUIRE(tss->value_type != nullptr);
    REQUIRE(tss->value_type->value_kind() == ValueTypeKind::Set);
    REQUIRE(tss->value_type->element_type == str_meta);
}

TEST_CASE("schema example: TSD - time-series dict (string -> TS[int])")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *str_meta = registry.register_scalar<std::string>("string");
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    // Common pattern: name → counter time-series.
    const auto *tsd = registry.tsd(str_meta, ts_int);

    REQUIRE(tsd->kind == TSTypeKind::TSD);
    REQUIRE(tsd->is_collection());
    REQUIRE(tsd->key_type() == str_meta);
    REQUIRE(tsd->element_ts() == ts_int);
}

TEST_CASE("schema example: TSL - fixed-size time-series list")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *ts_double   = registry.ts(double_meta);

    // Fixed list of 4 TS[double] (e.g. RGBA channels).
    const auto *tsl = registry.tsl(ts_double, /*fixed_size=*/4);

    REQUIRE(tsl->kind == TSTypeKind::TSL);
    REQUIRE(tsl->is_collection());
    REQUIRE(tsl->fixed_size() == 4);
    REQUIRE(tsl->element_ts() == ts_double);
}

TEST_CASE("schema example: TSL - dynamic time-series list")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    const auto *dyn_tsl = registry.tsl(ts_int, /*fixed_size=*/0);

    REQUIRE(dyn_tsl->kind == TSTypeKind::TSL);
    REQUIRE(dyn_tsl->fixed_size() == 0);
}

TEST_CASE("schema example: TSW - tick-count window")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");

    // 10-tick rolling window with 3 ticks of warm-up before it's "ready".
    const auto *win = registry.tsw(double_meta, /*period=*/10, /*min_period=*/3);

    REQUIRE(win->kind == TSTypeKind::TSW);
    REQUIRE(win->is_scalar_ts());
    REQUIRE_FALSE(win->is_duration_based());
    REQUIRE(win->period() == 10);
    REQUIRE(win->min_period() == 3);
    REQUIRE(win->value_type == double_meta);
}

TEST_CASE("schema example: TSW - duration window")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");

    // 1-second rolling window with 100ms warm-up.
    const auto *win = registry.tsw_duration(
        double_meta,
        TimeDelta{1'000'000},  // 1s in microseconds
        TimeDelta{100'000});   // 100ms warm-up

    REQUIRE(win->kind == TSTypeKind::TSW);
    REQUIRE(win->is_duration_based());
    REQUIRE(win->time_range() == TimeDelta{1'000'000});
    REQUIRE(win->min_time_range() == TimeDelta{100'000});
}

TEST_CASE("schema example: TSB - time-series bundle")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *int_meta    = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_double   = registry.ts(double_meta);
    const auto *ts_int      = registry.ts(int_meta);

    // A market-data tick: bid, ask, and volume each as their own TS.
    const auto *tick = registry.tsb(
        "ExampleMarketTick",
        {{"bid", ts_double}, {"ask", ts_double}, {"volume", ts_int}});

    REQUIRE(tick->kind == TSTypeKind::TSB);
    REQUIRE(tick->is_collection());
    REQUIRE(tick->field_count() == 3);
    REQUIRE(std::string(tick->fields()[0].name) == "bid");
    REQUIRE(std::string(tick->fields()[1].name) == "ask");
    REQUIRE(std::string(tick->fields()[2].name) == "volume");
    REQUIRE(tick->fields()[0].type == ts_double);
    REQUIRE(tick->fields()[2].type == ts_int);
    REQUIRE(registry.time_series_type("ExampleMarketTick") == tick);
}

TEST_CASE("schema example: REF - reference to a time-series target")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    // REF[TS[int]] — a forwardable handle to a TS[int] target.
    const auto *ref = registry.ref(ts_int);

    REQUIRE(ref->kind == TSTypeKind::REF);
    REQUIRE(ref->referenced_ts() == ts_int);
    REQUIRE(TypeRegistry::contains_ref(ref));
    // Dereferencing strips one level of REF.
    REQUIRE(registry.dereference(ref) == ts_int);
}

TEST_CASE("schema example: Signal - zero-payload tick stream")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();

    const auto *sig = registry.signal();

    REQUIRE(sig->kind == TSTypeKind::SIGNAL);
    REQUIRE(sig->is_scalar_ts());
    // Singleton — every call returns the same canonical pointer.
    REQUIRE(registry.signal() == sig);
    REQUIRE(registry.time_series_type("SIGNAL") == sig);
}

// ============================================================================
// Compositional examples — nesting and mixing schema kinds
// ============================================================================

TEST_CASE("compositional example: nested tuple - tuple of (tuple, scalar)")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    const auto *int_meta   = registry.register_scalar<std::int32_t>("int32");
    const auto *float_meta = registry.register_scalar<float>("float32");

    // Inner: (int, float). Outer: ((int, float), int).
    const auto *inner = registry.tuple({int_meta, float_meta});
    const auto *outer = registry.tuple({inner, int_meta});

    REQUIRE(outer->value_kind() == ValueTypeKind::Tuple);
    REQUIRE(outer->field_count == 2);
    REQUIRE(outer->fields[0].type == inner);
    REQUIRE(outer->fields[0].type->value_kind() == ValueTypeKind::Tuple);
    REQUIRE(outer->fields[1].type == int_meta);
}

TEST_CASE("compositional example: bundle of bundles")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *str_meta    = registry.register_scalar<std::string>("string");

    // Inner: { x: double, y: double }. Outer: { label: string, location: Inner }.
    const auto *point    = registry.bundle("ExamplePt", {{"x", double_meta}, {"y", double_meta}});
    const auto *labelled = registry.bundle("ExampleLabelledPt", {{"label", str_meta}, {"location", point}});

    REQUIRE(labelled->value_kind() == ValueTypeKind::Bundle);
    REQUIRE(labelled->field_count == 2);
    REQUIRE(labelled->fields[1].type == point);
    REQUIRE(labelled->fields[1].type->value_kind() == ValueTypeKind::Bundle);
    REQUIRE(std::string(labelled->fields[0].name) == "label");
    REQUIRE(std::string(labelled->fields[1].name) == "location");
}

TEST_CASE("compositional example: TSD with TSB value - keyed time-series of bundles")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *str_meta    = registry.register_scalar<std::string>("string");
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *ts_double   = registry.ts(double_meta);

    // TSB { x: TS[double], y: TS[double] }
    const auto *point_ts = registry.tsb("ExamplePointTS", {{"x", ts_double}, {"y", ts_double}});

    // TSD<string, ExamplePointTS> — named points evolving over time.
    const auto *named_points = registry.tsd(str_meta, point_ts);

    REQUIRE(named_points->kind == TSTypeKind::TSD);
    REQUIRE(named_points->key_type() == str_meta);
    REQUIRE(named_points->element_ts() == point_ts);
    REQUIRE(named_points->element_ts()->kind == TSTypeKind::TSB);
}

TEST_CASE("compositional example: REF wrapping a TSB - reference to a time-series bundle")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *bundle   = registry.tsb("ExamplePairTS", {{"a", ts_int}, {"b", ts_int}});

    const auto *ref = registry.ref(bundle);

    REQUIRE(ref->kind == TSTypeKind::REF);
    REQUIRE(ref->referenced_ts() == bundle);
    REQUIRE(TypeRegistry::contains_ref(ref));
    // dereference unwraps to the underlying bundle.
    REQUIRE(registry.dereference(ref) == bundle);
}

TEST_CASE("compositional example: TSL of TSD - list of dicts")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *str_meta = registry.register_scalar<std::string>("string");
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    // Each list slot holds a TSD<string, TS[int]>.
    const auto *tsd       = registry.tsd(str_meta, ts_int);
    const auto *list_of_d = registry.tsl(tsd, /*fixed_size=*/0);

    REQUIRE(list_of_d->kind == TSTypeKind::TSL);
    REQUIRE(list_of_d->element_ts() == tsd);
    REQUIRE(list_of_d->element_ts()->kind == TSTypeKind::TSD);
}
