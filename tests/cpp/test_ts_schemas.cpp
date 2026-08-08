// Tests for the value_schema / delta_value_schema properties on
// TSValueTypeMetaData. These are pre-computed by the registry during TS
// schema construction and read as plain field accesses.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/util/date_time.h>

#include <cstdint>
#include <string>

TEST_CASE("ts_schemas: TS<std::int32_t>.value_schema and delta_value_schema are int")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    REQUIRE(ts_int->value_schema == int_meta);
    REQUIRE(ts_int->delta_value_schema == int_meta);
}

TEST_CASE("ts_schemas: SIGNAL.value_schema and delta_value_schema are bool")
{
    using namespace hgraph;
    auto       &registry  = TypeRegistry::instance();
    const auto *bool_meta = registry.register_scalar<bool>("bool");
    const auto *sig       = registry.signal();

    REQUIRE(sig->value_schema == bool_meta);
    REQUIRE(sig->delta_value_schema == bool_meta);
}

TEST_CASE("ts_schemas: TSS<T>.value_schema is Set<T>, delta is Bundle{added: Set<T>, removed: Set<T>}")
{
    using namespace hgraph;
    auto       &registry  = TypeRegistry::instance();
    const auto *str_meta  = registry.register_scalar<std::string>("string");
    const auto *tss       = registry.tss(str_meta);

    // value_schema = Set<string>
    const auto *expected_value = registry.set(str_meta);
    REQUIRE(tss->value_schema == expected_value);
    REQUIRE(tss->value_schema->value_kind() == ValueTypeKind::Set);
    REQUIRE(tss->value_schema->element_type == str_meta);

    // delta_value_schema = Bundle{added: Set<string>, removed: Set<string>}
    REQUIRE(tss->delta_value_schema != nullptr);
    REQUIRE(tss->delta_value_schema->value_kind() == ValueTypeKind::Bundle);
    REQUIRE(tss->delta_value_schema->field_count == 2);
    REQUIRE(std::string(tss->delta_value_schema->fields[0].name) == "added");
    REQUIRE(std::string(tss->delta_value_schema->fields[1].name) == "removed");
    REQUIRE(tss->delta_value_schema->fields[0].type == expected_value);
    REQUIRE(tss->delta_value_schema->fields[1].type == expected_value);
}

TEST_CASE("ts_schemas: TSD<K, V>.value_schema is Map<K, V.value_schema>, delta is removed/modified bundle")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *str_meta = registry.register_scalar<std::string>("string");
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd      = registry.tsd(str_meta, ts_int);

    // value_schema = Map<string, int>
    const auto *expected_value = registry.map(str_meta, ts_int->value_schema);
    REQUIRE(tsd->value_schema == expected_value);
    REQUIRE(tsd->value_schema->value_kind() == ValueTypeKind::Map);
    REQUIRE(tsd->value_schema->key_type == str_meta);
    REQUIRE(tsd->value_schema->element_type == int_meta);

    // delta_value_schema = Bundle{removed: Set<string>, modified: Map<string, int>,
    //                             removed_strict: Set<string>}
    // (modified carries both new and updated entries; removed_strict carries
    // user-authored REMOVE keys — absent-key removal raises at apply)
    REQUIRE(tsd->delta_value_schema != nullptr);
    REQUIRE(tsd->delta_value_schema->value_kind() == ValueTypeKind::Bundle);
    REQUIRE(tsd->delta_value_schema->field_count == 3);
    REQUIRE(std::string(tsd->delta_value_schema->fields[0].name) == "removed");
    REQUIRE(std::string(tsd->delta_value_schema->fields[1].name) == "modified");
    REQUIRE(std::string(tsd->delta_value_schema->fields[2].name) == "removed_strict");

    const auto *expected_delta_map = registry.map(str_meta, ts_int->delta_value_schema);
    const auto *expected_removed   = registry.set(str_meta);
    REQUIRE(tsd->delta_value_schema->fields[0].type == expected_removed);
    REQUIRE(tsd->delta_value_schema->fields[1].type == expected_delta_map);
    REQUIRE(tsd->delta_value_schema->fields[2].type == expected_removed);
}

TEST_CASE("ts_schemas: TSL<T>.value_schema is List<T.value>, delta is Map<int, T.delta>")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *ts_double   = registry.ts(double_meta);

    SECTION("fixed-size TSL")
    {
        const auto *tsl = registry.tsl(ts_double, 4);

        // value_schema = List<double, 4>
        const auto *expected_value = registry.list(double_meta, 4);
        REQUIRE(tsl->value_schema == expected_value);
        REQUIRE(tsl->value_schema->value_kind() == ValueTypeKind::List);
        REQUIRE(tsl->value_schema->element_type == double_meta);
        REQUIRE(tsl->value_schema->fixed_size == 4);

        // delta_value_schema = Map<int, double>
        const auto *int_meta           = registry.value_type("int");
        REQUIRE(int_meta != nullptr);
        const auto *expected_delta_map = registry.map(int_meta, double_meta);
        REQUIRE(tsl->delta_value_schema == expected_delta_map);
    }

    SECTION("dynamic TSL")
    {
        const auto *tsl = registry.tsl(ts_double, /*fixed_size=*/0);

        // value_schema = List<double> (dynamic)
        const auto *expected_value = registry.list(double_meta, 0);
        REQUIRE(tsl->value_schema == expected_value);
        REQUIRE(tsl->value_schema->fixed_size == 0);

        // delta_value_schema = Map<int, double>
        const auto *int_meta           = registry.value_type("int");
        const auto *expected_delta_map = registry.map(int_meta, double_meta);
        REQUIRE(tsl->delta_value_schema == expected_delta_map);
    }
}

TEST_CASE("ts_schemas: TSW<T>.value_schema is List<T, period>, delta is T")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");

    SECTION("tick-based window")
    {
        const auto *win = registry.tsw(double_meta, /*period=*/10, /*min_period=*/3);

        // value_schema = List<double, 10>
        const auto *expected_value = registry.list(double_meta, 10);
        REQUIRE(win->value_schema == expected_value);
        REQUIRE(win->value_schema->value_kind() == ValueTypeKind::List);
        REQUIRE(win->value_schema->element_type == double_meta);
        REQUIRE(win->value_schema->fixed_size == 10);

        // delta_value_schema = double
        REQUIRE(win->delta_value_schema == double_meta);
    }

    SECTION("duration-based window")
    {
        const auto *win = registry.tsw_duration(double_meta,
                                                TimeDelta{1'000'000},
                                                TimeDelta{100'000});

        // value_schema = List<double> (dynamic; duration-based has no fixed length)
        const auto *expected_value = registry.list(double_meta, 0);
        REQUIRE(win->value_schema == expected_value);
        REQUIRE(win->value_schema->fixed_size == 0);

        // delta_value_schema = double
        REQUIRE(win->delta_value_schema == double_meta);
    }
}

TEST_CASE("ts_schemas: TSB<{f...}>.value_schema is Bundle{f.value...}, delta is Bundle{f.delta...}")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *int_meta    = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_double   = registry.ts(double_meta);
    const auto *ts_int      = registry.ts(int_meta);

    const auto *tsb = registry.tsb("TSBSchemaTest", {{"price", ts_double}, {"size", ts_int}});

    // value_schema = Bundle{price: double, size: int}
    REQUIRE(tsb->value_schema != nullptr);
    REQUIRE(tsb->value_schema->value_kind() == ValueTypeKind::Bundle);
    REQUIRE(tsb->value_schema->field_count == 2);
    REQUIRE(std::string(tsb->value_schema->fields[0].name) == "price");
    REQUIRE(std::string(tsb->value_schema->fields[1].name) == "size");
    REQUIRE(tsb->value_schema->fields[0].type == double_meta);
    REQUIRE(tsb->value_schema->fields[1].type == int_meta);

    // delta_value_schema = Bundle{price: double, size: int} — same shape since
    // each TS<T> has delta == T.
    REQUIRE(tsb->delta_value_schema != nullptr);
    REQUIRE(tsb->delta_value_schema->value_kind() == ValueTypeKind::Bundle);
    REQUIRE(tsb->delta_value_schema->field_count == 2);
    REQUIRE(tsb->delta_value_schema->fields[0].type == double_meta);
    REQUIRE(tsb->delta_value_schema->fields[1].type == int_meta);
}

TEST_CASE("ts_schemas: collection value schemas compose from child value_schema")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *str_meta    = registry.register_scalar<std::string>("string");
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *window      = registry.tsw(double_meta, 3, 1);

    const auto *list = registry.tsl(window, 2);
    REQUIRE(list->value_schema == registry.list(window->value_schema, 2));
    REQUIRE(list->value_schema->element_type == window->value_schema);
    REQUIRE(list->delta_value_schema == registry.map(registry.value_type("int"), window->delta_value_schema));

    const auto *dict = registry.tsd(str_meta, window);
    REQUIRE(dict->value_schema == registry.map(str_meta, window->value_schema));
    REQUIRE(dict->value_schema->element_type == window->value_schema);
    REQUIRE(dict->delta_value_schema->fields[1].type == registry.map(str_meta, window->delta_value_schema));

    const auto *bundle = registry.tsb("SchemaWindowChildBundle", {{"window", window}});
    REQUIRE(bundle->value_schema->fields[0].type == window->value_schema);
    REQUIRE(bundle->delta_value_schema->fields[0].type == window->delta_value_schema);
}

TEST_CASE("ts_schemas: REF<T> value/delta schemas are TimeSeriesReference, but the REF schemas themselves are distinct")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);

    // REF's runtime value is the reference token itself, backed by the
    // TimeSeriesReference atomic.
    const auto *ref_atom = registry.value_type("TimeSeriesReference");
    REQUIRE(ref_atom != nullptr);
    REQUIRE(ref_atom->value_kind() == ValueTypeKind::Atomic);

    REQUIRE(ref_int->value_schema == ref_atom);
    REQUIRE(ref_int->delta_value_schema == ref_atom);

    // The REF *schemas* over different targets are distinct (they carry
    // the wrapped T via referenced_ts()), but their value/delta schemas
    // both point at the canonical TimeSeriesReference atomic.
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *ts_double   = registry.ts(double_meta);
    const auto *ref_double  = registry.ref(ts_double);

    REQUIRE(ref_double != ref_int);                     // distinct schemas
    REQUIRE(ref_int->referenced_ts() == ts_int);        // T preserved on each
    REQUIRE(ref_double->referenced_ts() == ts_double);
    REQUIRE(ref_double->value_schema == ref_int->value_schema);              // shared
    REQUIRE(ref_double->delta_value_schema == ref_int->delta_value_schema);  // shared
}

TEST_CASE("ts_schemas: nested compositions resolve recursively")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    const auto *double_meta = registry.register_scalar<double>("double");
    const auto *str_meta    = registry.register_scalar<std::string>("string");
    const auto *ts_double   = registry.ts(double_meta);

    // TSD<string, TSL<TS<double>>> — keyed by string, value is a list of TS<double>.
    const auto *tsl  = registry.tsl(ts_double, /*fixed_size=*/4);
    const auto *tsd  = registry.tsd(str_meta, tsl);

    // tsd.value_schema = Map<string, List<double, 4>>
    const auto *expected_value = registry.map(str_meta, registry.list(double_meta, 4));
    REQUIRE(tsd->value_schema == expected_value);

    // tsd.delta_value_schema = Bundle{removed, modified, removed_strict} where
    // the modified-map values are Map<int, double> (TSL's delta).
    REQUIRE(tsd->delta_value_schema != nullptr);
    REQUIRE(tsd->delta_value_schema->value_kind() == ValueTypeKind::Bundle);
    REQUIRE(tsd->delta_value_schema->field_count == 3);

    const auto *int_meta               = registry.value_type("int");
    const auto *expected_inner_delta   = registry.map(int_meta, double_meta);  // tsl's delta
    const auto *expected_modified_map  = registry.map(str_meta, expected_inner_delta);
    REQUIRE(tsd->delta_value_schema->fields[1].type == expected_modified_map);
}
