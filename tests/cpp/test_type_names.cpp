// A type serialises as its name (RFC 0042): the registry-printed name of
// every schema parses back to the interned schema itself, and a type value
// crosses the binary and JSON codecs as its kind-tagged name.

#include <hgraph/lib/std/operators/impl/table_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_names.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/types/value/json_codec.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <array>
#include <chrono>
#include <vector>

namespace
{
    using namespace hgraph;

    [[nodiscard]] const ValueTypeMetaData *scalar(std::string_view name)
    {
        const auto *meta = TypeRegistry::instance().value_type(name);
        REQUIRE(meta != nullptr);
        return meta;
    }
}  // namespace

TEST_CASE("type names: every value schema's name parses back to the schema")
{
    stdlib::register_standard_operators();
    auto       &registry = TypeRegistry::instance();
    const auto *i        = scalar("int");
    const auto *s        = scalar("str");
    const auto *f        = scalar("float");

    const std::array<std::size_t, 2> dims{2, 3};
    const std::vector<const ValueTypeMetaData *> schemas{
        i,
        s,
        scalar("datetime"),
        scalar("type"),
        registry.tuple({i, s}),
        registry.tuple({}),
        registry.list(i, 0, true),
        registry.list(i),
        registry.fixed_list(i, 3),
        registry.fixed_list(i, 0),
        registry.nullable_tuple(i),
        registry.mutable_list(s),
        registry.set(s),
        registry.mutable_set(i),
        registry.map(s, i),
        registry.mutable_map(i, registry.list(s, 0, true)),
        registry.cyclic_buffer(f, 5),
        registry.queue(i, 0),
        registry.queue(i, 4),
        registry.array(f, 3),
        registry.array(f, 0),
        registry.array(f, std::span<const std::size_t>{dims}),
        registry.un_named_bundle({{"a", i}, {"b", registry.map(s, i)}}),
        registry.bundle("hgraph.test.names::Point", {{"x", i}, {"label", s}}),
        registry.map(s, registry.bundle("hgraph.test.names::Point", {{"x", i}, {"label", s}})),
        scalar_descriptor<stdlib::TableSchema>::value_meta(),
        registry.any(),
        registry.list(registry.any()),
        registry.opaque_python("hgraph.test.names::Opaque"),
    };
    for (const auto *schema : schemas)
    {
        INFO(schema->name());
        CHECK(parse_value_type_name(schema->name()) == schema);
    }
}

TEST_CASE("type names: every time-series schema's name parses back to the schema")
{
    stdlib::register_standard_operators();
    auto       &registry = TypeRegistry::instance();
    const auto *i        = scalar("int");
    const auto *s        = scalar("str");
    const auto *ts_int   = registry.ts(i);

    const auto *named_tsb = registry.tsb("hgraph.test.names::Pair", {{"a", ts_int}, {"b", registry.ts(s)}});
    const std::vector<const TSValueTypeMetaData *> schemas{
        ts_int,
        registry.ts(registry.list(i, 0, true)),
        registry.tss(s),
        registry.tsd(s, ts_int),
        registry.tsd(s, registry.tsd(i, registry.tss(s))),
        registry.tsl(ts_int, 3),
        registry.tsl(ts_int, 0),
        registry.tsl(ts_int, unbounded_tsl_size),
        registry.tsw(i, 5, 2),
        registry.tsw_duration(i, std::chrono::seconds{10}, std::chrono::seconds{1}),
        registry.ref(ts_int),
        registry.ref(registry.tsd(s, ts_int)),
        registry.un_named_tsb({{"a", ts_int}, {"b", registry.tss(s)}}),
        named_tsb,
        registry.tsd(s, named_tsb),
        registry.signal(),
        registry.ts(scalar_descriptor<stdlib::TableSchema>::value_meta()),
    };
    for (const auto *schema : schemas)
    {
        INFO(schema->name());
        CHECK(parse_ts_type_name(schema->name()) == schema);
    }
}

TEST_CASE("type names: an unknown or malformed name is an error, not a guess")
{
    stdlib::register_standard_operators();
    CHECK_THROWS_WITH(parse_value_type_name("hgraph.test.names::Nope"),
                      Catch::Matchers::ContainsSubstring("is not a value type the registry knows"));
    CHECK_THROWS_AS(parse_value_type_name("Map[int]"), std::invalid_argument);
    CHECK_THROWS_AS(parse_value_type_name("List[int,x]"), std::invalid_argument);
    CHECK_THROWS_AS(parse_ts_type_name("TS[int"), std::invalid_argument);
    CHECK_THROWS_AS(parse_ts_type_name("TSX[int]"), std::invalid_argument);
    CHECK_THROWS_AS(parse_type_value("int"), std::invalid_argument);  // no kind
}

TEST_CASE("type values: the serialised form names the kind, so a TSB and its bundle stay apart")
{
    stdlib::register_standard_operators();
    auto       &registry = TypeRegistry::instance();
    const auto *ts_int   = registry.ts(scalar("int"));
    const auto *tsb      = registry.tsb("hgraph.test.names::Tagged", {{"a", ts_int}});
    const auto *bundle   = registry.value_type("hgraph.test.names::Tagged");
    REQUIRE(bundle != nullptr);
    REQUIRE(tsb->name() == bundle->name());  // one name, two kinds

    const auto as_ts     = TypeCarrier::of_ts(tsb);
    const auto as_scalar = TypeCarrier::of_scalar(bundle);
    CHECK(serialise_type_value(as_ts) == "ts:hgraph.test.names::Tagged");
    CHECK(serialise_type_value(as_scalar) == "scalar:hgraph.test.names::Tagged");
    CHECK(parse_type_value(serialise_type_value(as_ts)) == as_ts);
    CHECK(parse_type_value(serialise_type_value(as_scalar)) == as_scalar);
    CHECK(parse_type_value("size:3") == TypeCarrier::of_size(3));
    CHECK(serialise_type_value(TypeCarrier::of_size(unbounded_tsl_size)) == "size:-1");
    CHECK(parse_type_value("size:-1") == TypeCarrier::of_size(unbounded_tsl_size));
    // The text stays the bare name.
    CHECK(Value{as_ts}.to_string() == "hgraph.test.names::Tagged");
}

TEST_CASE("type values: a TableSchema round-trips through the binary and JSON codecs")
{
    stdlib::register_standard_operators();
    const Value schema = stdlib::table_schema_value(ts_type<TSD<Str, TS<Int>>>(), table::TableConfig{});

    const Value from_binary = from_binary_string(schema.schema(), to_binary_string(schema.view()));
    CHECK(from_binary.view().equals(schema.view()));

    const std::string json = to_json_string(schema.view());
    CHECK_THAT(json, Catch::Matchers::ContainsSubstring("\"ts:TSD[str,TS[int]]\""));
    CHECK_THAT(json, Catch::Matchers::ContainsSubstring("\"scalar:int\""));
    const Value from_json = from_json_string(schema.schema(), json);
    CHECK(from_json.view().equals(schema.view()));
}
