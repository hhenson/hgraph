#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/impl/json_impl.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/temporal.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>

// JSON serialization — step 1 of the record/replay/table design record: the
// interned per-schema JsonConverter (serializer-ops pattern) plus the
// to_json/from_json operators. The wire format mirrors the Python
// implementation (ext/main/hgraph/_impl/_operators/_to_json.py).

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    template <typename T>
    std::string round_trip(const T &input)
    {
        Value       value{input};
        std::string text = to_json_string(value.view());
        Value       back = from_json_string(value.view().schema(), text);
        CHECK(back.view().template checked_as<T>() == input);
        return text;
    }

    template <typename T>
    T parse_json_value(std::string_view text)
    {
        return from_json_string(
                   scalar_descriptor<T>::value_meta(), text)
            .view().template checked_as<T>();
    }

    DateTime utc_instant(
        int year, unsigned month, unsigned day, int hour = 0,
        int minute = 0, int second = 0, int microsecond = 0)
    {
        using namespace std::chrono;
        const Date date{
            std::chrono::year{year}, std::chrono::month{month},
            std::chrono::day{day}};
        return DateTime{
            duration_cast<microseconds>(sys_days{date}.time_since_epoch()) +
            hours{hour} + minutes{minute} + seconds{second} +
            microseconds{microsecond}};
    }
}  // namespace

TEST_CASE("json: atomic values round-trip in the Python wire format")
{
    CHECK(round_trip(Int{42}) == "42");
    CHECK(round_trip(Int{-7}) == "-7");
    CHECK(round_trip(Bool{true}) == "true");
    CHECK(round_trip(Float{2.5}) == "2.5");
    CHECK(round_trip(Str{"plain"}) == "\"plain\"");
    CHECK(round_trip(Str{"quote \" slash \\ tab \t"}) == "\"quote \\\" slash \\\\ tab \\t\"");

    using namespace std::chrono;
    CHECK(round_trip(Date{year{2020}, month{1}, day{31}}) == "\"2020-01-31\"");
    CHECK(round_trip(DateTime{sys_days{Date{year{2021}, month{6}, day{2}}}.time_since_epoch() +
                              microseconds{3'723'000'014}}) ==
          "\"2021-06-02T01:02:03.000014Z\"");
    CHECK(round_trip(TimeDelta{microseconds{2 * 86'400'000'000 + 3'723'500'000}}) ==
          "\"176523500000us\"");
    CHECK(round_trip(time_of_day(9, 30, 5, 250)) == "\"09:30:05.000250\"");
    CHECK(round_trip(time_of_day(9, 30, 5)) == "\"09:30:05\"");
}

TEST_CASE("json: temporal version 2 scalar and range forms round-trip")
{
    using namespace std::chrono;
    const CivilDate day{year{2025}, month{11}, std::chrono::day{2}};
    const CivilDateTime local{day, 1, 30, 0, 123};
    CHECK(round_trip(local) == "\"2025-11-02T01:30:00.000123\"");
    CHECK(round_trip(Period{1, -2, 3}) ==
          "{\"months\": 10, \"days\": 3}");
    CHECK(round_trip(ZoneId{"America/New_York"}) ==
          "\"America/New_York\"");
    const auto provider = make_time_zone_provider();
    const ZonedDateTime zoned = resolve(
        CivilDateTime{day, 1, 30}, ZoneId{"America/New_York"},
        *provider, AmbiguousTimePolicy::Latest);
    CHECK(round_trip(zoned) ==
          "\"2025-11-02T01:30:00-05:00[America/New_York]\"");
    CHECK_THROWS_AS(
        from_json_string(
            scalar_descriptor<ZonedDateTime>::value_meta(),
            "\"2025-11-02T01:30:00-03:00[America/New_York]\""),
        std::invalid_argument);

    const InstantRange range = InstantRange::bounded(
        Instant{microseconds{0}}, Instant{microseconds{10}},
        Boundary::Closed, Boundary::Open);
    CHECK(round_trip(range) ==
          "{\"start\": \"1970-01-01T00:00:00Z\", \"end\": "
          "\"1970-01-01T00:00:00.000010Z\", \"lower\": \"closed\", "
          "\"upper\": \"open\"}");
    CHECK(round_trip(InstantRange::all()) ==
          "{\"start\": null, \"end\": null, \"lower\": \"open\", "
          "\"upper\": \"open\"}");
    CHECK(round_trip(InstantRange::make_empty()) ==
          "{\"empty\": true}");

    const InstantRangeSet ranges{
        range,
        InstantRange::bounded(Instant{microseconds{20}},
                              Instant{microseconds{30}})};
    CHECK(round_trip(ranges) ==
          "[{\"start\": \"1970-01-01T00:00:00Z\", \"end\": "
          "\"1970-01-01T00:00:00.000010Z\", \"lower\": \"closed\", "
          "\"upper\": \"open\"}, {\"start\": "
          "\"1970-01-01T00:00:00.000020Z\", \"end\": "
          "\"1970-01-01T00:00:00.000030Z\", \"lower\": \"closed\", "
          "\"upper\": \"open\"}]");
}

TEST_CASE("json: schema-directed temporal reads accept legacy version 1 text")
{
    using namespace std::chrono;
    const DateTime expected_instant{
        sys_days{Date{year{2024}, month{6}, day{13}}}.time_since_epoch() +
        hours{10} + minutes{15} + seconds{30} + microseconds{42}};
    const Value instant = from_json_string(
        scalar_descriptor<DateTime>::value_meta(),
        "\"2024-06-13 10:15:30.000042\"");
    CHECK(instant.view().checked_as<DateTime>() == expected_instant);
    CHECK(to_json_string(instant.view()) ==
          "\"2024-06-13T10:15:30.000042Z\"");

    const Value positive_duration = from_json_string(
        scalar_descriptor<TimeDelta>::value_meta(),
        "\"10:0:0:15.000042\"");
    CHECK(positive_duration.view().checked_as<TimeDelta>() ==
          TimeDelta{10 * 86'400'000'000LL + 15'000'042});
    CHECK(to_json_string(positive_duration.view()) ==
          "\"864015000042us\"");

    const Value negative_duration = from_json_string(
        scalar_descriptor<TimeDelta>::value_meta(),
        "\"-1:23:59:59.999999\"");
    CHECK(negative_duration.view().checked_as<TimeDelta>() ==
          TimeDelta{-1});
    CHECK(to_json_string(negative_duration.view()) == "\"-1us\"");

    const Value early_native_negative = from_json_string(
        scalar_descriptor<TimeDelta>::value_meta(),
        "\"0:0:0:0.-00001\"");
    CHECK(early_native_negative.view().checked_as<TimeDelta>() ==
          TimeDelta{-1});
}

TEST_CASE("json: temporal reads accept ISO, compact, fallback, and registered formats")
{
    CHECK(parse_json_value<DateTime>("\"2024-06-13T10:15\"") ==
          utc_instant(2024, 6, 13, 10, 15));
    CHECK(parse_json_value<DateTime>("\"2024-06-13\"") ==
          utc_instant(2024, 6, 13));
    CHECK(parse_json_value<DateTime>("\"2024-06-13T10:15:30.5\"") ==
          utc_instant(2024, 6, 13, 10, 15, 30, 500'000));
    CHECK(parse_json_value<DateTime>("\"20240613T101530\"") ==
          utc_instant(2024, 6, 13, 10, 15, 30));
    CHECK(parse_json_value<DateTime>("\"20240613101530\"") ==
          utc_instant(2024, 6, 13, 10, 15, 30));
    CHECK(parse_json_value<DateTime>("\"20240613101530123456\"") ==
          utc_instant(2024, 6, 13, 10, 15, 30, 123'456));
    CHECK(parse_json_value<DateTime>("\"2024-06-13T11:15:30+01:00\"") ==
          utc_instant(2024, 6, 13, 10, 15, 30));
    CHECK(parse_json_value<DateTime>("\"2024-06-13T05:15:30-05:00\"") ==
          utc_instant(2024, 6, 13, 10, 15, 30));
    CHECK(parse_json_value<DateTime>("\"2024/06/13 10:15:30\"") ==
          utc_instant(2024, 6, 13, 10, 15, 30));
    CHECK(parse_json_value<DateTime>("\"13-Jun-2024 10:15:30\"") ==
          utc_instant(2024, 6, 13, 10, 15, 30));
    CHECK(parse_json_value<DateTime>("\"13 Jun 2024\"") ==
          utc_instant(2024, 6, 13));

    CHECK(parse_json_value<Time>("\"10:15\"") == time_of_day(10, 15));
    CHECK(parse_json_value<Time>("\"101530\"") ==
          time_of_day(10, 15, 30));
    CHECK(parse_json_value<Time>("\"101530123456\"") ==
          time_of_day(10, 15, 30, 123'456));
    CHECK(parse_json_value<Date>("\"20240613\"") ==
          Date{std::chrono::year{2024}, std::chrono::June,
               std::chrono::day{13}});
    CHECK(parse_json_value<Date>("\"2024/06/13\"") ==
          Date{std::chrono::year{2024}, std::chrono::June,
               std::chrono::day{13}});
    CHECK(parse_json_value<Date>("\"13-Jun-2024\"") ==
          Date{std::chrono::year{2024}, std::chrono::June,
               std::chrono::day{13}});
    CHECK(parse_json_value<Date>("\"2024-06-13T10:15:30\"") ==
          Date{std::chrono::year{2024}, std::chrono::June,
               std::chrono::day{13}});

    register_json_datetime_format("%d/%m/%Y %H:%M:%S");
    CHECK(parse_json_value<DateTime>("\"13/06/2024 10:15:30\"") ==
          utc_instant(2024, 6, 13, 10, 15, 30));
    register_json_datetime_format("%Y-%m-%d %H:%M:%S,%f");
    CHECK(parse_json_value<DateTime>(
              "\"2024-06-13 10:15:30,123456\"") ==
          utc_instant(2024, 6, 13, 10, 15, 30, 123'456));
    register_json_datetime_format("%I.%M.%S %p", true);
    CHECK(parse_json_value<Time>("\"10.15.30 PM\"") ==
          time_of_day(22, 15, 30));
    CHECK(parse_json_value<Time>("\"12.00.00 AM\"") ==
          time_of_day(0, 0, 0));
    CHECK(parse_json_value<Time>("\"12.00.00 PM\"") ==
          time_of_day(12, 0, 0));
    register_json_datetime_format("%H:%M:%S,%f", true);
    CHECK(parse_json_value<Time>("\"10:15:30,000042\"") ==
          time_of_day(10, 15, 30, 42));

    try
    {
        static_cast<void>(
            parse_json_value<DateTime>("\"not a datetime\""));
        FAIL("invalid datetime text should be rejected");
    }
    catch (const std::invalid_argument &error)
    {
        CHECK(std::string{error.what()}.find("not a datetime") !=
              std::string::npos);
    }
}

TEST_CASE("json: containers round-trip (list, set, map with string and int keys)")
{
    const Value list = stdlib::make_list<Int>({1, 2, 3});
    CHECK(to_json_string(list.view()) == "[1, 2, 3]");
    const Value list_back = from_json_string(list.view().schema(), "[1, 2, 3]");
    CHECK(list_back.view().as_list().size() == 3);
    CHECK(list_back.view().as_list().at(1).checked_as<Int>() == Int{2});

    const Value set = stdlib::make_set<Int>({5});
    CHECK(to_json_string(set.view()) == "[5]");
    const Value set_back = from_json_string(set.view().schema(), "[5, 6]");
    CHECK(set_back.view().as_set().size() == 2);

    const Value map = stdlib::make_map<Str, Int>({{Str{"a"}, Int{1}}});
    CHECK(to_json_string(map.view()) == "{\"a\": 1}");
    const Value map_back = from_json_string(map.view().schema(), "{\"a\": 1, \"b\": 2}");
    CHECK(map_back.view().as_map().size() == 2);

    // Non-string keys render their token quoted (the Python rule).
    const Value int_map = stdlib::make_map<Int, Str>({{Int{42}, Str{"x"}}});
    CHECK(to_json_string(int_map.view()) == "{\"42\": \"x\"}");
    const Value int_map_back = from_json_string(int_map.view().schema(), "{\"42\": \"x\"}");
    CHECK(int_map_back.view().as_map().size() == 1);

    // Structured temporal keys are encoded as escaped JSON object text and
    // decoded as JSON again, rather than being treated as plain strings.
    const Value period_map =
        stdlib::make_map<Period, Str>({{Period{0, 1, 2}, Str{"month"}}});
    CHECK(to_json_string(period_map.view()) ==
          "{\"{\\\"months\\\": 1, \\\"days\\\": 2}\": \"month\"}");
    const Value period_map_back = from_json_string(
        period_map.view().schema(),
        "{\"{\\\"months\\\": 1, \\\"days\\\": 2}\": \"month\"}");
    REQUIRE(period_map_back.view().as_map().size() == 1);
    const auto [period_key, period_value] =
        *period_map_back.view().as_map().begin();
    CHECK(period_key.checked_as<Period>() == Period{0, 1, 2});
    CHECK(period_value.checked_as<Str>() == Str{"month"});
}

TEST_CASE("json: bundles serialize as objects; unknown fields are skipped on read")
{
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *bundle_meta = registry.un_named_bundle({{"count", int_meta}, {"label", str_meta}});

    const auto binding = ValuePlanFactory::instance().type_for(bundle_meta);
    BundleBuilder builder{binding};
    builder.set("count", Value{Int{3}});
    builder.set("label", Value{Str{"here"}});
    const Value bundle = builder.build();

    const std::string text = to_json_string(bundle.view());
    CHECK(text == "{\"count\": 3, \"label\": \"here\"}");

    // Field order and unknown fields are tolerated on read.
    const Value back = from_json_string(
        bundle_meta, "{\"label\": \"here\", \"ignored\": [1, {\"x\": 2}], \"count\": 3}");
    CHECK(back.view().as_bundle().at(0).checked_as<Int>() == Int{3});
    CHECK(back.view().as_bundle().at(1).checked_as<Str>() == Str{"here"});
}

TEST_CASE("json: unset bundle fields are omitted on write and null on read")
{
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *bundle_meta = registry.un_named_bundle({{"count", int_meta}, {"label", str_meta}});

    const auto binding = ValuePlanFactory::instance().type_for(bundle_meta);
    BundleBuilder builder{binding};
    builder.set("count", Value{Int{4}});
    const Value partial = builder.build();

    CHECK(to_json_string(partial.view()) == "{\"count\": 4}");

    const Value back = from_json_string(bundle_meta, "{\"count\": 5, \"label\": null}");
    auto        bundle = back.view().as_bundle();
    CHECK(bundle.at("count").checked_as<Int>() == Int{5});
    CHECK_FALSE(bundle.at("label").has_value());
}

namespace
{
    struct FromJsonGraph
    {
        [[maybe_unused]] static constexpr auto name = "from_json_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            return wire<stdlib::from_json, TS<Int>>(w, ts).as<TS<Int>>();
        }
    };

    struct FromJsonDateTimeGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "from_json_datetime_graph";

        static Port<TS<DateTime>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            return wire<stdlib::from_json, TS<DateTime>>(w, ts)
                .as<TS<DateTime>>();
        }
    };

    struct JsonRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_round_trip_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> ts)
        {
            auto text = wire<stdlib::to_json>(w, ts);
            return wire<stdlib::from_json, TS<Float>>(w, text).as<TS<Float>>();
        }
    };

    struct JsonDynamicLeafGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_leaf_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            auto decoded = wire<stdlib::json_decode>(w, ts);
            auto nested  = wire<stdlib::getitem_>(w, decoded, Str{"nested"});
            auto values  = wire<stdlib::getitem_>(w, nested, Str{"values"});
            auto last    = wire<stdlib::getitem_>(w, values, Int{-1});
            return wire<stdlib::json_as_int>(w, last).as<TS<Int>>();
        }
    };

    struct JsonDynamicEncodeGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_encode_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            auto decoded = wire<stdlib::json_decode>(w, ts);
            return wire<stdlib::json_encode, TS<Str>>(w, decoded).as<TS<Str>>();
        }
    };

    struct JsonDynamicEqualityGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_equality_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> lhs, Port<TS<Str>> rhs)
        {
            auto decoded_lhs = wire<stdlib::json_decode>(w, lhs);
            auto decoded_rhs = wire<stdlib::json_decode>(w, rhs);
            return wire<stdlib::eq_>(w, decoded_lhs, decoded_rhs).as<TS<Bool>>();
        }
    };

    [[nodiscard]] Value eager_reference_json()
    {
        const auto &str_binding = ValuePlanFactory::instance().type_for(scalar_descriptor<Str>::value_meta());

        MapBuilder target_entries{str_binding, stdlib::json_tree::json_value_binding()};
        Value      answer_key{Str{"answer"}};
        Value      answer_node = stdlib::json_tree::box(Value{Int{41}});
        target_entries.set_item_copy(answer_key.view().data(), answer_node.view().data());

        MapBuilder root_entries{str_binding, stdlib::json_tree::json_value_binding()};
        Value      target_key{Str{"target"}};
        Value      target_node = stdlib::json_tree::box(target_entries.build());
        root_entries.set_item_copy(target_key.view().data(), target_node.view().data());
        return stdlib::json_tree::box(root_entries.build());
    }

    struct EagerJsonReferenceNode
    {
        static constexpr auto name = "eager_json_reference_node";

        static void resolve_default_types(ResolutionMap &resolution)
        {
            if (resolution.find_ts("O") != nullptr) { return; }
            resolution.bind_ts("O", TypeRegistry::instance().ts(stdlib::json_tree::json_meta()));
        }

        static void eval(In<"trigger", TS<Str>> trigger, Out<TsVar<"O">> out)
        {
            static_cast<void>(trigger);
            stdlib::json_tree::publish(static_cast<const TSOutputView &>(out), eager_reference_json());
        }
    };

    struct JsonDynamicLazyEagerEqualityGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_lazy_eager_equality_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> raw)
        {
            auto decoded = wire<stdlib::json_decode>(w, raw);
            auto eager   = wire<EagerJsonReferenceNode>(w, raw);
            return wire<stdlib::eq_>(w, decoded, eager).as<TS<Bool>>();
        }
    };

    struct JsonDynamicLazyEagerInequalityGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_lazy_eager_inequality_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> raw)
        {
            auto decoded = wire<stdlib::json_decode>(w, raw);
            auto eager   = wire<EagerJsonReferenceNode>(w, raw);
            return wire<stdlib::ne_>(w, decoded, eager).as<TS<Bool>>();
        }
    };

    struct JsonDynamicLazyEagerCompareGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_lazy_eager_compare_graph";

        static Port<TS<stdlib::CmpResult>> compose(Wiring &w, Port<TS<Str>> raw)
        {
            auto decoded = wire<stdlib::json_decode>(w, raw);
            auto eager   = wire<EagerJsonReferenceNode>(w, raw);
            return wire<stdlib::cmp_>(w, decoded, eager).as<TS<stdlib::CmpResult>>();
        }
    };
}  // namespace

TEST_CASE("json operators: to_json serializes per tick")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Int>(42, none, -1)),
                 values<Str>(Str{"42"}, none, Str{"-1"}));
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Str>(Str{"a\"b"})), values<Str>(Str{"\"a\\\"b\""}));
    // The delta of a TS<scalar> IS its value: delta mode matches value mode.
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Int>(7), true), values<Str>(Str{"7"}));
}

TEST_CASE("json operators: from_json parses into the resolved output type")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<FromJsonGraph>(values<Str>(Str{"3"}, none, Str{"-9"})),
                 values<Int>(3, none, -9));
}

TEST_CASE("json operators: temporal parsing uses the shared native format registry")
{
    stdlib::register_standard_operators();
    register_json_datetime_format("%d/%m/%Y %H:%M:%S");
    CHECK_OUTPUT(
        eval_node<FromJsonDateTimeGraph>(values<Str>(
            Str{"\"2024-06-13T11:15:30+01:00\""},
            Str{"\"13/06/2024 10:15:30\""})),
        values<DateTime>(
            utc_instant(2024, 6, 13, 10, 15, 30),
            utc_instant(2024, 6, 13, 10, 15, 30)));
}

TEST_CASE("json operators: to_json -> from_json round-trips through a graph")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonRoundTripGraph>(values<Float>(1.5, none, -0.25)),
                 values<Float>(1.5, none, -0.25));
}

TEST_CASE("dynamic json operators: decoded values support lazy path extraction")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicLeafGraph>(
                     values<Str>(Str{"{\"nested\":{\"values\":[1,2,3,4]}}"},
                                 Str{"{\"nested\":{\"values\":[5,6]}}"})),
                 values<Int>(4, 6));
}

TEST_CASE("dynamic json operators: decoded values encode canonically")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicEncodeGraph>(
                     values<Str>(Str{"{\"a\":1,\"b\":[true,null,\"x\"]}"})),
                 values<Str>(Str{"{\"a\": 1, \"b\": [true, null, \"x\"]}"}));
}

TEST_CASE("dynamic json operators: equality is semantic not raw string equality")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicEqualityGraph>(
                     values<Str>(Str{"{\"a\":1,\"b\":[2,3]}"}),
                     values<Str>(Str{"{ \"b\" : [2, 3], \"a\" : 1 }"})),
                 values<Bool>(true));
}

TEST_CASE("dynamic json operators: equality is independent of lazy or eager storage")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicLazyEagerEqualityGraph>(
                     values<Str>(Str{"{\"target\":{\"answer\":41}}"},
                                 Str{"{\"target\":{\"answer\":42}}"})),
                 values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<JsonDynamicLazyEagerInequalityGraph>(
                     values<Str>(Str{"{\"target\":{\"answer\":41}}"},
                                 Str{"{\"target\":{\"answer\":42}}"})),
                 values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<JsonDynamicLazyEagerCompareGraph>(
                     values<Str>(Str{"{\"target\":{\"answer\":41}}"})),
                 values<stdlib::CmpResult>(stdlib::CmpResult::EQ));
}
