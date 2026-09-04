#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/impl/json_impl.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/temporal.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <string>

// JSON serialization — step 1 of the record/replay/table design record: the
// interned per-schema JsonConverter (serializer-ops pattern) plus the
// to_json/from_json operators. The wire format mirrors the Python
// implementation (release/0.5:hgraph/_impl/_operators/_to_json.py).

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

TEST_CASE("json: control characters escape as RFC 8259 requires")
{
    // Every character below 0x20 MUST be escaped: \b and \f have short
    // forms, anything else falls back to \u00XX. Escaping walks runs, so a
    // clean span either side of an escape must survive the bulk copy.
    CHECK(round_trip(Str{"back\bspace"}) == "\"back\\bspace\"");
    CHECK(round_trip(Str{"form\ffeed"}) == "\"form\\ffeed\"");
    CHECK(round_trip(Str{"unit\x1f" "sep"}) == "\"unit\\u001fsep\"");
    CHECK(round_trip(Str{std::string("nul\0byte", 8)}) == "\"nul\\u0000byte\"");
    CHECK(round_trip(Str{"lead\ttrail"}) == "\"lead\\ttrail\"");
    CHECK(round_trip(Str{"\x01" "\x02"}) == "\"\\u0001\\u0002\"");
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

TEST_CASE("json: a bound structural converter decodes without type-system locks")
{
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *datetime_meta =
        registry.register_scalar<DateTime>("datetime");
    const auto *items_meta = registry.list(int_meta);
    const auto *tags_meta = registry.set(int_meta);
    const auto *counts_meta = registry.map(str_meta, int_meta);
    const auto *bundle_meta = registry.un_named_bundle(
        {{"count", int_meta},
         {"label", str_meta},
         {"as_of", datetime_meta},
         {"items", items_meta},
         {"tags", tags_meta},
         {"counts", counts_meta}});
    const auto converter = bind_json_converter(bundle_meta);

    const auto before = type_system_lock_count();
    for (int i = 0; i < 64; ++i)
    {
        const Value decoded = from_json_string(
            converter,
            R"({"count": 5, "label": "ready", "as_of": "2024-06-13T10:15:30.123456+00:00", "items": [1, 2, 3], "tags": [2, 4], "counts": {"left": 7}})");
        const auto bundle = decoded.view().as_bundle();
        CHECK(bundle.at("count").checked_as<Int>() == Int{5});
        CHECK(bundle.at("label").checked_as<Str>() == Str{"ready"});
        CHECK(bundle.at("as_of").checked_as<DateTime>() ==
              utc_instant(2024, 6, 13, 10, 15, 30, 123'456));
        const auto items = bundle.at("items").as_list();
        REQUIRE(items.size() == 3);
        CHECK(items.at(2).checked_as<Int>() == Int{3});
        CHECK(bundle.at("tags").as_set().size() == 2);
        CHECK(bundle.at("counts").as_map().size() == 1);
    }
    CHECK(type_system_lock_count() == before);
}

TEST_CASE("json: a run-bound polymorphic converter owns its complete plan")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<Int>("int");
    const auto *base = registry.bundle(
        "tests.bound_json", "Base", {{"id", integer}}, {}, true);
    const auto *child = registry.bundle(
        "tests.bound_json", "Child",
        {{"id", integer}, {"quantity", integer}}, {base});
    const auto realization = TypeRealizationSnapshot::capture(registry);

    BoundJsonConverter converter;
    {
        TypeRealizationScope scope{realization.get()};
        converter = bind_json_converter(base);
    }
    const ValueTypeRef expected_binding = realization->type_for(base);

    const auto before = type_system_lock_count();
    for (int i = 0; i < 64; ++i)
    {
        const Value decoded = from_json_string(
            converter,
            R"({"__type__": "tests.bound_json::Child", "id": 1, "quantity": 2})");
        REQUIRE(decoded.binding() == expected_binding);
        REQUIRE(decoded.view().concrete().schema() == child);

        std::string encoded;
        converter.write(decoded.view(), encoded);
        CHECK(encoded.find(
                  R"("__type__": "tests.bound_json::Child")") !=
              std::string::npos);
    }
    CHECK(type_system_lock_count() == before);
}

TEST_CASE("json: ad-hoc binding captures polymorphism outside a graph scope")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<Int>("int");
    const auto *base = registry.bundle(
        "tests.adhoc_bound_json", "Base", {{"id", integer}}, {}, true);
    const auto *child = registry.bundle(
        "tests.adhoc_bound_json", "Child",
        {{"id", integer}, {"quantity", integer}}, {base});
    const auto realization = TypeRealizationSnapshot::capture(registry);

    Value escaped;
    {
        TypeRealizationScope scope{realization.get()};
        const auto graph_converter = bind_json_converter(base);
        escaped = from_json_string(
            graph_converter,
            R"({"__type__": "tests.adhoc_bound_json::Child", "id": 1, "quantity": 2})");
    }
    REQUIRE(active_type_realization() == nullptr);
    REQUIRE(escaped.view().concrete().schema() == child);

    const auto ad_hoc_converter = bind_json_converter(base);
    const auto before = type_system_lock_count();
    std::string encoded;
    ad_hoc_converter.write(escaped.view(), encoded);
    CHECK(encoded.find(
              R"("__type__": "tests.adhoc_bound_json::Child")") !=
          std::string::npos);
    CHECK(encoded.find(R"("quantity": 2)") != std::string::npos);

    const Value decoded = from_json_string(ad_hoc_converter, encoded);
    CHECK(decoded.view().concrete().schema() == child);
    CHECK(type_system_lock_count() == before);
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
