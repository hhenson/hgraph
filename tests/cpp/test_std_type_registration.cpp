#include <catch2/catch_test_macros.hpp>

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <type_traits>

TEST_CASE("standard scalar constructors and literals produce Python-aligned C++ types")
{
    using namespace hgraph;
    using namespace hgraph::literals;

    STATIC_REQUIRE(std::is_same_v<decltype(int_(7)), Int>);
    STATIC_REQUIRE(std::is_same_v<decltype(float_(1)), Float>);
    STATIC_REQUIRE(std::is_same_v<decltype(bool_(true)), Bool>);
    STATIC_REQUIRE(std::is_same_v<decltype(7_i), Int>);
    STATIC_REQUIRE(std::is_same_v<decltype(7_f), Float>);
    STATIC_REQUIRE(std::is_same_v<decltype(1.25_f), Float>);
    STATIC_REQUIRE(std::is_same_v<decltype("x"_str), Str>);

    CHECK(int_(7) == Int{7});
    CHECK(float_(1.5) == Float{1.5});
    CHECK(bool_(true) == Bool{true});
    CHECK(str_("abc") == Str{"abc"});
    CHECK(7_i == Int{7});
    CHECK(7_f == Float{7.0});
    CHECK(1.25_f == Float{1.25});
    CHECK("abc"_str == Str{"abc"});

    const Value int_value = stdlib::value<Int>(7);
    const Value str_value = stdlib::value<Str>("abc");
    CHECK(int_value.view().checked_as<Int>() == Int{7});
    CHECK(str_value.view().checked_as<Str>() == Str{"abc"});
}

TEST_CASE("stdlib::register_standard_types registers scalar aliases")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();

    STATIC_REQUIRE(std::is_same_v<Bool, bool>);
    STATIC_REQUIRE(std::is_same_v<Int, std::int64_t>);
    STATIC_REQUIRE(std::is_same_v<Float, double>);
    STATIC_REQUIRE(std::is_same_v<Date, std::chrono::year_month_day>);
    STATIC_REQUIRE(std::is_same_v<DateTime,
                                  std::chrono::time_point<engine_clock, std::chrono::microseconds>>);
    STATIC_REQUIRE(std::is_same_v<TimeDelta, std::chrono::microseconds>);
    STATIC_REQUIRE(std::is_same_v<Str, std::string>);

    const auto *bool_type      = registry.scalar_type<Bool>().schema();
    const auto *int_type       = registry.scalar_type<Int>().schema();
    const auto *float_type     = registry.scalar_type<Float>().schema();
    const auto *date_type      = registry.scalar_type<Date>().schema();
    const auto *datetime_type  = registry.scalar_type<DateTime>().schema();
    const auto *timedelta_type = registry.scalar_type<TimeDelta>().schema();
    const auto *str_type       = registry.scalar_type<Str>().schema();

    CHECK(registry.value_type("bool") == bool_type);
    CHECK(registry.value_type("int") == int_type);
    CHECK(registry.value_type("int64") == int_type);
    CHECK(registry.value_type("float") == float_type);
    CHECK(registry.value_type("float64") == float_type);
    CHECK(registry.value_type("double") == float_type);
    CHECK(registry.value_type("date") == date_type);
    CHECK(registry.value_type("datetime") == datetime_type);
    CHECK(registry.value_type("timedelta") == timedelta_type);
    CHECK(registry.value_type("str") == str_type);
    CHECK(registry.value_type("string") == str_type);

    CHECK(std::string{bool_type->name()} == "bool");
    CHECK(std::string{int_type->name()} == "int");
    CHECK(std::string{float_type->name()} == "float");
    CHECK(std::string{str_type->name()} == "str");

    CHECK(scalar_descriptor<Bool>::value_meta() == bool_type);
    CHECK(scalar_descriptor<Int>::value_meta() == int_type);
    CHECK(scalar_descriptor<Float>::value_meta() == float_type);
    CHECK(scalar_descriptor<Str>::value_meta() == str_type);

    CHECK(registry.value_type("int8") == registry.scalar_type<std::int8_t>().schema());
    CHECK(registry.value_type("int16") == registry.scalar_type<std::int16_t>().schema());
    CHECK(registry.value_type("int32") == registry.scalar_type<std::int32_t>().schema());
    CHECK(registry.value_type("uint8") == registry.scalar_type<std::uint8_t>().schema());
    CHECK(registry.value_type("uint16") == registry.scalar_type<std::uint16_t>().schema());
    CHECK(registry.value_type("uint32") == registry.scalar_type<std::uint32_t>().schema());
    CHECK(registry.value_type("uint64") == registry.scalar_type<std::uint64_t>().schema());
    CHECK(registry.value_type("float32") == registry.scalar_type<float>().schema());

    const auto types = stdlib::register_standard_types();
    CHECK(types.bool_type == bool_type);
    CHECK(types.int_type == int_type);
    CHECK(types.int64_type == int_type);
    CHECK(types.float_type == float_type);
    CHECK(types.float64_type == float_type);
    CHECK(types.str_type == str_type);
    CHECK(types.int32_type != int_type);
    CHECK(types.float32_type != float_type);
}

TEST_CASE("stdlib::register_standard_types registers common TS and TSS aliases")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *bool_type      = registry.value_type("bool");
    const auto *int_type       = registry.value_type("int");
    const auto *float_type     = registry.value_type("float");
    const auto *date_type      = registry.value_type("date");
    const auto *datetime_type  = registry.value_type("datetime");
    const auto *timedelta_type = registry.value_type("timedelta");
    const auto *str_type       = registry.value_type("str");

    CHECK(registry.time_series_type("TS[bool]") == registry.ts(bool_type));
    CHECK(registry.time_series_type("TS[int]") == registry.ts(int_type));
    CHECK(registry.time_series_type("TS[int64]") == registry.ts(int_type));
    CHECK(registry.time_series_type("TS[float]") == registry.ts(float_type));
    CHECK(registry.time_series_type("TS[float64]") == registry.ts(float_type));
    CHECK(registry.time_series_type("TS[double]") == registry.ts(float_type));
    CHECK(registry.time_series_type("TS[date]") == registry.ts(date_type));
    CHECK(registry.time_series_type("TS[datetime]") == registry.ts(datetime_type));
    CHECK(registry.time_series_type("TS[timedelta]") == registry.ts(timedelta_type));
    CHECK(registry.time_series_type("TS[str]") == registry.ts(str_type));
    CHECK(registry.time_series_type("TS[string]") == registry.ts(str_type));

    CHECK(registry.time_series_type("TSS[bool]") == registry.tss(bool_type));
    CHECK(registry.time_series_type("TSS[int]") == registry.tss(int_type));
    CHECK(registry.time_series_type("TSS[int64]") == registry.tss(int_type));
    CHECK(registry.time_series_type("TSS[float]") == registry.tss(float_type));
    CHECK(registry.time_series_type("TSS[float64]") == registry.tss(float_type));
    CHECK(registry.time_series_type("TSS[double]") == registry.tss(float_type));
    CHECK(registry.time_series_type("TSS[date]") == registry.tss(date_type));
    CHECK(registry.time_series_type("TSS[datetime]") == registry.tss(datetime_type));
    CHECK(registry.time_series_type("TSS[timedelta]") == registry.tss(timedelta_type));
    CHECK(registry.time_series_type("TSS[str]") == registry.tss(str_type));
    CHECK(registry.time_series_type("TSS[string]") == registry.tss(str_type));

    CHECK(registry.time_series_type("TS[datetime]")->value_schema == datetime_type);
    CHECK(registry.time_series_type("TSS[datetime]")->value_schema == registry.set(datetime_type));

    CHECK(registry.time_series_type("TS[int8]") == registry.ts(registry.value_type("int8")));
    CHECK(registry.time_series_type("TS[int32]") == registry.ts(registry.value_type("int32")));
    CHECK(registry.time_series_type("TSS[uint32]") == registry.tss(registry.value_type("uint32")));
    CHECK(registry.time_series_type("TS[float32]") == registry.ts(registry.value_type("float32")));

    const auto types = stdlib::register_standard_types();
    CHECK(types.ts_bool == registry.time_series_type("TS[bool]"));
    CHECK(types.ts_int == registry.time_series_type("TS[int]"));
    CHECK(types.ts_float == registry.time_series_type("TS[float]"));
    CHECK(types.ts_str == registry.time_series_type("TS[str]"));
    CHECK(types.tss_bool == registry.time_series_type("TSS[bool]"));
    CHECK(types.tss_int == registry.time_series_type("TSS[int]"));
    CHECK(types.tss_float == registry.time_series_type("TSS[float]"));
    CHECK(types.tss_str == registry.time_series_type("TSS[str]"));
    CHECK(types.ts_int == registry.time_series_type("TS[int64]"));
    CHECK(types.ts_float == registry.time_series_type("TS[float64]"));
    CHECK(types.ts_str == registry.time_series_type("TS[string]"));
    CHECK(registry.time_series_type("TS[int32]")->value_schema == types.int32_type);
    CHECK(registry.time_series_type("TS[int32]") != types.ts_int);
}

TEST_CASE("stdlib::register_standard_types is idempotent")
{
    using namespace hgraph;

    const auto *existing_int = TypeRegistry::instance().value_type("int");
    const auto *existing_str = TypeRegistry::instance().value_type("str");

    const auto first  = stdlib::register_standard_types();
    const auto second = stdlib::register_standard_types();

    CHECK(first.int_type == existing_int);
    CHECK(first.str_type == existing_str);
    CHECK(first.int_type == second.int_type);
    CHECK(first.float_type == second.float_type);
    CHECK(first.bool_type == second.bool_type);
    CHECK(first.ts_int == second.ts_int);
    CHECK(first.ts_float == second.ts_float);
    CHECK(first.tss_str == second.tss_str);
    CHECK(TypeRegistry::instance().value_type("int") == first.int_type);
    CHECK(TypeRegistry::instance().value_type("int64") == first.int_type);
    CHECK(TypeRegistry::instance().value_type("float64") == first.float_type);
    CHECK(TypeRegistry::instance().value_type("string") == first.str_type);
    CHECK(TypeRegistry::instance().time_series_type("TS[str]") == first.ts_str);
    CHECK(TypeRegistry::instance().time_series_type("TS[string]") == first.ts_str);
    CHECK(TypeRegistry::instance().time_series_type("TSS[int64]") == first.tss_int);
}

TEST_CASE("TypeRegistry::reset leaves the standard types registered by default")
{
    using namespace hgraph;

    // reset() restores the default-seeded vocabulary, so the standard scalar types are
    // always available without any explicit registration (the same as on construction).
    // The documented reset path (registry_reset.h): TypeRegistry::reset()
    // alone leaves borrower caches (plan factory et al) holding pointers
    // into the dropped generation - address reuse then collides.
    hgraph::reset_all_registries();

    const TypeRegistry &registry = TypeRegistry::instance();
    for (const char *name : {"bool", "int", "int64", "float", "float64", "str", "string", "date", "datetime",
                             "timedelta", "time", "bytes"})
    {
        CHECK(registry.value_type(name) != nullptr);
    }
    // The standard TS / TSS aliases come back too.
    CHECK(registry.time_series_type("TS[int]") != nullptr);
    CHECK(registry.time_series_type("TS[timedelta]") != nullptr);
    CHECK(registry.time_series_type("TS[time]") != nullptr);
    CHECK(registry.time_series_type("TS[bytes]") != nullptr);
}

TEST_CASE("time and bytes scalar atoms behave as value-layer scalars")
{
    using namespace hgraph;

    // Distinct schema identity: ``time`` is NOT ``timedelta`` and ``bytes``
    // is NOT ``str`` (the whole point of the strong types).
    CHECK(scalar_descriptor<Time>::value_meta() != scalar_descriptor<TimeDelta>::value_meta());
    CHECK(scalar_descriptor<Bytes>::value_meta() != scalar_descriptor<Str>::value_meta());
    CHECK(std::string{scalar_descriptor<Time>::value_meta()->name()} == "time");
    CHECK(std::string{scalar_descriptor<Bytes>::value_meta()->name()} == "bytes");

    const Time noon = time_of_day(12, 30, 15, 250);
    CHECK(noon == Time{45'015'000'250});
    CHECK(time_of_day(0, 0) < noon);
    CHECK(time_of_day(DateTime{TimeDelta{86'400'000'000 + 45'015'000'250}}) == noon);

    Value time_value{noon};
    CHECK(time_value.view().checked_as<Time>() == noon);
    CHECK(time_value.view().to_string() == "12:30:15.000250");

    const Bytes payload = bytes_(std::string_view{"ab\0c", 4});
    CHECK(payload.data.size() == 4);  // byte-safe: the embedded NUL survives
    Value bytes_value{payload};
    CHECK(bytes_value.view().checked_as<Bytes>() == payload);
    CHECK(bytes_value.view().to_string() == "b'ab\\x00c'");

    // Hashable — usable as TSS elements / TSD keys.
    CHECK(std::hash<Time>{}(noon) == std::hash<Time>{}(Time{45'015'000'250}));
    CHECK(std::hash<Bytes>{}(payload) == std::hash<Bytes>{}(bytes_(std::string_view{"ab\0c", 4})));
}

TEST_CASE("date-time aliases and constants match the 2603 runtime definitions")
{
    using namespace hgraph;

    STATIC_REQUIRE(std::is_same_v<engine_clock, std::chrono::system_clock>);
    STATIC_REQUIRE(std::is_same_v<DateTime,
                                  std::chrono::time_point<engine_clock, std::chrono::microseconds>>);
    STATIC_REQUIRE(std::is_same_v<TimeDelta, std::chrono::microseconds>);
    STATIC_REQUIRE(std::is_same_v<Date, std::chrono::year_month_day>);

    CHECK(MIN_DT == min_time());
    CHECK(MAX_DT == max_time());
    CHECK(MIN_ST == min_start_time());
    CHECK(MAX_ET == max_end_time());
    CHECK(MIN_TD == smallest_time_increment());

    CHECK(std::string{scalar_descriptor<Date>::value_meta()->name()} == "date");
    CHECK(std::string{scalar_descriptor<DateTime>::value_meta()->name()} == "datetime");
    CHECK(std::string{scalar_descriptor<TimeDelta>::value_meta()->name()} == "timedelta");
}
