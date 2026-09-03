// Tests for the temporal literal module: the "Literals" and "Canonical
// spelling" rules of docs/developer-guide/syntax-and-semantics.md.

#include "syntax/temporal.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace
{
    using hgl::syntax::CivilDate;
    using hgl::syntax::TemporalKind;
    using hgl::syntax::TemporalParseResult;
    using hgl::syntax::TemporalValue;

    constexpr std::int64_t us_per_second = 1'000'000;
    constexpr std::int64_t us_per_minute = 60 * us_per_second;
    constexpr std::int64_t us_per_hour = 60 * us_per_minute;
    constexpr std::int64_t us_per_day = 24 * us_per_hour;

    // Independently derived expectations (days since 1970-01-01, UTC micros).
    constexpr std::int64_t days_2026_09_03 = 20699;
    constexpr std::int64_t days_2026_11_01 = 20758;
    constexpr std::int64_t instant_2026_09_03_0930z = 1'788'427'800'000'000;
    constexpr std::int64_t instant_2026_11_01_0530z = 1'793'511'000'000'000;
    constexpr std::int64_t instant_2026_11_01_0630z = 1'793'514'600'000'000;
    constexpr std::int64_t civil_2026_09_03_1030 = 1'788'431'400'000'000;

    [[nodiscard]] TemporalValue parse_ok(std::string_view spelling)
    {
        const TemporalParseResult result = hgl::syntax::parse_temporal_literal(spelling);
        INFO("spelling: " << spelling << " error: " << result.error);
        REQUIRE(result.value.has_value());
        REQUIRE(result.error.empty());
        return *result.value;
    }

    [[nodiscard]] TemporalParseResult parse_fail(std::string_view spelling)
    {
        const TemporalParseResult result = hgl::syntax::parse_temporal_literal(spelling);
        INFO("spelling: " << spelling);
        REQUIRE_FALSE(result.value.has_value());
        REQUIRE_FALSE(result.error.empty());
        return result;
    }

    [[nodiscard]] TemporalValue duration(std::int64_t micros)
    {
        return TemporalValue{.kind = TemporalKind::Duration, .micros = micros};
    }

    [[nodiscard]] bool contains(std::string_view text, std::string_view needle)
    {
        return text.find(needle) != std::string_view::npos;
    }
} // namespace

// ---- kind names -------------------------------------------------------------

TEST_CASE("temporal kind names are the language's type names", "[temporal]")
{
    CHECK(hgl::syntax::temporal_kind_name(TemporalKind::Date) == "date");
    CHECK(hgl::syntax::temporal_kind_name(TemporalKind::Time) == "time");
    CHECK(hgl::syntax::temporal_kind_name(TemporalKind::DateTime) == "datetime");
    CHECK(hgl::syntax::temporal_kind_name(TemporalKind::CivilDateTime) == "civil_datetime");
    CHECK(hgl::syntax::temporal_kind_name(TemporalKind::ZonedDateTime) == "zoned_datetime");
    CHECK(hgl::syntax::temporal_kind_name(TemporalKind::ZonedTime) == "zoned_time");
    CHECK(hgl::syntax::temporal_kind_name(TemporalKind::TimeZone) == "timezone");
    CHECK(hgl::syntax::temporal_kind_name(TemporalKind::Duration) == "duration");
}

// ---- calendar arithmetic ----------------------------------------------------

TEST_CASE("days_from_civil counts days since the epoch", "[temporal]")
{
    CHECK(hgl::syntax::days_from_civil(1970, 1, 1) == 0);
    CHECK(hgl::syntax::days_from_civil(1969, 12, 31) == -1);
    CHECK(hgl::syntax::days_from_civil(2026, 9, 3) == days_2026_09_03);
    CHECK(hgl::syntax::days_from_civil(2026, 11, 1) == days_2026_11_01);
    CHECK(hgl::syntax::days_from_civil(2000, 2, 29) == 11016);
    CHECK(hgl::syntax::days_from_civil(1, 1, 1) == -719162);
    CHECK(hgl::syntax::days_from_civil(9999, 12, 31) == 2932896);
}

TEST_CASE("days_from_civil rejects dates that are not on the calendar", "[temporal]")
{
    CHECK_FALSE(hgl::syntax::days_from_civil(2026, 2, 29).has_value());
    CHECK_FALSE(hgl::syntax::days_from_civil(1900, 2, 29).has_value());
    CHECK(hgl::syntax::days_from_civil(2024, 2, 29).has_value());
    CHECK(hgl::syntax::days_from_civil(2000, 2, 29).has_value());
    CHECK_FALSE(hgl::syntax::days_from_civil(2026, 4, 31).has_value());
    CHECK_FALSE(hgl::syntax::days_from_civil(2026, 13, 1).has_value());
    CHECK_FALSE(hgl::syntax::days_from_civil(2026, 0, 1).has_value());
    CHECK_FALSE(hgl::syntax::days_from_civil(2026, 1, 0).has_value());
    CHECK_FALSE(hgl::syntax::days_from_civil(2026, 1, 32).has_value());
    CHECK_FALSE(hgl::syntax::days_from_civil(0, 1, 1).has_value());
    CHECK_FALSE(hgl::syntax::days_from_civil(10000, 1, 1).has_value());
    CHECK_FALSE(hgl::syntax::days_from_civil(-1, 1, 1).has_value());
}

TEST_CASE("civil_from_days inverts days_from_civil", "[temporal]")
{
    const CivilDate epoch = hgl::syntax::civil_from_days(0);
    CHECK(epoch.year == 1970);
    CHECK(epoch.month == 1);
    CHECK(epoch.day == 1);

    const CivilDate before = hgl::syntax::civil_from_days(-1);
    CHECK(before.year == 1969);
    CHECK(before.month == 12);
    CHECK(before.day == 31);

    const CivilDate leap = hgl::syntax::civil_from_days(11016);
    CHECK(leap.year == 2000);
    CHECK(leap.month == 2);
    CHECK(leap.day == 29);

    // Every day of every spellable year round-trips and the calendar is monotonic.
    std::int64_t mismatches = 0;
    CivilDate previous = hgl::syntax::civil_from_days(-719162);
    for (std::int64_t day = -719162; day <= 2932896; ++day)
    {
        const CivilDate date = hgl::syntax::civil_from_days(day);
        const auto back = hgl::syntax::days_from_civil(date.year, date.month, date.day);
        if (!back || *back != day)
        {
            ++mismatches;
        }
        if (day != -719162)
        {
            const bool later = date.year > previous.year ||
                               (date.year == previous.year &&
                                (date.month > previous.month ||
                                 (date.month == previous.month && date.day == previous.day + 1)));
            if (!later)
            {
                ++mismatches;
            }
        }
        previous = date;
    }
    CHECK(mismatches == 0);
}

// ---- zone names -------------------------------------------------------------

TEST_CASE("zone names follow the RFC 9557 syntax", "[temporal]")
{
    CHECK(hgl::syntax::is_valid_zone_name("UTC"));
    CHECK(hgl::syntax::is_valid_zone_name("Europe/London"));
    CHECK(hgl::syntax::is_valid_zone_name("America/New_York"));
    CHECK(hgl::syntax::is_valid_zone_name("America/Argentina/Buenos_Aires"));
    CHECK(hgl::syntax::is_valid_zone_name("America/Port-au-Prince"));
    CHECK(hgl::syntax::is_valid_zone_name("Etc/GMT+1"));
    CHECK(hgl::syntax::is_valid_zone_name("Etc/GMT-14"));
    CHECK(hgl::syntax::is_valid_zone_name("posix/Europe/London"));
    CHECK(hgl::syntax::is_valid_zone_name("a.b"));
    CHECK(hgl::syntax::is_valid_zone_name("_"));
    CHECK(hgl::syntax::is_valid_zone_name("Zone1"));
    CHECK(hgl::syntax::is_valid_zone_name("...a"));

    CHECK_FALSE(hgl::syntax::is_valid_zone_name(""));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("/Europe/London"));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("Europe/London/"));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("Europe//London"));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("/"));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("."));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name(".."));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("Europe/./London"));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("Europe/../London"));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("Europe/London/."));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("Europe London"));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("Europe\tLondon"));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("Europe/Z\xc3\xbcrich"));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("Europe/London]"));
    CHECK_FALSE(hgl::syntax::is_valid_zone_name("Europe\\London"));

    const std::string longest(255, 'a');
    CHECK(hgl::syntax::is_valid_zone_name(longest));
    const std::string too_long(256, 'a');
    CHECK_FALSE(hgl::syntax::is_valid_zone_name(too_long));
}

// ---- date literals ----------------------------------------------------------

TEST_CASE("date literals are days since the epoch", "[temporal]")
{
    const TemporalValue value = parse_ok("@2026-09-03");
    CHECK(value.kind == TemporalKind::Date);
    CHECK(value.micros == days_2026_09_03);
    CHECK(value.offset_seconds == 0);
    CHECK(value.zone.empty());

    CHECK(parse_ok("@1970-01-01").micros == 0);
    CHECK(parse_ok("@1969-12-31").micros == -1);
    CHECK(parse_ok("@2000-02-29").micros == 11016);
    CHECK(parse_ok("@0001-01-01").micros == -719162);
    CHECK(parse_ok("@9999-12-31").micros == 2932896);
}

TEST_CASE("date literals must be calendar dates with exact digit counts", "[temporal]")
{
    CHECK(parse_fail("@2026-02-29").error == "'@2026-02-29' is not a calendar date");
    CHECK(parse_fail("@1900-02-29").error == "'@1900-02-29' is not a calendar date");
    CHECK(parse_fail("@2026-04-31").error == "'@2026-04-31' is not a calendar date");
    CHECK(parse_fail("@2026-13-01").error == "'@2026-13-01' is not a calendar date");
    CHECK(parse_fail("@2026-00-10").error == "'@2026-00-10' is not a calendar date");
    CHECK(parse_fail("@2026-01-00").error == "'@2026-01-00' is not a calendar date");
    CHECK(parse_fail("@0000-01-01").error == "'@0000-01-01' is not a calendar date");

    CHECK(parse_fail("@2026-9-3").error == "'@2026-9-3' does not have the shape YYYY-MM-DD");
    CHECK(parse_fail("@2026-09-3").error == "'@2026-09-3' does not have the shape YYYY-MM-DD");
    CHECK(parse_fail("@26-09-03").error == "'@26-09-03' does not have the shape YYYY-MM-DD");
    CHECK(parse_fail("@2026-09-").error == "'@2026-09-' does not have the shape YYYY-MM-DD");
    CHECK(parse_fail("@12026-09-03").error == "'@12026-09-03' does not have the shape YYYY-MM-DD");
    CHECK(parse_fail("@2026-09-031").error == "'@2026-09-031' has unexpected '1' after the literal");
    CHECK(parse_fail("@2026-09-03 ").error == "'@2026-09-03 ' has unexpected ' ' after the literal");
}

// ---- time literals ----------------------------------------------------------

TEST_CASE("time literals are microseconds since midnight", "[temporal]")
{
    const TemporalValue value = parse_ok("@09:30");
    CHECK(value.kind == TemporalKind::Time);
    CHECK(value.micros == 9 * us_per_hour + 30 * us_per_minute);
    CHECK(value.zone.empty());

    CHECK(parse_ok("@00:00").micros == 0);
    CHECK(parse_ok("@09:30:15").micros == 9 * us_per_hour + 30 * us_per_minute + 15 * us_per_second);
    CHECK(parse_ok("@09:30:15.250").micros == 9 * us_per_hour + 30 * us_per_minute + 15 * us_per_second + 250'000);
    CHECK(parse_ok("@23:59:59.999999").micros == us_per_day - 1);
}

TEST_CASE("time fractions are one to six digits and mean fractions of a second", "[temporal]")
{
    const std::int64_t base = 9 * us_per_hour + 30 * us_per_minute + 15 * us_per_second;
    CHECK(parse_ok("@09:30:15.2").micros == base + 200'000);
    CHECK(parse_ok("@09:30:15.25").micros == base + 250'000);
    CHECK(parse_ok("@09:30:15.250").micros == base + 250'000);
    CHECK(parse_ok("@09:30:15.2500").micros == base + 250'000);
    CHECK(parse_ok("@09:30:15.25000").micros == base + 250'000);
    CHECK(parse_ok("@09:30:15.250000").micros == base + 250'000);
    CHECK(parse_ok("@09:30:15.000001").micros == base + 1);
    CHECK(parse_ok("@09:30:15.000000").micros == base);

    CHECK(parse_fail("@09:30:15.").error ==
          "'@09:30:15.' has an empty fraction; write one to six digits after the point");
    CHECK(parse_fail("@09:30:15.1234567").error ==
          "'@09:30:15.1234567' has a fraction longer than six digits; times resolve to microseconds");
    // A fraction needs seconds to hang off.
    CHECK(parse_fail("@09:30.5").error == "'@09:30.5' has unexpected '.' after the literal");
}

TEST_CASE("time literals are earlier than 24:00:00 and have exact digit counts", "[temporal]")
{
    CHECK(parse_fail("@24:00").error == "'@24:00' is not a time of day");
    CHECK(parse_fail("@24:00:00").error == "'@24:00:00' is not a time of day");
    CHECK(parse_fail("@25:00").error == "'@25:00' is not a time of day");
    CHECK(parse_fail("@23:60").error == "'@23:60' is not a time of day");
    CHECK(parse_fail("@23:59:61").error == "'@23:59:61' is not a time of day");
    CHECK(parse_fail("@99:99").error == "'@99:99' is not a time of day");

    const TemporalParseResult leap = parse_fail("@23:59:60");
    CHECK(contains(leap.error, "'@23:59:60'"));
    CHECK(contains(leap.error, "leap second"));

    CHECK(parse_fail("@9:30").error == "'@9:30' does not have the shape HH:MM[:SS[.ffffff]]");
    CHECK(parse_fail("@09:3").error == "'@09:3' does not have the shape HH:MM[:SS[.ffffff]]");
    CHECK(parse_fail("@09:30:5").error == "'@09:30:5' does not have the shape HH:MM[:SS[.ffffff]]");
    CHECK(parse_fail("@09:30:").error == "'@09:30:' does not have the shape HH:MM[:SS[.ffffff]]");
    CHECK(parse_fail("@009:30").error == "'@009:30' does not have the shape HH:MM[:SS[.ffffff]]");
    CHECK(parse_fail("@09:30:155").error == "'@09:30:155' has unexpected '5' after the literal");
    CHECK(parse_fail("@09:30Z").error == "'@09:30Z' has unexpected 'Z' after the literal");
}

// ---- datetime literals ------------------------------------------------------

TEST_CASE("datetime literals with an offset are UTC instants", "[temporal]")
{
    const TemporalValue utc = parse_ok("@2026-09-03T09:30Z");
    CHECK(utc.kind == TemporalKind::DateTime);
    CHECK(utc.micros == instant_2026_09_03_0930z);
    CHECK(utc.offset_seconds == 0);
    CHECK(utc.zone.empty());

    // The spec's three spellings of the same instant.
    CHECK(parse_ok("@2026-09-03T10:30:00+01:00") == utc);
    CHECK(parse_ok("@2026-09-03T10:30+01") == utc);
    CHECK(parse_ok("@2026-09-03T04:00-05:30") == utc);
    CHECK(parse_ok("@2026-09-03T09:30:00.000000+00:00") == utc);
    CHECK(parse_ok("@2026-09-03T09:30-00") == utc);

    CHECK(parse_ok("@2026-09-03T09:30:15.250Z").micros == instant_2026_09_03_0930z + 15 * us_per_second + 250'000);
    CHECK(parse_ok("@1970-01-01T00:00Z").micros == 0);
    CHECK(parse_ok("@1970-01-01T00:00+01").micros == -us_per_hour);
    CHECK(parse_ok("@1969-12-31T23:59:59.5Z").micros == -500'000);
    CHECK(parse_ok("@0001-01-01T00:00Z").micros == -62'135'596'800'000'000);
    CHECK(parse_ok("@0001-01-01T23:59-00:01").micros == -62'135'596'800'000'000 + us_per_day);
    CHECK(parse_ok("@0001-01-02T00:00+23:59").micros == -62'135'596'800'000'000 + us_per_minute);
    CHECK(parse_ok("@9999-12-31T23:59:59.999999Z").micros == 253'402'300'799'999'999);
    CHECK(parse_ok("@9999-12-31T00:00-23:59").micros == 253'402'300'799'999'999 - 59 * us_per_second - 999'999);
    CHECK(parse_ok("@9999-12-30T23:59:59.999999-23:59").micros == 253'402'300'799'999'999 - us_per_minute);
}

TEST_CASE("datetime instants stay within the years 0001 to 9999 in UTC", "[temporal]")
{
    // The canonical spelling is UTC, so an offset that carries the instant
    // into year 0 or year 10000 leaves nothing to round-trip to.
    constexpr std::string_view range = " denotes an instant outside the years 0001 to 9999 in UTC";
    CHECK(parse_fail("@0001-01-01T00:00+00:01").error == "'@0001-01-01T00:00+00:01'" + std::string{range});
    CHECK(parse_fail("@0001-01-01T00:00+23:59").error == "'@0001-01-01T00:00+23:59'" + std::string{range});
    CHECK(parse_fail("@0001-01-01T00:00+01[Europe/London]").error ==
          "'@0001-01-01T00:00+01[Europe/London]'" + std::string{range});
    CHECK(parse_fail("@9999-12-31T23:59:59.999999-00:01").error ==
          "'@9999-12-31T23:59:59.999999-00:01'" + std::string{range});
    CHECK(parse_fail("@9999-12-31T23:59:59.999999-23:59").error ==
          "'@9999-12-31T23:59:59.999999-23:59'" + std::string{range});
    CHECK(parse_fail("@9999-12-31T23:59-01[America/New_York]").error ==
          "'@9999-12-31T23:59-01[America/New_York]'" + std::string{range});
    // Civil datetimes carry no offset, so the local date alone bounds them.
    CHECK(parse_ok("@0001-01-01T00:00").kind == TemporalKind::CivilDateTime);
    CHECK(parse_ok("@9999-12-31T23:59:59.999999").kind == TemporalKind::CivilDateTime);
}

TEST_CASE("datetime literals without an offset are civil datetimes", "[temporal]")
{
    const TemporalValue civil = parse_ok("@2026-09-03T10:30");
    CHECK(civil.kind == TemporalKind::CivilDateTime);
    CHECK(civil.micros == civil_2026_09_03_1030);
    CHECK(civil.offset_seconds == 0);
    CHECK(civil.zone.empty());

    CHECK(parse_ok("@2026-09-03T10:30:00.5").micros == civil_2026_09_03_1030 + 500'000);
    CHECK(parse_ok("@1970-01-01T00:00").micros == 0);
    CHECK(parse_ok("@1969-12-31T23:00").micros == -us_per_hour);

    // A civil datetime and the same-looking instant are different values.
    CHECK(parse_ok("@2026-09-03T10:30") != parse_ok("@2026-09-03T10:30Z"));
}

TEST_CASE("datetime separators and offsets are validated", "[temporal]")
{
    CHECK(parse_fail("@2026-09-03t09:30Z").error ==
          "'@2026-09-03t09:30Z' separates the date and time with 't'; the separator is an upper-case 'T'");
    CHECK(parse_fail("@2026-09-03T09:30z").error ==
          "'@2026-09-03T09:30z' spells the UTC offset as 'z'; it is an upper-case 'Z'");
    CHECK(parse_fail("@2026-09-03 09:30Z").error == "'@2026-09-03 09:30Z' has unexpected ' ' after the literal");

    constexpr std::string_view offset_shape =
        " has an invalid offset; write Z, +HH, -HH, +HH:MM or -HH:MM with hours 00-23 and minutes 00-59";
    CHECK(parse_fail("@2026-09-03T09:30+1").error == "'@2026-09-03T09:30+1'" + std::string{offset_shape});
    CHECK(parse_fail("@2026-09-03T09:30+").error == "'@2026-09-03T09:30+'" + std::string{offset_shape});
    CHECK(parse_fail("@2026-09-03T09:30+01:").error == "'@2026-09-03T09:30+01:'" + std::string{offset_shape});
    CHECK(parse_fail("@2026-09-03T09:30+01:5").error == "'@2026-09-03T09:30+01:5'" + std::string{offset_shape});
    CHECK(parse_fail("@2026-09-03T09:30+24:00").error == "'@2026-09-03T09:30+24:00'" + std::string{offset_shape});
    CHECK(parse_fail("@2026-09-03T09:30-24").error == "'@2026-09-03T09:30-24'" + std::string{offset_shape});
    CHECK(parse_fail("@2026-09-03T09:30+01:60").error == "'@2026-09-03T09:30+01:60'" + std::string{offset_shape});
    CHECK(parse_fail("@2026-09-03T09:30UTC").error == "'@2026-09-03T09:30UTC'" + std::string{offset_shape});
    CHECK(parse_fail("@2026-09-03T09:30+0100").error == "'@2026-09-03T09:30+0100' has unexpected '0' after the literal");
    CHECK(parse_fail("@2026-09-03T09:30ZZ").error == "'@2026-09-03T09:30ZZ' has unexpected 'Z' after the literal");

    CHECK(parse_fail("@2026-09-03T24:00Z").error == "'@2026-09-03T24:00Z' is not a time of day");
    CHECK(parse_fail("@2026-02-29T09:30Z").error == "'@2026-02-29T09:30Z' is not a calendar date");
    CHECK(parse_fail("@2026-09-03T9:30Z").error == "'@2026-09-03T9:30Z' does not have the shape HH:MM[:SS[.ffffff]]");
    CHECK(parse_fail("@2026-09-03T").error == "'@2026-09-03T' does not have the shape HH:MM[:SS[.ffffff]]");
}

// ---- zoned datetime literals ------------------------------------------------

TEST_CASE("zoned datetime literals keep the instant, the offset and the zone", "[temporal]")
{
    const TemporalValue london = parse_ok("@2026-09-03T10:30+01:00[Europe/London]");
    CHECK(london.kind == TemporalKind::ZonedDateTime);
    CHECK(london.micros == instant_2026_09_03_0930z);
    CHECK(london.offset_seconds == 3600);
    CHECK(london.zone == "Europe/London");

    // The two sides of a fold are distinct values one hour apart.
    const TemporalValue first = parse_ok("@2026-11-01T01:30-04:00[America/New_York]");
    const TemporalValue second = parse_ok("@2026-11-01T01:30-05:00[America/New_York]");
    CHECK(first.kind == TemporalKind::ZonedDateTime);
    CHECK(first.micros == instant_2026_11_01_0530z);
    CHECK(first.offset_seconds == -4 * 3600);
    CHECK(first.zone == "America/New_York");
    CHECK(second.micros == instant_2026_11_01_0630z);
    CHECK(second.offset_seconds == -5 * 3600);
    CHECK(second.zone == "America/New_York");
    CHECK(first != second);
    CHECK(second.micros - first.micros == us_per_hour);

    const TemporalValue zulu = parse_ok("@2026-09-03T09:30Z[Europe/London]");
    CHECK(zulu.micros == instant_2026_09_03_0930z);
    CHECK(zulu.offset_seconds == 0);
    CHECK(zulu.zone == "Europe/London");

    const TemporalValue kolkata = parse_ok("@2026-09-03T15:00+05:30[Asia/Kolkata]");
    CHECK(kolkata.micros == instant_2026_09_03_0930z);
    CHECK(kolkata.offset_seconds == 5 * 3600 + 30 * 60);

    // A zoned value is never equal to the plain instant, even with the same micros.
    CHECK(zulu != parse_ok("@2026-09-03T09:30Z"));

    // Zone existence and offset agreement are not checked here.
    const TemporalValue unchecked = parse_ok("@2026-09-03T10:30+11:00[Europe/London]");
    CHECK(unchecked.offset_seconds == 11 * 3600);
    CHECK(parse_ok("@2026-09-03T10:30+01:00[Not/A_Zone]").zone == "Not/A_Zone");
}

TEST_CASE("a zoned datetime literal needs an offset", "[temporal]")
{
    const TemporalParseResult result = parse_fail("@2026-09-03T10:30[Europe/London]");
    CHECK(result.error == "'@2026-09-03T10:30[Europe/London]' has no offset; add it or use resolve()");
    CHECK_FALSE(result.hint.empty());
    CHECK(contains(result.hint, "resolve"));
    CHECK(contains(result.hint, "Europe/London"));

    // Other diagnostics do not carry the resolve hint.
    CHECK(parse_fail("@2026-02-29").hint.empty());
    CHECK(parse_fail("30m1h").hint.empty());
}

TEST_CASE("zone annotations are validated syntactically", "[temporal]")
{
    CHECK(parse_fail("@2026-09-03T10:30+01:00[Europe//London]").error == "'[Europe//London]' is not a valid zone name");
    CHECK(parse_fail("@2026-09-03T10:30+01:00[]").error == "'[]' is not a valid zone name");
    CHECK(parse_fail("@2026-09-03T10:30+01:00[/Europe]").error == "'[/Europe]' is not a valid zone name");
    CHECK(parse_fail("@2026-09-03T10:30+01:00[Europe/]").error == "'[Europe/]' is not a valid zone name");
    CHECK(parse_fail("@2026-09-03T10:30+01:00[Europe/..]").error == "'[Europe/..]' is not a valid zone name");
    CHECK(parse_fail("@2026-09-03T10:30+01:00[Europe London]").error == "'[Europe London]' is not a valid zone name");
    CHECK(parse_fail("@2026-09-03T10:30+01:00[Europe/London").error ==
          "'@2026-09-03T10:30+01:00[Europe/London' has an unterminated zone annotation");
    CHECK(parse_fail("@2026-09-03T10:30+01:00[Europe/London]]").error ==
          "'@2026-09-03T10:30+01:00[Europe/London]]' has unexpected ']' after the literal");
    CHECK(parse_fail("@2026-09-03T10:30+01:00[Europe/London][UTC]").error ==
          "'@2026-09-03T10:30+01:00[Europe/London][UTC]' has unexpected '[' after the literal");
    CHECK(parse_fail("@2026-09-03T10:30+01:00 [Europe/London]").error ==
          "'@2026-09-03T10:30+01:00 [Europe/London]' has unexpected ' ' after the literal");
}

// ---- zoned time literals ----------------------------------------------------

TEST_CASE("zoned time literals pair a time of day with a zone", "[temporal]")
{
    const TemporalValue value = parse_ok("@09:30[America/New_York]");
    CHECK(value.kind == TemporalKind::ZonedTime);
    CHECK(value.micros == 9 * us_per_hour + 30 * us_per_minute);
    CHECK(value.offset_seconds == 0);
    CHECK(value.zone == "America/New_York");

    const TemporalValue precise = parse_ok("@09:30:15.25[Europe/London]");
    CHECK(precise.micros == 9 * us_per_hour + 30 * us_per_minute + 15 * us_per_second + 250'000);
    CHECK(precise.zone == "Europe/London");

    CHECK(value != parse_ok("@09:30"));

    CHECK(parse_fail("@09:30[Europe//London]").error == "'[Europe//London]' is not a valid zone name");
    CHECK(parse_fail("@09:30[]").error == "'[]' is not a valid zone name");
    CHECK(parse_fail("@09:30[Europe/London").error == "'@09:30[Europe/London' has an unterminated zone annotation");
    CHECK(parse_fail("@09:30[Europe/London]x").error == "'@09:30[Europe/London]x' has unexpected 'x' after the literal");
    CHECK(parse_fail("@24:00[Europe/London]").error == "'@24:00[Europe/London]' is not a time of day");
    // A time with an offset is not a literal form.
    CHECK(parse_fail("@09:30+01:00[Europe/London]").error ==
          "'@09:30+01:00[Europe/London]' has unexpected '+' after the literal");
}

// ---- timezone literals ------------------------------------------------------

TEST_CASE("timezone literals name a zone", "[temporal]")
{
    const TemporalValue value = parse_ok("@[Europe/London]");
    CHECK(value.kind == TemporalKind::TimeZone);
    CHECK(value.micros == 0);
    CHECK(value.offset_seconds == 0);
    CHECK(value.zone == "Europe/London");

    CHECK(parse_ok("@[UTC]").zone == "UTC");
    CHECK(parse_ok("@[Etc/GMT+1]").zone == "Etc/GMT+1");
    CHECK(parse_ok("@[America/Argentina/Buenos_Aires]").zone == "America/Argentina/Buenos_Aires");
    CHECK(parse_ok("@[Made/Up]").zone == "Made/Up");

    CHECK(parse_fail("@[Europe//London]").error == "'[Europe//London]' is not a valid zone name");
    CHECK(parse_fail("@[]").error == "'[]' is not a valid zone name");
    CHECK(parse_fail("@[/Europe]").error == "'[/Europe]' is not a valid zone name");
    CHECK(parse_fail("@[.]").error == "'[.]' is not a valid zone name");
    CHECK(parse_fail("@[Europe/London").error == "'@[Europe/London' has an unterminated zone annotation");
    CHECK(parse_fail("@[Europe/London]x").error == "'@[Europe/London]x' has unexpected 'x' after the literal");
}

TEST_CASE("unrecognised @ shapes are diagnosed", "[temporal]")
{
    constexpr std::string_view shape =
        " is not a temporal literal; write @YYYY-MM-DD, @HH:MM, @YYYY-MM-DDTHH:MM with an offset, or @[Zone]";
    CHECK(parse_fail("@").error == "'@'" + std::string{shape});
    CHECK(parse_fail("@abc").error == "'@abc'" + std::string{shape});
    CHECK(parse_fail("@2026").error == "'@2026'" + std::string{shape});
    CHECK(parse_fail("@2026/09/03").error == "'@2026/09/03'" + std::string{shape});
    CHECK(parse_fail("@T09:30").error == "'@T09:30'" + std::string{shape});
    CHECK(parse_fail("@ 2026-09-03").error == "'@ 2026-09-03'" + std::string{shape});
    CHECK(parse_fail("@5m").error == "'@5m'" + std::string{shape});
}

// ---- duration literals ------------------------------------------------------

TEST_CASE("duration literals sum their parts in microseconds", "[temporal]")
{
    const TemporalValue value = parse_ok("5m");
    CHECK(value.kind == TemporalKind::Duration);
    CHECK(value.micros == 5 * us_per_minute);
    CHECK(value.offset_seconds == 0);
    CHECK(value.zone.empty());

    CHECK(parse_ok("1h30m").micros == 90 * us_per_minute);
    CHECK(parse_ok("1.5h").micros == 90 * us_per_minute);
    CHECK(parse_ok("250ms").micros == 250'000);
    CHECK(parse_ok("1d").micros == us_per_day);
    CHECK(parse_ok("1s").micros == us_per_second);
    CHECK(parse_ok("1us").micros == 1);
    CHECK(parse_ok("0s").micros == 0);
    CHECK(parse_ok("0us").micros == 0);
    CHECK(parse_ok("1s500ms").micros == 1'500'000);
    CHECK(parse_ok("2m30.5s").micros == 150 * us_per_second + 500'000);
    CHECK(parse_ok("1d1h1m1s1ms1us").micros == us_per_day + us_per_hour + us_per_minute + us_per_second + 1'000 + 1);
    CHECK(parse_ok("36h").micros == 36 * us_per_hour);
    CHECK(parse_ok("5400s").micros == 90 * us_per_minute);
    CHECK(parse_ok("1500000us").micros == 1'500'000);
    CHECK(parse_ok("007s").micros == 7 * us_per_second);
    CHECK(parse_ok("00d").micros == 0);
    CHECK(parse_ok("1d0h").micros == us_per_day);
    CHECK(parse_ok("90m").micros == 90 * us_per_minute);
}

TEST_CASE("duration fractions use exact decimal arithmetic", "[temporal]")
{
    CHECK(parse_ok("0.5ms").micros == 500);
    CHECK(parse_ok("0.25d").micros == 6 * us_per_hour);
    CHECK(parse_ok("0.1h").micros == 6 * us_per_minute);
    CHECK(parse_ok("1.000001s").micros == 1'000'001);
    CHECK(parse_ok("0.000001s").micros == 1);
    CHECK(parse_ok("0.001ms").micros == 1);
    CHECK(parse_ok("1.0us").micros == 1);
    CHECK(parse_ok("1.000000000000000000000000us").micros == 1);
    CHECK(parse_ok("0.00000001d").micros == 864);
    CHECK(parse_ok("0.000000005d").micros == 432);
    CHECK(parse_ok("0.0000000125d").micros == 1080);
    CHECK(parse_ok("2.5m").micros == 150 * us_per_second);
    CHECK(parse_ok("0.999999s").micros == 999'999);
    CHECK(parse_ok("0.00390625d").micros == 337'500'000);
    CHECK(parse_ok("1.0000000000000000000000d").micros == us_per_day);

    CHECK(parse_fail("0.5us").error == "'0.5us' is not a whole number of microseconds");
    CHECK(parse_fail("0.0001ms").error == "'0.0001ms' is not a whole number of microseconds");
    CHECK(parse_fail("1.0000001s").error == "'1.0000001s' is not a whole number of microseconds");
    CHECK(parse_fail("0.000000001d").error == "'0.000000001d' is not a whole number of microseconds");
    CHECK(parse_fail("0.00000000000001d").error == "'0.00000000000001d' is not a whole number of microseconds");
    CHECK(parse_fail("0.3333333333333333333d").error == "'0.3333333333333333333d' is not a whole number of microseconds");
    CHECK(parse_fail("1.9999999999d").error == "'1.9999999999d' is not a whole number of microseconds");
    CHECK(parse_fail("1h0.5us").error == "'1h0.5us' is not a whole number of microseconds");
}

TEST_CASE("duration parts are strictly descending, each unit at most once", "[temporal]")
{
    CHECK(parse_fail("30m1h").error == "'30m1h' lists duration units out of descending order");
    CHECK(parse_fail("1ms1s").error == "'1ms1s' lists duration units out of descending order");
    CHECK(parse_fail("1us1ms").error == "'1us1ms' lists duration units out of descending order");
    CHECK(parse_fail("1h1d").error == "'1h1d' lists duration units out of descending order");
    CHECK(parse_fail("1h1h").error == "'1h1h' lists duration unit 'h' twice");
    CHECK(parse_fail("1d2h3h").error == "'1d2h3h' lists duration unit 'h' twice");
    CHECK(parse_fail("1ms2ms").error == "'1ms2ms' lists duration unit 'ms' twice");
    CHECK(parse_fail("1.5h30m").error == "'1.5h30m' has a fraction on a part that is not last");
    CHECK(parse_fail("1.0d1h").error == "'1.0d1h' has a fraction on a part that is not last");
}

TEST_CASE("duration units are lower case and drawn from d h m s ms us", "[temporal]")
{
    CHECK(parse_fail("5min").error == "'5min' has unknown duration unit 'min'; units are d h m s ms us");
    CHECK(parse_fail("5M").error == "'5M' has unknown duration unit 'M'; units are d h m s ms us");
    CHECK(parse_fail("1H").error == "'1H' has unknown duration unit 'H'; units are d h m s ms us");
    CHECK(parse_fail("1D").error == "'1D' has unknown duration unit 'D'; units are d h m s ms us");
    CHECK(parse_fail("5sec").error == "'5sec' has unknown duration unit 'sec'; units are d h m s ms us");
    CHECK(parse_fail("5ns").error == "'5ns' has unknown duration unit 'ns'; units are d h m s ms us");
    CHECK(parse_fail("1w").error == "'1w' has unknown duration unit 'w'; units are d h m s ms us");
    CHECK(parse_fail("1h30mins").error == "'1h30mins' has unknown duration unit 'mins'; units are d h m s ms us");
    CHECK(parse_fail("1hm").error == "'1hm' has unknown duration unit 'hm'; units are d h m s ms us");
    CHECK(parse_fail("5").error == "'5' is missing a duration unit; units are d h m s ms us");
    CHECK(parse_fail("1h30").error == "'1h30' is missing a duration unit; units are d h m s ms us");
    CHECK(parse_fail("1.5").error == "'1.5' is missing a duration unit; units are d h m s ms us");
}

TEST_CASE("duration shapes are digits, an optional fraction and a unit", "[temporal]")
{
    constexpr std::string_view shape =
        " is not a duration literal; write a number followed by one of d h m s ms us";
    CHECK(parse_fail("").error == "''" + std::string{shape});
    CHECK(parse_fail("h").error == "'h'" + std::string{shape});
    CHECK(parse_fail("-5m").error == "'-5m'" + std::string{shape});
    CHECK(parse_fail("+5m").error == "'+5m'" + std::string{shape});
    CHECK(parse_fail(".5s").error == "'.5s'" + std::string{shape});
    CHECK(parse_fail(" 5m").error == "' 5m'" + std::string{shape});
    CHECK(parse_fail("1.h").error == "'1.h' has an empty fraction; write digits after the point");
    CHECK(parse_fail("1.").error == "'1.' has an empty fraction; write digits after the point");
    CHECK(parse_fail("1h ").error == "'1h ' has unexpected ' ' after the literal");
    CHECK(parse_fail("1h-30m").error == "'1h-30m' has unexpected '-' after the literal");
    CHECK(parse_fail("1h.5m").error == "'1h.5m' has unexpected '.' after the literal");
    CHECK(parse_fail("1h+").error == "'1h+' has unexpected '+' after the literal");
}

TEST_CASE("duration literals fit the 64-bit microsecond range", "[temporal]")
{
    constexpr std::int64_t max = std::numeric_limits<std::int64_t>::max();
    CHECK(parse_ok("9223372036854775807us").micros == max);
    CHECK(parse_ok("9223372036854775ms807us").micros == max);
    CHECK(parse_ok("106751991d").micros == 106751991 * us_per_day);
    CHECK(parse_ok("106751991d4h54s775ms807us").micros == max);

    constexpr std::string_view range = " is outside the 64-bit microsecond range of a duration";
    CHECK(parse_fail("9223372036854775808us").error == "'9223372036854775808us'" + std::string{range});
    CHECK(parse_fail("106751992d").error == "'106751992d'" + std::string{range});
    CHECK(parse_fail("2562047789h").error == "'2562047789h'" + std::string{range});
    CHECK(parse_fail("106751991d4h54s775ms808us").error == "'106751991d4h54s775ms808us'" + std::string{range});
    CHECK(parse_fail("99999999999999999999s").error == "'99999999999999999999s'" + std::string{range});
    CHECK(parse_fail("9223372036854775807us1us").error == "'9223372036854775807us1us' lists duration unit 'us' twice");
    CHECK(parse_fail("106751991.9d").error == "'106751991.9d'" + std::string{range});
}

// ---- canonical spelling -----------------------------------------------------

TEST_CASE("canonical dates are @YYYY-MM-DD", "[temporal]")
{
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::Date, .micros = days_2026_09_03}) ==
          "@2026-09-03");
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::Date, .micros = 0}) == "@1970-01-01");
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::Date, .micros = -1}) == "@1969-12-31");
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::Date, .micros = -719162}) == "@0001-01-01");
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::Date, .micros = 2932896}) == "@9999-12-31");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@0100-01-05")) == "@0100-01-05");
}

TEST_CASE("canonical times are the shortest spelling", "[temporal]")
{
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30")) == "@09:30");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:00")) == "@09:30");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:00.000000")) == "@09:30");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:15")) == "@09:30:15");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:15.000")) == "@09:30:15");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:15.250")) == "@09:30:15.25");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:15.25")) == "@09:30:15.25");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:15.2")) == "@09:30:15.2");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:15.000001")) == "@09:30:15.000001");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:00.5")) == "@09:30:00.5");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@00:00")) == "@00:00");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@23:59:59.999999")) == "@23:59:59.999999");
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::Time, .micros = 0}) == "@00:00");
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::Time, .micros = 1}) == "@00:00:00.000001");
}

TEST_CASE("canonical datetimes are always UTC with the shortest time", "[temporal]")
{
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T09:30Z")) == "@2026-09-03T09:30Z");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T10:30:00+01:00")) == "@2026-09-03T09:30Z");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T10:30+01")) == "@2026-09-03T09:30Z");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T04:00-05:30")) == "@2026-09-03T09:30Z");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T09:30:15.250Z")) == "@2026-09-03T09:30:15.25Z");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T23:30-01")) == "@2026-09-04T00:30Z");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T00:30+01")) == "@2026-09-02T23:30Z");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@1969-12-31T23:59:59.5Z")) == "@1969-12-31T23:59:59.5Z");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@1970-01-01T00:00+01")) == "@1969-12-31T23:00Z");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@0001-01-01T00:00Z")) == "@0001-01-01T00:00Z");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@9999-12-31T23:59:59.999999Z")) == "@9999-12-31T23:59:59.999999Z");
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::DateTime, .micros = 0}) ==
          "@1970-01-01T00:00Z");
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::DateTime, .micros = -1}) ==
          "@1969-12-31T23:59:59.999999Z");
}

TEST_CASE("canonical civil datetimes have no offset", "[temporal]")
{
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T10:30")) == "@2026-09-03T10:30");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T10:30:00")) == "@2026-09-03T10:30");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T10:30:00.500")) == "@2026-09-03T10:30:00.5");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@1969-12-31T23:00")) == "@1969-12-31T23:00");
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::CivilDateTime, .micros = 0}) ==
          "@1970-01-01T00:00");
}

TEST_CASE("canonical zoned datetimes keep the local time, shortest offset and zone", "[temporal]")
{
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T10:30+01:00[Europe/London]")) ==
          "@2026-09-03T10:30+01[Europe/London]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T10:30:00.000+01[Europe/London]")) ==
          "@2026-09-03T10:30+01[Europe/London]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-11-01T01:30-04:00[America/New_York]")) ==
          "@2026-11-01T01:30-04[America/New_York]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-11-01T01:30-05:00[America/New_York]")) ==
          "@2026-11-01T01:30-05[America/New_York]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T15:00+05:30[Asia/Kolkata]")) ==
          "@2026-09-03T15:00+05:30[Asia/Kolkata]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T06:00-03:30[America/St_Johns]")) ==
          "@2026-09-03T06:00-03:30[America/St_Johns]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T09:30+00:00[Europe/London]")) ==
          "@2026-09-03T09:30Z[Europe/London]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T09:30-00:00[Europe/London]")) ==
          "@2026-09-03T09:30Z[Europe/London]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T09:30:15.5Z[Europe/London]")) ==
          "@2026-09-03T09:30:15.5Z[Europe/London]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@2026-09-03T23:30+13:45[Pacific/Chatham]")) ==
          "@2026-09-03T23:30+13:45[Pacific/Chatham]");
    // Composed directly from an instant and offset.
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::ZonedDateTime,
                                                        .micros = instant_2026_09_03_0930z,
                                                        .offset_seconds = -7 * 3600,
                                                        .zone = "America/Los_Angeles"}) ==
          "@2026-09-03T02:30-07[America/Los_Angeles]");
}

TEST_CASE("canonical zoned times and timezones", "[temporal]")
{
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30[America/New_York]")) == "@09:30[America/New_York]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:00[America/New_York]")) == "@09:30[America/New_York]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@09:30:15.250[Europe/London]")) == "@09:30:15.25[Europe/London]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@[Europe/London]")) == "@[Europe/London]");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("@[UTC]")) == "@[UTC]");
    CHECK(hgl::syntax::canonical_spelling(TemporalValue{.kind = TemporalKind::TimeZone, .zone = "Etc/GMT+1"}) ==
          "@[Etc/GMT+1]");
}

TEST_CASE("canonical durations use descending integer parts", "[temporal]")
{
    CHECK(hgl::syntax::canonical_spelling(parse_ok("5400s")) == "1h30m");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("1500000us")) == "1s500ms");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("36h")) == "1d12h");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("1.5h")) == "1h30m");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("1h30m")) == "1h30m");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("5m")) == "5m");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("250ms")) == "250ms");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("0.25ms")) == "250us");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("90m")) == "1h30m");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("1d0h")) == "1d");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("1d1h1m1s1ms1us")) == "1d1h1m1s1ms1us");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("86400000000us")) == "1d");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("0s")) == "0s");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("0ms")) == "0s");
    CHECK(hgl::syntax::canonical_spelling(parse_ok("0.000d")) == "0s");
    CHECK(hgl::syntax::canonical_spelling(duration(0)) == "0s");
    CHECK(hgl::syntax::canonical_spelling(duration(1)) == "1us");
    CHECK(hgl::syntax::canonical_spelling(duration(-1)) == "-1us");
    CHECK(hgl::syntax::canonical_spelling(duration(-90 * us_per_minute)) == "-1h30m");
    CHECK(hgl::syntax::canonical_spelling(duration(-us_per_day)) == "-1d");
    CHECK(hgl::syntax::canonical_spelling(duration(std::numeric_limits<std::int64_t>::max())) ==
          "106751991d4h54s775ms807us");
    CHECK(hgl::syntax::canonical_spelling(duration(std::numeric_limits<std::int64_t>::min())) ==
          "-106751991d4h54s775ms808us");
}

// ---- round trips ------------------------------------------------------------

TEST_CASE("parsing a canonical spelling gives back the same value", "[temporal]")
{
    const std::vector<std::string_view> spellings{
        "@2026-09-03",
        "@1970-01-01",
        "@1969-12-31",
        "@0001-01-01",
        "@9999-12-31",
        "@2000-02-29",
        "@09:30",
        "@09:30:15",
        "@09:30:15.250",
        "@09:30:15.000001",
        "@00:00",
        "@23:59:59.999999",
        "@2026-09-03T09:30Z",
        "@2026-09-03T10:30:00+01:00",
        "@2026-09-03T10:30+01",
        "@2026-09-03T09:30:15.250Z",
        "@1969-12-31T23:59:59.5Z",
        "@0001-01-01T00:00Z",
        "@0001-01-02T00:00+23:59",
        "@9999-12-31T23:59:59.999999Z",
        "@9999-12-30T23:59:59.999999-23:59",
        "@2026-09-03T10:30",
        "@2026-09-03T10:30:00.5",
        "@1969-12-31T23:00",
        "@2026-09-03T10:30+01:00[Europe/London]",
        "@2026-11-01T01:30-04:00[America/New_York]",
        "@2026-11-01T01:30-05:00[America/New_York]",
        "@2026-09-03T15:00+05:30[Asia/Kolkata]",
        "@2026-09-03T09:30Z[Europe/London]",
        "@2026-09-03T23:30+13:45[Pacific/Chatham]",
        "@09:30[America/New_York]",
        "@09:30:15.25[Europe/London]",
        "@[Europe/London]",
        "@[UTC]",
        "@[Etc/GMT+1]",
        "5m",
        "1h30m",
        "1.5h",
        "250ms",
        "5400s",
        "1500000us",
        "36h",
        "0s",
        "0.5ms",
        "2m30.5s",
        "1d1h1m1s1ms1us",
        "9223372036854775807us",
    };
    for (const std::string_view spelling : spellings)
    {
        INFO("spelling: " << spelling);
        const TemporalValue value = parse_ok(spelling);
        const std::string canonical = hgl::syntax::canonical_spelling(value);
        const TemporalValue again = parse_ok(canonical);
        CHECK(again == value);
        CHECK(hgl::syntax::canonical_spelling(again) == canonical);
    }
}

TEST_CASE("values built by hand round-trip through their canonical spelling", "[temporal]")
{
    const std::vector<TemporalValue> values{
        TemporalValue{.kind = TemporalKind::Date, .micros = 0},
        TemporalValue{.kind = TemporalKind::Date, .micros = -719162},
        TemporalValue{.kind = TemporalKind::Date, .micros = 2932896},
        TemporalValue{.kind = TemporalKind::Time, .micros = 0},
        TemporalValue{.kind = TemporalKind::Time, .micros = 1},
        TemporalValue{.kind = TemporalKind::Time, .micros = us_per_day - 1},
        TemporalValue{.kind = TemporalKind::DateTime, .micros = 0},
        TemporalValue{.kind = TemporalKind::DateTime, .micros = -1},
        TemporalValue{.kind = TemporalKind::DateTime, .micros = 1},
        TemporalValue{.kind = TemporalKind::DateTime, .micros = instant_2026_09_03_0930z},
        TemporalValue{.kind = TemporalKind::CivilDateTime, .micros = civil_2026_09_03_1030},
        TemporalValue{.kind = TemporalKind::CivilDateTime, .micros = -1},
        TemporalValue{.kind = TemporalKind::ZonedDateTime, .micros = 0, .offset_seconds = -3600, .zone = "Atlantic/Azores"},
        TemporalValue{.kind = TemporalKind::ZonedDateTime, .micros = instant_2026_09_03_0930z, .offset_seconds = 3600, .zone = "Europe/London"},
        TemporalValue{.kind = TemporalKind::ZonedDateTime, .micros = instant_2026_09_03_0930z, .offset_seconds = 19800, .zone = "Asia/Kolkata"},
        TemporalValue{.kind = TemporalKind::ZonedTime, .micros = 34'200 * us_per_second, .zone = "America/New_York"},
        TemporalValue{.kind = TemporalKind::TimeZone, .zone = "Europe/London"},
        duration(0),
        duration(1),
        duration(1'000),
        duration(us_per_second),
        duration(us_per_minute),
        duration(us_per_hour),
        duration(us_per_day),
        duration(us_per_day + us_per_hour + us_per_minute + us_per_second + 1'000 + 1),
        duration(std::numeric_limits<std::int64_t>::max()),
    };
    for (const TemporalValue &value : values)
    {
        const std::string canonical = hgl::syntax::canonical_spelling(value);
        INFO("canonical: " << canonical);
        CHECK(parse_ok(canonical) == value);
    }
}
