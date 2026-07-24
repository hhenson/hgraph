#include <hgraph/lib/std/standard_types.h>
#include <hgraph/types/temporal.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_set>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace std::chrono;

    [[nodiscard]] CivilDate date(int year_value, unsigned month_value,
                                 unsigned day_value)
    {
        return CivilDate{year{year_value}, month{month_value}, day{day_value}};
    }

    [[nodiscard]] Instant instant(CivilDate value, CivilTime time = {})
    {
        return Instant{
            sys_days{value}.time_since_epoch() +
            microseconds{time.microseconds}};
    }

    class FakeTimeZoneProvider final : public TimeZoneProvider
    {
      public:
        [[nodiscard]] std::string_view version() const noexcept override
        {
            return "test-2026a";
        }

        [[nodiscard]] bool contains(ZoneId zone) const noexcept override
        {
            return zone.valid() && zone.name() == "Test/Zone";
        }

        [[nodiscard]] OffsetInfo at(Instant value,
                                    ZoneId zone) const override
        {
            if (!contains(zone))
            {
                throw std::invalid_argument("unknown test zone");
            }
            ++at_calls;
            return OffsetInfo{3600, value, checked_add(value, Duration{1})};
        }

        [[nodiscard]] LocalResolution resolve(
            CivilDateTime local, ZoneId zone) const override
        {
            if (!contains(zone))
            {
                throw std::invalid_argument("unknown test zone");
            }
            ++resolve_calls;
            if (local.hour() == 1)
            {
                return LocalResolution{
                    LocalResolutionKind::Ambiguous,
                    {Instant{Duration{100}}, 3600},
                    {Instant{Duration{200}}, 0}};
            }
            if (local.hour() == 2)
            {
                return LocalResolution{
                    LocalResolutionKind::Nonexistent,
                    {Instant{Duration{299}}, 3600},
                    {Instant{Duration{300}}, 7200}};
            }
            return LocalResolution{
                LocalResolutionKind::Unique,
                {Instant{Duration{local.epoch_microseconds()}}, 3600}, {}};
        }

        mutable std::size_t at_calls{0};
        mutable std::size_t resolve_calls{0};
    };
}  // namespace

static_assert(std::is_standard_layout_v<hgraph::CivilDateTime>);
static_assert(std::is_trivially_copyable_v<hgraph::CivilDateTime>);
static_assert(std::is_standard_layout_v<hgraph::Period>);
static_assert(std::is_trivially_copyable_v<hgraph::Period>);
static_assert(std::is_standard_layout_v<hgraph::ZoneId>);
static_assert(std::is_trivially_copyable_v<hgraph::ZoneId>);
static_assert(std::is_standard_layout_v<hgraph::ZonedDateTime>);
static_assert(std::is_trivially_copyable_v<hgraph::ZonedDateTime>);
static_assert(std::is_standard_layout_v<hgraph::InstantRange>);
static_assert(std::is_trivially_copyable_v<hgraph::InstantRange>);
static_assert(std::is_standard_layout_v<hgraph::InstantRangeSet>);
static_assert(std::is_trivially_copyable_v<hgraph::InstantRangeSet>);

TEST_CASE("temporal checked arithmetic rejects overflow and rounds ties to even")
{
    CHECK(checked_add(Instant{Duration{10}}, Duration{2}) ==
          Instant{Duration{12}});
    CHECK(checked_subtract(Instant{Duration{10}}, Instant{Duration{3}}) ==
          Duration{7});
    CHECK_THROWS_AS(
        checked_add(Instant::max(), Duration{1}), std::overflow_error);
    CHECK_THROWS_AS(
        checked_negate(Duration{std::numeric_limits<std::int64_t>::min()}),
        std::overflow_error);

    CHECK(checked_multiply(Duration{1}, 0.5) == Duration{0});
    CHECK(checked_multiply(Duration{3}, 0.5) == Duration{2});
    CHECK(checked_multiply(Duration{-3}, 0.5) == Duration{-2});
    CHECK(checked_divide(Duration{1}, std::int64_t{2}) == Duration{0});
    CHECK(checked_divide(Duration{3}, std::int64_t{2}) == Duration{2});
    CHECK(checked_divide(Duration{-3}, std::int64_t{2}) == Duration{-2});

    CHECK(round(Duration{500'000}, Duration{1'000'000}) == Duration{0});
    CHECK(round(Duration{-500'000}, Duration{1'000'000}) == Duration{0});
    CHECK(round(Duration{1'500'000}, Duration{1'000'000}) ==
          Duration{2'000'000});
    CHECK(round(Duration{-1'500'000}, Duration{1'000'000}) ==
          Duration{-2'000'000});
    CHECK(floor(Duration{-1}, Duration{1'000'000}) ==
          Duration{-1'000'000});
    CHECK(ceil(Duration{-1}, Duration{1'000'000}) == Duration{0});
    CHECK(ceil(Duration::min(), Duration::max()) ==
          Duration{-std::numeric_limits<std::int64_t>::max()});
    CHECK_THROWS_AS(floor(Duration::min(), Duration::max()),
                    std::overflow_error);

    CHECK(ceil(Instant::min(), Duration::max(), Instant::max()) ==
          Instant{Duration{-std::numeric_limits<std::int64_t>::max()}});
    CHECK(round(Instant::min(), Duration::max(), Instant::max()) ==
          Instant{Duration{-std::numeric_limits<std::int64_t>::max()}});
    CHECK_THROWS_AS(
        floor(Instant::min(), Duration::max(), Instant::max()),
        std::overflow_error);
    CHECK(floor(Instant::max(), Duration::max(), Instant::min()) ==
          Instant{Duration{
              std::numeric_limits<std::int64_t>::max() - 1}});
    CHECK_THROWS_AS(
        ceil(Instant::max(), Duration::max(), Instant::min()),
        std::overflow_error);
}

TEST_CASE("civil datetime validates fields and supplies Python-compatible accessors")
{
    const CivilDateTime value{date(2026, 7, 23), 21, 5, 6, 7};
    CHECK(value.date() == date(2026, 7, 23));
    CHECK(value.time() == time_of_day(21, 5, 6, 7));
    CHECK(value.year() == 2026);
    CHECK(value.month() == 7);
    CHECK(value.day() == 23);
    CHECK(value.hour() == 21);
    CHECK(value.minute() == 5);
    CHECK(value.second() == 6);
    CHECK(value.microsecond() == 7);
    CHECK(value.weekday() == 3);  // Python-compatible Monday == 0.
    CHECK(value.isoweekday() == 4);
    CHECK(value.day_of_year() == 204);
    CHECK(format_civil_datetime(value) == "2026-07-23T21:05:06.000007");

    CHECK_THROWS_AS(
        (CivilDateTime{date(2026, 2, 29), CivilTime{}}),
        std::invalid_argument);
    CHECK_THROWS_AS(
        (CivilDateTime{date(2026, 1, 1), 24}), std::invalid_argument);
}

TEST_CASE("periods normalize months and apply explicit month-end policies")
{
    CHECK(Period{1} == Period{0, 12});
    CHECK(checked_add(Period{0, 10, 2}, Period{0, 3, -1}) ==
          Period{1, 1, 1});

    const CivilDate january_end = date(2025, 1, 31);
    CHECK_THROWS_AS(
        apply_period(january_end, Period{0, 1},
                     MonthEndPolicy::Reject),
        std::invalid_argument);
    CHECK(apply_period(january_end, Period{0, 1},
                       MonthEndPolicy::Clamp) ==
          date(2025, 2, 28));
    CHECK(apply_period(date(2024, 2, 29), Period{1},
                       MonthEndPolicy::PreserveEndOfMonth) ==
          date(2025, 2, 28));
    CHECK(apply_period(date(2024, 3, 31), Period{0, -1},
                       MonthEndPolicy::Clamp) ==
          date(2024, 2, 29));
    CHECK(apply_period(date(2025, 1, 30), Period{0, 1, 1},
                       MonthEndPolicy::Clamp) ==
          date(2025, 3, 1));
}

TEST_CASE("zone identifiers are compact interned exact-name values")
{
    const ZoneId utc{"UTC"};
    const ZoneId utc_again{"UTC"};
    const ZoneId new_york{"America/New_York"};
    CHECK(utc.valid());
    CHECK(utc == utc_again);
    CHECK(utc != new_york);
    CHECK(utc.name() == "UTC");
    CHECK(ZoneId::valid_syntax("Etc/GMT+4"));
    CHECK_FALSE(ZoneId::valid_syntax("../UTC"));
    CHECK_FALSE(ZoneId::valid_syntax("America//New_York"));
    CHECK_FALSE(ZoneId::valid_syntax("America\\New_York"));
    CHECK_FALSE(ZoneId::from_payload({utc.slot(), 0, utc.name_tag()}).valid());
    CHECK_FALSE(ZoneId::from_payload(
                    {utc.slot(), utc.generation(),
                     static_cast<std::uint16_t>(utc.name_tag() + 1)})
                    .valid());

    std::unordered_set<ZoneId> values{utc, utc_again, new_york};
    CHECK(values.size() == 2);

    clear_zone_name_registry();
    CHECK_FALSE(utc.valid());
    CHECK_THROWS_AS(utc.name(), std::invalid_argument);
    const ZoneId reloaded{"UTC"};
    CHECK(reloaded.slot() == utc.slot());
    CHECK(reloaded.generation() ==
          static_cast<std::uint16_t>(utc.generation() + 1));
    CHECK(reloaded.name() == "UTC");
}

TEST_CASE("native zone bindings are withdrawn before zone generations reset")
{
    const auto provider = make_time_zone_provider();
    const ZoneId original{"UTC"};
    const Instant sample{Duration{0}};
    CHECK(at_zone(sample, original, *provider).offset_seconds() == 0);
    CHECK(at_zone(sample, original, *provider).offset_seconds() == 0);

    clear_time_zone_provider_cache();
    clear_zone_name_registry();
    CHECK_FALSE(original.valid());

    const ZoneId reloaded{"UTC"};
    CHECK(reloaded.slot() == original.slot());
    CHECK(reloaded.generation() ==
          static_cast<std::uint16_t>(original.generation() + 1));
    CHECK(at_zone(sample, reloaded, *provider).offset_seconds() == 0);
}

TEST_CASE("range algebra preserves endpoint topology and fixed capacity")
{
    using R = InstantRange;
    const Instant t0{Duration{0}};
    const Instant t1{Duration{1}};
    const Instant t2{Duration{2}};
    const Instant t3{Duration{3}};

    const R left = R::bounded(t0, t1);
    const R right = R::bounded(t1, t2);
    CHECK(left.adjacent(right));
    CHECK(left.mergeable(right));
    CHECK(left.merge(right) == R::bounded(t0, t2));
    CHECK_FALSE(R::bounded(t0, t1, Boundary::Open, Boundary::Open)
                    .adjacent(R::bounded(t1, t2, Boundary::Open,
                                         Boundary::Open)));

    const auto split =
        R::bounded(t0, t3).difference(R::bounded(t1, t2));
    REQUIRE(split.size() == 2);
    CHECK(split[0] == R::bounded(t0, t1));
    CHECK(split[1] == R::bounded(t2, t3));

    const auto separated = left.set_union(R::bounded(t2, t3));
    REQUIRE(separated.size() == 2);
    CHECK(separated[0] == left);
    CHECK(separated[1] == R::bounded(t2, t3));
    CHECK(R::make_empty().contains(R::make_empty()));
    CHECK(R::all().contains(left));
}

TEST_CASE("range algebra is exhaustive over a small endpoint domain")
{
    using R = TimeRange<int>;
    std::vector<R> ranges{R::make_empty(), R::all()};
    const auto add_unique = [&](R value) {
        if (std::find(ranges.begin(), ranges.end(), value) == ranges.end())
        {
            ranges.push_back(value);
        }
    };
    for (int endpoint = 0; endpoint <= 2; ++endpoint)
    {
        add_unique(R::from(endpoint, Boundary::Open));
        add_unique(R::from(endpoint, Boundary::Closed));
        add_unique(R::until(endpoint, Boundary::Open));
        add_unique(R::until(endpoint, Boundary::Closed));
    }
    for (int lower = 0; lower <= 2; ++lower)
    {
        for (int upper = lower; upper <= 2; ++upper)
        {
            for (const Boundary lower_boundary :
                 {Boundary::Open, Boundary::Closed})
            {
                for (const Boundary upper_boundary :
                     {Boundary::Open, Boundary::Closed})
                {
                    add_unique(R::bounded(lower, upper, lower_boundary,
                                          upper_boundary));
                }
            }
        }
    }

    for (const R &left : ranges)
    {
        CHECK(left.intersection(left) == left);
        for (const R &right : ranges)
        {
            const R intersection = left.intersection(right);
            CHECK(intersection == right.intersection(left));
            CHECK(left.contains(right) ==
                  (intersection == right));

            const auto difference = left.difference(right);
            const auto union_result = left.set_union(right);
            CHECK(difference.size() <= 2);
            CHECK(union_result.size() <= 2);
            for (int point = -1; point <= 3; ++point)
            {
                const bool in_difference = std::any_of(
                    difference.begin(), difference.end(),
                    [&](const R &part) { return part.contains(point); });
                const bool in_union = std::any_of(
                    union_result.begin(), union_result.end(),
                    [&](const R &part) { return part.contains(point); });
                CHECK(intersection.contains(point) ==
                      (left.contains(point) && right.contains(point)));
                CHECK(in_difference ==
                      (left.contains(point) && !right.contains(point)));
                CHECK(in_union ==
                      (left.contains(point) || right.contains(point)));
            }
        }
    }
}

TEST_CASE("iteration, partitioning, shifting, and bucketing are allocation-free surfaces")
{
    const InstantRange range =
        InstantRange::bounded(Instant{Duration{0}}, Instant{Duration{5}});
    std::vector<std::int64_t> coordinates;
    iterate(range, Duration{2}, [&](Instant value) {
        coordinates.push_back(value.time_since_epoch().count());
    });
    CHECK(coordinates == std::vector<std::int64_t>{0, 2, 4});

    std::vector<InstantRange> pieces;
    partition(range, Duration{2},
              [&](InstantRange value) { pieces.push_back(value); });
    REQUIRE(pieces.size() == 3);
    CHECK(pieces.back() ==
          InstantRange::bounded(Instant{Duration{4}}, Instant{Duration{5}}));

    CHECK(bucket(Instant{Duration{-1}}, Duration{10}) ==
          InstantRange::bounded(Instant{Duration{-10}},
                                Instant{Duration{0}}));
    CHECK(extent(shift(range, Duration{10})) == Duration{5});

    std::vector<Instant> upper_edge;
    iterate(InstantRange::bounded(Instant::max(), Instant::max(),
                                  Boundary::Closed, Boundary::Closed),
            Duration{1},
            [&](Instant value) { upper_edge.push_back(value); });
    CHECK(upper_edge == std::vector<Instant>{Instant::max()});

    std::vector<InstantRange> clipped_at_upper_edge;
    partition(
        InstantRange::bounded(
            checked_subtract(Instant::max(), Duration{5}), Instant::max()),
        Duration{10},
        [&](InstantRange value) { clipped_at_upper_edge.push_back(value); });
    REQUIRE(clipped_at_upper_edge.size() == 1);
    CHECK(clipped_at_upper_edge.front() ==
          InstantRange::bounded(
              checked_subtract(Instant::max(), Duration{5}), Instant::max()));
}

TEST_CASE("duration canonical form has exactly one representation")
{
    CHECK(format_duration(Duration{0}) == "0us");
    CHECK(format_duration(Duration{-86'400'000'000}) ==
          "-86400000000us");
    CHECK(parse_duration("1us") == Duration{1});
    CHECK(parse_duration("-9223372036854775808us") ==
          Duration{std::numeric_limits<std::int64_t>::min()});
    CHECK_THROWS_AS(parse_duration("+1us"), std::invalid_argument);
    CHECK_THROWS_AS(parse_duration("01us"), std::invalid_argument);
    CHECK_THROWS_AS(parse_duration("-0us"), std::invalid_argument);
    CHECK_THROWS_AS(parse_duration("1"), std::invalid_argument);
    CHECK_THROWS_AS(parse_duration("9223372036854775808us"),
                    std::invalid_argument);
    CHECK_THROWS_AS(parse_duration("PT1S"), std::invalid_argument);
}

TEST_CASE("date tz provider resolves timeline, folds, and gaps deterministically")
{
    const auto provider = make_time_zone_provider();
    REQUIRE(provider);
    CHECK_FALSE(provider->version().empty());
    const ZoneId new_york{"America/New_York"};
    REQUIRE(provider->contains(new_york));

    const Instant winter = instant(date(2025, 1, 15), time_of_day(12, 0));
    const ZonedDateTime zoned = at_zone(winter, new_york, *provider);
    CHECK(zoned.offset_seconds() == -5 * 3600);
    CHECK(zoned.civil() == CivilDateTime{date(2025, 1, 15), 7});

    const CivilDateTime fold{date(2025, 11, 2), 1, 30};
    CHECK_THROWS_AS(resolve(fold, new_york, *provider),
                    std::invalid_argument);
    const auto early =
        resolve(fold, new_york, *provider, AmbiguousTimePolicy::Earliest);
    const auto late =
        resolve(fold, new_york, *provider, AmbiguousTimePolicy::Latest);
    CHECK(checked_subtract(late.instant(), early.instant()) ==
          std::chrono::hours{1});
    CHECK(early.offset_seconds() == -4 * 3600);
    CHECK(late.offset_seconds() == -5 * 3600);

    const CivilDateTime gap{date(2025, 3, 9), 2, 30};
    CHECK_THROWS_AS(resolve(gap, new_york, *provider),
                    std::invalid_argument);
    const auto before = resolve(
        gap, new_york, *provider, AmbiguousTimePolicy::Reject,
        NonexistentTimePolicy::PreviousValid);
    const auto after = resolve(
        gap, new_york, *provider, AmbiguousTimePolicy::Reject,
        NonexistentTimePolicy::NextValid);
    CHECK(before.civil() ==
          CivilDateTime{date(2025, 3, 9), 1, 59, 59, 999999});
    CHECK(after.civil() == CivilDateTime{date(2025, 3, 9), 3});
    CHECK(checked_subtract(after.instant(), before.instant()) == Duration{1});
}

TEST_CASE("date tz provider covers non-hour transitions, skipped days, history, and links")
{
    const auto provider = make_time_zone_provider();
    REQUIRE(provider);

    const ZoneId lord_howe{"Australia/Lord_Howe"};
    REQUIRE(provider->contains(lord_howe));
    const CivilDateTime half_hour_fold{
        date(2025, 4, 6), 1, 45};
    const auto half_hour_early =
        resolve(half_hour_fold, lord_howe, *provider,
                AmbiguousTimePolicy::Earliest);
    const auto half_hour_late =
        resolve(half_hour_fold, lord_howe, *provider,
                AmbiguousTimePolicy::Latest);
    CHECK(checked_subtract(half_hour_late.instant(),
                           half_hour_early.instant()) ==
          std::chrono::minutes{30});
    CHECK(half_hour_early.offset_seconds() == 11 * 3600);
    CHECK(half_hour_late.offset_seconds() == 10 * 3600 + 30 * 60);

    const ZoneId apia{"Pacific/Apia"};
    REQUIRE(provider->contains(apia));
    const CivilDateTime skipped_day{date(2011, 12, 30), 12};
    CHECK_THROWS_AS(resolve(skipped_day, apia, *provider),
                    std::invalid_argument);
    const auto skipped_before = resolve(
        skipped_day, apia, *provider, AmbiguousTimePolicy::Reject,
        NonexistentTimePolicy::PreviousValid);
    const auto skipped_after = resolve(
        skipped_day, apia, *provider, AmbiguousTimePolicy::Reject,
        NonexistentTimePolicy::NextValid);
    CHECK(skipped_before.civil() ==
          CivilDateTime{date(2011, 12, 29), 23, 59, 59, 999999});
    CHECK(skipped_after.civil() ==
          CivilDateTime{date(2011, 12, 31), 0});
    CHECK(checked_subtract(skipped_after.instant(),
                           skipped_before.instant()) == Duration{1});

    const ZoneId paris{"Europe/Paris"};
    REQUIRE(provider->contains(paris));
    CHECK(at_zone(instant(date(1900, 1, 1)), paris, *provider)
              .offset_seconds() == 561);

    const ZoneId link{"US/Eastern"};
    const ZoneId canonical{"America/New_York"};
    REQUIRE(provider->contains(link));
    const Instant sample =
        instant(date(2025, 7, 1), time_of_day(12, 0));
    const auto linked = at_zone(sample, link, *provider);
    const auto canonical_value =
        at_zone(sample, canonical, *provider);
    CHECK(linked.zone() != canonical_value.zone());
    CHECK(linked.same_instant(canonical_value));
    CHECK(linked.offset_seconds() ==
          canonical_value.offset_seconds());
}

TEST_CASE("provider boundary handles unique, ambiguous, nonexistent, and unknown zones")
{
    const FakeTimeZoneProvider provider;
    const ZoneId zone{"Test/Zone"};
    const ZoneId unknown{"Test/Unknown"};
    const CivilDate test_day = date(2026, 1, 1);

    const auto projected =
        at_zone(Instant{Duration{10}}, zone, provider);
    CHECK(projected.offset_seconds() == 3600);
    CHECK(provider.at_calls == 1);

    const auto unique =
        resolve(CivilDateTime{test_day, 0}, zone, provider);
    CHECK(unique.offset_seconds() == 3600);
    CHECK(provider.resolve_calls == 1);

    CHECK_THROWS_AS(
        resolve(CivilDateTime{test_day, 1}, zone, provider),
        std::invalid_argument);
    CHECK(resolve(CivilDateTime{test_day, 1}, zone, provider,
                  AmbiguousTimePolicy::Earliest)
              .instant() == Instant{Duration{100}});
    CHECK(resolve(CivilDateTime{test_day, 1}, zone, provider,
                  AmbiguousTimePolicy::Latest)
              .instant() == Instant{Duration{200}});

    CHECK_THROWS_AS(
        resolve(CivilDateTime{test_day, 2}, zone, provider),
        std::invalid_argument);
    CHECK(resolve(CivilDateTime{test_day, 2}, zone, provider,
                  AmbiguousTimePolicy::Reject,
                  NonexistentTimePolicy::PreviousValid)
              .instant() == Instant{Duration{299}});
    CHECK(resolve(CivilDateTime{test_day, 2}, zone, provider,
                  AmbiguousTimePolicy::Reject,
                  NonexistentTimePolicy::NextValid)
              .instant() == Instant{Duration{300}});

    CHECK_THROWS_AS(
        at_zone(Instant{}, unknown, provider), std::invalid_argument);
    CHECK_THROWS_AS(
        resolve(CivilDateTime{test_day, 0}, unknown, provider),
        std::invalid_argument);
}

TEST_CASE("temporal schemas participate in standard registration")
{
    const auto types = hgraph::stdlib::register_standard_types();
    CHECK(types.period_type != nullptr);
    CHECK(types.ts_period != nullptr);
    CHECK(types.zone_id_type != nullptr);
    CHECK(types.ts_zoned_datetime != nullptr);
    CHECK(types.instant_range_set_type != nullptr);
}
