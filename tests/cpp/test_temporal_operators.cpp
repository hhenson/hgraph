#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/types/temporal.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::chrono;

    [[nodiscard]] CivilDate date(int year_value, unsigned month_value,
                                 unsigned day_value)
    {
        return CivilDate{
            year{year_value}, month{month_value}, day{day_value}};
    }
}  // namespace

TEST_CASE("temporal graph operators use checked arithmetic and wiring-time policies")
{
    hgraph::stdlib::register_standard_operators();
    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::add_>(
            values<CivilDate>(date(2025, 1, 31)),
            values<Period>(Period{0, 1}),
            arg<"month_end_policy">(MonthEndPolicy::Clamp)),
        values<CivilDate>(date(2025, 2, 28)));

    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::temporal_round>(
            values<Duration>(
                Duration{500'000}, Duration{1'500'000},
                Duration{-500'000}, Duration{-1'500'000}),
            values<Duration>(
                Duration{1'000'000}, Duration{1'000'000},
                Duration{1'000'000}, Duration{1'000'000})),
        values<Duration>(
            Duration{0}, Duration{2'000'000}, Duration{0},
            Duration{-2'000'000}));

    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::add_>(
            values<CivilDate>(date(2025, 1, 31)),
            values<CivilTime>(time_of_day(9, 30))),
        values<CivilDateTime>(
            CivilDateTime{date(2025, 1, 31), 9, 30}));

    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::temporal_floor>(
            values<Instant>(Instant{Duration{15}}),
            values<Duration>(Duration{10}),
            arg<"origin">(Instant{Duration{5}})),
        values<Instant>(Instant{Duration{15}}));

    CHECK_THROWS_AS(
        eval_node<hgraph::stdlib::add_>(
            values<Instant>(Instant::max()),
            values<Duration>(Duration{1})),
        std::exception);
}

TEST_CASE("temporal zone graph operators resolve through GlobalState")
{
    hgraph::stdlib::register_standard_operators();
    GlobalState state;
    const auto provider = make_time_zone_provider();
    set_time_zone_provider(state.view(), provider);
    GlobalContext context{state};

    const ZoneId zone{"America/New_York"};
    const Instant instant{
        sys_days{date(2025, 1, 15)}.time_since_epoch() + hours{12}};
    const ZonedDateTime expected =
        hgraph::at_zone(instant, zone, *provider);
    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::at_zone>(
            values<Instant>(instant), values<ZoneId>(zone)),
        values<ZonedDateTime>(expected));
    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::add_>(
            values<ZonedDateTime>(expected),
            values<Duration>(hours{24})),
        values<ZonedDateTime>(
            hgraph::checked_add(
                expected, hours{24}, *provider)));

    const CivilDateTime fold{date(2025, 11, 2), 1, 30};
    const auto latest = hgraph::resolve(
        fold, zone, *provider, AmbiguousTimePolicy::Latest);
    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::resolve_civil>(
            values<CivilDateTime>(fold), values<ZoneId>(zone),
            arg<"ambiguous">(AmbiguousTimePolicy::Latest),
            arg<"nonexistent">(NonexistentTimePolicy::Reject)),
        values<ZonedDateTime>(latest));
}

TEST_CASE("temporal range graph operators preserve concrete schemas")
{
    hgraph::stdlib::register_standard_operators();
    const InstantRange whole =
        InstantRange::bounded(Instant{Duration{0}},
                              Instant{Duration{10}});
    const InstantRange middle =
        InstantRange::bounded(Instant{Duration{2}},
                              Instant{Duration{8}});

    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::range_contains>(
            values<InstantRange>(whole),
            values<InstantRange>(middle)),
        values<Bool>(true));
    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::range_difference>(
            values<InstantRange>(whole),
            values<InstantRange>(middle)),
        values<InstantRangeSet>(
            InstantRangeSet{
                InstantRange::bounded(
                    Instant{Duration{0}}, Instant{Duration{2}}),
                InstantRange::bounded(
                    Instant{Duration{8}}, Instant{Duration{10}})}));
    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::range_overlaps>(
            values<InstantRange>(whole),
            values<InstantRange>(middle)),
        values<Bool>(true));
    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::range_hull>(
            values<InstantRange>(
                InstantRange::bounded(
                    Instant{Duration{0}}, Instant{Duration{2}})),
            values<InstantRange>(
                InstantRange::bounded(
                    Instant{Duration{8}}, Instant{Duration{10}}))),
        values<InstantRange>(whole));
    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::range_shift>(
            values<InstantRange>(whole),
            values<Duration>(Duration{5})),
        values<InstantRange>(
            InstantRange::bounded(
                Instant{Duration{5}}, Instant{Duration{15}})));
    CHECK_OUTPUT(
        eval_node<hgraph::stdlib::range_extent>(
            values<InstantRange>(whole)),
        values<Duration>(Duration{10}));
}
