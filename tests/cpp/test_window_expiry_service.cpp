#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

#include <array>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using Window = TSWDuration<Int, 2, 0>;

    struct Append
    {
        static void eval(In<"sample", TS<Int>> sample, Out<Window> out)
        {
            out.begin_mutation(out.evaluation_time()).push(sample.base().value());
        }
    };

    struct DirectWindow
    {
        static Port<Window> compose(Wiring &w, Port<TS<Int>> samples)
        {
            return wire<Append>(w, samples);
        }
    };

    struct StandardWindow
    {
        static Port<Window> compose(Wiring &w, Port<TS<Int>> samples)
        {
            return wire<stdlib::to_window>(w, samples, 2 * MIN_TD, TimeDelta{}).as<Window>();
        }
    };

    struct ResettableWindow
    {
        static Port<Window> compose(Wiring &w, Port<TS<Int>> samples, Port<SIGNAL> reset)
        {
            return wire<stdlib::to_window>(w, samples, 2 * MIN_TD, TimeDelta{}, reset).as<Window>();
        }
    };

    Int total(const TSWInputView &window)
    {
        Int result{};
        for (const ValueView value : window.values()) { result += value.checked_as<Int>(); }
        return result;
    }

    struct SumBoth
    {
        static void eval(In<"left", Window, InputValidity::Unchecked> left,
                         In<"right", Window, InputValidity::Unchecked> right, Out<TS<Int>> out)
        {
            out.set(total(left) + total(right));
        }
    };

    struct IndependentWindows
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> left, Port<TS<Int>> right)
        {
            return wire<SumBoth>(w, wire<Append>(w, left), wire<Append>(w, right));
        }
    };

    using WindowPair = TSB<"ExpiryWindowPair", Field<"windows", TSL<Window, 2>>>;

    struct WritePair
    {
        static void eval(In<"sample", TS<Int>> sample, Out<WindowPair> out)
        {
            auto windows = out.field<"windows">();
            windows[0].begin_mutation(out.evaluation_time()).push(sample.base().value());
            windows[1].begin_mutation(out.evaluation_time()).push(sample.base().value());
        }
    };

    struct SumPair
    {
        static void eval(In<"pair", WindowPair> pair, Out<TS<Int>> out)
        {
            auto windows = pair.field<"windows">();
            out.set(total(windows[0]) + total(windows[1]));
        }
    };

    struct BundledWindows
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> samples)
        {
            return wire<SumPair>(w, wire<WritePair>(w, samples));
        }
    };

    struct SumWindow
    {
        static void eval(In<"window", Window, InputValidity::Unchecked> window, Out<TS<Int>> out)
        {
            out.set(total(window));
        }
    };

    struct WindowBranch
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> samples)
        {
            return wire<SumWindow>(w, wire<Append>(w, samples));
        }
    };

    struct PassBranch
    {
        static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> samples) { return samples; }
    };

    struct SwitchedWindow
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> key, Port<TS<Int>> samples)
        {
            return wire<stdlib::switch_>(
                w, key, stdlib::switch_cases({{Value{Int{0}}, fn<WindowBranch>()},
                                              {Value{Int{1}}, fn<PassBranch>()}}),
                samples).as<TS<Int>>();
        }
    };

    struct InspectReplay
    {
        static void eval(In<"at", SIGNAL, InputValidity::Unchecked>,
                         In<"window", Window, InputValidity::Unchecked> window, Out<TS<Int>> out)
        {
            out.set(total(window));
        }
    };

    struct InstallHistory
    {
        static void eval(In<"latest", TS<Int>> latest, Out<Window> out)
        {
            const DateTime now = out.evaluation_time();
            ListBuilder samples{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())};
            samples.push_back(Int{7});
            samples.push_back(Int{3});
            samples.push_back(latest.value());
            const std::array times{now - MIN_TD, now - MIN_TD, now};
            out.begin_mutation(now).replace_samples(samples.build().view(), times);
        }
    };

    struct SampledWindow
    {
        static Port<Window> compose(Wiring &w, Port<SIGNAL> trigger, Port<TS<Int>> values)
        {
            return wire<stdlib::sample>(w, trigger, StandardWindow::compose(w, values)).as<Window>();
        }
    };

    struct ThrottledWindow
    {
        static Port<Window> compose(Wiring &w, Port<TS<Int>> samples)
        {
            return wire<stdlib::throttle>(w, StandardWindow::compose(w, samples), 5 * MIN_TD, true).as<Window>();
        }
    };

    struct VariableLagWindow
    {
        static Port<Window> compose(Wiring &w, Port<TS<Int>> samples, Port<TS<TimeDelta>> delay)
        {
            return wire<stdlib::lag>(w, StandardWindow::compose(w, samples), delay).as<Window>();
        }
    };

    struct ProxyLagWindow
    {
        static Port<Window> compose(Wiring &w, Port<TS<Int>> samples, Port<TS<Bool>> pulse)
        {
            return wire<stdlib::lag>(w, StandardWindow::compose(w, samples), Int{1}, pulse).as<Window>();
        }
    };

    struct LatestGatedWindow
    {
        static Port<Window> compose(Wiring &w, Port<TS<Int>> samples, Port<TS<Bool>> open)
        {
            return wire<stdlib::gate>(w, open, StandardWindow::compose(w, samples), Int{-1}).as<Window>();
        }
    };

    struct LaggedWindow
    {
        static Port<Window> compose(Wiring &w, Port<TS<Int>> samples)
        {
            return wire<stdlib::lag>(w, StandardWindow::compose(w, samples), MIN_TD).as<Window>();
        }
    };

    struct TakenWindow
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> samples, Port<SIGNAL> inspect)
        {
            auto taken = wire<stdlib::take>(w, StandardWindow::compose(w, samples), Int{2});
            return wire<InspectReplay>(w, inspect, taken.as<Window>());
        }
    };

    struct FilteredWindow
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> open, Port<TS<Int>> samples, Port<SIGNAL> inspect)
        {
            auto filtered = wire<stdlib::filter_>(w, open, StandardWindow::compose(w, samples));
            return wire<InspectReplay>(w, inspect, filtered.as<Window>());
        }
    };

    struct HistoricalWindow
    {
        static Port<TS<Int>> compose(Wiring &w, Port<SIGNAL> inspect, Port<Window> recorded)
        {
            return wire<InspectReplay>(w, inspect, recorded);
        }
    };
}

TEST_CASE("duration window expires one tick after its inclusive boundary without another input")
{
    stdlib::register_standard_operators();

    // A single sample remains for ages 0, 1 and 2. Its only other publication is
    // the departure at age 3, carrying the time of the original arrival.
    const auto expected = values<Value>(window_delta<Int>(7), none, none,
                                        window_delta<Int>(std::nullopt, {{7, MIN_ST}}));
    CHECK_OUTPUT(eval_node<DirectWindow>(values<Int>(7)), expected);
    CHECK_OUTPUT(eval_node<StandardWindow>(values<Int>(7)), expected);
}

TEST_CASE("duration window expiry and a coincident reset and arrival publish one ordered delta")
{
    stdlib::register_standard_operators();

    // At t3 the oldest sample expires, reset clears the second, and 9 arrives.
    // Clearing the second sample cancels its old t4 deadline.
    CHECK_OUTPUT(eval_node<ResettableWindow>(values<Int>(4, 5, none, 9),
                                             values<Bool>(none, none, none, true)),
                 values<Value>(window_delta<Int>(4), window_delta<Int>(5), none,
                               window_delta<Int>(9, {{4, MIN_ST}, {5, MIN_ST + MIN_TD}}, true),
                               none, none, window_delta<Int>(std::nullopt, {{9, MIN_ST + 3 * MIN_TD}})));
}

TEST_CASE("independent duration window owners expire only their own samples")
{
    CHECK_OUTPUT(eval_node<IndependentWindows>(values<Int>(2, none, 5), values<Int>(none, 11)),
                 values<Int>(2, 13, 18, 16, 5, 0));
}

TEST_CASE("equal duration window deadlines under a bundle and fixed list reach one consumer")
{
    CHECK_OUTPUT(eval_node<BundledWindows>(values<Int>(7)), values<Int>(14, none, none, 0));
}

TEST_CASE("switching a duration window branch drops its deadline and a new branch still expires")
{
    stdlib::register_standard_operators();

    // The first branch stops at t1, before its t3 deadline. Its replacement at
    // t4 receives a new sample and must wake its own downstream sum at t7.
    CHECK_OUTPUT(eval_node<SwitchedWindow>(values<Int>(0, 1, none, none, 0),
                                           values<Int>(4, 9, none, none, 7)),
                 values<Int>(4, 9, none, none, 7, none, none, 0));
}

TEST_CASE("historical duration window replay keeps a recorded departure later than the live deadline")
{
    const auto events = values<Value>(window_delta<Int>(7), none, none, none, none,
                                      window_delta<Int>(std::nullopt, {{7, MIN_ST}}));
    // Reading at t3, when a live window would expire, still finds the sample.
    CHECK_OUTPUT(eval_node<HistoricalWindow>(values<Bool>(none, none, none, true), events),
                 values<Int>(7, none, none, 7, none, 0));
}

TEST_CASE("duration window removes equal-timestamp snapshot samples together and in insertion order")
{
    // At t1 install 7 and 3 from t0 plus 11 from t1. Installing the complete
    // history must arm the oldest deadline and rearm for the remaining sample.
    const auto events = eval_node<InstallHistory>(values<Int>(none, 11));
    REQUIRE(events.size() == 5);
    CHECK_FALSE(events[0].has_value());
    REQUIRE(events[1].has_value());
    CHECK(events[1]->view().as_bundle().at("replacement").as_indexed_view().size() == 3);
    CHECK_FALSE(events[2].has_value());
    REQUIRE(events[3].has_value());
    CHECK(events[3]->equals(window_delta<Int>(std::nullopt, {{7, MIN_ST}, {3, MIN_ST}})));
    REQUIRE(events[4].has_value());
    CHECK(events[4]->equals(window_delta<Int>(std::nullopt, {{11, MIN_ST + MIN_TD}})));
}

TEST_CASE("sampling a held duration window keeps the time of each retained observation")
{
    stdlib::register_standard_operators();
    const auto events = eval_node<SampledWindow>(values<Bool>(none, none, true), values<Int>(4, 7));
    REQUIRE(events.size() == 3);
    REQUIRE(events[2].has_value());
    auto samples = events[2]->view().as_bundle().at("replacement").as_indexed_view();
    REQUIRE(samples.size() == 2);
    CHECK(samples.at(0).as_bundle().at("value").checked_as<Int>() == 4);
    CHECK(samples.at(0).as_bundle().at("time").checked_as<DateTime>() == MIN_ST);
    CHECK(samples.at(1).as_bundle().at("value").checked_as<Int>() == 7);
    CHECK(samples.at(1).as_bundle().at("time").checked_as<DateTime>() == MIN_ST + MIN_TD);
}

TEST_CASE("buffered duration windows release the surviving source observations as one snapshot")
{
    stdlib::register_standard_operators();
    // 3 arrives at t0 and leaves at t3. 7 arrives at t4, before release.
    const auto inputs = values<Int>(3, none, none, none, 7);
    const auto check_release = [](const auto &events, std::size_t release) {
        REQUIRE(events.size() > release);
        for (std::size_t i = 0; i < release; ++i) { CHECK_FALSE(events[i].has_value()); }
        REQUIRE(events[release].has_value());
        const auto retained = events[release]->view().as_bundle().at("replacement").as_indexed_view();
        REQUIRE(retained.size() == 1);
        CHECK(retained.at(0).as_bundle().at("value").template checked_as<Int>() == 7);
        CHECK(retained.at(0).as_bundle().at("time").template checked_as<DateTime>() == MIN_ST + 4 * MIN_TD);
    };

    SECTION("throttle") { check_release(eval_node<ThrottledWindow>(inputs), 5); }
    SECTION("variable time lag")
    {
        const auto delay = values<TimeDelta>(6 * MIN_TD, none, none, 3 * MIN_TD, 2 * MIN_TD);
        check_release(eval_node<VariableLagWindow>(inputs, delay), 6);
    }
    SECTION("proxy lag")
    {
        const auto pulse = values<Bool>(true, none, none, none, none, true, none, none, none, true);
        check_release(eval_node<ProxyLagWindow>(inputs, pulse), 5);
    }
    SECTION("truncated gate")
    {
        const auto opened = values<Bool>(false, none, none, none, none, true);
        check_release(eval_node<LatestGatedWindow>(inputs, opened), 5);
    }
    SECTION("arrival and departure cancel before first delivery")
    {
        const auto events = eval_node<ThrottledWindow>(values<Int>(3));
        REQUIRE(events.size() == 6);
        REQUIRE(events[5].has_value());
        const auto replacement = events[5]->view().as_bundle().at("replacement");
        REQUIRE(replacement.has_value());
        CHECK(replacement.as_indexed_view().size() == 0);
    }
}

TEST_CASE("lag forwards a duration window departure one tick later with its original sample time")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<LaggedWindow>(values<Int>(7)),
                 values<Value>(none, window_delta<Int>(7), none, none,
                               window_delta<Int>(std::nullopt, {{7, MIN_ST}})));
}

TEST_CASE("take keeps the last captured duration window after its update limit")
{
    stdlib::register_standard_operators();
    // The source loses its samples at t3 and t4. An independent t5 inspection
    // sees the two values held by take, after take stopped forwarding updates.
    CHECK_OUTPUT(eval_node<TakenWindow>(values<Int>(3, 7), values<Bool>(none, none, none, none, none, true)),
                 values<Int>(3, 10, none, none, none, 10));
}

TEST_CASE("a closed filter holds its captured duration window until it resynchronizes")
{
    stdlib::register_standard_operators();
    // The t4 inspection sees the last captured value while the filter is closed.
    // Reopening at t5 captures the source's now-empty window.
    CHECK_OUTPUT(eval_node<FilteredWindow>(values<Bool>(true, false, none, none, none, true),
                                          values<Int>(3, 7), values<Bool>(none, none, none, none, true)),
                 values<Int>(3, none, none, none, 3, 0));
}
