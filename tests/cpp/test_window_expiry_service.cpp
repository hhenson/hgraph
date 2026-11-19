#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/time_series/ts_data/checkpoint.h>
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

    struct FirstSampleTime
    {
        static void eval(In<"pair", WindowPair> pair, Out<TS<DateTime>> out)
        {
            out.set(pair.field<"windows">()[0].time_at(0));
        }
    };

    struct SampleNestedWindows
    {
        static Port<TS<DateTime>> compose(Wiring &w, Port<SIGNAL> trigger, Port<TS<Int>> values)
        {
            auto windows = wire<WritePair>(w, values);
            return wire<FirstSampleTime>(w, wire<stdlib::sample>(w, trigger, windows).as<WindowPair>());
        }
    };

    using ScalarPair = TSL<TS<Int>, 2>;

    struct PartialScalarPair
    {
        static void eval(In<"phase", TS<Int>> phase, Out<ScalarPair> out)
        {
            if (phase.value() == 0)
            {
                out[0].set(Int{4});
                out[1].set(Int{9});
            }
            else
            {
                auto first = out[0];
                static_cast<void>(first.begin_mutation(out.evaluation_time()).invalidate());
            }
        }
    };

    struct ReadFirstScalar
    {
        static void eval(In<"pair", ScalarPair> pair, Out<TS<Int>> out)
        {
            auto first = pair[0];
            out.set(first.valid() ? first.value() : Int{-1});
        }
    };

    struct SamplePartialScalars
    {
        static Port<TS<Int>> compose(Wiring &w, Port<SIGNAL> trigger, Port<TS<Int>> phases)
        {
            auto pair = wire<PartialScalarPair>(w, phases);
            return wire<ReadFirstScalar>(w, wire<stdlib::sample>(w, trigger, pair).as<ScalarPair>());
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

TEST_CASE("sampling a nested window retains its timestamp and sampling plain slots keeps patch semantics")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<SampleNestedWindows>(values<Bool>(none, true), values<Int>(4)),
                 values<DateTime>(none, MIN_ST));
    // When the source loses slot 0, the next sample of the partial plain
    // list retains its previously published 4 at that position.
    CHECK_OUTPUT(eval_node<SamplePartialScalars>(values<Bool>(true, none, true), values<Int>(0, 1)),
                 values<Int>(4, none, 4));
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

namespace
{
    using GrowingWindows = TSL<Window>;
    using WindowGroups = TSD<Str, GrowingWindows>;

    struct WriteWindowGroup
    {
        static void eval(In<"command", TS<Str>> command, Out<WindowGroups> groups)
        {
            const auto &op = command.value();
            if (op == "seed siblings")
            {
                const auto now = groups.evaluation_time();
                groups["desk"][0].begin_mutation(now).push(Value{Int{2}}.view());
                groups["desk"][1].begin_mutation(now).push(Value{Int{3}}.view());
                groups["other"][0].begin_mutation(now).push(Value{Int{5}}.view());
                return;
            }
            if (op == "drop siblings")
            {
                static_cast<void>(groups.erase("other"));
                groups["desk"].resize(1);
                return;
            }
            if (op == "drop key" || op == "revive key")
            {
                static_cast<void>(groups.erase("desk"));
                if (op == "revive key") { static_cast<void>(groups["desk"]); }
                return;
            }
            auto lanes = groups["desk"];
            if (op == "drop lane" || op == "revive lane")
            {
                lanes.resize(0);
                if (op == "revive lane") { lanes.resize(1); }
                return;
            }
            lanes.resize(1);
            auto window = lanes[0].begin_mutation(groups.evaluation_time());
            if (op == "reset") { window.clear(); }
            const Value sample{op == "reset" ? Int{9} : static_cast<Int>(std::stoll(op))};
            window.push(sample.view());
        }
    };

    struct ReadWindowGroup
    {
        static void eval(In<"groups", WindowGroups> groups, Out<TS<Int>> out)
        {
            out.set(groups.contains("desk") && groups["desk"].size()
                        ? static_cast<Int>(groups["desk"][0].size()) : Int{-1});
        }
    };

    struct WindowGroupGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Str>> commands)
        {
            return wire<ReadWindowGroup>(w, wire<WriteWindowGroup>(w, commands));
        }
    };

    struct InspectWindowSiblings
    {
        static Str retained_sample(const TSWInputView &window)
        {
            return std::to_string(window.at(0).checked_as<Int>()) + "@" +
                   std::to_string((window.time_at(0) - MIN_ST) / MIN_TD);
        }

        static void eval(In<"groups", WindowGroups> groups, Out<TS<Str>> out)
        {
            Int sum{};
            for (const auto lanes : groups.values())
            {
                for (std::size_t i = 0; i < lanes.size(); ++i) { sum += total(lanes[i]); }
            }
            Str event = std::to_string(sum);
            for (const auto &[key, removed] : groups.removed_items())
            {
                event += " " + key.checked_as<Str>() + "[0]=" + retained_sample(removed[0]);
            }
            auto desk = groups["desk"];
            for (const auto &[index, removed] : desk.removed_items())
            {
                event += " desk[" + std::to_string(index) + "]=" + retained_sample(removed.as_window());
            }
            out.set(event);
        }
    };

    struct WindowSiblingsGraph
    {
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> commands)
        {
            return wire<InspectWindowSiblings>(w, wire<WriteWindowGroup>(w, commands));
        }
    };

    struct ReadGroupDepartures
    {
        static void eval(In<"groups", WindowGroups> groups, Out<TS<Str>> out)
        {
            auto window = groups["desk"][0];
            Str event;
            for (auto time : window.removed_value_times())
            {
                if (!event.empty()) { event += ","; }
                event += std::to_string((time - MIN_ST) / MIN_TD);
            }
            out.set(event);
        }
    };

    struct WindowGroupDepartures
    {
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> commands)
        {
            return wire<ReadGroupDepartures>(w, wire<WriteWindowGroup>(w, commands));
        }
    };

    struct InspectRecordedWindowGroup
    {
        static void eval(In<"groups", WindowGroups> groups, In<"probe", SIGNAL, InputValidity::Unchecked>,
                         Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(groups["desk"][0].size()));
        }
    };
}

TEST_CASE("dynamic window expiry follows the lifetime of a key and a growing list position")
{
    for (const Str drop : {"drop key", "drop lane"})
    {
        CAPTURE(drop);
        CHECK_OUTPUT(eval_node<WindowGroupGraph>(values<Str>("4", drop, "7")),
                     values<Int>(1, -1, 1, none, none, 0));
    }
    for (const Str revive : {"revive key", "revive lane"})
    {
        CAPTURE(revive);
        CHECK_OUTPUT(eval_node<WindowGroupGraph>(values<Str>("4", revive)), values<Int>(1, 1, none, 0));
    }
}

TEST_CASE("dynamic window expiry keeps a sibling armed when other keys and positions are removed")
{
    CHECK_OUTPUT(eval_node<WindowSiblingsGraph>(values<Str>("seed siblings", "drop siblings", "7")),
                 values<Str>("10", "2 other[0]=5@0 desk[1]=3@0", "9", "7", none, "0"));
}

TEST_CASE("dynamic window expiry merges the due departures with a reset and arrival on the same key")
{
    // t3 expires the t0 sample, clears the one from t1 and receives 9. The
    // second sample's former t4 deadline is cancelled. Only 9 leaves at t6.
    const auto commands = values<Str>("4", "5", none, "reset");
    CHECK_OUTPUT(eval_node<WindowGroupGraph>(commands), values<Int>(1, 2, none, 1, none, none, 0));
    CHECK_OUTPUT(eval_node<WindowGroupDepartures>(commands), values<Str>("", "", none, "0,1", none, none, "3"));
}

TEST_CASE("dynamic window expiry leaves authoritative nested replay on its recorded clock")
{
    const auto edit = dict_delta<Str, GrowingWindows>({{"desk", list_delta<Window>({{0, window_delta<Int>(7)}})}});
    CHECK_OUTPUT(eval_node<InspectRecordedWindowGroup>(values<Value>(edit), values<Bool>(none, none, none, none, true)),
                 values<Int>(1, none, none, none, 1));
}

namespace
{
    struct RestoreWindowGroup
    {
        static void eval(Out<WindowGroups>) {}

        static void start(Out<WindowGroups> target, GlobalStateView state)
        {
            if (!state.contains("restore group")) { return; }
            TSOutput original{schema_descriptor<WindowGroups>::ts_meta()};
            Out<WindowGroups> groups{original.view(MIN_ST), MIN_ST};
            groups["desk"][0].begin_mutation(MIN_ST).push(Value{Int{4}}.view());
            restore_ts_checkpoint(target.base().data_view(), capture_ts_checkpoint(original.data_view()));
        }
    };

    struct FailNestedStop final : LifecycleObserver
    {
        bool called{false};
        void on_before_stop_graph(const GraphView &g) override
        {
            if (!g.is_root())
            {
                called = true;
                throw std::runtime_error("test observer interrupted child stop");
            }
        }
    };
}

TEST_CASE("dynamic window expiry restores a nested history and disarms it even if child stop throws")
{
    NodeBuilder owner;
    owner.implementation<RestoreWindowGroup>();
    GraphBuilder graph;
    graph.add_node(owner);
    FailNestedStop observer;
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(graph).mode(GraphExecutorMode::ExternallyDriven).add_lifecycle_observer(&observer);
    auto executor = executor_builder.make_executor();
    auto engine = executor.view();
    auto root = engine.graph();
    const auto start = MIN_ST + 2 * MIN_TD;
    const auto due = MIN_ST + 3 * MIN_TD;
    engine.start_external(start);
    root.global_state().set("restore group", Value{true});
    {
        auto child = graph.make_nested_graph(root.node_at(0).pointer());
        child.view().start(start);
        REQUIRE(root.next_scheduled_time() == due);

        SECTION("silent recovery rearms the restored sample with its original time")
        {
            REQUIRE(engine.step(due));
            Out<WindowGroups> groups{child.view().node_at(0).output(due), due};
            const auto window = groups["desk"][0];
            REQUIRE(window.empty());
            auto removed = window.removed_value_times();
            REQUIRE(removed.limit == 1);
            CHECK(*removed.begin() == MIN_ST);
        }
        SECTION("a failing stop detaches the structural subscription before its nodes are destroyed")
        {
            // Leave an armed child for its destructor to stop.
        }
    }
    CHECK(observer.called);
    CHECK(root.next_scheduled_time() == MAX_DT);
    engine.stop_external();
}

namespace
{
    using PriceRow = TSB<"ExpiryPriceRow", Field<"prices", TSD<Str, Window>>>;
    using PriceRows = TSL<PriceRow>;

    struct WritePriceRows
    {
        static void eval(In<"seed", TS<Bool>> seed, Out<PriceRows> rows)
        {
            if (seed.value())
                rows[0].field<"prices">()["old"].begin_mutation(rows.evaluation_time()).push(Value{Int{7}}.view());
            else
                rows.resize(0);
        }
    };

    struct ReadRemovedPrice
    {
        static void eval(In<"rows", PriceRows> rows, Out<TS<Str>> out)
        {
            for (auto [index, child] : rows.removed_items())
            {
                In<"", PriceRow> row{std::move(child)};
                auto price = row.field<"prices">()["old"];
                out.set(std::to_string(index) + ":" + InspectWindowSiblings::retained_sample(price));
            }
        }
    };

    struct RemovedPrices
    {
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Bool>> seed)
        {
            return wire<ReadRemovedPrice>(w, wire<WritePriceRows>(w, seed));
        }
    };
}

TEST_CASE("dynamic window expiry leaves a removed list element's bundled dictionary readable")
{
    CHECK_OUTPUT(eval_node<RemovedPrices>(values<Bool>(true, false)), values<Str>(none, "0:7@0"));
}
