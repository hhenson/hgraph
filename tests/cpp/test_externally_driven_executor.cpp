// The ``ExternallyDriven`` graph executor (RFC 0037).
//
// Neither the schedule nor the wall clock decides when a cycle runs: the
// caller does. The executor is stepped rather than run, so the run loop is
// turned inside out -- ``start_external`` / ``step`` / ``stop_external``
// replace ``run()``, and the evaluation time is an argument.
//
// This is the substrate for a distributed nested graph. A worker is handed an
// evaluation time, evaluates one cycle, and reports what its children want
// next through ``GraphView::next_scheduled_time()``. The property that makes
// the whole design worth having is that supplying the time from outside
// changes NOTHING about the result -- so every test here is the same
// assertion: stepped output equals the simulation output.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <array>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

namespace
{
    using namespace hgraph;

    /** Carries a running total, so a dropped or repeated cycle is visible. */
    struct RunningTotal
    {
        static constexpr auto name = "running_total";

        static void eval(In<"ts", TS<Int>> ts, State<Int> total, Out<TS<Int>> out)
        {
            total.modify() += ts.value();
            out.set(total.get());
        }
    };

    /** Fails from inside ``graph.evaluate``, which is what cleanup_on_error
        governs -- as distinct from step() refusing a bad argument. */
    struct Boom
    {
        static constexpr auto name = "boom";

        static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out)
        {
            static_cast<void>(ts);
            static_cast<void>(out);
            throw std::runtime_error("boom");
        }
    };

    struct BoomGraph
    {
        static constexpr auto name = "externally_driven_boom_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto out = wire<Boom>(w, src);
            wire<stdlib::dense_record_impl>(w, out, Str{"out"});
        }
    };

    struct TotalGraph
    {
        static constexpr auto name = "externally_driven_total_graph";
        static void           compose(Wiring &w)
        {
            // The dense replay re-arms itself with ``sched.schedule(MIN_TD)``
            // after every entry, so this graph drives its own schedule -- which
            // is the half of the contract a caller-supplied time has to honour.
            auto src   = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto total = wire<RunningTotal>(w, src);
            wire<stdlib::dense_record_impl>(w, total, Str{"out"});
        }
    };

    struct CountWindowSamples
    {
        static void eval(In<"samples", TSWDuration<Int, 2, 0>, InputValidity::Unchecked> samples,
                         GlobalStateView state)
        {
            state.set("retained", Value{static_cast<Int>(samples.size())});
        }
    };

    struct ExpiringWindowGraph
    {
        static void compose(Wiring &w)
        {
            auto sample = wire<stdlib::const_, TS<Int>>(w, Int{7});
            auto window = wire<stdlib::to_window>(w, sample, 2 * MIN_TD, TimeDelta{});
            wire<CountWindowSamples>(w, window);
        }
    };

    using Recorded = std::vector<std::optional<Int>>;

    GraphBuilder seeded_graph(const std::vector<std::optional<Int>> &values)
    {
        GraphBuilder gb = build_graph<TotalGraph>();
        testing::set_replay_values<Int>(gb.global_state(), "in", values);
        return gb;
    }

    const DateTime test_end = MIN_ST + TimeDelta{100};

    Recorded run_simulated(const std::vector<std::optional<Int>> &values)
    {
        GraphExecutorBuilder eb;
        eb.graph_builder(seeded_graph(values)).start_time(MIN_ST).end_time(test_end);
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out");
    }

    /**
     * Drive the graph the way a distributed caller would: step at a time we
     * supply, then ask the graph what it wants next and honour it.
     *
     * This loop IS the RFC's per-cycle contract -- evaluation time in,
     * ``next_scheduled_time`` out -- with the transport removed.
     */
    Recorded run_stepped(const std::vector<std::optional<Int>> &values, std::size_t &cycles)
    {
        GraphExecutorBuilder eb;
        eb.graph_builder(seeded_graph(values))
            .mode(GraphExecutorMode::ExternallyDriven)
            .start_time(MIN_ST)
            .end_time(test_end);
        GraphExecutorValue ex   = eb.make_executor();
        auto               view = ex.view();

        view.start_external(MIN_ST);
        cycles = 0;
        for (DateTime when = MIN_ST; when < test_end;)
        {
            REQUIRE(view.step(when));
            ++cycles;
            const DateTime next = view.graph().next_scheduled_time();
            if (next == MAX_DT || next >= test_end) { break; }
            when = next;
        }
        auto recorded = testing::get_recorded_values<Int>(view.graph().global_state(), "out");
        view.stop_external();
        return recorded;
    }
}  // namespace

TEST_CASE("externally driven: stepping at the graph's own times equals a simulation run")
{
    const std::vector<std::optional<Int>> values{Int{1}, Int{2}, Int{3}, Int{4}};

    std::size_t      cycles = 0;
    const auto       stepped   = run_stepped(values, cycles);
    const auto expected = run_simulated(values);

    CHECK(stepped == expected);
    // Not vacuous: the graph really did run a cycle per replayed value.
    CHECK(cycles == values.size());
}

TEST_CASE("externally driven: the caller chooses the time, and the graph reports what it wants")
{
    GraphExecutorBuilder eb;
    eb.graph_builder(seeded_graph({Int{7}, Int{8}}))
        .mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST)
        .end_time(test_end);
    GraphExecutorValue ex   = eb.make_executor();
    auto               view = ex.view();

    view.start_external(MIN_ST);

    // Before any cycle the replay is armed for the start time.
    CHECK(view.graph().next_scheduled_time() == MIN_ST);

    REQUIRE(view.step(MIN_ST));
    // Having consumed one entry it re-arms one engine tick later; that value
    // is the caller's whole view of the child's scheduling intent.
    CHECK(view.graph().next_scheduled_time() == MIN_ST + MIN_TD);

    REQUIRE(view.step(MIN_ST + MIN_TD));
    // Both entries consumed: nothing further is wanted.
    CHECK(view.graph().next_scheduled_time() == MAX_DT);

    view.stop_external();
}

TEST_CASE("externally driven: copying live configuration leaves window deadlines in their own roots")
{
    stdlib::register_standard_operators();
    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<ExpiringWindowGraph>())
        .mode(GraphExecutorMode::ExternallyDriven).end_time(test_end);
    auto first_executor = builder.make_executor();
    auto second_executor = builder.make_executor();
    auto first = first_executor.view();
    auto second = second_executor.view();
    const auto later = MIN_ST + 10 * MIN_TD;
    first.start_external(MIN_ST);
    second.start_external(later);
    REQUIRE(first.step(MIN_ST));
    REQUIRE(second.step(later));

    auto first_state = first.graph().global_state();
    auto second_state = second.graph().global_state();
    REQUIRE(first_state.get_as<Int>("retained") == 1);
    REQUIRE(second_state.get_as<Int>("retained") == 1);
    // The source remains active on both roots while their public values are copied.
    first_state.set("window_expiry", Value{Int{42}});
    second_state.copy_from(first_state);
    REQUIRE(second_state.size() == 2);
    CHECK(second_state.get_as<Int>("window_expiry") == 42);

    const auto first_expiry = MIN_ST + 3 * MIN_TD;
    const auto second_expiry = later + 3 * MIN_TD;
    REQUIRE(first.graph().next_scheduled_time() == first_expiry);
    REQUIRE(second.graph().next_scheduled_time() == second_expiry);
    CHECK_THROWS_AS(first.step(first_expiry + MIN_TD), std::invalid_argument);
    REQUIRE(first.step(first_expiry));
    CHECK(first_state.get_as<Int>("retained") == 0);
    CHECK(second_state.get_as<Int>("retained") == 1);
    CHECK(first.graph().next_scheduled_time() == MAX_DT);
    first.stop_external();

    REQUIRE(second.step(second_expiry));
    CHECK(second_state.get_as<Int>("retained") == 0);
    CHECK(second.graph().next_scheduled_time() == MAX_DT);
    second.stop_external();
}

TEST_CASE("externally driven: an arrival from an after-evaluation callback gets its own window expiry")
{
    stdlib::register_standard_operators();
    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<ExpiringWindowGraph>())
        .mode(GraphExecutorMode::ExternallyDriven).end_time(test_end);
    auto executor = builder.make_executor();
    auto engine = executor.view();
    engine.start_external(MIN_ST);
    REQUIRE(engine.step(MIN_ST));

    auto graph = engine.graph();
    auto owner = graph.node_at(1).output(MIN_ST);
    const auto &data = owner.data_view();
    REQUIRE(data.schema()->is_duration_based());
    const auto first_expiry = MIN_ST + 3 * MIN_TD;
    const auto next_expiry = first_expiry + 3 * MIN_TD;
    REQUIRE(graph.next_scheduled_time() == first_expiry);
    engine.engine_control().add_after_evaluation_notification([&] {
        // Callbacks registered during this boundary run in the same cycle.
        engine.engine_control().add_after_evaluation_notification([&] {
            REQUIRE(data.as_window().empty());
            Value sample{Int{15}};
            data.as_window().begin_mutation(first_expiry).push(sample.view());
        });
    });

    REQUIRE(engine.step(first_expiry));
    REQUIRE(data.as_window().size() == 1);
    REQUIRE(graph.next_scheduled_time() == next_expiry);
    REQUIRE(engine.step(next_expiry));
    REQUIRE(data.as_window().empty());
    const auto removed_times = data.as_window().removed_value_times(next_expiry);
    REQUIRE(removed_times.limit == 1);
    CHECK(*removed_times.begin() == first_expiry);
    CHECK(graph.next_scheduled_time() == MAX_DT);
    engine.stop_external();
}

TEST_CASE("externally driven: stepping over due work is refused, not silently dropped")
{
    // The defect this pins: ``evaluate_impl`` runs a node only when its slot
    // is EXACTLY the evaluation time. A slot already in the past is neither
    // evaluated nor folded into next_scheduled_time, so overrunning it would
    // destroy the scheduled tick with no error and no trace -- and a caller
    // polling next_scheduled_time() would be told the child wants nothing.
    GraphExecutorBuilder eb;
    eb.graph_builder(seeded_graph({Int{1}, Int{2}}))
        .mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST)
        .end_time(test_end);
    GraphExecutorValue ex   = eb.make_executor();
    auto               view = ex.view();

    view.start_external(MIN_ST);
    REQUIRE(view.graph().next_scheduled_time() == MIN_ST);   // work IS due
    CHECK_THROWS_WITH(view.step(MIN_ST + TimeDelta{10}),
                      Catch::Matchers::ContainsSubstring("skip work already due"));

    // Refused, not half-applied: the due work is still there afterwards.
    CHECK(view.graph().next_scheduled_time() == MIN_ST);
    REQUIRE(view.step(MIN_ST));
    CHECK(view.graph().next_scheduled_time() == MIN_ST + MIN_TD);
    view.stop_external();
}

TEST_CASE("externally driven: a genuinely idle graph accepts any forward time")
{
    // The other side of the rule. Once nothing is scheduled the caller may
    // step wherever its own graph is -- which is the normal case for a
    // distributed child whose parent had a cycle it was not involved in.
    GraphExecutorBuilder eb;
    eb.graph_builder(seeded_graph({Int{5}}))
        .mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST)
        .end_time(test_end);
    GraphExecutorValue ex   = eb.make_executor();
    auto               view = ex.view();

    view.start_external(MIN_ST);
    REQUIRE(view.step(MIN_ST));
    REQUIRE(view.step(MIN_ST + MIN_TD));   // drains the re-arm
    REQUIRE(view.graph().next_scheduled_time() == MAX_DT);

    const auto before = testing::get_recorded_values<Int>(view.graph().global_state(), "out");
    REQUIRE_NOTHROW(view.step(MIN_ST + TimeDelta{50}));
    const auto after = testing::get_recorded_values<Int>(view.graph().global_state(), "out");
    CHECK(after == before);   // idle really is idle: no invented tick
    view.stop_external();
}

TEST_CASE("externally driven: starting twice is refused")
{
    // GraphView::start early-returns on a started graph, so a second
    // start_external would reset the executor's evaluation time while the
    // graph kept its own -- letting the next step move time backwards past
    // the guard.
    GraphExecutorBuilder eb;
    eb.graph_builder(seeded_graph({Int{1}, Int{2}}))
        .mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST)
        .end_time(test_end);
    GraphExecutorValue ex   = eb.make_executor();
    auto               view = ex.view();

    view.start_external(MIN_ST);
    CHECK_THROWS_WITH(view.start_external(MIN_ST),
                      Catch::Matchers::ContainsSubstring("twice"));
    view.stop_external();
}

TEST_CASE("externally driven: start_time reports where the graph actually started")
{
    // The builder's start_time and the stepped start time may differ; the
    // injectable behind start_time() must report the real one.
    GraphExecutorBuilder eb;
    eb.graph_builder(seeded_graph({Int{1}}))
        .mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST)
        .end_time(test_end);
    GraphExecutorValue ex   = eb.make_executor();
    auto               view = ex.view();

    const DateTime actual = MIN_ST + TimeDelta{7};
    view.start_external(actual);
    CHECK(view.start_time() == actual);
    view.stop_external();
}

TEST_CASE("externally driven: stop is idempotent, so a catch block may call it blind")
{
    GraphExecutorBuilder eb;
    eb.graph_builder(seeded_graph({Int{1}}))
        .mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST)
        .end_time(test_end);
    GraphExecutorValue ex   = eb.make_executor();
    auto               view = ex.view();

    view.start_external(MIN_ST);
    REQUIRE(view.step(MIN_ST));
    view.stop_external();
    // The caller owns the lifecycle here, so a second stop -- the shape a
    // catch block produces -- must not run the stop phase again.
    CHECK_NOTHROW(view.stop_external());
}

TEST_CASE("externally driven: stopping without ever stepping is harmless")
{
    GraphExecutorBuilder eb;
    eb.graph_builder(seeded_graph({Int{1}}))
        .mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST)
        .end_time(test_end);
    GraphExecutorValue ex   = eb.make_executor();
    auto               view = ex.view();
    CHECK_NOTHROW(view.stop_external());   // never started
    view.start_external(MIN_ST);
    CHECK_NOTHROW(view.stop_external());   // started, no cycle
}

TEST_CASE("externally driven: the driving calls are refused on the looping modes")
{
    GraphExecutorBuilder eb;
    eb.graph_builder(seeded_graph({Int{1}})).start_time(MIN_ST).end_time(test_end);
    GraphExecutorValue ex = eb.make_executor();  // Simulation
    CHECK_THROWS_WITH(ex.view().step(MIN_ST),
                      Catch::Matchers::ContainsSubstring("ExternallyDriven"));
    CHECK_THROWS_WITH(ex.view().start_external(MIN_ST),
                      Catch::Matchers::ContainsSubstring("ExternallyDriven"));
}

TEST_CASE("externally driven: a failing evaluation honours cleanup_on_error")
{
    // Accepting the option on the builder and then ignoring it would make it
    // silently mean its opposite, so both settings are pinned. Note this is an
    // error INSIDE evaluation; step() refusing a bad argument deliberately
    // leaves the graph alone, since nothing was evaluated.
    for (const bool cleanup : {true, false})
    {
        GraphBuilder gb = build_graph<BoomGraph>();
        testing::set_replay_values<Int>(gb.global_state(), "in", {Int{1}});
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb))
            .mode(GraphExecutorMode::ExternallyDriven)
            .start_time(MIN_ST)
            .end_time(test_end)
            .cleanup_on_error(cleanup);
        GraphExecutorValue ex   = eb.make_executor();
        auto               view = ex.view();
        view.start_external(MIN_ST);
        REQUIRE(view.graph().started());
        CHECK_THROWS(view.step(MIN_ST));
        CHECK(view.graph().started() == !cleanup);
    }
}

TEST_CASE("externally driven: run() is refused, because stepping is the model")
{
    GraphExecutorBuilder eb;
    eb.graph_builder(seeded_graph({Int{1}}))
        .mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST)
        .end_time(test_end);
    GraphExecutorValue ex = eb.make_executor();
    CHECK_THROWS_WITH(ex.view().run(),
                      Catch::Matchers::ContainsSubstring("stepped, not run"));
}

TEST_CASE("externally driven: configured component recovery is refused before start")
{
    auto graph = seeded_graph({Int{1}});
    bool committed = false;
    configure_component_recovery(graph.global_state(), {
        .component_id = "component", .commit = [&](const auto &) { committed = true; }});
    GraphExecutorBuilder builder;
    builder.graph_builder(std::move(graph)).mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST).end_time(test_end);
    auto executor = builder.make_executor();
    CHECK_THROWS_WITH(executor.view().start_external(MIN_ST),
                      Catch::Matchers::ContainsSubstring("requires simulation"));
    CHECK_FALSE(executor.view().graph().started());
    CHECK_FALSE(committed);
}

TEST_CASE("externally driven: embedding wrappers surround every complete phase")
{
    std::vector<GraphExecutorPhase> phases;
    GraphExecutorBuilder builder;
    builder.graph_builder(seeded_graph({Int{1}})).mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST).end_time(test_end)
        .phase_runner([&](GraphExecutorPhase phase, GraphExecutorPhaseAction action) {
            phases.push_back(phase);
            action();
        });
    auto executor = builder.make_executor();
    executor.view().start_external(MIN_ST);
    REQUIRE(executor.view().step(MIN_ST));
    executor.view().stop_external();
    CHECK(phases == std::vector{GraphExecutorPhase::Start, GraphExecutorPhase::Evaluation,
                               GraphExecutorPhase::Stop});
}

TEST_CASE("externally driven: time may not run backwards, and a step needs a start")
{
    GraphExecutorBuilder eb;
    eb.graph_builder(seeded_graph({Int{1}, Int{2}}))
        .mode(GraphExecutorMode::ExternallyDriven)
        .start_time(MIN_ST)
        .end_time(test_end);
    GraphExecutorValue ex   = eb.make_executor();
    auto               view = ex.view();

    CHECK_THROWS_WITH(view.step(MIN_ST),
                      Catch::Matchers::ContainsSubstring("start_external"));

    view.start_external(MIN_ST);
    // Drain the due work first -- stepping over it is refused by its own rule,
    // and this test is about the backwards guard, not that one.
    REQUIRE(view.step(MIN_ST));
    REQUIRE(view.step(MIN_ST + MIN_TD));
    REQUIRE(view.graph().next_scheduled_time() == MAX_DT);

    REQUIRE(view.step(MIN_ST + TimeDelta{10}));
    // A caller that replays a cycle out of order would silently corrupt every
    // stateful child, so it is refused rather than tolerated.
    CHECK_THROWS_WITH(view.step(MIN_ST + TimeDelta{5}),
                      Catch::Matchers::ContainsSubstring("backwards"));
    view.stop_external();
}

namespace
{
    using ClockWindow = TSWDuration<Int, 2, 0>;
    using CountWindow = TSW<Int, 1, 1>;

    struct TwoWindowSamples
    {
        static void start(NodeScheduler clock) { clock.schedule(clock.now()); }

        static void eval(Out<ClockWindow> out, NodeScheduler clock)
        {
            const auto now = clock.now();
            const bool first = now == MIN_ST;
            out.begin_mutation(now).push(Value{first ? Int{4} : Int{5}}.view());
            if (first) { clock.schedule(MIN_TD); }
        }
    };

    template<typename Owner>
    GraphExecutorValue window_executor(GraphExecutorMode mode = GraphExecutorMode::ExternallyDriven,
                                       bool cleanup_on_error = true)
    {
        NodeBuilder node;
        node.implementation<Owner>();
        GraphBuilder graph;
        graph.add_node(node);
        GraphExecutorBuilder builder;
        builder.graph_builder(graph).mode(mode).start_time(MIN_ST).end_time(MIN_ST + 8 * MIN_TD)
            .cleanup_on_error(cleanup_on_error);
        return builder.make_executor();
    }

    void check_departure(const TSWDataView &window, DateTime tick, Int value, DateTime sample_time)
    {
        const auto removed = window.removed_values(tick);
        const auto times = window.removed_value_times(tick);
        REQUIRE(removed.limit == 1);
        REQUIRE(times.limit == 1);
        CHECK((*removed.begin()).checked_as<Int>() == value);
        CHECK(*times.begin() == sample_time);
    }

    struct ClearReplayedWindow
    {
        static constexpr bool replays_output_events = true;
        static void start(NodeScheduler clock) { clock.schedule(clock.now()); }
        static void eval(Out<ClockWindow> out, NodeScheduler clock)
        {
            if (clock.now() == MIN_ST)
            {
                out.apply(window_delta<Int>(7).view());
                clock.schedule(5 * MIN_TD);
            }
            else
            {
                out.apply(window_delta<Int>(9, {{7, MIN_ST}}, true).view());
            }
        }
    };

    using CountBook = TSD<Str, CountWindow>;

    struct ReusedCountWindow
    {
        static void start(NodeScheduler clock) { clock.schedule(clock.now()); }
        static void eval(Out<CountBook> book, NodeScheduler clock)
        {
            const auto now = clock.now();
            const auto offset = (now - MIN_ST) / MIN_TD;
            book["desk"].begin_mutation(now).push(Value{Int{10} + offset}.view());
            // The replaced value is still published by the removed child at t1.
            // At t2 the same key has new storage and its own removal at t3.
            if (offset == 1) { static_cast<void>(book.erase("desk")); }
            if (offset < 3) { clock.schedule(MIN_TD); }
        }
    };
}

TEST_CASE("completed window step: repeated and refused times preserve the borrowed departure until a later step")
{
    const auto first_due = MIN_ST + 3 * MIN_TD;
    const auto second_due = first_due + MIN_TD;
    Value owned;
    {
        auto executor = window_executor<TwoWindowSamples>();
        auto engine = executor.view();
        engine.start_external(MIN_ST);
        REQUIRE(engine.step(MIN_ST));
        REQUIRE(engine.step(MIN_ST + MIN_TD));
        REQUIRE(engine.step(first_due));
        auto out = engine.graph().node_at(0).output(first_due);
        auto window = out.as_window().data_view();
        check_departure(window, first_due, 4, MIN_ST);
        owned = capture_window_delta(window, first_due);

        CHECK_THROWS_AS(engine.step(first_due - MIN_TD), std::invalid_argument);
        CHECK_THROWS_AS(engine.step(second_due + MIN_TD), std::invalid_argument);
        REQUIRE(engine.step(first_due));
        check_departure(window, first_due, 4, MIN_ST);

        REQUIRE(engine.step(second_due));
        check_departure(window, second_due, 5, MIN_ST + MIN_TD);
        REQUIRE(engine.graph().next_scheduled_time() == MAX_DT);
        // The clock advances, but no window or node mutates at this time.
        engine.engine_control().add_before_evaluation_notification([&] {
            CHECK(window.removed_values(second_due).limit == 0);
        });
        REQUIRE(engine.step(second_due + MIN_TD));
        CHECK(window.removed_values(second_due).limit == 0);
        CHECK(window.removed_value_times(second_due).limit == 0);
        CHECK_FALSE(capture_window_delta(window, second_due).has_value());
        CHECK(window.last_modified_time() == second_due);
        engine.stop_external();
    }
    CHECK(owned.equals(window_delta<Int>(std::nullopt, {{4, MIN_ST}})));
}

TEST_CASE("completed window step: explicit release closes only the completed timestamp")
{
    auto executor = window_executor<TwoWindowSamples>();
    auto engine = executor.view();
    CHECK_THROWS_AS(engine.release_completed_step(), std::logic_error);
    engine.start_external(MIN_ST);
    CHECK_THROWS_AS(engine.release_completed_step(), std::logic_error);
    REQUIRE(engine.step(MIN_ST));
    REQUIRE(engine.step(MIN_ST + MIN_TD));
    const auto first_due = MIN_ST + 3 * MIN_TD;
    engine.engine_control().add_after_evaluation_notification([&] {
        auto out = engine.graph().node_at(0).output(first_due);
        check_departure(out.as_window().data_view(), first_due, 4, MIN_ST);
        CHECK_THROWS_AS(engine.release_completed_step(), std::logic_error);
    });
    REQUIRE(engine.step(first_due));
    auto out = engine.graph().node_at(0).output(first_due);
    auto window = out.as_window().data_view();
    check_departure(window, first_due, 4, MIN_ST);

    engine.graph().as_root().release_completed_step();
    engine.release_completed_step();
    CHECK(window.removed_values(first_due).limit == 0);
    REQUIRE(window.size() == 1);
    CHECK(window.time_at(0) == MIN_ST + MIN_TD);
    CHECK_THROWS(engine.step(first_due));
    const auto second_due = first_due + MIN_TD;
    REQUIRE(engine.step(second_due));
    check_departure(window, second_due, 5, MIN_ST + MIN_TD);
    engine.stop_external();
    check_departure(window, second_due, 5, MIN_ST + MIN_TD);
    SECTION("the driver closes the stopped graph's last step")
    {
        engine.release_completed_step();
        CHECK(window.removed_values(second_due).limit == 0);
        CHECK_NOTHROW(engine.release_completed_step());
    }
    SECTION("a fresh start has no completed step until its first evaluation")
    {
        const auto next_start = second_due + 2 * MIN_TD;
        engine.start_external(next_start);
        CHECK_THROWS_AS(engine.release_completed_step(), std::logic_error);
        CHECK(window.removed_values(second_due).limit == 0);
        REQUIRE(engine.step(next_start));
        CHECK_NOTHROW(engine.release_completed_step());
        engine.stop_external();
    }
}

TEST_CASE("completed window step: normal loop completion leaves its final authoritative clear inspectable")
{
    for (auto mode : {GraphExecutorMode::Simulation, GraphExecutorMode::RealTime})
    {
        CAPTURE(static_cast<int>(mode));
        auto executor = window_executor<ClearReplayedWindow>(mode);
        auto engine = executor.view();
        engine.run();
        const auto cleared = MIN_ST + 5 * MIN_TD;
        auto out = engine.graph().node_at(0).output(cleared);
        auto window = out.as_window().data_view();
        check_departure(window, cleared, 7, MIN_ST);
        const auto owned = capture_window_delta(window, cleared);
        engine.release_completed_step();
        CHECK(window.removed_values(cleared).limit == 0);
        CHECK_FALSE(out.as_window().window_delta().has_value());
        CHECK_FALSE(capture_window_delta(window, cleared).has_value());
        REQUIRE(window.size() == 1);
        CHECK(window.back().checked_as<Int>() == 9);
        CHECK(owned.equals(window_delta<Int>(9, {{7, MIN_ST}}, true)));
    }
}

TEST_CASE("completed window step: a failed repeated evaluation preserves diagnostics and cannot be released")
{
    for (bool cleanup : {false, true})
    {
        CAPTURE(cleanup);
        auto executor = window_executor<TwoWindowSamples>(GraphExecutorMode::ExternallyDriven, cleanup);
        auto engine = executor.view();
        engine.start_external(MIN_ST);
        REQUIRE(engine.step(MIN_ST));
        REQUIRE(engine.step(MIN_ST + MIN_TD));
        const auto due = MIN_ST + 3 * MIN_TD;
        REQUIRE(engine.step(due));
        engine.engine_control().add_after_evaluation_notification([] { throw std::runtime_error("test failure"); });
        CHECK_THROWS_WITH(engine.step(due), Catch::Matchers::ContainsSubstring("test failure"));
        CHECK(engine.graph().started() == !cleanup);
        CHECK_THROWS_AS(engine.release_completed_step(), std::logic_error);
        auto out = engine.graph().node_at(0).output(due);
        check_departure(out.as_window().data_view(), due, 4, MIN_ST);
        engine.stop_external();
    }
}

TEST_CASE("completed window step: deleting and reusing a key keeps fixed-window removals in their own generation")
{
    auto executor = window_executor<ReusedCountWindow>();
    auto engine = executor.view();
    engine.start_external(MIN_ST);
    REQUIRE(engine.step(MIN_ST));
    const auto removed_at = MIN_ST + MIN_TD;
    REQUIRE(engine.step(removed_at));
    auto out = engine.graph().node_at(0).output(removed_at);
    auto book = out.as_dict();
    auto removed = book.removed_items();
    REQUIRE(removed.begin() != removed.end());
    auto [key, child] = *removed.begin();
    CHECK(key.checked_as<Str>() == "desk");
    const auto saved = child.as_window().window_delta();
    CHECK(saved.equals(window_delta<Int>(11, {{10, MIN_ST}})));

    // Advancing retires the old child's borrowed data before slot reuse.
    REQUIRE(engine.step(removed_at + MIN_TD));
    const auto replaced_at = removed_at + 2 * MIN_TD;
    REQUIRE(engine.step(replaced_at));
    auto now = engine.graph().node_at(0).output(replaced_at);
    auto live = now.as_dict().at(Value{Str{"desk"}}.view());
    auto window = live.as_window().data_view();
    check_departure(window, replaced_at, 12, removed_at + MIN_TD);
    engine.release_completed_step();
    REQUIRE(window.size() == 1);
    CHECK(window.back().checked_as<Int>() == 13);
    CHECK(window.removed_values(replaced_at).limit == 0);
    CHECK(saved.equals(window_delta<Int>(11, {{10, MIN_ST}})));
    engine.stop_external();
}

namespace
{
    using WindowBatch = TSB<"CompletedWindowBatch", Field<"prices", ClockWindow>>;

    struct SeedWindowBatch
    {
        static void eval(Out<WindowBatch>) {}
        static void start(Out<WindowBatch> out)
        {
            const auto now = out.evaluation_time();
            ListBuilder values{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())};
            for (const Int value : {4, 5, 6}) { values.push_back(Int{value}); }
            const std::array timestamps{now, now, now};
            out.field<"prices">().begin_mutation(now).replace_samples(values.build().view(), timestamps);
        }
    };
}

TEST_CASE("completed window step: the directly driven root reclaims the departure batch and its cached projection")
{
    auto executor = window_executor<SeedWindowBatch>();
    auto engine = executor.view();
    engine.start_external(MIN_ST);
    auto root = engine.graph().as_root();
    const auto due = MIN_ST + 3 * MIN_TD;
    REQUIRE(root.next_scheduled_time() == due);
    REQUIRE(root.evaluate(due));
    auto endpoint = root.node_at(0).output(due);
    auto bundle = endpoint.as_bundle();
    auto prices = bundle.at(0);
    const auto &data = prices.data_view();
    auto window = data.as_window();
    // A bundle projects its window field's canonical delta lazily. Read it
    // before release so the memory check includes the borrowed projection.
    auto fields = endpoint.data_view().delta_value(due).as_bundle();
    REQUIRE(fields.at("prices").as_bundle().at("removed").as_indexed_view().size() == 3);
    const auto saved = capture_window_delta(window, due);
    const auto before = data.dynamic_storage_metrics();
    REQUIRE(before.live_bytes > 0);

    SECTION("explicit release") { root.release_completed_step(); }
    SECTION("another root evaluation, with no node scheduled") { REQUIRE(root.evaluate(due + MIN_TD)); }
    const auto after = data.dynamic_storage_metrics();
    CHECK(after.live_bytes < before.live_bytes);
    CHECK(after.reserved_bytes < before.reserved_bytes);
    CHECK(window.removed_values(due).limit == 0);
    CHECK_FALSE(data.delta_value(due).has_value());
    CHECK_FALSE(capture_window_delta(window, due).has_value());
    CHECK(saved.equals(window_delta<Int>(std::nullopt, {{4, MIN_ST}, {5, MIN_ST}, {6, MIN_ST}})));
    CHECK(root.next_scheduled_time() == MAX_DT);
    engine.stop_external();
}

TEST_CASE("completed window step: a later replacement without removals keeps its original observation")
{
    auto executor = window_executor<TwoWindowSamples>();
    auto engine = executor.view();
    engine.start_external(MIN_ST);
    REQUIRE(engine.step(MIN_ST));
    REQUIRE(engine.step(MIN_ST + MIN_TD));
    REQUIRE(engine.step(MIN_ST + 3 * MIN_TD));
    const auto last_due = MIN_ST + 4 * MIN_TD;
    REQUIRE(engine.step(last_due));
    auto endpoint = engine.graph().node_at(0).output(last_due);
    auto window = endpoint.as_window().data_view();
    engine.release_completed_step();

    const auto next = last_due + MIN_TD;
    engine.engine_control().add_before_evaluation_notification([&] {
        ListBuilder samples{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())};
        samples.push_back(Int{11});
        window.begin_mutation(next).replace_samples(samples.build().view(), std::array{next});
    });
    REQUIRE(engine.step(next));
    const auto replacement = capture_window_delta(window, next);
    REQUIRE(replacement.has_value());
    auto sample = replacement.view().as_bundle().at("replacement").as_indexed_view().at(0).as_bundle();
    CHECK(sample.at("value").checked_as<Int>() == 11);
    CHECK(sample.at("time").checked_as<DateTime>() == next);
    engine.release_completed_step();
    CHECK(capture_window_delta(window, next).equals(replacement));
    engine.stop_external();
}

TEST_CASE("completed window step: a write at a closed time cannot reopen a removal-bearing change")
{
    auto executor = window_executor<SeedWindowBatch>();
    auto engine = executor.view();
    engine.start_external(MIN_ST);
    const auto idle = MIN_ST + MIN_TD;
    REQUIRE(engine.step(idle));
    engine.release_completed_step();

    auto out = engine.graph().node_at(0).output(idle);
    auto bundle = out.as_bundle();
    auto prices = bundle.at(0);
    auto window = prices.as_window().data_view();
    REQUIRE(window.size() == 3);
    {
        auto cursor = window.begin_mutation(idle);
        cursor.clear();
        cursor.push(Value{Int{99}}.view());
    }
    REQUIRE(window.size() == 1);
    CHECK(window.back().checked_as<Int>() == 99);
    CHECK(window.removed_values(idle).limit == 0);
    CHECK_FALSE(prices.as_window().window_delta().has_value());
    engine.stop_external();
}

namespace
{
    using FourCountWindows = TSL<CountWindow, 4>;

    struct SeedFourWindows
    {
        static void eval(Out<FourCountWindows>) {}
        static void start(Out<FourCountWindows> out)
        {
            for (Int index = 0; index < 4; ++index)
                out[index].begin_mutation(out.evaluation_time()).push(Value{index}.view());
        }
    };
}

TEST_CASE("completed window step: destroying a nested owner leaves other outstanding departures inspectable")
{
    auto executor = window_executor<SeedFourWindows>();
    auto engine = executor.view();
    engine.start_external(MIN_ST);
    auto root = engine.graph();
    NodeBuilder node;
    node.implementation<SeedFourWindows>();
    GraphBuilder graph;
    graph.add_node(node);
    const auto tick = MIN_ST + MIN_TD;
    auto anchor = root.node_at(0);
    Out<FourCountWindows> survivor{anchor.output(tick), tick};
    {
        auto child = graph.make_nested_graph(anchor.pointer());
        child.view().start(MIN_ST);
        Out<FourCountWindows> retired{child.view().node_at(0).output(tick), tick};
        engine.engine_control().add_before_evaluation_notification([&] {
            // Both graphs share one completed step. Alternate the owners so
            // destroying the child must leave non-adjacent survivors intact.
            for (Int index = 0; index < 4; ++index)
            {
                retired[index].begin_mutation(tick).push(Value{Int{100} + index}.view());
                survivor[index].begin_mutation(tick).push(Value{Int{200} + index}.view());
            }
        });
        REQUIRE(engine.step(tick));
        for (Int index = 0; index < 4; ++index)
            check_departure(retired[index].data_view(), tick, index, MIN_ST);
    }
    for (Int index = 0; index < 4; ++index)
        check_departure(survivor[index].data_view(), tick, index, MIN_ST);
    engine.release_completed_step();
    for (Int index = 0; index < 4; ++index)
    {
        auto window = survivor[index];
        CHECK(window.removed_values().limit == 0);
        CHECK(window.back().checked_as<Int>() == 200 + index);
    }
    engine.stop_external();
}

namespace
{
    using CountRings = TSB<"CompletedFixedWindows", Field<"numbers", TSW<Int, 3, 1>>, Field<"labels", TSW<Str, 3, 1>>>;

    template<typename T>
    struct ExternallyPopulatedCountWindow
    {
        static void eval(Out<TSW<T, 1, 1>>) {}
    };

    struct RollCountRings
    {
        static void start(NodeScheduler clock) { clock.schedule(clock.now()); }
        static void eval(Out<CountRings> out, NodeScheduler clock)
        {
            const auto time = clock.now();
            const Int index = (time - MIN_ST) / MIN_TD;
            out.field<"numbers">().begin_mutation(time).push(Value{index}.view());
            const Str label(256, static_cast<char>('a' + index));
            out.field<"labels">().begin_mutation(time).push(Value{label}.view());
            if (index < 6) { clock.schedule(MIN_TD); }
        }
    };
}

TEST_CASE("completed window step: fixed ring departures and projected caches retire together")
{
    const auto last = MIN_ST + 6 * MIN_TD;
    const auto departed = MIN_ST + 3 * MIN_TD;
    Value saved;
    std::optional<TSOutput> detached;
    {
        auto executor = window_executor<RollCountRings>();
        auto engine = executor.view();
        engine.start_external(MIN_ST);
        for (Int i = 0; i < 6; ++i) { REQUIRE(engine.step(MIN_ST + i * MIN_TD)); }
        auto output = engine.graph().node_at(0).output(last);
        auto fields = output.as_bundle();
        auto numbers = fields.at(0);
        auto labels = fields.at(1);
        auto numeric = numbers.as_window().data_view();
        auto text = labels.as_window().data_view();
        const auto front = text.front();
        const void *old_slot = front.data();
        const void *old_characters = front.checked_as<Str>().data();
        REQUIRE(engine.step(last));
        check_departure(numeric, last, 3, departed);
        const auto word = text.removed_value(last);
        CHECK(word.data() == old_slot);
        CHECK(static_cast<const void *>(word.checked_as<Str>().data()) == old_characters);
        REQUIRE(word.checked_as<Str>() == Str(256, 'd'));
        const auto times = text.removed_value_times(last);
        REQUIRE(times.limit == 1);
        CHECK(*times.begin() == departed);
        saved = capture_window_delta(text, last);
        detached.emplace(*output.output());

        const auto before_projection = numbers.data_view().dynamic_storage_metrics().live_bytes;
        const auto owning_bytes = labels.data_view().dynamic_storage_metrics().live_bytes;
        // Reading the bundle asks each window for its borrowed canonical projection.
        auto delta = output.data_view().delta_value(last).as_bundle();
        REQUIRE(delta.at("numbers").as_bundle().at("removed").as_indexed_view().size() == 1);
        REQUIRE(numbers.data_view().dynamic_storage_metrics().live_bytes > before_projection);

        SECTION("explicit release") { engine.release_completed_step(); }
        SECTION("quiet root advance") { REQUIRE(engine.step(last + MIN_TD)); }
        CHECK_FALSE(capture_window_delta(numeric, last).has_value());
        CHECK_FALSE(capture_window_delta(text, last).has_value());
        CHECK_FALSE(numeric.delta_value(last).has_value());
        CHECK_FALSE(numeric.has_removed_value(last));
        CHECK(numeric.removed_value_times(last).limit == 0);
        CHECK(text.removed_values(last).limit == 0);
        CHECK(numbers.data_view().dynamic_storage_metrics().live_bytes <= before_projection);
        CHECK(labels.data_view().dynamic_storage_metrics().live_bytes < owning_bytes);
        CHECK(numeric.back().checked_as<Int>() == 6);
        CHECK(text.back().checked_as<Str>() == Str(256, 'g'));
        engine.stop_external();
    }
    CHECK(saved.equals(window_delta<Str>(Str(256, 'g'), {{Str(256, 'd'), departed}})));
    auto copied_output = detached->view(last);
    auto fields = copied_output.as_bundle();
    auto labels = fields.at(1);
    CHECK(labels.as_window().window_delta().equals(saved));
}

TEST_CASE("completed window step: restarting at or before a fixed ring departure cannot revive it")
{
    auto executor = window_executor<ExternallyPopulatedCountWindow<Int>>();
    auto engine = executor.view();
    engine.start_external(MIN_ST);
    const auto rolled = MIN_ST + MIN_TD;
    auto out = engine.graph().node_at(0).output(rolled);
    auto window = out.as_window().data_view();
    engine.engine_control().add_before_evaluation_notification([&] {
        window.begin_mutation(MIN_ST).push(Value{Int{4}}.view());
    });
    REQUIRE(engine.step(MIN_ST));
    engine.engine_control().add_before_evaluation_notification([&] {
        window.begin_mutation(rolled).push(Value{Int{5}}.view());
    });
    REQUIRE(engine.step(rolled));
    check_departure(window, rolled, 4, MIN_ST);
    engine.stop_external();

    SECTION("restart at the final timestamp") { engine.start_external(rolled); }
    SECTION("rewind and visit the final timestamp again")
    {
        engine.start_external(MIN_ST);
        REQUIRE(engine.step(MIN_ST));
    }
    REQUIRE(engine.step(rolled));
    CHECK(window.removed_values(rolled).limit == 0);
    CHECK_FALSE(capture_window_delta(window, rolled).has_value());
    CHECK(window.back().checked_as<Int>() == 5);
    TSOutput detached{*out.output()};
    auto copy_output = detached.view(rolled);
    auto copy = copy_output.as_window().data_view();
    CHECK(copy.removed_values(rolled).limit == 0);
    CHECK_FALSE(copy.delta_value(rolled).has_value());
    CHECK(copy.back().checked_as<Int>() == 5);
    engine.stop_external();
}

namespace hgraph::static_schema_detail
{
    template<>
    struct scalar_name<std::shared_ptr<int>>
    {
        static constexpr std::string_view value{"window_removal_lifetime"};
    };
}

TEST_CASE("completed window step: a fixed window drops its last owner of a departed resource")
{
    using Resource = std::shared_ptr<int>;
    auto executor = window_executor<ExternallyPopulatedCountWindow<Resource>>();
    auto engine = executor.view();
    engine.start_external(MIN_ST);
    auto output = engine.graph().node_at(0).output(MIN_ST);
    auto window = output.as_window().data_view();
    std::weak_ptr<int> departed;
    const auto push = [&](DateTime time, int value) {
        auto resource = std::make_shared<int>(value);
        window.begin_mutation(time).push(Value{window.layout().element_binding, &resource}.view());
        return std::weak_ptr<int>{resource};
    };
    engine.engine_control().add_before_evaluation_notification([&] { departed = push(MIN_ST, 4); });
    REQUIRE(engine.step(MIN_ST));
    const auto rolled = MIN_ST + MIN_TD;
    engine.engine_control().add_before_evaluation_notification([&] { push(rolled, 5); });
    REQUIRE(engine.step(rolled));
    REQUIRE_FALSE(departed.expired());
    REQUIRE(window.removed_value(rolled).checked_as<Resource>() == departed.lock());

    SECTION("explicit release") { engine.release_completed_step(); }
    SECTION("quiet root advance") { REQUIRE(engine.step(rolled + MIN_TD)); }
    CHECK(departed.expired());
    CHECK(*window.back().checked_as<Resource>() == 5);
    engine.stop_external();
}
