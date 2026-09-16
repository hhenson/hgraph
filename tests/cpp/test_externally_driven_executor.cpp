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
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

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
