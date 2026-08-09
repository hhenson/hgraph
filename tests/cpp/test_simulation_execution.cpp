// Characterization tests for simple-TS graph execution in simulation mode.
//
// These tests pin down what the runtime ACTUALLY does today and isolate the
// real gap for the current milestone (see CLAUDE.md sec.5 / memory
// "runtime execution status"):
//
//   * Notification-driven scheduling WORKS: when a bound output re-ticks, the
//     downstream node is automatically rescheduled and re-evaluated. This is
//     proven in isolation below by manually re-scheduling only the source.
//   * Source tick INJECTION is missing: a source writes a constant on eval and
//     never reschedules itself, so the executor runs a single cycle (all nodes
//     are scheduled once at start_time) and then idles until end_time. The
//     "[!shouldfail][milestone]" test documents the target behaviour and will
//     start passing once Step 2 lands a real simulation source — at which point
//     Catch2 flags it so we remove the tag.
//
// They deliberately count node evaluations (via a captured counter) instead of
// only checking the final value, which is what made the existing runtime tests
// false-positive on the multi-cycle semantics.

#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>

#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;

    // A source that writes a fixed value and counts how many times it is evaluated.
    NodeBuilder counting_source(const TSValueTypeMetaData *ts_int, int value, std::int32_t *eval_count)
    {
        NodeTypeMetaData schema;
        schema.display_name = "source";
        schema.output_schema = ts_int;
        schema.node_kind = NodeKind::PullSource;
        schema.schedule_on_start = true;  // a source initiates itself at the start cycle

        NodeCallbacks callbacks;
        callbacks.evaluate = [value, eval_count](const NodeView &view, DateTime evaluation_time) {
            ++*eval_count;
            testing::set_output_value(view, evaluation_time, value);
        };
        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    // A compute node that emits input + 1 and counts how many times it is evaluated.
    NodeBuilder counting_add_one(const TSValueTypeMetaData *input_schema,
                                 const TSValueTypeMetaData *ts_int, std::int32_t *eval_count)
    {
        NodeTypeMetaData schema;
        schema.display_name = "add_one";
        schema.input_schema = input_schema;
        schema.output_schema = ts_int;
        schema.node_kind = NodeKind::Compute;

        NodeCallbacks callbacks;
        callbacks.evaluate = [eval_count](const NodeView &view, DateTime evaluation_time) {
            ++*eval_count;
            auto      root  = view.input(evaluation_time);
            auto      bundle = root.as_bundle();
            auto      input = bundle[0];
            const std::int32_t value = input.value().checked_as<std::int32_t>();
            testing::set_output_value(view, evaluation_time, value + 1);
        };

        auto endpoint = TSEndpointSchema::non_peered(
            input_schema,
            {
                TSEndpointSchema::peered(ts_int),
            });
        return NodeBuilder::native(std::move(schema), std::move(callbacks), std::move(endpoint));
    }

    // A *real* simulation source: it reschedules itself each cycle (via the
    // NodeScheduler injectable) until it has emitted ``count`` values, emitting
    // 0, 1, 2, ... and recording the cycle count into the GlobalState. This is
    // the data-driven, multi-cycle counterpart to the constant source above.
    struct TickingSource
    {
        static constexpr auto name              = "ticking_source";
        static constexpr bool schedule_on_start = true;  // initiate at the start cycle
        static void           eval(NodeScheduler sched, GlobalStateView gs, Scalar<"count", std::int32_t> count,
                                    State<std::int32_t> emitted, Out<TS<std::int32_t>> out)
        {
            const std::int32_t n = emitted.get();
            out.set(n);
            gs.set("ticks", Value{n + 1});
            emitted.set(n + 1);
            if (n + 1 < count.value()) { sched.schedule(MIN_TD); }  // re-arm for the next cycle
        }
    };

    struct AddOneNode
    {
        static constexpr auto name = "add_one";
        static void           eval(In<"in", TS<std::int32_t>> in, Out<TS<std::int32_t>> out) { out.set(in.value() + 1); }
    };

    struct PhaseRunnerNode
    {
        static constexpr auto name                  = "phase_runner_node";
        static constexpr bool requires_phase_runner = true;
        static void eval(In<"input", TS<std::int32_t>>) {}
    };

    struct PhaseRunnerGraph
    {
        static constexpr auto name = "phase_runner_graph";
        static void compose(Wiring &w)
        {
            wire<PhaseRunnerNode>(w, wire<TickingSource>(w, 1));
        }
    };

    struct TickGraph
    {
        static constexpr auto name = "tick_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<TickingSource>(w, 3);  // count = 3
            wire<AddOneNode>(w, src);
        }
    };
}  // namespace

TEST_CASE("graph builder records a static node phase-runner requirement")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<PhaseRunnerGraph>();
    REQUIRE(graph_builder.node_count() == 2);
    CHECK(graph_builder.nodes().back().type().schema()->requires_phase_runner);
    CHECK(graph_builder.requires_phase_runner());
}

TEST_CASE("simulation: a constant source drives exactly one cycle (no tick injection yet)")
{
    using namespace hgraph;

    auto       &registry     = TypeRegistry::instance();
    const auto *int_meta     = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int       = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("SimInput", {{"value", ts_int}});

    std::int32_t source_evals  = 0;
    std::int32_t add_one_evals = 0;

    GraphBuilder graph_builder;
    graph_builder.add_node(counting_source(ts_int, 7, &source_evals))
        .add_node(counting_add_one(input_schema, ts_int, &add_one_evals))
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{3});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    // The single cycle produced the right value...
    auto graph = executor_view.graph();
    REQUIRE(graph.node_at(1).output(MIN_ST).value().checked_as<std::int32_t>() == 8);

    // ...but across the whole [start, end) window the source ran exactly once.
    // There is no data-driven tick injection, so after the first cycle nothing
    // reschedules and the loop ends. This pins the current limitation.
    CHECK(source_evals == 1);
    CHECK(add_one_evals == 1);
}

TEST_CASE("simulation: phase runner wraps each complete executor phase once")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    std::optional<GraphExecutorPhase> active_phase;
    std::vector<GraphExecutorPhase>   runner_phases;
    std::vector<GraphExecutorPhase>   node_phases;
    std::vector<GraphExecutorPhase>   notification_phases;

    NodeTypeMetaData schema;
    schema.display_name      = "phase_observer_source";
    schema.output_schema     = ts_int;
    schema.node_kind         = NodeKind::PullSource;
    schema.schedule_on_start = true;
    NodeCallbacks callbacks;
    callbacks.start = [&](const NodeView &view, DateTime) {
        REQUIRE(active_phase == GraphExecutorPhase::Start);
        node_phases.push_back(*active_phase);
        const EngineControlView engine = view.graph().executor().engine_control();
        engine.add_before_evaluation_notification([&] {
            REQUIRE(active_phase == GraphExecutorPhase::Evaluation);
            notification_phases.push_back(*active_phase);
        });
        engine.add_after_evaluation_notification([&] {
            REQUIRE(active_phase == GraphExecutorPhase::Evaluation);
            notification_phases.push_back(*active_phase);
        });
    };
    callbacks.evaluate = [&](const NodeView &view, DateTime evaluation_time) {
        REQUIRE(active_phase == GraphExecutorPhase::Evaluation);
        node_phases.push_back(*active_phase);
        testing::set_output_value(view, evaluation_time, 1);
    };
    callbacks.stop = [&](const NodeView &, DateTime) {
        REQUIRE(active_phase == GraphExecutorPhase::Stop);
        node_phases.push_back(*active_phase);
    };

    GraphBuilder graph_builder;
    graph_builder.add_node(
        NodeBuilder::native(std::move(schema), std::move(callbacks)));

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{3})
        .phase_runner([&](GraphExecutorPhase phase,
                          GraphExecutorPhaseAction action) {
            REQUIRE_FALSE(active_phase.has_value());
            active_phase = phase;
            runner_phases.push_back(phase);
            try
            {
                action();
            }
            catch (...)
            {
                active_phase.reset();
                throw;
            }
            active_phase.reset();
        });

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    REQUIRE(runner_phases.size() == 3);
    CHECK(runner_phases[0] == GraphExecutorPhase::Start);
    CHECK(runner_phases[1] == GraphExecutorPhase::Evaluation);
    CHECK(runner_phases[2] == GraphExecutorPhase::Stop);
    CHECK(node_phases == runner_phases);
    REQUIRE(notification_phases.size() == 2);
    CHECK(notification_phases[0] == GraphExecutorPhase::Evaluation);
    CHECK(notification_phases[1] == GraphExecutorPhase::Evaluation);
    CHECK_FALSE(active_phase.has_value());
}

TEST_CASE("simulation: phase runner unwinds before failed-run cleanup")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    NodeTypeMetaData schema;
    schema.display_name      = "failing_phase_source";
    schema.output_schema     = ts_int;
    schema.node_kind         = NodeKind::PullSource;
    schema.schedule_on_start = true;
    NodeCallbacks callbacks;
    callbacks.evaluate = [](const NodeView &, DateTime) {
        throw std::runtime_error("phase failure");
    };

    GraphBuilder graph_builder;
    graph_builder.add_node(
        NodeBuilder::native(std::move(schema), std::move(callbacks)));

    std::optional<GraphExecutorPhase> active_phase;
    std::vector<GraphExecutorPhase>   runner_phases;
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{3})
        .phase_runner([&](GraphExecutorPhase phase,
                          GraphExecutorPhaseAction action) {
            REQUIRE_FALSE(active_phase.has_value());
            active_phase = phase;
            runner_phases.push_back(phase);
            try
            {
                action();
            }
            catch (...)
            {
                active_phase.reset();
                throw;
            }
            active_phase.reset();
        });

    GraphExecutorValue executor = executor_builder.make_executor();
    CHECK_THROWS_AS(executor.view().run(), std::runtime_error);
    REQUIRE(runner_phases.size() == 3);
    CHECK(runner_phases[0] == GraphExecutorPhase::Start);
    CHECK(runner_phases[1] == GraphExecutorPhase::Evaluation);
    CHECK(runner_phases[2] == GraphExecutorPhase::Stop);
    CHECK_FALSE(active_phase.has_value());
}

TEST_CASE("simulation: retained failed run enters its stop phase at destruction")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    std::optional<GraphExecutorPhase> active_phase;
    std::vector<GraphExecutorPhase>   runner_phases;
    std::vector<GraphExecutorPhase>   node_phases;

    NodeTypeMetaData schema;
    schema.display_name      = "retained_failing_phase_source";
    schema.output_schema     = ts_int;
    schema.node_kind         = NodeKind::PullSource;
    schema.schedule_on_start = true;
    NodeCallbacks callbacks;
    callbacks.evaluate = [&](const NodeView &, DateTime) {
        REQUIRE(active_phase == GraphExecutorPhase::Evaluation);
        node_phases.push_back(*active_phase);
        throw std::runtime_error("retained phase failure");
    };
    callbacks.stop = [&](const NodeView &, DateTime) {
        REQUIRE(active_phase == GraphExecutorPhase::Stop);
        node_phases.push_back(*active_phase);
    };

    GraphBuilder graph_builder;
    graph_builder.add_node(
        NodeBuilder::native(std::move(schema), std::move(callbacks)));

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{3})
        .cleanup_on_error(false)
        .phase_runner([&](GraphExecutorPhase phase,
                          GraphExecutorPhaseAction action) {
            REQUIRE_FALSE(active_phase.has_value());
            active_phase = phase;
            runner_phases.push_back(phase);
            try
            {
                action();
            }
            catch (...)
            {
                active_phase.reset();
                throw;
            }
            active_phase.reset();
        });

    {
        GraphExecutorValue executor = executor_builder.make_executor();
        CHECK_THROWS_AS(executor.view().run(), std::runtime_error);
        const std::vector before_destruction{
            GraphExecutorPhase::Start,
            GraphExecutorPhase::Evaluation,
        };
        CHECK(runner_phases == before_destruction);
        CHECK(node_phases ==
              (std::vector<GraphExecutorPhase>{GraphExecutorPhase::Evaluation}));
    }

    const std::vector after_destruction{
        GraphExecutorPhase::Start,
        GraphExecutorPhase::Evaluation,
        GraphExecutorPhase::Stop,
    };
    CHECK(runner_phases == after_destruction);
    CHECK(node_phases == (std::vector{
        GraphExecutorPhase::Evaluation,
        GraphExecutorPhase::Stop,
    }));
    CHECK_FALSE(active_phase.has_value());
}

TEST_CASE("simulation: a node that does not schedule itself in start is never evaluated")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    // A source with NO start hook: nothing schedules it, so it must never run.
    std::int32_t              evals = 0;
    NodeTypeMetaData schema;
    schema.display_name  = "unscheduled_source";
    schema.output_schema = ts_int;
    schema.node_kind     = NodeKind::PullSource;
    NodeCallbacks callbacks;
    callbacks.evaluate = [&evals](const NodeView &view, DateTime evaluation_time) {
        ++evals;
        testing::set_output_value(view, evaluation_time, 1);
    };

    GraphBuilder graph_builder;
    graph_builder.add_node(NodeBuilder::native(std::move(schema), std::move(callbacks)));

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{5});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    // Default state is not-scheduled: with no self-scheduling in start, the node
    // is never evaluated (the executor idles to end_time).
    CHECK(evals == 0);
}

TEST_CASE("simulation: re-ticking a source reschedules its downstream via notification")
{
    using namespace hgraph;

    auto       &registry     = TypeRegistry::instance();
    const auto *int_meta     = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int       = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("NotifyInput", {{"value", ts_int}});

    std::int32_t source_evals  = 0;
    std::int32_t add_one_evals = 0;

    GraphBuilder builder;
    builder.add_node(counting_source(ts_int, 5, &source_evals))
        .add_node(counting_add_one(input_schema, ts_int, &add_one_evals))
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    testing::MockRootGraph graph{builder};
    auto       view  = graph.graph();

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};

    view.start(t1);
    view.evaluate(t1);
    CHECK(source_evals == 1);
    CHECK(add_one_evals == 1);
    CHECK(view.node_at(1).output(t1).value().checked_as<std::int32_t>() == 6);

    // Nothing is scheduled now: the source does not reschedule itself.
    CHECK(view.next_scheduled_time() == MAX_DT);

    // Re-schedule ONLY the source at t2. When it re-ticks its output, the
    // notification chain must reschedule add_one for t2 automatically -- this
    // is the notification-driven scheduling working in isolation, not the
    // "everything scheduled at start" effect.
    view.schedule_node(0, t2);
    view.evaluate(t2);
    CHECK(source_evals == 2);
    CHECK(add_one_evals == 2);
    CHECK(view.node_at(1).output(t2).modified());

    // A cycle where nothing was scheduled evaluates nothing.
    view.evaluate(t3);
    CHECK(source_evals == 2);
    CHECK(add_one_evals == 2);

    view.stop();
}

// MILESTONE: data-driven, multi-cycle evaluation over simulated time. A source
// that reschedules itself (via the NodeScheduler injectable) drives the graph for
// as many cycles as it ticks, and its downstream is re-evaluated each cycle. The
// cycle count is recorded into the GlobalState; the source/add_one outputs track
// the per-cycle values. (This retired the former "[!shouldfail]" placeholder.)
TEST_CASE("simulation: a self-rescheduling source drives multiple cycles over time", "[milestone]")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    GraphBuilder graph_builder = build_graph<TickGraph>();  // TickingSource(count=3) -> AddOneNode

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{10});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();

    // The source rescheduled itself for exactly three cycles.
    CHECK(graph.global_state().get_as<std::int32_t>("ticks") == 3);

    // It emitted 0, 1, 2 over successive cycles (last value retained on the
    // output); the downstream add_one was re-evaluated each cycle and tracked it.
    CHECK(graph.node_at(0).output(MIN_ST).value().checked_as<std::int32_t>() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<std::int32_t>() == 3);
}

namespace
{
    using namespace hgraph;

    hgraph::NodeBuilder immediate_rescheduling_source(const char *display_name,
                                                      int *eval_count,
                                                      int burst_length,
                                                      int total_evaluations)
    {
        auto       &registry = TypeRegistry::instance();
        const auto *int_meta = registry.register_scalar<Int>("int");
        const auto *ts_int   = registry.ts(int_meta);

        NodeTypeMetaData schema;
        schema.display_name      = display_name;
        schema.output_schema     = ts_int;
        schema.node_kind         = NodeKind::PullSource;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [eval_count, burst_length, total_evaluations](const NodeView &view,
                                                                           DateTime evaluation_time) {
            ++(*eval_count);
            hgraph::testing::set_output_value(view, evaluation_time, Int{*eval_count});
            if (*eval_count >= total_evaluations) { return; }
            const bool burst_boundary = burst_length > 0 && (*eval_count % burst_length) == 0;
            const TimeDelta delay = burst_boundary ? TimeDelta{1'000} : MIN_TD;
            view.graph_value()->schedule_node(view.node_index(), evaluation_time + delay);
        };

        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }
}  // namespace

TEST_CASE("simulation: the opt-in recursion guard trips on a sustained MIN_TD loop")
{
    using namespace hgraph;

    int eval_count = 0;

    GraphBuilder graph_builder;
    // burst_length 0: reschedules +MIN_TD forever (until the guard trips).
    graph_builder.add_node(immediate_rescheduling_source("busy_loop_source", &eval_count, 0, 1'000'000));

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{3'600'000'000})
        .max_consecutive_immediate_cycles(50);

    GraphExecutorValue executor = executor_builder.make_executor();

    std::string message;
    try
    {
        executor.view().run();
        FAIL("expected RecursiveEvaluationError");
    }
    catch (const RecursiveEvaluationError &error)
    {
        message = error.what();
    }

    // The guard trips at the limit, records one further cycle, then throws.
    CHECK(eval_count >= 50);
    CHECK(eval_count <= 55);
    CHECK(message.find("potential recursive evaluation loop") != std::string::npos);
    CHECK(message.find("busy_loop_source") != std::string::npos);
}

TEST_CASE("simulation: immediate bursts below the guard limit do not trip it")
{
    using namespace hgraph;

    int eval_count = 0;

    GraphBuilder graph_builder;
    // Bursts of 10 immediate cycles separated by 1ms jumps, 60 evaluations total.
    graph_builder.add_node(immediate_rescheduling_source("bursty_source", &eval_count, 10, 60));

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{3'600'000'000})
        .max_consecutive_immediate_cycles(50);

    GraphExecutorValue executor = executor_builder.make_executor();
    CHECK_NOTHROW(executor.view().run());
    CHECK(eval_count == 60);
}
