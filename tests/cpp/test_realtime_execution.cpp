#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    NodeBuilder delayed_source(TimeDelta delay,
                               std::atomic_int *eval_count,
                               DateTime *observed_evaluation_time,
                               DateTime *observed_clock_evaluation_time,
                               DateTime *observed_clock_now)
    {
        auto       &registry = TypeRegistry::instance();
        const auto *int_meta = registry.register_scalar<Int>("int");
        const auto *ts_int   = registry.ts(int_meta);

        NodeTypeMetaData schema;
        schema.display_name  = "delayed_realtime_source";
        schema.output_schema = ts_int;
        schema.node_kind     = NodeKind::PullSource;

        NodeCallbacks callbacks;
        callbacks.start = [delay](const NodeView &view, DateTime start_time) {
            view.graph_value()->schedule_node(view.node_index(), start_time + delay);
        };
        callbacks.evaluate =
            [eval_count, observed_evaluation_time, observed_clock_evaluation_time, observed_clock_now](
                const NodeView &view,
                DateTime evaluation_time) {
                ++(*eval_count);
                const EvaluationClockView clock = view.graph().executor().evaluation_clock();
                *observed_evaluation_time = evaluation_time;
                *observed_clock_evaluation_time = clock.evaluation_time();
                *observed_clock_now = clock.now();
                set_output_value(view, evaluation_time, Int{1});
            };

        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    NodeBuilder scheduled_push_source(const TSValueTypeMetaData &output_schema, std::int32_t &eval_count)
    {
        NodeTypeMetaData schema;
        schema.display_name = "scheduled_push_source";
        schema.output_schema = &output_schema;
        schema.node_kind = NodeKind::PushSource;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [&eval_count](const NodeView &view, DateTime evaluation_time) {
            const Int value{eval_count};
            testing::set_output_value(view, evaluation_time, value);
            ++eval_count;
            if (eval_count < 2)
            {
                view.graph_value()->schedule_node(view.node_index(), evaluation_time + MIN_TD);
            }
        };

        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    struct WallClockScheduledSource
    {
        static constexpr auto name = "wall_clock_scheduled_source";

        static void start(Scalar<"delay", TimeDelta> delay,
                          NodeScheduler sched,
                          EvaluationClockView clock,
                          GlobalStateView gs)
        {
            const DateTime target = std::max(sched.now(), clock.now()) + delay.value();
            gs.set("wall_target", Value{target});
            sched.schedule(target, "wall", /*on_wall_clock=*/true);
        }

        static void eval(Scalar<"delay", TimeDelta>,
                         DateTime evaluation_time,
                         EvaluationClockView clock,
                         GlobalStateView gs,
                         Out<TS<Int>> out)
        {
            gs.set("wall_evaluation_time", Value{evaluation_time});
            gs.set("wall_now", Value{clock.now()});
            out.set(Int{1});
        }
    };

    struct WallClockScheduledGraph
    {
        static constexpr auto name = "wall_clock_scheduled_graph";

        static void compose(Wiring &w, Scalar<"delay", TimeDelta> delay)
        {
            wire<WallClockScheduledSource>(w, delay);
        }
    };

    struct PushPolymorphicSchemas
    {
        const ValueTypeMetaData *base{nullptr};
        const ValueTypeMetaData *small{nullptr};
        const ValueTypeMetaData *large{nullptr};
        const TSValueTypeMetaData *dict{nullptr};
        std::shared_ptr<const TypeRealizationSnapshot> realization{};
    };

    [[nodiscard]] PushPolymorphicSchemas push_polymorphic_schemas()
    {
        auto &registry = TypeRegistry::instance();
        const auto *integer = registry.register_scalar<Int>("int");
        const auto *text = registry.register_scalar<Str>("str");
        const auto *base = registry.bundle(
            "tests.push_pool", "Value", {{"id", integer}}, {}, true);
        const auto *small = registry.bundle(
            "tests.push_pool", "Small", {{"id", integer}}, {base});
        const auto *large = registry.bundle(
            "tests.push_pool", "Large",
            {{"id", integer}, {"a", text}, {"b", text}, {"c", text}},
            {base});
        return PushPolymorphicSchemas{
            .base = base,
            .small = small,
            .large = large,
            .dict = registry.tsd(base, registry.ts(integer)),
            .realization = TypeRealizationSnapshot::capture(
                registry,
                TypeRealizationOptions{
                    .polymorphic_compound_storage =
                        PolymorphicCompoundStoragePolicy::Pooled,
                }),
        };
    }

    [[nodiscard]] Value make_push_leaf(
        const ValueTypeMetaData *schema, Int id, std::string label = {})
    {
        BundleBuilder builder{ValuePlanFactory::instance().type_for(schema)};
        builder.set("id", Value{id});
        if (schema->field_count > 1)
        {
            builder.set("a", Value{Str{label + "-a"}});
            builder.set("b", Value{Str{label + "-b"}});
            builder.set("c", Value{Str{label + "-c"}});
        }
        return builder.build();
    }

    [[nodiscard]] Value make_push_union(
        const PushPolymorphicSchemas &schemas, const Value &leaf)
    {
        const auto binding = schemas.realization->type_for(schemas.base);
        Value result{binding};
        binding.ops_ref().copy_assign_from(
            binding, result.begin_mutation().mutable_data(), leaf.binding(),
            leaf.view().data());
        return result;
    }

    [[nodiscard]] Value make_push_dict_delta(
        const PushPolymorphicSchemas &schemas,
        std::span<const std::pair<const Value *, Int>> modified,
        std::span<const Value> removed)
    {
        const auto key_binding = schemas.realization->type_for(schemas.base);
        const auto value_binding = ValuePlanFactory::instance().type_for(
            TypeRegistry::instance().value_type("int"));
        MapBuilder modified_builder{key_binding, value_binding};
        for (const auto &[leaf, value] : modified)
        {
            const Value key = make_push_union(schemas, *leaf);
            modified_builder.set_item_copy(key.view().data(),
                                           std::addressof(value));
        }
        SetBuilder removed_builder{key_binding};
        for (const Value &leaf : removed)
        {
            const Value key = make_push_union(schemas, leaf);
            static_cast<void>(removed_builder.insert_copy(key.view().data()));
        }
        BundleBuilder delta{
            schemas.realization->type_for(schemas.dict->delta_value_schema)};
        delta.set("removed", removed_builder.build());
        delta.set("modified", modified_builder.build());
        return delta.build();
    }
}  // namespace

TEST_CASE("real-time executor defaults an omitted start time to the wall clock")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    const DateTime before = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .end_time(before + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    const DateTime after = hgraph::testing::wall_now();

    CHECK(executor.view().start_time() >= before);
    CHECK(executor.view().start_time() <= after);

    GraphExecutorBuilder explicit_builder;
    explicit_builder.graph_builder(GraphBuilder{})
        .mode(GraphExecutorMode::RealTime)
        .start_time(MIN_DT)
        .end_time(MIN_ST);

    GraphExecutorValue explicit_executor = explicit_builder.make_executor();
    CHECK(explicit_executor.view().start_time() == MIN_DT);
}

TEST_CASE("real-time executor waits for the future scheduled time")
{
    using namespace hgraph;

    constexpr TimeDelta start_offset{10'000};
    constexpr TimeDelta delay{20'000};

    std::atomic_int eval_count{0};
    DateTime        observed_evaluation_time{MIN_DT};
    DateTime        observed_clock_evaluation_time{MIN_DT};
    DateTime        observed_clock_now{MIN_DT};

    GraphBuilder graph_builder;
    graph_builder.add_node(delayed_source(delay,
                                          &eval_count,
                                          &observed_evaluation_time,
                                          &observed_clock_evaluation_time,
                                          &observed_clock_now));

    const DateTime start_time = hgraph::testing::wall_now() + start_offset;
    const DateTime target_time = start_time + delay;

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{200'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    view.run();

    CHECK(eval_count.load() == 1);
    CHECK(observed_evaluation_time == target_time);
    CHECK(observed_clock_evaluation_time == observed_evaluation_time);
    CHECK(observed_clock_now >= observed_clock_evaluation_time);
    CHECK(hgraph::testing::wall_now() >= target_time);
}

TEST_CASE("real-time phase runner releases between complete executor phases")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    std::optional<GraphExecutorPhase> active_phase;
    std::vector<GraphExecutorPhase>   runner_phases;
    std::vector<GraphExecutorPhase>   node_phases;

    constexpr TimeDelta delay{2'000};
    NodeTypeMetaData schema;
    schema.display_name  = "realtime_phase_source";
    schema.output_schema = ts_int;
    schema.node_kind     = NodeKind::PullSource;
    NodeCallbacks callbacks;
    callbacks.start = [&](const NodeView &view, DateTime start_time) {
        REQUIRE(active_phase == GraphExecutorPhase::Start);
        node_phases.push_back(*active_phase);
        view.graph_value()->schedule_node(view.node_index(), start_time + delay);
    };
    callbacks.evaluate = [&](const NodeView &view, DateTime evaluation_time) {
        REQUIRE(active_phase == GraphExecutorPhase::Evaluation);
        node_phases.push_back(*active_phase);
        testing::set_output_value(view, evaluation_time, Int{1});
    };
    callbacks.stop = [&](const NodeView &, DateTime) {
        REQUIRE(active_phase == GraphExecutorPhase::Stop);
        node_phases.push_back(*active_phase);
    };

    GraphBuilder graph_builder;
    graph_builder.add_node(
        NodeBuilder::native(std::move(schema), std::move(callbacks)));

    const DateTime start_time = hgraph::testing::wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{100'000})
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

    const std::vector expected{
        GraphExecutorPhase::Start,
        GraphExecutorPhase::Evaluation,
        GraphExecutorPhase::Stop,
    };
    CHECK(runner_phases == expected);
    CHECK(node_phases == expected);
    CHECK_FALSE(active_phase.has_value());
}

TEST_CASE("real-time NodeScheduler supports wall-clock alarms")
{
    using namespace hgraph;

    constexpr TimeDelta delay{20'000};

    GraphBuilder graph_builder = build_graph<WallClockScheduledGraph>(delay);

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    view.run();

    auto graph = view.graph();
    REQUIRE(graph.node_count() == 1);

    const DateTime target          = graph.global_state().get_as<DateTime>("wall_target");
    const DateTime evaluation_time = graph.global_state().get_as<DateTime>("wall_evaluation_time");
    const DateTime wall_now        = graph.global_state().get_as<DateTime>("wall_now");

    CHECK(evaluation_time == target);
    CHECK(wall_now >= evaluation_time);
    CHECK(hgraph::testing::wall_now() >= target);
    CHECK(graph.node_at(0).output(evaluation_time).value().checked_as<Int>() == Int{1});
}

TEST_CASE("real-time NodeScheduler delivers a wall-clock alarm that became due while scheduling")
{
    using namespace hgraph;

    // A polling node computes its next wall-clock boundary and then calls
    // schedule(); the wall clock may cross that boundary in between (the
    // boundary distance is arbitrarily small for interval polls). A due
    // alarm must deliver on the next evaluatable cycle — dropping it kills
    // the self-rescheduling chain (observed as the intermittent
    // test_sql_subscription_polling_reissues_rendered_request CI failure).
    constexpr int target_evaluations = 3;

    std::atomic_int eval_count{0};

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    NodeTypeMetaData schema;
    schema.display_name      = "due_wall_alarm_source";
    schema.output_schema     = ts_int;
    schema.node_kind         = NodeKind::PullSource;
    schema.schedule_on_start = true;
    schema.uses_scheduler    = true;

    NodeCallbacks callbacks;
    callbacks.evaluate = [&eval_count](const NodeView &view, DateTime evaluation_time) {
        ++eval_count;
        testing::set_output_value(view, evaluation_time, Int{eval_count.load()});
        if (eval_count.load() >= target_evaluations) { return; }
        const NodeScheduler scheduler{view.scheduler_state(), view.graph_value(),
                                      view.node_index(), evaluation_time,
                                      view.started(), view.evaluation_clock(),
                                      /*supports_wall_clock=*/true};
        // Aim just past now, then let the wall clock overtake the target
        // before schedule() re-reads it.
        const DateTime target = hgraph::testing::wall_now() + TimeDelta{200};
        std::this_thread::sleep_for(std::chrono::milliseconds{2});
        scheduler.schedule(target, "wall", /*on_wall_clock=*/true);
    };

    GraphBuilder graph_builder;
    graph_builder.add_node(NodeBuilder::native(std::move(schema), std::move(callbacks)));

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{5'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    CHECK(eval_count.load() == target_evaluations);
    // The chain never waits for end_time: each due alarm fires next cycle.
    CHECK(hgraph::testing::wall_now() - start_time < TimeDelta{4'000'000});
}

TEST_CASE("real-time executor stop request wakes a sleeping executor")
{
    using namespace hgraph;

    constexpr TimeDelta start_offset{10'000};
    constexpr TimeDelta delay{2'000'000};

    std::atomic_int eval_count{0};
    DateTime        observed_evaluation_time{MIN_DT};
    DateTime        observed_clock_evaluation_time{MIN_DT};
    DateTime        observed_clock_now{MIN_DT};

    GraphBuilder graph_builder;
    graph_builder.add_node(delayed_source(delay,
                                          &eval_count,
                                          &observed_evaluation_time,
                                          &observed_clock_evaluation_time,
                                          &observed_clock_now));

    const DateTime run_started = hgraph::testing::wall_now();
    const DateTime start_time  = run_started + start_offset;

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{5'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    hgraph::testing::AsyncGraphExecutorRun runner{view};

    std::this_thread::sleep_for(std::chrono::milliseconds{30});
    view.request_stop();
    runner.join();

    CHECK(eval_count.load() == 0);
    CHECK(hgraph::testing::wall_now() < run_started + TimeDelta{500'000});
}

TEST_CASE("real-time executor honours end_time on the wall clock under busy rescheduling")
{
    using namespace hgraph;

    // A node that re-schedules itself every MIN_TD while each cycle burns
    // real wall time is the shape of a permanently failing adaptor retry
    // loop: evaluation time advances one microsecond per cycle, so the
    // logical end_time bound alone would need hundreds of thousands of
    // cycles. Once the wall clock passes end_time, the bounded drain
    // (max_immediate_drain_cycles) must end the run.
    constexpr TimeDelta run_window{250'000};
    constexpr auto      cycle_cost = std::chrono::milliseconds{1};

    std::atomic_int eval_count{0};

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    NodeTypeMetaData schema;
    schema.display_name      = "busy_rescheduling_source";
    schema.output_schema     = ts_int;
    schema.node_kind         = NodeKind::PullSource;
    schema.schedule_on_start = true;

    NodeCallbacks callbacks;
    callbacks.evaluate = [&eval_count, cycle_cost](const NodeView &view, DateTime evaluation_time) {
        ++eval_count;
        // Burn a measured millisecond instead of sleeping. Windows commonly
        // rounds a 1 ms sleep to its ~15.6 ms scheduler quantum, which turns
        // the executor's 1024-cycle drain bound into a 16 second test without
        // changing the runtime behavior under test.
        const auto cycle_end = std::chrono::steady_clock::now() + cycle_cost;
        while (std::chrono::steady_clock::now() < cycle_end) {}
        testing::set_output_value(view, evaluation_time, Int{eval_count.load()});
        view.graph_value()->schedule_node(view.node_index(), evaluation_time + MIN_TD);
    };

    GraphBuilder graph_builder;
    graph_builder.add_node(NodeBuilder::native(std::move(schema), std::move(callbacks)));

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + run_window);

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    const TimeDelta elapsed = hgraph::testing::wall_now() - start_time;
    CHECK(eval_count.load() > 0);
    // Generous CI margin: the bounded drain returns after at most
    // run_window plus ~max_immediate_drain_cycles cycles at cycle_cost;
    // the starved executor needs run_window / MIN_TD cycles.
    CHECK(elapsed < TimeDelta{15'000'000});
}

TEST_CASE("real-time executor recursion guard trips without waiting for end_time")
{
    using namespace hgraph;

    // With the opt-in guard armed, a busy MIN_TD loop throws promptly during
    // the run window instead of spinning until the drain bound cuts it off.
    std::atomic_int eval_count{0};

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    NodeTypeMetaData schema;
    schema.display_name      = "busy_guarded_source";
    schema.output_schema     = ts_int;
    schema.node_kind         = NodeKind::PullSource;
    schema.schedule_on_start = true;

    NodeCallbacks callbacks;
    callbacks.evaluate = [&eval_count](const NodeView &view, DateTime evaluation_time) {
        ++eval_count;
        testing::set_output_value(view, evaluation_time, Int{eval_count.load()});
        view.graph_value()->schedule_node(view.node_index(), evaluation_time + MIN_TD);
    };

    GraphBuilder graph_builder;
    graph_builder.add_node(NodeBuilder::native(std::move(schema), std::move(callbacks)));

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{60'000'000})
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

    CHECK(message.find("busy_guarded_source") != std::string::npos);
    // Nowhere near the 60s end_time: the guard tripped after ~51 cycles.
    CHECK(hgraph::testing::wall_now() - start_time < TimeDelta{10'000'000});
}

TEST_CASE("real-time executor drains a lagging graph to its logical end_time")
{
    using namespace hgraph;

    // A graph whose cycles cost more wall time than their logical spacing
    // lags the wall clock; scheduled work must still evaluate at its
    // scheduled logical times after the wall clock passes end_time (the
    // ported wall-clock scheduler tests rely on late alarm delivery). Ticks
    // land at start, +50ms and +100ms; each costs ~80ms wall, so the wall
    // clock passes the 150ms end_time before the drain completes.
    constexpr TimeDelta step{50'000};
    constexpr TimeDelta run_window{150'000};
    constexpr auto      cycle_cost = std::chrono::milliseconds{80};

    std::atomic_int       eval_count{0};
    std::vector<DateTime> stamps;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    NodeTypeMetaData schema;
    schema.display_name      = "lagging_source";
    schema.output_schema     = ts_int;
    schema.node_kind         = NodeKind::PullSource;
    schema.schedule_on_start = true;

    NodeCallbacks callbacks;
    callbacks.evaluate = [&eval_count, &stamps, cycle_cost, step](const NodeView &view, DateTime evaluation_time) {
        ++eval_count;
        stamps.push_back(evaluation_time);
        std::this_thread::sleep_for(cycle_cost);
        testing::set_output_value(view, evaluation_time, Int{eval_count.load()});
        view.graph_value()->schedule_node(view.node_index(), evaluation_time + step);
    };

    GraphBuilder graph_builder;
    graph_builder.add_node(NodeBuilder::native(std::move(schema), std::move(callbacks)));

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + run_window);

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    REQUIRE(eval_count.load() == 3);
    CHECK(stamps == std::vector<DateTime>{start_time, start_time + step, start_time + step + step});
}

TEST_CASE("real-time executor evaluates root push queues after pending update signal")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_int);

    Int              observed_value{0};
    std::int32_t     sink_eval_count{0};
    PushSourceSender sender;

    GraphBuilder graph_builder;
    graph_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));
    graph_builder.add_node(hgraph::testing::recording_scalar_sink<Int>(
        *input_schema,
        *ts_int,
        observed_value,
        sink_eval_count));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    hgraph::testing::AsyncGraphExecutorRun runner{view};

    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    REQUIRE(sender.valid());
    sender.send(Int{42});
    runner.join();

    CHECK(sink_eval_count == 1);
    CHECK(observed_value == Int{42});
}

TEST_CASE("push source exposes pending work through the data-only inspection contract")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    PushSourceSender sender;
    GraphBuilder graph_builder;
    graph_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));

    const DateTime start_time = hgraph::testing::wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    GraphView graph = executor.view().graph();
    graph.start(start_time);
    REQUIRE(sender.valid());

    sender.send(Int{41});
    sender.send(Int{42});
    REQUIRE(graph.node_at(0).inspection_metrics().pending_items.has_value());
    CHECK(*graph.node_at(0).inspection_metrics().pending_items == 2);

    graph.node_at(0).evaluate(start_time);
    CHECK(*graph.node_at(0).inspection_metrics().pending_items == 1);
    graph.stop(start_time);
}

TEST_CASE("real-time executor evaluates scheduled root push source nodes without a pending queue signal")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_int);

    std::int32_t     source_eval_count{0};
    std::vector<Int> observed_values;

    GraphBuilder graph_builder;
    graph_builder.add_node(scheduled_push_source(*ts_int, source_eval_count));
    graph_builder.add_node(hgraph::testing::collecting_scalar_sink<Int>(
        *input_schema,
        *ts_int,
        observed_values,
        2));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    CHECK(source_eval_count == 2);
    REQUIRE(observed_values.size() == 2);
    CHECK(observed_values[0] == Int{0});
    CHECK(observed_values[1] == Int{1});
}

TEST_CASE("real-time push source drains multiple queued values across cycles")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_int);

    std::vector<Int> observed_values;
    PushSourceSender sender;

    GraphBuilder graph_builder;
    graph_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));
    graph_builder.add_node(hgraph::testing::collecting_scalar_sink<Int>(
        *input_schema,
        *ts_int,
        observed_values,
        2));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    hgraph::testing::AsyncGraphExecutorRun runner{view};

    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    REQUIRE(sender.valid());
    sender.send(Int{1});
    sender.send(Int{2});
    runner.join();

    REQUIRE(observed_values.size() == 2);
    CHECK(observed_values[0] == Int{1});
    CHECK(observed_values[1] == Int{2});
}

TEST_CASE("real-time push source applies queued collection deltas in order")
{
    using namespace hgraph;
    using namespace std::string_literals;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd_int  = registry.tsd(str_meta, ts_int);
    const auto *input_schema = hgraph::testing::single_input_schema(*tsd_int);

    std::vector<Value> observed_values;

    GraphBuilder graph_builder;
    graph_builder.add_node(make_push_source_node(
        *tsd_int,
        [](PushSourceSender sender) {
            sender.send(dict_delta<Str, TS<Int>>({{"a"s, Int{1}}, {"b"s, Int{2}}}));
            sender.send(dict_delta<Str, TS<Int>>({{"a"s, Int{3}}}, {"b"s}));
        }));

    NodeTypeMetaData sink_schema;
    sink_schema.display_name = "testing_collecting_value_sink";
    sink_schema.input_schema = input_schema;
    sink_schema.node_kind = NodeKind::Sink;
    NodeCallbacks sink_callbacks;
    sink_callbacks.evaluate = [&observed_values](const NodeView &view, DateTime evaluation_time) {
        auto       root   = view.input(evaluation_time);
        auto       bundle = root.as_bundle();
        const auto input  = bundle[0];
        observed_values.emplace_back(input.value());
        if (observed_values.size() == 2) { view.graph().executor().request_stop(); }
    };
    graph_builder.add_node(NodeBuilder::native(
        std::move(sink_schema),
        std::move(sink_callbacks),
        hgraph::testing::single_input_endpoint(*input_schema, *tsd_int)));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});
    auto executor = executor_builder.make_executor();
    executor.view().run();

    REQUIRE(observed_values.size() == 2);
    const auto first = observed_values[0].view().as_map();
    CHECK(first.at(Value{Str{"a"}}.view()).checked_as<Int>() == Int{1});
    CHECK(first.at(Value{Str{"b"}}.view()).checked_as<Int>() == Int{2});
    const auto second = observed_values[1].view().as_map();
    CHECK(second.at(Value{Str{"a"}}.view()).checked_as<Int>() == Int{3});
    CHECK_FALSE(second.contains(Value{Str{"b"}}.view()));
}

TEST_CASE("real-time push source moves external polymorphic keys into graph pools")
{
    using namespace hgraph;

    const auto schemas = push_polymorphic_schemas();
    REQUIRE(schemas.realization->inspect(schemas.base).representation ==
            GraphValueRepresentation::PooledUnion);

    const Value small = make_push_leaf(schemas.small, Int{1});
    const Value large = make_push_leaf(schemas.large, Int{2}, "large");
    const std::array<std::pair<const Value *, Int>, 2> initial{
        std::pair{&small, Int{10}}, std::pair{&large, Int{20}}};
    const std::array<std::pair<const Value *, Int>, 1> update{
        std::pair{&small, Int{30}}};
    const std::array<Value, 1> remove_large{large};
    const Value initial_delta = make_push_dict_delta(schemas, initial, {});
    const Value update_delta =
        make_push_dict_delta(schemas, update, remove_large);

    const auto *input_schema =
        hgraph::testing::single_input_schema(*schemas.dict);
    std::vector<Value> observed_values;
    PushSourceSender sender;

    GraphBuilder graph_builder;
    set_pooled_compound_scalar_storage(graph_builder.global_state());
    graph_builder.type_realization(schemas.realization);
    TypeRealizationScope realization_scope{schemas.realization.get()};
    graph_builder.add_node(
        hgraph::testing::capturing_push_source(*schemas.dict, sender));

    NodeTypeMetaData sink_schema;
    sink_schema.display_name = "testing_collecting_polymorphic_push_sink";
    sink_schema.input_schema = input_schema;
    sink_schema.node_kind = NodeKind::Sink;
    NodeCallbacks sink_callbacks;
    sink_callbacks.evaluate = [&observed_values](
                                  const NodeView &view,
                                  DateTime evaluation_time) {
        auto root = view.input(evaluation_time);
        auto bundle = root.as_bundle();
        observed_values.emplace_back(bundle[0].value());
        if (observed_values.size() == 2)
        {
            view.graph().executor().request_stop();
        }
    };
    graph_builder.add_node(NodeBuilder::native(
        std::move(sink_schema), std::move(sink_callbacks),
        hgraph::testing::single_input_endpoint(*input_schema, *schemas.dict)));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();
    {
        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(graph_builder))
            .mode(GraphExecutorMode::RealTime)
            .start_time(start_time)
            .end_time(start_time + TimeDelta{1'000'000});
        GraphExecutorValue executor = executor_builder.make_executor();
        auto view = executor.view();
        hgraph::testing::AsyncGraphExecutorRun runner{view};

        std::this_thread::sleep_for(std::chrono::milliseconds{20});
        REQUIRE(sender.valid());
        sender.send(initial_delta);
        sender.send(update_delta);
        runner.join();

        const auto pools = view.graph().compound_scalar_storage().inspect();
        REQUIRE(pools.leaf_pool_count >= 2);
        REQUIRE(pools.live_slot_count > 0);
    }

    REQUIRE(observed_values.size() == 2);
    const Value small_key = make_push_union(schemas, small);
    const Value large_key = make_push_union(schemas, large);
    const auto first = observed_values[0].view().as_map();
    REQUIRE(first.size() == 2);
    CHECK(first.at(small_key.view()).checked_as<Int>() == Int{10});
    CHECK(first.at(large_key.view()).checked_as<Int>() == Int{20});
    const auto second = observed_values[1].view().as_map();
    REQUIRE(second.size() == 1);
    CHECK(second.at(small_key.view()).checked_as<Int>() == Int{30});
    CHECK_FALSE(second.contains(large_key.view()));
}

TEST_CASE("real-time conflating push source emits only the latest pending value")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_int);

    Int          observed_value{0};
    std::int32_t sink_eval_count{0};

    GraphBuilder graph_builder;
    graph_builder.add_node(make_push_source_node(
        *ts_int,
        make_push_source_conflating_policy(*ts_int->value_schema),
        [](PushSourceSender sender) {
            sender.send(Int{1});
            sender.send(Int{2});
        }));
    graph_builder.add_node(hgraph::testing::recording_scalar_sink<Int>(
        *input_schema,
        *ts_int,
        observed_value,
        sink_eval_count));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    CHECK(sink_eval_count == 1);
    CHECK(observed_value == Int{2});
}

TEST_CASE("real-time conflating push source applies collection deltas before emitting")
{
    using namespace hgraph;
    using namespace std::string_literals;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd_int  = registry.tsd(str_meta, ts_int);
    const auto *input_schema = hgraph::testing::single_input_schema(*tsd_int);

    Value        observed_value;
    std::int32_t sink_eval_count{0};

    GraphBuilder graph_builder;
    graph_builder.add_node(make_push_source_node(
        *tsd_int,
        make_push_source_conflating_policy(*tsd_int->delta_value_schema),
        [](PushSourceSender sender) {
            sender.send(dict_delta<Str, TS<Int>>({{"a"s, Int{1}}}));
            sender.send(dict_delta<Str, TS<Int>>({{"b"s, Int{2}}}));
            sender.send(dict_delta<Str, TS<Int>>({{"a"s, Int{3}}}));
        }));
    graph_builder.add_node(hgraph::testing::recording_value_sink(
        *input_schema,
        *tsd_int,
        observed_value,
        sink_eval_count));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    REQUIRE(sink_eval_count == 1);
    const auto map = observed_value.view().as_map();
    REQUIRE(map.size() == 2);
    CHECK(map.at(Value{Str{"a"}}.view()).checked_as<Int>() == Int{3});
    CHECK(map.at(Value{Str{"b"}}.view()).checked_as<Int>() == Int{2});
}

TEST_CASE("push source policy validates output shape and sender payload schema separately")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *ts_int   = registry.ts(int_meta);

    CHECK_THROWS_AS(
        (void)make_push_source_node(*ts_int, PushSourcePolicy{}),
        std::invalid_argument);

    CHECK_THROWS_AS(
        (void)make_push_source_node(*ts_int, make_push_source_queue_policy(*str_meta)),
        std::invalid_argument);

    PushSourceSender sender;
    GraphBuilder graph_builder;
    graph_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    hgraph::testing::AsyncGraphExecutorRun runner{view};

    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    REQUIRE(sender.valid());
    CHECK_THROWS_AS(sender.send(Str{"wrong-schema"}), std::invalid_argument);
    view.request_stop();
    runner.join();
}

TEST_CASE("push source sender ignores values after graph shutdown")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    const auto check_policy = [&](PushSourcePolicy policy) {
        PushSourceSender sender;
        GraphBuilder graph_builder;
        graph_builder.add_node(make_push_source_node(
            *ts_int,
            std::move(policy),
            [&sender](PushSourceSender live_sender) { sender = std::move(live_sender); }));
        NodeTypeMetaData stop_sender_schema;
        stop_sender_schema.display_name = "send_during_stop";
        NodeCallbacks stop_sender_callbacks;
        stop_sender_callbacks.stop = [&sender](const NodeView &, DateTime) { sender.send(Int{41}); };
        graph_builder.add_node(NodeBuilder::native(
            std::move(stop_sender_schema), std::move(stop_sender_callbacks)));

        const DateTime start_time = hgraph::testing::wall_now();
        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(graph_builder))
            .mode(GraphExecutorMode::RealTime)
            .start_time(start_time)
            .end_time(start_time + TimeDelta{1'000'000});

        auto executor = executor_builder.make_executor();
        auto view = executor.view();
        hgraph::testing::AsyncGraphExecutorRun runner{view};

        std::this_thread::sleep_for(std::chrono::milliseconds{20});
        REQUIRE(sender.valid());
        view.request_stop();
        runner.join();

        CHECK_NOTHROW(sender.send(Int{42}));
    };

    SECTION("queue policy") { check_policy(make_push_source_queue_policy(*int_meta)); }
    SECTION("conflating policy") { check_policy(make_push_source_conflating_policy(*int_meta)); }
}

TEST_CASE("push source nodes require a real-time root graph executor")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    PushSourceSender sender;
    GraphBuilder graph_builder;
    graph_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::Simulation)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{1});

    CHECK_THROWS_AS(executor_builder.make_executor(), std::invalid_argument);

    NodeTypeMetaData parent_schema;
    parent_schema.display_name = "test_nested_parent";
    NodeValue parent = NodeBuilder::native(std::move(parent_schema)).make_node();
    NodeView  parent_view = parent.view();

    GraphBuilder nested_builder;
    nested_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));

    CHECK_THROWS_AS(nested_builder.make_nested_graph(parent_view.pointer()),
                    std::invalid_argument);

}
