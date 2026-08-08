// Execution/lifecycle observer tests (runtime/lifecycle_observer.h).
//
// Verifies the "Lifecycle Observers" design record in
// docs/source/developer_guide/architecture.rst: hook ordering (start in rank
// order, stop in reverse-rank order, before/after graph+node evaluation
// brackets), registration-order fan-out to multiple observers, the resolved
// exception-safety rules (best-effort "after" notifications except for
// node-start, which is plain sequential), the runtime add/remove API
// (including reentrant self-removal), and that a single registration on the
// root executor observes nested graphs and their nodes too.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;

    struct LogEntry
    {
        std::string observer;
        std::string event;
        std::string subject;

        [[nodiscard]] std::string to_string() const { return observer + ":" + event + ":" + subject; }
    };

    [[nodiscard]] std::string graph_id(const GraphView &graph)
    {
        const auto *schema = graph.schema();
        const char *name   = (schema != nullptr && schema->display_name != nullptr) ? schema->display_name : "";
        return std::string("graph:") + name;
    }

    [[nodiscard]] std::string node_id(const NodeView &node)
    {
        return graph_id(node.graph()) + "/node:" + std::to_string(node.node_index());
    }

    /**
     * Appends one entry per notification to a shared log. Several instances
     * sharing one log, tagged differently, let a test verify registration-order
     * fan-out across multiple observers as well as per-observer sequencing.
     */
    class RecordingObserver : public LifecycleObserver
    {
      public:
        RecordingObserver(std::string tag, std::vector<LogEntry> &log) : tag_(std::move(tag)), log_(&log) {}

        void on_before_start_graph(const GraphView &graph) override { record("before_start_graph", graph_id(graph)); }
        void on_after_start_graph(const GraphView &graph) override { record("after_start_graph", graph_id(graph)); }
        void on_before_start_node(const NodeView &node) override { record("before_start_node", node_id(node)); }
        void on_after_start_node(const NodeView &node) override { record("after_start_node", node_id(node)); }

        void on_before_graph_evaluation(const GraphView &graph) override
        {
            record("before_graph_evaluation", graph_id(graph));
        }
        void on_after_graph_evaluation(const GraphView &graph) override
        {
            record("after_graph_evaluation", graph_id(graph));
        }
        void on_before_node_evaluation(const NodeView &node) override { record("before_node_evaluation", node_id(node)); }
        void on_after_node_evaluation(const NodeView &node) override { record("after_node_evaluation", node_id(node)); }
        void on_after_graph_push_nodes_evaluation(const GraphView &graph) override
        {
            record("after_graph_push_nodes_evaluation", graph_id(graph));
        }

        void on_before_stop_node(const NodeView &node) override { record("before_stop_node", node_id(node)); }
        void on_after_stop_node(const NodeView &node) override { record("after_stop_node", node_id(node)); }
        void on_before_stop_graph(const GraphView &graph) override { record("before_stop_graph", graph_id(graph)); }
        void on_after_stop_graph(const GraphView &graph) override { record("after_stop_graph", graph_id(graph)); }

      private:
        void record(std::string event, std::string subject) { log_->push_back({tag_, std::move(event), std::move(subject)}); }

        std::string            tag_;
        std::vector<LogEntry> *log_;
    };

    [[nodiscard]] std::vector<std::string> as_strings(const std::vector<LogEntry> &log)
    {
        std::vector<std::string> out;
        out.reserve(log.size());
        for (const auto &entry : log) { out.push_back(entry.to_string()); }
        return out;
    }

    [[nodiscard]] std::size_t count_events(const std::vector<LogEntry> &log, std::string_view event)
    {
        std::size_t count = 0;
        for (const auto &entry : log)
        {
            if (entry.event == event) { ++count; }
        }
        return count;
    }

    // ---- fixtures: a plain two-node source -> add_one graph, built directly
    // from NodeBuilder::native (no wiring layer needed for start/stop/evaluate
    // sequencing tests). ----

    NodeBuilder trivial_source(const TSValueTypeMetaData *ts_int, Int value)
    {
        NodeTypeMetaData schema;
        schema.display_name      = "source";
        schema.output_schema     = ts_int;
        schema.node_kind         = NodeKind::PullSource;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [value](const NodeView &view, DateTime evaluation_time) {
            testing::set_output_value(view, evaluation_time, value);
        };
        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    NodeBuilder trivial_add_one(const TSValueTypeMetaData *input_schema, const TSValueTypeMetaData *ts_int)
    {
        NodeTypeMetaData schema;
        schema.display_name  = "add_one";
        schema.input_schema  = input_schema;
        schema.output_schema = ts_int;
        schema.node_kind     = NodeKind::Compute;

        NodeCallbacks callbacks;
        callbacks.evaluate = [](const NodeView &view, DateTime evaluation_time) {
            TSInputView        input  = view.input(evaluation_time);
            auto               bundle = input.as_bundle();
            const Int          value  = bundle[0].value().checked_as<Int>();
            testing::set_output_value(view, evaluation_time, value + 1);
        };

        auto endpoint = TSEndpointSchema::non_peered(input_schema, {TSEndpointSchema::peered(ts_int)});
        return NodeBuilder::native(std::move(schema), std::move(callbacks), std::move(endpoint));
    }

    // ---- fixture: a self-rescheduling multi-cycle source (mirrors
    // test_simulation_execution.cpp's TickingSource) so evaluation pairing can
    // be checked across more than one cycle. ----

    struct LifecycleTickingSource
    {
        static constexpr auto name              = "lifecycle_ticking_source";
        static constexpr bool schedule_on_start = true;
        static void           eval(NodeScheduler sched, Scalar<"count", Int> count,
                                    State<Int> emitted, Out<TS<Int>> out)
        {
            const Int n = emitted.get();
            out.set(n);
            emitted.set(n + 1);
            if (n + 1 < count.value()) { sched.schedule(MIN_TD); }
        }
    };

    struct LifecycleTickAddOne
    {
        static constexpr auto name = "lifecycle_tick_add_one";
        static void           eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
    };

    struct LifecycleTickGraph
    {
        static constexpr auto name = "lifecycle_tick_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<LifecycleTickingSource>(w, 3);
            wire<LifecycleTickAddOne>(w, src);
        }
    };

    // ---- fixture: a nested sub-graph, to prove a single root-level
    // registration also observes nested graphs and their nodes. ----

    struct LifecycleNestedAddOneSubGraph
    {
        static constexpr auto name = "lifecycle_nested_add_one_subgraph";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> in)
        {
            using namespace hgraph::stdlib::syntax;
            return (in + Int{1}).as<TS<Int>>();
        }
    };

    struct LifecycleNestedGraph
    {
        static constexpr auto name = "lifecycle_nested_graph";
        static void           compose(Wiring &w)
        {
            auto in  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto out = nested_<LifecycleNestedAddOneSubGraph>(w, in);
            wire<stdlib::dense_record_impl>(w, out, Str{"out"});
        }
    };

    NodeBuilder throwing_source(int &stop_calls)
    {
        NodeTypeMetaData schema;
        schema.display_name = "throwing_source";
        schema.node_kind = NodeKind::PullSource;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [](const NodeView &, DateTime) {
            throw std::runtime_error("evaluation failed");
        };
        callbacks.stop = [&stop_calls](const NodeView &, DateTime) {
            ++stop_calls;
        };
        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    NodeBuilder scheduled_push_source(int &evaluation_calls)
    {
        NodeTypeMetaData schema;
        schema.display_name      = "scheduled_push_source";
        schema.node_kind         = NodeKind::PushSource;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [&evaluation_calls](const NodeView &, DateTime) {
            ++evaluation_calls;
        };
        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    NodeBuilder two_cycle_pull_source(int &evaluation_calls)
    {
        NodeTypeMetaData schema;
        schema.display_name      = "two_cycle_pull_source";
        schema.node_kind         = NodeKind::PullSource;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [&evaluation_calls](const NodeView &node,
                                                  DateTime evaluation_time) {
            ++evaluation_calls;
            if (evaluation_calls == 1)
            {
                node.graph_value()->schedule_node(
                    node.node_index(), evaluation_time + MIN_TD);
            }
        };
        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }
}  // namespace

TEST_CASE("lifecycle observers: registration order and start/stop/evaluate sequencing")
{
    using namespace hgraph;

    stdlib::register_standard_operators();
    auto       &registry     = TypeRegistry::instance();
    const auto *int_meta     = registry.scalar_type<Int>().schema();
    const auto *ts_int       = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("LifecycleSeqInput", {{"value", ts_int}});

    GraphBuilder graph_builder;
    graph_builder.add_node(trivial_source(ts_int, Int{41}))
        .add_node(trivial_add_one(input_schema, ts_int))
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    std::vector<LogEntry> log;
    RecordingObserver      obs1("obs1", log);
    RecordingObserver      obs2("obs2", log);

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .add_lifecycle_observer(&obs1)
        .add_lifecycle_observer(&obs2)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{1});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    const std::string g  = "graph:";  // display_name left unset on this raw GraphBuilder
    const std::string n0 = g + "/node:0";
    const std::string n1 = g + "/node:1";

    const std::vector<std::string> expected = {
        "obs1:before_start_graph:" + g,
        "obs2:before_start_graph:" + g,
        "obs1:before_start_node:" + n0,
        "obs2:before_start_node:" + n0,
        "obs1:after_start_node:" + n0,
        "obs2:after_start_node:" + n0,
        "obs1:before_start_node:" + n1,
        "obs2:before_start_node:" + n1,
        "obs1:after_start_node:" + n1,
        "obs2:after_start_node:" + n1,
        "obs1:after_start_graph:" + g,
        "obs2:after_start_graph:" + g,
        "obs1:before_graph_evaluation:" + g,
        "obs2:before_graph_evaluation:" + g,
        "obs1:before_node_evaluation:" + n0,
        "obs2:before_node_evaluation:" + n0,
        "obs1:after_node_evaluation:" + n0,
        "obs2:after_node_evaluation:" + n0,
        "obs1:before_node_evaluation:" + n1,
        "obs2:before_node_evaluation:" + n1,
        "obs1:after_node_evaluation:" + n1,
        "obs2:after_node_evaluation:" + n1,
        "obs1:after_graph_evaluation:" + g,
        "obs2:after_graph_evaluation:" + g,
        "obs1:before_stop_graph:" + g,
        "obs2:before_stop_graph:" + g,
        "obs1:before_stop_node:" + n1,
        "obs2:before_stop_node:" + n1,
        "obs1:after_stop_node:" + n1,
        "obs2:after_stop_node:" + n1,
        "obs1:before_stop_node:" + n0,
        "obs2:before_stop_node:" + n0,
        "obs1:after_stop_node:" + n0,
        "obs2:after_stop_node:" + n0,
        "obs1:after_stop_graph:" + g,
        "obs2:after_stop_graph:" + g,
    };

    CHECK(as_strings(log) == expected);
}

TEST_CASE("lifecycle observers: before/after graph and node evaluation bracket each cycle")
{
    using namespace hgraph;

    stdlib::register_standard_operators();

    GraphBuilder graph_builder = build_graph<LifecycleTickGraph>();

    std::vector<LogEntry> log;
    RecordingObserver      obs("obs", log);

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .add_lifecycle_observer(&obs)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{10});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    // Three cycles: the source re-schedules itself twice more after the first.
    CHECK(count_events(log, "before_graph_evaluation") == 3);
    CHECK(count_events(log, "after_graph_evaluation") == 3);

    // Both nodes (source + add_one) are re-evaluated every cycle via
    // notification-driven scheduling: 3 cycles * 2 nodes = 6.
    CHECK(count_events(log, "before_node_evaluation") == 6);
    CHECK(count_events(log, "after_node_evaluation") == 6);
}

TEST_CASE("lifecycle observers: a node evaluation exception still fires matching after-notifications")
{
    using namespace hgraph;

    NodeTypeMetaData schema;
    schema.display_name      = "throwing_eval";
    schema.node_kind         = NodeKind::PullSource;
    schema.schedule_on_start = true;

    NodeCallbacks callbacks;
    callbacks.evaluate = [](const NodeView &, DateTime) { throw std::runtime_error("boom"); };

    GraphBuilder graph_builder;
    graph_builder.add_node(NodeBuilder::native(std::move(schema), std::move(callbacks)));

    std::vector<LogEntry> log;
    RecordingObserver      obs("obs", log);

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .add_lifecycle_observer(&obs)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{1});

    GraphExecutorValue executor = executor_builder.make_executor();
    REQUIRE_THROWS(executor.view().run());

    CHECK(count_events(log, "before_node_evaluation") == 1);
    CHECK(count_events(log, "after_node_evaluation") == 1);  // best-effort: fires despite the throw
    CHECK(count_events(log, "before_graph_evaluation") == 1);
    CHECK(count_events(log, "after_graph_evaluation") == 1);  // best-effort: fires despite the throw
}

TEST_CASE("lifecycle observers: a node's own start exception skips its after-start notification")
{
    using namespace hgraph;

    NodeTypeMetaData okSchema;
    okSchema.display_name = "ok_node";

    NodeTypeMetaData failSchema;
    failSchema.display_name = "failing_start_node";
    NodeCallbacks failCallbacks;
    failCallbacks.start = [](const NodeView &, DateTime) { throw std::runtime_error("start failed"); };

    GraphBuilder graph_builder;
    graph_builder.add_node(NodeBuilder::native(std::move(okSchema)))
        .add_node(NodeBuilder::native(std::move(failSchema), std::move(failCallbacks)));

    std::vector<LogEntry> log;
    RecordingObserver      obs("obs", log);

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .add_lifecycle_observer(&obs)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{1});

    GraphExecutorValue executor = executor_builder.make_executor();
    REQUIRE_THROWS(executor.view().run());

    // Both nodes' starts were attempted, but only the successful one (node 0)
    // gets a matching after-start notification.
    CHECK(count_events(log, "before_start_node") == 2);
    CHECK(count_events(log, "after_start_node") == 1);

    // Node 0 was rolled back (stopped) when node 1's start failed, through the
    // normal before/after-stop-node pair.
    CHECK(count_events(log, "before_stop_node") == 1);
    CHECK(count_events(log, "after_stop_node") == 1);

    // The graph itself never finished starting: no after-start-graph, and
    // since graph.start() threw, the executor's run loop never reaches
    // graph.stop() either.
    CHECK(count_events(log, "before_start_graph") == 1);
    CHECK(count_events(log, "after_start_graph") == 0);
    CHECK(count_events(log, "before_stop_graph") == 0);
    CHECK(count_events(log, "after_stop_graph") == 0);
}

TEST_CASE("lifecycle observers: a node's stop exception still fires its own and the graph's after-stop")
{
    using namespace hgraph;

    NodeTypeMetaData okSchema;
    okSchema.display_name = "ok_node";

    NodeTypeMetaData failSchema;
    failSchema.display_name = "failing_stop_node";
    NodeCallbacks failCallbacks;
    failCallbacks.stop = [](const NodeView &, DateTime) { throw std::runtime_error("stop failed"); };

    GraphBuilder graph_builder;
    graph_builder.add_node(NodeBuilder::native(std::move(okSchema)))
        .add_node(NodeBuilder::native(std::move(failSchema), std::move(failCallbacks)));

    std::vector<LogEntry> log;
    RecordingObserver      obs("obs", log);

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .add_lifecycle_observer(&obs)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{1});

    GraphExecutorValue executor = executor_builder.make_executor();
    REQUIRE_THROWS(executor.view().run());

    // Both nodes get a before/after-stop pair despite node 1's stop() throwing
    // (FirstExceptionRecorder defers the throw; the after-stop scope-exit
    // fires regardless).
    CHECK(count_events(log, "before_stop_node") == 2);
    CHECK(count_events(log, "after_stop_node") == 2);

    // The graph's own after-stop fires too, before the deferred exception is
    // rethrown at the very end of stop_impl.
    CHECK(count_events(log, "after_stop_graph") == 1);
}

TEST_CASE("lifecycle observers: an observer may remove itself from within its own callback")
{
    using namespace hgraph;

    struct SelfRemovingObserver : LifecycleObserver
    {
        LifecycleObserverList *list = nullptr;
        int                     calls = 0;

        void on_before_start_graph(const GraphView &) override
        {
            ++calls;
            list->remove(this);
        }
    };

    NodeTypeMetaData schema;
    schema.display_name = "solo_node";

    GraphBuilder graph_builder;
    graph_builder.add_node(NodeBuilder::native(std::move(schema)));

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{1});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto                view = executor.view();

    SelfRemovingObserver obs;
    obs.list = &view.lifecycle_observers();
    view.lifecycle_observers().add(&obs);

    view.run();

    // Removed itself on the very first notification: no later hook fired.
    CHECK(obs.calls == 1);
    CHECK(view.lifecycle_observers().empty());
}

TEST_CASE("lifecycle observers: add/remove via the executor view, and the root graph shares the same list")
{
    using namespace hgraph;

    NodeTypeMetaData schema;
    schema.display_name = "solo_node";

    GraphBuilder graph_builder;
    graph_builder.add_node(NodeBuilder::native(std::move(schema)));

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{1});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto                view = executor.view();

    CHECK(view.lifecycle_observers().empty());

    std::vector<LogEntry> log;
    RecordingObserver      obs("dyn", log);
    view.lifecycle_observers().add(&obs);
    CHECK_FALSE(view.lifecycle_observers().empty());

    // The root graph's accessor reaches the SAME executor-owned list.
    CHECK_FALSE(view.graph().lifecycle_observers().empty());

    view.lifecycle_observers().remove(&obs);
    CHECK(view.lifecycle_observers().empty());
}

TEST_CASE("lifecycle observers: a single executor-level registration observes nested graphs and their nodes")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    stdlib::register_standard_operators();

    GraphBuilder graph_builder = build_graph<LifecycleNestedGraph>();
    set_replay_values<Int>(graph_builder.global_state(), "in", testing::values<Int>(1, 2, 3));

    std::vector<LogEntry> log;
    RecordingObserver      obs("obs", log);

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .add_lifecycle_observer(&obs)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{10});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    std::set<std::string> evaluated_graphs;
    std::set<std::string> evaluated_nodes;
    for (const auto &entry : log)
    {
        if (entry.event == "before_graph_evaluation") { evaluated_graphs.insert(entry.subject); }
        if (entry.event == "before_node_evaluation") { evaluated_nodes.insert(entry.subject); }
    }

    // The root graph AND the nested sub-graph both evaluated, both observed
    // through the single registration on the root executor.
    CHECK(evaluated_graphs.size() >= 2);
    // Distinct graph-qualified node ids: at least replay + record (root) plus
    // the nested sub-graph's own node(s).
    CHECK(evaluated_nodes.size() >= 3);

    // Start/stop notifications reached the nested graph too, not just evaluation.
    CHECK(count_events(log, "before_start_graph") >= 2);
    CHECK(count_events(log, "before_stop_graph") >= 2);
}

TEST_CASE("graph executor cleanup policy controls stop during evaluation failure")
{
    using namespace hgraph;

    const auto run = [](bool cleanup_on_error, int &stop_calls) {
        GraphBuilder graph;
        graph.add_node(throwing_source(stop_calls));

        GraphExecutorBuilder builder;
        builder.graph_builder(std::move(graph))
            .start_time(MIN_ST)
            .end_time(MIN_ST + TimeDelta{2})
            .cleanup_on_error(cleanup_on_error);

        GraphExecutorValue executor = builder.make_executor();
        CHECK(executor.view().cleanup_on_error() == cleanup_on_error);
        CHECK_THROWS(executor.view().run());
        CHECK(executor.view().graph().started() == !cleanup_on_error);
        CHECK(stop_calls == (cleanup_on_error ? 1 : 0));
    };

    int cleaned_stop_calls = 0;
    run(true, cleaned_stop_calls);
    CHECK(cleaned_stop_calls == 1);

    int retained_stop_calls = 0;
    run(false, retained_stop_calls);
    // Executor destruction remains the final ownership boundary even when
    // immediate error cleanup is disabled.
    CHECK(retained_stop_calls == 1);
}

TEST_CASE("lifecycle observers: push phase notification requires an evaluated push phase")
{
    using namespace hgraph;

    int push_evaluations = 0;
    int pull_evaluations = 0;
    GraphBuilder graph;
    graph.add_node(scheduled_push_source(push_evaluations));
    graph.add_node(two_cycle_pull_source(pull_evaluations));

    std::vector<LogEntry> log;
    RecordingObserver observer{"observer", log};
    GraphExecutorBuilder builder;
    builder.graph_builder(std::move(graph))
        .mode(GraphExecutorMode::RealTime)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{3})
        .add_lifecycle_observer(&observer);

    GraphExecutorValue executor = builder.make_executor();
    GraphView graph_view = executor.view().graph();
    graph_view.start(MIN_ST);
    REQUIRE(graph_view.evaluate(MIN_ST));
    REQUIRE(graph_view.evaluate(MIN_ST + MIN_TD));
    graph_view.stop();

    CHECK(push_evaluations == 1);
    CHECK(pull_evaluations == 2);
    CHECK(count_events(log, "after_graph_push_nodes_evaluation") == 1);
}
