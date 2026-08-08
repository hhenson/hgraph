#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>

#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series_reference.h>

namespace
{
    using namespace hgraph;

    struct SharedOutputSource
    {
        static constexpr auto name              = "shared_output_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<std::int32_t>> out)
        {
            out.set(101);
        }
    };

    struct SharedOutputTwoTickSource
    {
        static constexpr auto name              = "shared_output_two_tick_source";
        static constexpr bool schedule_on_start = true;

        static void eval(NodeScheduler sched, State<std::int32_t> count, Out<TS<std::int32_t>> out)
        {
            const auto next = count.get() + 1;
            count.set(next);
            out.set(100 + next);
            if (next == 1) { sched.schedule(MIN_TD); }
        }
    };

    struct SharedOutputConsumer
    {
        static constexpr auto name = "shared_output_consumer";

        static void eval(In<"value", TS<std::int32_t>> value, Out<TS<std::int32_t>> out)
        {
            out.set(value.value());
        }
    };
}  // namespace

TEST_CASE("shared output keys follow the Python namespace")
{
    using namespace hgraph;

    CHECK(output_key("svc://x/out") == "svc://x/out");
    CHECK_THROWS_AS(output_key(""), std::invalid_argument);
}

TEST_CASE("shared output source publishes the reference captured during start")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto  path     = std::string{"svc://prices/to_graph"};

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<SharedOutputSource>());
    builder.add_node(make_shared_output_source_node(path, *ts_int));
    builder.add_node(make_shared_output_capture_node(path, *ts_int));
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 2, .target_path = {0}});
    builder.add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {1}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    view.evaluate(t1);

    REQUIRE(view.node_at(1).output(t1).modified());
    const auto published = view.node_at(1).output(t1).value().checked_as<TimeSeriesReference>();
    CHECK(published == TimeSeriesReference{view.node_at(0).output(t1)});
    CHECK_FALSE(view.global_state().contains(path));

    view.stop();
    auto        state_view = view.node_at(1).state();
    const auto &cleared    = state_view.checked_as<TimeSeriesReference>();
    CHECK(cleared.is_empty());
    CHECK(cleared.target_schema() == nullptr);
}

TEST_CASE("shared output source can feed a dereferenced consumer through normal graph notification")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto  path     = std::string{"svc://prices/out"};

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<SharedOutputSource>());
    builder.add_node(make_shared_output_source_node(path, *ts_int));
    builder.add_node(make_shared_output_capture_node(path, *ts_int));
    builder.add_node(NodeBuilder{}.implementation<SharedOutputConsumer>());
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 2, .target_path = {0}});
    builder.add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {1}});
    builder.add_edge(GraphEdge{.source_node = 1, .target_node = 3, .target_path = {0}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    view.evaluate(t1);

    CHECK(view.node_at(3).output(t1).valid());
    CHECK(view.node_at(3).output(t1).value().checked_as<std::int32_t>() == 101);
}

TEST_CASE("shared output strict source fails when missing")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    GraphBuilder builder;
    builder.add_node(make_shared_output_source_node("svc://missing/out", *ts_int));

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    CHECK_THROWS_AS(view.evaluate(t1), std::runtime_error);
}

TEST_CASE("shared output non-strict source skips a missing reference")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    GraphBuilder builder;
    builder.add_node(make_shared_output_source_node("svc://optional/out", *ts_int, false));

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    CHECK_NOTHROW(view.evaluate(t1));
    CHECK_FALSE(view.node_at(0).output(t1).valid());
}

TEST_CASE("shared output capture does not republish the same reference on target value ticks")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto  path     = std::string{"svc://prices/refreshing"};

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<SharedOutputTwoTickSource>());
    builder.add_node(make_shared_output_source_node(path, *ts_int));
    builder.add_node(make_shared_output_capture_node(path, *ts_int));
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 2, .target_path = {0}});
    builder.add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {1}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;
    const auto             t2   = t1 + MIN_TD;

    view.start(t1);
    view.evaluate(t1);
    auto capture_input = view.node_at(2).input(t1);
    auto capture_bundle = capture_input.as_bundle();
    CHECK_FALSE(capture_bundle.at(0).active());
    REQUIRE(view.node_at(1).output(t1).modified());
    CHECK(view.node_at(1).output(t1).value().checked_as<TimeSeriesReference>() ==
          TimeSeriesReference{view.node_at(0).output(t1)});

    view.evaluate(t2);
    CHECK_FALSE(view.node_at(1).output(t2).modified());
    CHECK(view.node_at(0).output(t2).modified());
}
