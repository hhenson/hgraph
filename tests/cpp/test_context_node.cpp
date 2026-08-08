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

    struct ContextSource
    {
        static constexpr auto name              = "context_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<std::int32_t>> out)
        {
            out.set(42);
        }
    };

    struct ContextConsumer
    {
        static constexpr auto name = "context_consumer";

        static void eval(In<"value", TS<std::int32_t>> value, Out<TS<std::int32_t>> out)
        {
            out.set(value.value());
        }
    };
}  // namespace

TEST_CASE("context keys follow the shared namespace")
{
    using namespace hgraph;

    CHECK(context_output_key("root", "price") == "context-root-price");
    CHECK_THROWS_AS(context_output_key("", "price"), std::invalid_argument);
    CHECK_THROWS_AS(context_output_key("root", ""), std::invalid_argument);
}

TEST_CASE("context source publishes the reference captured during start")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto  key      = context_output_key("root", "captured");

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<ContextSource>());
    builder.add_node(make_context_source_node(key, *ts_int));
    builder.add_node(make_context_capture_node(key, *ts_int));
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
    CHECK_FALSE(view.global_state().contains(key));

    view.stop();
    auto        state_view = view.node_at(1).state();
    const auto &cleared    = state_view.checked_as<TimeSeriesReference>();
    CHECK(cleared.is_empty());
    CHECK(cleared.target_schema() == nullptr);
}

TEST_CASE("context source can feed a dereferenced consumer through normal graph notification")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto  key      = context_output_key("root", "shared");

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<ContextSource>());
    builder.add_node(make_context_source_node(key, *ts_int));
    builder.add_node(make_context_capture_node(key, *ts_int));
    builder.add_node(NodeBuilder{}.implementation<ContextConsumer>());
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 2, .target_path = {0}});
    builder.add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {1}});
    builder.add_edge(GraphEdge{.source_node = 1, .target_node = 3, .target_path = {0}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    view.evaluate(t1);

    CHECK(view.node_at(3).output(t1).valid());
    CHECK(view.node_at(3).output(t1).value().checked_as<std::int32_t>() == 42);
}

TEST_CASE("context source fails when the captured reference is absent")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    GraphBuilder builder;
    builder.add_node(make_context_source_node(context_output_key("root", "missing"), *ts_int));

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    CHECK_THROWS_AS(view.evaluate(t1), std::runtime_error);
}
