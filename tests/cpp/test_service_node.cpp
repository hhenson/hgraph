#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>

#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_output/set_view.h>

namespace
{
    using namespace hgraph;

    struct ConstantKey
    {
        static constexpr auto name              = "constant_key";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<std::int32_t>> out)
        {
            out.set(7);
        }
    };

    struct SwitchingKey
    {
        static constexpr auto name              = "switching_key";
        static constexpr bool schedule_on_start = true;

        static void eval(NodeScheduler sched, State<std::int32_t> count, Out<TS<std::int32_t>> out)
        {
            const auto next = count.get() + 1;
            count.set(next);
            out.set(next == 1 ? 7 : 8);
            if (next == 1) { sched.schedule(MIN_TD); }
        }
    };
}  // namespace

TEST_CASE("service subscription keys publish captured keys on the next cycle")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto  path     = std::string{"svc://prices/subscriptions"};

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<ConstantKey>());
    builder.add_node(make_subscription_key_source_node(path, *int_meta));
    builder.add_node(make_subscription_key_capture_node(path, *int_meta));
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 2, .target_path = {0}});
    builder.add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {1}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;
    const auto             t2   = t1 + MIN_TD;

    view.start(t1);
    view.evaluate(t1);
    CHECK_FALSE(view.node_at(1).output(t1).modified());

    view.evaluate(t2);
    auto output_view = view.node_at(1).output(t2);
    auto output      = output_view.as_set();
    REQUIRE(output.modified());
    CHECK(output.contains(Value{std::int32_t{7}}.view()));
}

TEST_CASE("service subscription keys are reference counted across captures")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto  path     = std::string{"svc://prices/ref-counted-subscriptions"};

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<SwitchingKey>());
    builder.add_node(NodeBuilder{}.implementation<ConstantKey>());
    builder.add_node(make_subscription_key_source_node(path, *int_meta));
    builder.add_node(make_subscription_key_capture_node(path, *int_meta));
    builder.add_node(make_subscription_key_capture_node(path, *int_meta));
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 3, .target_path = {0}});
    builder.add_edge(GraphEdge{.source_node = 2, .target_node = 3, .target_path = {1}});
    builder.add_edge(GraphEdge{.source_node = 1, .target_node = 4, .target_path = {0}});
    builder.add_edge(GraphEdge{.source_node = 2, .target_node = 4, .target_path = {1}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;
    const auto             t2   = t1 + MIN_TD;
    const auto             t3   = t2 + MIN_TD;

    view.start(t1);
    view.evaluate(t1);

    view.evaluate(t2);
    auto first_view = view.node_at(2).output(t2);
    auto first      = first_view.as_set();
    REQUIRE(first.modified());
    CHECK(first.contains(Value{std::int32_t{7}}.view()));
    CHECK_FALSE(first.contains(Value{std::int32_t{8}}.view()));

    view.evaluate(t3);
    auto second_view = view.node_at(2).output(t3);
    auto second      = second_view.as_set();
    REQUIRE(second.modified());
    CHECK(second.contains(Value{std::int32_t{7}}.view()));
    CHECK(second.contains(Value{std::int32_t{8}}.view()));
}

TEST_CASE("request input teardown supersedes a pending same-time value")
{
    using namespace hgraph;

    auto       &registry       = TypeRegistry::instance();
    const auto *request_schema = registry.ts(
        registry.register_scalar<std::int32_t>("int32"));
    const auto  path = std::string{"svc://requests/same-time-teardown"};

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<ConstantKey>());
    builder.add_node(make_request_input_source_node(path, *request_schema));
    builder.add_node(make_request_id_source_node());
    builder.add_node(make_request_input_capture_node(path, *request_schema));
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 3, .target_path = {0}});
    builder.add_edge(GraphEdge{.source_node = 1, .target_node = 3, .target_path = {1}});
    builder.add_edge(GraphEdge{.source_node = 2, .target_node = 3, .target_path = {2}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;
    const auto             t2   = t1 + MIN_TD;

    view.start(t1);
    view.evaluate(t1);
    const Int request_id = view.node_at(2).output(t1).value().checked_as<Int>();

    // Dynamic nested clients can start and stop during one parent evaluation.
    // Their stop must replace the value captured for the same observed time.
    view.node_at(3).stop(t1);
    view.evaluate(t2);

    auto requests = view.node_at(1).output(t2);
    CHECK_FALSE(requests.as_dict().contains(Value{request_id}.view()));
}
