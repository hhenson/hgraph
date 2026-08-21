#include <hgraph/lib/std/standard_types.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/type_resolution.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <thread>
#include <vector>

namespace
{
    using namespace hgraph;

    [[nodiscard]] NodeBuilder collecting_tuple_sink(const TSValueTypeMetaData &input_schema,
                                                    const TSValueTypeMetaData &input_ts,
                                                    std::vector<std::vector<Int>> &observed,
                                                    std::size_t stop_after_count = 1)
    {
        NodeTypeMetaData schema;
        schema.display_name = "collecting_tuple_sink";
        schema.input_schema = &input_schema;
        schema.node_kind = NodeKind::Sink;

        NodeCallbacks callbacks;
        callbacks.evaluate = [&observed, stop_after_count](const NodeView &view, DateTime evaluation_time)
        {
            auto root = view.input(evaluation_time);
            auto bundle = root.as_bundle();
            auto tuple = bundle[0].value().as_list();
            std::vector<Int> values;
            values.reserve(tuple.size());
            for (std::size_t index = 0; index < tuple.size(); ++index)
            {
                values.push_back(tuple[index].checked_as<Int>());
            }
            observed.push_back(std::move(values));
            if (observed.size() >= stop_after_count)
            {
                view.graph().executor().request_stop();
            }
        };

        return NodeBuilder::native(std::move(schema), std::move(callbacks),
                                   hgraph::testing::single_input_endpoint(input_schema, input_ts));
    }

    void connect_source_to_sink(GraphBuilder &builder)
    {
        builder.add_edge(GraphEdge{
            .source_node = make_graph_edge_source(0),
            .source_path = {},
            .target_node = 1,
            .target_path = {0},
        });
    }
} // namespace

TEST_CASE("bounded push-source queue refuses at capacity and releases on dequeue")
{
    using namespace hgraph;

    const auto *ts_int = ts_type<TS<Int>>();
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_int);
    std::vector<Int> observed;
    std::thread producer;
    std::atomic_bool blocking_send_completed{false};

    PushSourceNodeExtension extension;
    extension.on_start = [&](PushSourceSender sender, const NodeView &, DateTime)
    {
        REQUIRE(sender.try_send(Int{1}));
        CHECK_FALSE(sender.try_send(Int{99}));
        producer = std::thread(
            [sender, &blocking_send_completed]
            {
                sender.send_blocking(Int{2});
                blocking_send_completed = true;
            });
    };
    extension.on_stop = [&](const NodeView &)
    {
        if (producer.joinable())
        {
            producer.join();
        }
    };

    GraphBuilder builder;
    builder.add_node(
        make_push_source_node_with_view(*ts_int, make_push_source_queue_policy(*ts_int, 1), std::move(extension)));
    builder.add_node(hgraph::testing::collecting_scalar_sink<Int>(*input_schema, *ts_int, observed, 2));
    connect_source_to_sink(builder);

    const DateTime start_time = hgraph::testing::wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});
    auto executor = executor_builder.make_executor();
    executor.view().run();

    CHECK(blocking_send_completed);
    const std::vector<Int> expected{Int{1}, Int{2}};
    CHECK(observed == expected);
}

TEST_CASE("blocking push-source sender wakes with PushSourceStopped on stop")
{
    using namespace hgraph;

    const auto *ts_int = ts_type<TS<Int>>();
    PushSourceSender sender;
    GraphBuilder builder;
    builder.add_node(make_push_source_node(*ts_int, make_push_source_queue_policy(*ts_int, 1),
                                           [&sender](PushSourceSender started) { sender = std::move(started); }));

    const DateTime start_time = hgraph::testing::wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});
    auto executor = executor_builder.make_executor();
    auto graph = executor.view().graph();
    graph.start(start_time);

    REQUIRE(sender.try_send(Int{1}));
    std::promise<void> attempted;
    auto attempted_future = attempted.get_future();
    std::atomic_bool stopped{false};
    std::thread blocked(
        [&]
        {
            attempted.set_value();
            try
            {
                sender.send_blocking(Int{2});
            }
            catch (const PushSourceStopped &)
            {
                stopped = true;
            }
        });
    attempted_future.wait();
    std::this_thread::sleep_for(std::chrono::milliseconds{10});

    graph.stop(start_time);
    blocked.join();

    CHECK(stopped);
    CHECK_FALSE(sender.valid());
    CHECK_FALSE(sender.try_send(Int{3}));
    CHECK_THROWS_AS(sender.send_blocking(Int{4}), PushSourceStopped);
}

TEST_CASE("blocking push-source sender refuses to wait on the evaluation thread")
{
    using namespace hgraph;

    const auto *ts_int = ts_type<TS<Int>>();
    bool rejected_wait{false};
    GraphBuilder builder;
    builder.add_node(make_push_source_node(*ts_int, make_push_source_queue_policy(*ts_int, 1),
                                           [&rejected_wait](PushSourceSender sender)
                                           {
                                               sender.send_blocking(Int{1});
                                               try
                                               {
                                                   sender.send_blocking(Int{2});
                                               }
                                               catch (const std::logic_error &error)
                                               {
                                                   rejected_wait = std::string_view{error.what()}.contains(
                                                       "graph evaluation thread");
                                               }
                                           }));

    const DateTime start_time = hgraph::testing::wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});
    auto executor = executor_builder.make_executor();
    auto graph = executor.view().graph();
    graph.start(start_time);
    graph.stop(start_time);

    CHECK(rejected_wait);
}

TEST_CASE("burst delivery shares queue admission and emits one pending tuple")
{
    using namespace hgraph;

    const auto *ts_tuple = ts_type<TS<HomogeneousTuple<Int>>>();
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_tuple);
    std::vector<std::vector<Int>> observed;
    std::thread producer;
    std::atomic_bool blocking_send_completed{false};
    bool refused{false};

    PushSourceNodeExtension extension;
    extension.on_start = [&](PushSourceSender sender, const NodeView &, DateTime)
    {
        sender.send_blocking(Int{1});
        sender.send_blocking(Int{2});
        sender.send_blocking(Int{3});
        refused = !sender.try_send(Int{4});
        CHECK_THROWS_AS(sender.try_send(Str{"wrong schema"}), std::invalid_argument);
        producer = std::thread(
            [sender, &blocking_send_completed]
            {
                sender.send_blocking(Int{4});
                blocking_send_completed = true;
            });
    };
    extension.on_stop = [&](const NodeView &)
    {
        if (producer.joinable())
        {
            producer.join();
        }
    };

    GraphBuilder builder;
    builder.add_node(
        make_push_source_node_with_view(*ts_tuple, make_push_source_burst_policy(*ts_tuple, 3), std::move(extension)));
    builder.add_node(collecting_tuple_sink(*input_schema, *ts_tuple, observed, 2));
    connect_source_to_sink(builder);

    const DateTime start_time = hgraph::testing::wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});
    auto executor = executor_builder.make_executor();
    executor.view().run();

    CHECK(refused);
    CHECK(blocking_send_completed);
    REQUIRE(observed.size() == 2);
    const std::vector<Int> expected_first{Int{1}, Int{2}, Int{3}};
    const std::vector<Int> expected_second{Int{4}};
    CHECK(observed[0] == expected_first);
    CHECK(observed[1] == expected_second);
}

TEST_CASE("burst policy rejects non-variadic-tuple output schemas")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    CHECK_THROWS_AS(make_push_source_burst_policy(*registry.ts(integer)), std::invalid_argument);
    CHECK_THROWS_AS(make_push_source_burst_policy(*registry.ts(registry.list(integer))), std::invalid_argument);
    CHECK_THROWS_AS(make_push_source_burst_policy(*registry.ts(registry.list(integer, 2))), std::invalid_argument);
    CHECK_THROWS_AS(make_push_source_policy(PushSourcePolicyKind::Burst, *integer), std::invalid_argument);
}

TEST_CASE("push-source sender supports concurrent producers")
{
    using namespace hgraph;

    constexpr std::size_t producer_count = 4;
    constexpr std::size_t values_per_producer = 32;
    constexpr std::size_t total_values = producer_count * values_per_producer;

    const auto *ts_int = ts_type<TS<Int>>();
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_int);
    std::vector<Int> observed;
    std::vector<std::thread> producers;

    PushSourceNodeExtension extension;
    extension.on_start = [&](PushSourceSender sender, const NodeView &, DateTime)
    {
        producers.reserve(producer_count);
        for (std::size_t producer = 0; producer < producer_count; ++producer)
        {
            producers.emplace_back(
                [sender, producer]
                {
                    for (std::size_t index = 0; index < values_per_producer; ++index)
                    {
                        sender.send_blocking(Int{static_cast<std::int64_t>(producer * values_per_producer + index)});
                    }
                });
        }
    };
    extension.on_stop = [&](const NodeView &)
    {
        for (auto &producer : producers)
        {
            if (producer.joinable())
            {
                producer.join();
            }
        }
    };

    GraphBuilder builder;
    builder.add_node(
        make_push_source_node_with_view(*ts_int, make_push_source_queue_policy(*ts_int, 16), std::move(extension)));
    builder.add_node(hgraph::testing::collecting_scalar_sink<Int>(*input_schema, *ts_int, observed, total_values));
    connect_source_to_sink(builder);

    const DateTime start_time = hgraph::testing::wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{2'000'000});
    auto executor = executor_builder.make_executor();
    executor.view().run();

    REQUIRE(observed.size() == total_values);
    std::ranges::sort(observed);
    for (std::size_t index = 0; index < total_values; ++index)
    {
        CHECK(observed[index] == Int{static_cast<std::int64_t>(index)});
    }
}
