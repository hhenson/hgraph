#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <typeindex>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;

    struct ConcurrentProgress
    {
        std::mutex              mutex{};
        std::condition_variable changed{};
        bool                    blocked_graph_entered{false};
        std::size_t             independent_graph_outputs{0};
    };

    struct BlockedSourceTag
    {
    };

    struct IndependentSourceTag
    {
    };

    NodeBuilder blocked_source(const TSValueTypeMetaData &ts_int,
                               ConcurrentProgress &progress)
    {
        NodeTypeMetaData schema;
        schema.display_name      = "blocked_concurrent_source";
        schema.output_schema     = &ts_int;
        schema.node_kind         = NodeKind::PullSource;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [&progress](const NodeView &view,
                                         DateTime evaluation_time) {
            {
                std::unique_lock lock{progress.mutex};
                progress.blocked_graph_entered = true;
                progress.changed.notify_all();
                if (!progress.changed.wait_for(
                        lock,
                        std::chrono::seconds{5},
                        [&] { return progress.independent_graph_outputs == 3; }))
                {
                    throw std::runtime_error(
                        "the independent graph did not progress while its peer was blocked");
                }
            }
            view.graph().global_state().set("progress", Value{Int{1}});
            testing::set_output_value(view, evaluation_time, Int{101});
        };
        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    NodeBuilder independent_source(const TSValueTypeMetaData &ts_int,
                                   ConcurrentProgress &progress,
                                   std::int32_t &emitted)
    {
        NodeTypeMetaData schema;
        schema.display_name      = "independent_concurrent_source";
        schema.output_schema     = &ts_int;
        schema.node_kind         = NodeKind::PullSource;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [&progress, &emitted](const NodeView &view,
                                                   DateTime evaluation_time) {
            ++emitted;
            view.graph().global_state().set("progress", Value{Int{emitted}});
            testing::set_output_value(view, evaluation_time, Int{200 + emitted});
            {
                std::lock_guard lock{progress.mutex};
                progress.independent_graph_outputs =
                    static_cast<std::size_t>(emitted);
            }
            progress.changed.notify_all();
            if (emitted < 3)
            {
                view.graph_value()->schedule_node(
                    view.node_index(), evaluation_time + MIN_TD);
            }
        };
        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    template <typename Tag>
    void wire_recorded_source(Wiring &wiring, NodeBuilder source,
                              std::string_view key)
    {
        WiringPortRef source_ref = wiring.add_unique_node(
            std::type_index(typeid(Tag)), std::move(source),
            std::span<const WiringPortRef>{}, Value{});
        wire<stdlib::dense_record_impl>(
            wiring, Port<TS<Int>>{wiring, std::move(source_ref)}, Str{key});
    }

    GraphExecutorValue make_simulation_executor(Wiring wiring)
    {
        GraphExecutorBuilder builder;
        builder.graph_builder(std::move(wiring).finish())
            .mode(GraphExecutorMode::Simulation)
            .start_time(MIN_ST)
            .end_time(MIN_ST + TimeDelta{10});
        return builder.make_executor();
    }
}  // namespace

TEST_CASE("distinct native graph engines progress independently on different threads",
          "[runtime][concurrency]")
{
    using namespace hgraph;

    auto       &registry     = TypeRegistry::instance();
    const auto *int_meta     = registry.register_scalar<Int>("int");
    const auto *ts_int       = registry.ts(int_meta);

    ConcurrentProgress progress;
    std::int32_t       independent_emitted{0};

    Wiring blocked_graph;
    wire_recorded_source<BlockedSourceTag>(
        blocked_graph, blocked_source(*ts_int, progress), "blocked");

    Wiring independent_graph;
    wire_recorded_source<IndependentSourceTag>(
        independent_graph,
        independent_source(*ts_int, progress, independent_emitted),
        "independent");

    GraphExecutorValue blocked_executor =
        make_simulation_executor(std::move(blocked_graph));
    GraphExecutorValue independent_executor =
        make_simulation_executor(std::move(independent_graph));
    auto blocked_view     = blocked_executor.view();
    auto independent_view = independent_executor.view();
    REQUIRE_FALSE(blocked_view.graph().compound_scalar_storage().available());
    REQUIRE_FALSE(
        independent_view.graph().compound_scalar_storage().available());

    testing::AsyncGraphExecutorRun blocked_run{blocked_view};
    {
        std::unique_lock lock{progress.mutex};
        REQUIRE(progress.changed.wait_for(
            lock,
            std::chrono::seconds{5},
            [&] { return progress.blocked_graph_entered; }));
    }

    testing::AsyncGraphExecutorRun independent_run{independent_view};
    independent_run.join();
    blocked_run.join();

    CHECK(testing::get_recorded_values<Int>(
              blocked_view.graph().global_state(), "blocked") ==
          std::vector<std::optional<Int>>{Int{101}});
    CHECK(testing::get_recorded_values<Int>(
              independent_view.graph().global_state(), "independent") ==
          std::vector<std::optional<Int>>{Int{201}, Int{202}, Int{203}});
    CHECK(blocked_view.graph().global_state().get_as<Int>("progress") == Int{1});
    CHECK(independent_view.graph().global_state().get_as<Int>("progress") == Int{3});
}
