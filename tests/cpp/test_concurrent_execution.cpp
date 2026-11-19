#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <future>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <thread>
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

namespace
{
    struct ResolvedOutputProbe
    {
        static constexpr auto name              = "concurrent_resolved_output_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Int>> out) { out.set(Int{1}); }
    };
}  // namespace

TEST_CASE("a node builder publishes one immutable output type record per realization to concurrent constructors",
          "[node][builder][concurrency]")
{
    // The active type realization is thread-local, so two engines sharing a
    // wired graph can construct from one builder under different
    // realizations at once. Each must see a record for its own realization,
    // and the record it holds must not change under it.
    NodeBuilder builder{};
    builder.label("probe").implementation<ResolvedOutputProbe>();
    SECTION("owned output") {}
    SECTION("forwarding output") {
        builder.output_endpoint(TSEndpointSchema::peered(builder.type().schema()->output_schema));
    }
    const auto snapshot = TypeRealizationSnapshot::capture(TypeRegistry::instance());
    REQUIRE(snapshot);

    constexpr std::size_t rounds = 4000;
    std::atomic<bool>     mismatched{false};
    const auto            worker = [&](const TypeRealizationSnapshot *scope_snapshot) {
        const TypeRealizationScope scope{scope_snapshot};
        const auto                 first = builder.resolved_output_types();
        if (!first || !first->output.bound()) { mismatched = true; return; }
        for (std::size_t i = 0; i < rounds && !mismatched; ++i)
        {
            const auto record = builder.resolved_output_types();
            if (!record || record->snapshot != scope_snapshot || record->output.record() != first->output.record())
            {
                mismatched = true;
            }
            // The first record stays what it was however often the other
            // thread republishes its own.
            if (first->snapshot != scope_snapshot) { mismatched = true; }
        }
    };
    std::thread plain{worker, nullptr};
    std::thread realized{worker, snapshot.get()};
    plain.join();
    realized.join();
    CHECK_FALSE(mismatched);
}

TEST_CASE("concurrent root REF adapters share one descriptor per realization, schema pair and storage mode",
          "[reference][concurrency]")
{
    auto &registry = TypeRegistry::instance();
    const auto *scalar = registry.ts(scalar_descriptor<Int>::value_meta());
    const auto *xy = registry.tsb("ConcurrentRootRefXY", {{"x", scalar}, {"y", scalar}});
    const auto *yx = registry.tsb("ConcurrentRootRefYX", {{"y", scalar}, {"x", scalar}});
    const auto *ref_xy = registry.ref(xy);
    const auto *ref_yx = registry.ref(yx);
    const auto *requested_xy = registry.un_named_tsb({{"x", scalar}, {"y", scalar}});
    const auto *requested_yx = registry.un_named_tsb({{"y", scalar}, {"x", scalar}});
    const auto snapshot = TypeRealizationSnapshot::capture(registry);
    const auto other = TypeRealizationSnapshot::capture(registry, {PolymorphicCompoundStoragePolicy::Pooled});
    REQUIRE(snapshot != other);

    const auto bind = [](const TSValueTypeMetaData *source, const TSValueTypeMetaData *requested) {
        TSOutput owner{source};
        return owner.binding_for(owner.view(MIN_ST), *requested).bound();
    };

    // Both threads create their own endpoints for the same first cache entry.
    std::barrier start{2};
    const auto first_binding = [&] {
        const TypeRealizationScope scope{snapshot.get()};
        start.arrive_and_wait();
        return bind(ref_xy, requested_xy);
    };
    const auto before = snapshot->from_ref_plan_storage_metrics().live_bytes;
    const auto other_before = other->from_ref_plan_storage_metrics().live_bytes;
    auto peer = std::async(std::launch::async, first_binding);
    CHECK(first_binding());
    REQUIRE(peer.get());

    const auto first = snapshot->from_ref_plan_storage_metrics();
    REQUIRE(first.live_bytes > before);
    CHECK(other->from_ref_plan_storage_metrics().live_bytes == other_before);
    {
        const TypeRealizationScope scope{other.get()};
        REQUIRE(bind(ref_xy, requested_xy));
        // These two fresh realizations now each own exactly one descriptor,
        // even though two threads asked for the same one in the first.
        CHECK(other->from_ref_plan_storage_metrics().live_bytes - other_before == first.live_bytes - before);
    }

    const TypeRealizationScope scope{snapshot.get()};
    REQUIRE(bind(ref_xy, requested_xy));
    CHECK(snapshot->from_ref_plan_storage_metrics().live_bytes == first.live_bytes);
    CHECK(snapshot->from_ref_plan_storage_metrics().reserved_bytes == first.reserved_bytes);

    REQUIRE(bind(ref_yx, requested_xy));
    const auto changed_source = snapshot->from_ref_plan_storage_metrics().live_bytes;
    CHECK(changed_source > first.live_bytes);
    REQUIRE(bind(ref_xy, requested_yx));
    const auto changed_request = snapshot->from_ref_plan_storage_metrics().live_bytes;
    CHECK(changed_request > changed_source);
    {
        const GraphValueRealizationScope graph_storage;
        REQUIRE(bind(ref_xy, requested_xy));
        CHECK(snapshot->from_ref_plan_storage_metrics().live_bytes > changed_request);
    }
}

TEST_CASE("a root REF adapter retains its realization until the owning output releases it",
          "[reference][lifetime]")
{
    auto &registry = TypeRegistry::instance();
    const auto *scalar = registry.ts(scalar_descriptor<Int>::value_meta());
    const auto *schema = registry.tsb("RootRefLifetime", {{"value", scalar}});
    const auto *reference = registry.ref(schema);
    const auto snapshot = TypeRealizationSnapshot::capture(registry);
    const auto original_owners = snapshot.use_count();

    TSOutput standalone{reference};
    REQUIRE(standalone.binding_for(standalone.view(MIN_ST), *schema).bound());
    const auto standalone_storage = standalone.dynamic_storage_metrics();
    {
        TSOutput owner{reference};
        {
            const TypeRealizationScope scope{snapshot.get()};
            REQUIRE(owner.binding_for(owner.view(MIN_ST), *schema).bound());
        }
        CHECK(snapshot.use_count() > original_owners);
        // Standalone outputs account for the plan they own. Under a realization
        // the same plan is charged once to the snapshot.
        const auto shared_storage = owner.dynamic_storage_metrics();
        CHECK(shared_storage.live_bytes < standalone_storage.live_bytes);
        CHECK(shared_storage.reserved_bytes < standalone_storage.reserved_bytes);
    }
    // The descriptor index remains populated without keeping its owner alive.
    CHECK(snapshot->from_ref_plan_storage_metrics().live_bytes > 0);
    CHECK(snapshot.use_count() == original_owners);
}
