#ifndef HGRAPH_RUNTIME_SPAWN_H
#define HGRAPH_RUNTIME_SPAWN_H

#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/distributed_worker.h>
#include <chrono>
#include <hgraph/types/wired_fn.h>
#include <hgraph/types/operator_dispatch.h>

#include <functional>
#include <memory>
#include <tuple>
#include <vector>

namespace hgraph
{
    /** Admission limits per channel. Side inputs and the preceding stage have
        separate capacity, so future side inputs cannot obstruct earlier flow. */
    struct SpawnConfig
    {
        std::size_t capacity_frames{256};
        std::size_t capacity_bytes{64 * 1024 * 1024};
        std::chrono::milliseconds worker_timeout{60'000};
        std::string worker_program{};
        std::vector<std::string> worker_arguments{};
    };

    /** Embedding hook for a synchronous blocking operation (e.g. release the
        Python GIL). The action must run exactly once before the hook returns. */
    using SpawnWaitRunner = std::function<void(const std::function<void()> &)>;

    struct SpawnStage
    {
        WiredFn function{};
        std::vector<std::pair<std::string, WiringPortRef>> bindings{};
        std::shared_ptr<const void> owner{};
        std::string recipe{};
        std::string bootstrap{};
        // Wiring-only frontend hook: serialize configuration and concrete input
        // schemas before execution. It is never called by a transport thread.
        std::function<std::string(std::span<const TSValueTypeMetaData *const>)> describe{};
    };

    struct SpawnPipeline
    {
        std::vector<SpawnStage> stages{};
    };

    [[nodiscard]] HGRAPH_EXPORT SpawnStage bind_(
        WiredFn function, std::vector<std::pair<std::string, WiringPortRef>> bindings = {});
    [[nodiscard]] HGRAPH_EXPORT SpawnStage bind_(
        SpawnStage stage, std::vector<std::pair<std::string, WiringPortRef>> bindings);
    [[nodiscard]] HGRAPH_EXPORT SpawnPipeline pipeline_(std::vector<SpawnStage> stages);

    /** Bind a native stage to a registered factory in the worker executable.
        Bootstrap is immutable application configuration, never live pointers. */
    [[nodiscard]] HGRAPH_EXPORT SpawnStage process_stage(
        SpawnStage stage, std::string recipe, std::string bootstrap = {});

    struct SpawnWorkerPlan
    {
        GraphBuilder graph{};
        distributed::BoundarySlots slots{};
        const TSValueTypeMetaData *output{};
        std::string boundary_identity{};
    };
    [[nodiscard]] HGRAPH_EXPORT SpawnWorkerPlan prepare_spawn_worker(
        WiredFn function, std::span<const TSValueTypeMetaData *const> inputs);
    using SpawnWorkerFactory = SpawnWorkerPlan (*)(std::string_view bootstrap);
    HGRAPH_EXPORT void register_spawn_worker_recipe(std::string name, SpawnWorkerFactory factory);
    HGRAPH_EXPORT void serve_spawn_worker(distributed::PipeEndpoint &channel, SpawnWorkerPlan plan,
        DateTime start, DateTime end, GraphExecutorPhaseRunner phase_runner = {});
    // Called by the shared distributed worker argv entry point.
    HGRAPH_EXPORT void serve_registered_spawn_worker(distributed::PipeEndpoint &channel,
        std::string_view recipe, DateTime start, DateTime end);
    inline constexpr std::string_view spawn_worker_prefix{"@hgraph-spawn:1:"};

    /** Wire a sink-only asynchronous execution plan. No result port is exposed.
        Every stage runs in a separate process with a private externally driven executor; ordinary graphs
        within a stage retain their normal composition semantics. */
    HGRAPH_EXPORT void wire_spawn(Wiring &wiring, SpawnPipeline pipeline,
                                 std::span<const WiringArg> arguments = {}, SpawnConfig config = {},
                                 SpawnWaitRunner wait_runner = {});
    HGRAPH_EXPORT void wire_spawn(Wiring &wiring, SpawnStage sink,
                                 std::span<const WiringArg> arguments = {}, SpawnConfig config = {},
                                 SpawnWaitRunner wait_runner = {});

    namespace spawn_detail
    {
        template <typename Graph, bool IsGraph = graph_wiring_detail::is_graph_def<Graph>>
        struct StageSignature : StaticNodeSignature<Graph>
        {
            using param_types = typename StaticNodeSignature<Graph>::wire_param_types;
        };
        template <typename Graph>
        struct StageSignature<Graph, true> : StaticGraphSignature<Graph> {};

        template <typename Graph, typename... Scalars>
        struct ConfiguredStage
        {
            std::tuple<Scalars...> scalars;

            static constexpr std::size_t arity = StageSignature<Graph>::input_count();

            static std::span<const std::string_view> names()
            {
                static const auto names = [] {
                    std::array<std::string_view, arity> result{};
                    using Params = typename StageSignature<Graph>::param_types;
                    std::size_t next = 0;
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        ([&] {
                            using P = std::tuple_element_t<I, Params>;
                            if constexpr (graph_wiring_detail::is_named_port<P>::value ||
                                          static_node_detail::is_input_selector<P>::value)
                                result[next++] = P::field_name.sv();
                            else if constexpr (graph_wiring_detail::is_port<P>::value) ++next;
                        }(), ...);
                    }(std::make_index_sequence<std::tuple_size_v<Params>>{});
                    return result;
                }();
                return names;
            }

            static WiringPortRef wire_call(const void *context, Wiring &wiring,
                                           std::span<const WiringPortRef> inputs)
            {
                if (inputs.size() != arity)
                    throw std::invalid_argument("spawn_fn: input count does not match configured stage");
                const auto &self = *static_cast<const ConfiguredStage *>(context);
                return [&]<std::size_t... I>(std::index_sequence<I...>) {
                    return std::apply([&](const auto &...values) -> WiringPortRef {
                        if constexpr (std::is_void_v<decltype(wire<Graph>(
                            wiring, Port<void>{wiring, inputs[I]}..., values...))>)
                        {
                            wire<Graph>(wiring, Port<void>{wiring, inputs[I]}..., values...);
                            return {};
                        }
                        else
                        {
                            return wire<Graph>(wiring, Port<void>{wiring, inputs[I]}..., values...).erased();
                        }
                    }, self.scalars);
                }(std::make_index_sequence<arity>{});
            }
        };
    }

    /** Native wiring-time scalar binding. Supply named scalar arguments when
        the graph interleaves scalar and time-series parameters. */
    template <typename Graph, typename... Scalars>
    [[nodiscard]] SpawnStage spawn_fn(Scalars... scalars)
    {
        if constexpr (sizeof...(Scalars) == 0) return bind_(fn<Graph>());
        else
        {
            using Payload = spawn_detail::ConfiguredStage<Graph, Scalars...>;
            auto payload = std::make_shared<const Payload>(Payload{{std::move(scalars)...}});
            static const WiredFnOps ops = [] {
                WiredFnOps table{};
                table.wire = &Payload::wire_call;
                table.param_names = [](const void *) { return Payload::names(); };
                table.input_pattern = [](const void *, std::size_t index) {
                    return wired_fn_detail::input_pattern_thunk<Graph>(index);
                };
                table.output_schema = [](const void *) { return wired_fn_detail::output_schema_thunk<Graph>(); };
                table.output_pattern = [](const void *) { return wired_fn_detail::output_pattern_thunk<Graph>(); };
                return table;
            }();
            WiredFn function{
                .ops = &ops, .context = payload.get(), .identity = &typeid(Graph),
                .arity = Payload::arity, .has_output = wired_fn_detail::has_output_of<Graph>()};
            return {function, {}, std::move(payload)};
        }
    }
}
#endif
