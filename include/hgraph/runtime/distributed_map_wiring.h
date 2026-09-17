#ifndef HGRAPH_RUNTIME_DISTRIBUTED_MAP_WIRING_H
#define HGRAPH_RUNTIME_DISTRIBUTED_MAP_WIRING_H

#include <hgraph/runtime/distributed_map.h>

namespace hgraph::distributed
{
    /** Reusable native worker plan for embedding frontends.
     * The bootstrap rebuilds this child from importable application code;
     * neither callables nor live graph storage cross the process boundary.
     */
    struct DistributedMapPlan
    {
        GraphBuilder child{};
        BoundarySlots slots{};
        const TSValueTypeMetaData *output{};
        WorkerPoolConfig config{};
        std::string recipe{};
        GraphExecutorPhaseRunner phase_runner{};
    };
    using DistributedMapPlanPtr = std::shared_ptr<const DistributedMapPlan>;

    inline DistributedMapPlan prepare_distributed_map(
        const WiredFn &func, const TSValueTypeMetaData *input)
    {
        if (input == nullptr || input->kind != TSTypeKind::TSD ||
            input->element_ts()->kind != TSTypeKind::TS)
            throw std::invalid_argument("dmap_ requires one TSD of scalar TS inputs");
        Wiring worker;
        auto source = wire<boundary_source_impl>(worker, Str{"in"}, input);
        auto result = wire<stdlib::map_>(worker, func, source);
        const auto *output = result.erased().schema;
        if (output == nullptr || output->kind != TSTypeKind::TSD ||
            output->element_ts()->kind != TSTypeKind::TS)
            throw std::invalid_argument("dmap_ requires a scalar TS child result");
        wire<boundary_sink_impl>(worker, result, Str{"out"});
        DistributedMapPlan plan{.child = std::move(worker).finish(), .output = output};
        plan.slots.add("in", input->delta_value_schema, SlotDirection::Input);
        plan.slots.add("out", output->delta_value_schema, SlotDirection::Output);
        return plan;
    }
}

namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<distributed::DistributedMapPlanPtr>
    {
        static constexpr std::string_view value{"hgraph.distributed.map_plan"};
    };
}

namespace hgraph::distributed
{
    struct prepared_dmap_impl
    {
        static constexpr auto name = "dmap_";
        using Input = TSD<ScalarVar<"K">, TS<ScalarVar<"V">>>;
        using Output = TSD<ScalarVar<"K">, TS<ScalarVar<"R">>>;

        static void start(Scalar<"plan", DistributedMapPlanPtr> plan,
                          EngineControlView engine, State<DistributedMapState> state)
        {
            auto config = plan.value()->config;
            config.start_time = engine.start_time();
            config.end_time = engine.end_time();
            auto pool = WorkerPool::build(plan.value()->child, plan.value()->slots,
                plan.value()->recipe, config, plan.value()->phase_runner);
            state.modify().pool = pool.release();
        }

        static void eval(In<"ts", Input> ts, Scalar<"plan", DistributedMapPlanPtr>,
                         TypeArg<"result", Output>, State<DistributedMapState> state,
                         NodeScheduler scheduler, DateTime now, Out<Output> out)
        {
            const auto next = state.get().pool->evaluate(ts.base(), out.base(), now);
            if (next != MAX_DT) { scheduler.schedule(std::max(next, now + MIN_TD)); }
        }

        static void stop(State<DistributedMapState> state)
        {
            std::unique_ptr<WorkerPool> pool{std::exchange(state.modify().pool, nullptr)};
            if (pool) { pool->stop(); }
        }
    };

    inline Port<void> wire_distributed_map(Wiring &wiring, const WiringPortRef &input,
                                           DistributedMapPlanPtr plan)
    {
        const auto *output = plan->output;
        return Port<void>{wiring, wire<prepared_dmap_impl>(wiring, Port<void>{wiring, input}, plan, output).erased()};
    }
}

#endif
