#ifndef HGRAPH_RUNTIME_DISTRIBUTED_MAP_WIRING_H
#define HGRAPH_RUNTIME_DISTRIBUTED_MAP_WIRING_H

#include <hgraph/runtime/distributed_map.h>
#include <hgraph/runtime/distributed_boundary.h>

namespace hgraph::distributed
{
    struct DistributedMapInput
    {
        const TSValueTypeMetaData *schema{};
        WiringPortRef::ArgTag tag{WiringPortRef::ArgTag::None};
        std::string name{};
    };

    /** A process reconstructs this plan from application code and immutable
     * configuration. Graph storage and references never cross the boundary. */
    struct DistributedMapPlan
    {
        GraphBuilder child{};
        BoundarySlots slots{};
        const TSValueTypeMetaData *output{};
        const TSValueTypeMetaData *input_schema{};
        std::vector<BoundaryTransferPtr> input_transfers{};
        BoundaryTransferPtr output_transfer{};
        std::vector<GraphBuilder> children{};
        WorkerPoolConfig config{};
        std::vector<std::string> recipes{};
        GraphExecutorPhaseRunner phase_runner{};
    };
    using DistributedMapPlanPtr = std::shared_ptr<const DistributedMapPlan>;

    /** Build one worker partition using ordinary map classification. */
    [[nodiscard]] HGRAPH_EXPORT DistributedMapPlan prepare_distributed_map(
        const WiredFn &func, std::span<const DistributedMapInput> inputs,
        std::optional<std::string> key_arg = {}, std::size_t group = 0, std::size_t groups = 1);
    /** Prepare every partition once, before graph execution. */
    [[nodiscard]] HGRAPH_EXPORT DistributedMapPlan prepare_distributed_map_pool(
        const WiredFn &func, std::span<const DistributedMapInput> inputs,
        std::optional<std::string> key_arg, WorkerPoolConfig config);
    [[nodiscard]] HGRAPH_EXPORT DistributedMapPlan prepare_distributed_map(
        const WiredFn &func, const TSValueTypeMetaData *input);
    HGRAPH_EXPORT void bind_distributed_map_recipe(DistributedMapPlan &plan, std::string_view name);
    [[nodiscard]] HGRAPH_EXPORT Port<void> wire_distributed_map(
        Wiring &wiring, std::span<const WiringPortRef> inputs, DistributedMapPlanPtr plan);
    [[nodiscard]] HGRAPH_EXPORT Port<void> wire_distributed_map(
        Wiring &wiring, const WiringPortRef &input, DistributedMapPlanPtr plan);
}
#endif
