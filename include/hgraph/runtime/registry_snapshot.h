#ifndef HGRAPH_RUNTIME_REGISTRY_SNAPSHOT_H
#define HGRAPH_RUNTIME_REGISTRY_SNAPSHOT_H

#include <hgraph/hgraph_export.h>

#include <cstddef>
#include <cstdint>

namespace hgraph
{
    /**
     * Cold-path cardinalities for process-lifetime runtime registries.
     *
     * Runtime type records deliberately have stable addresses and therefore
     * outlive individual graph executions.  This snapshot makes that retained
     * structural state observable without attaching a lifecycle observer or
     * adding work to graph evaluation.  Call it only while graph wiring is
     * quiescent; the runtime registries themselves are wiring-thread owned.
     */
    struct HGRAPH_EXPORT RuntimeRegistrySnapshot
    {
        std::size_t node_runtime_types{0};
        std::size_t graph_programs{0};
        std::size_t graph_runtime_types{0};
        std::size_t executor_runtime_types{0};
        std::size_t type_records{0};
        /**
         * Process-lifetime count of type-system lock acquisitions
         * (see hgraph::type_system_lock_count). Wiring moves it; evaluation
         * of a wired graph must not: an N-cycle and a 2N-cycle run of the
         * same graph leave identical deltas here.
         */
        std::uint64_t type_system_lock_acquisitions{0};

        [[nodiscard]] constexpr bool operator==(
            const RuntimeRegistrySnapshot &) const noexcept = default;
    };

    /** Capture current process-lifetime runtime registry cardinalities. */
    [[nodiscard]] HGRAPH_EXPORT RuntimeRegistrySnapshot runtime_registry_snapshot();
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_REGISTRY_SNAPSHOT_H
