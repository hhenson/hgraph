#ifndef HGRAPH_TYPES_WIRING_INSPECTION_H
#define HGRAPH_TYPES_WIRING_INSPECTION_H
#include <hgraph/types/graph_wiring.h>

namespace hgraph
{
    struct WiringDependency
    {
        std::size_t source_index;
        bool passive;
        bool operator==(const WiringDependency &) const = default;
    };

    /** Find source boundaries upstream of output, in sources order.
     * Traversal stops at a matching source (including projections of it).
     * A dependency is passive only if every path to it contains a passive
     * input. This inspects declared wiring, not runtime activity or values.
     * Ports must belong to the live wiring being composed. Bound delayed
     * inputs, structural inputs and inline graphs are supported; unbound
     * delayed inputs and compiled child graphs throw rather than return an
     * incomplete dependency set. Results own no graph pointers.
     */
    [[nodiscard]] HGRAPH_EXPORT std::vector<WiringDependency> inspect_wiring_dependencies(
        const WiringPortRef &output, std::span<const WiringPortRef> sources);
}
#endif
