#ifndef HGRAPH_RUNTIME_WIRING_INPUT_ACTIVITY_H
#define HGRAPH_RUNTIME_WIRING_INPUT_ACTIVITY_H

#include <hgraph/hgraph_export.h>
#include <cstddef>
#include <span>

namespace hgraph
{
    class NodeBuilder;
    namespace wiring_input_activity_detail
    {
        HGRAPH_EXPORT bool schema_activity(const NodeBuilder &, std::span<const std::size_t>);
    }

    /** Declarative input activity for wiring inspection, never runtime activity.
     * Packed-input implementations override this to describe individual child
     * paths. The default uses the node schema's top-level active selectors.
     * Runtime make_active/make_passive calls are deliberately not predicted.
     */
    struct WiringInputActivityOps
    {
        bool (*is_active)(const NodeBuilder &, std::span<const std::size_t>){
            &wiring_input_activity_detail::schema_activity};
    };
}
#endif
