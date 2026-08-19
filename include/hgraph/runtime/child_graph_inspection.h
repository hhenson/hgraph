#ifndef HGRAPH_RUNTIME_CHILD_GRAPH_INSPECTION_H
#define HGRAPH_RUNTIME_CHILD_GRAPH_INSPECTION_H

#include <hgraph/hgraph_export.h>

namespace hgraph
{
    class GraphBuilder;
    class NodeBuilder;

    /** Borrowed callback used to inspect one immediate compiled child graph.
     *
     * The graph reference is valid only for the duration of the callback.
     * Inspection is a wiring/build-time operation; runtime evaluation never
     * dispatches this contract.
     */
    using ChildGraphVisitor = void (*)(void *visitor_context, const GraphBuilder &child);

    namespace child_graph_inspection_detail
    {
        /** Program-wide canonical no-op used by node types without children. */
        HGRAPH_EXPORT void visit_none(const void *, const NodeBuilder &, void *, ChildGraphVisitor);
    }  // namespace child_graph_inspection_detail

    /** Passive type-erased contract for a node builder's compiled children.
     *
     * Concrete nested-node strategies install a visitor which exposes their
     * immediate child graph templates without leaking their private context
     * representation. The canonical no-op visitor keeps ordinary nodes'
     * inspection contract non-null.
     */
    struct HGRAPH_EXPORT ChildGraphInspectionOps
    {
        const void *context{nullptr};
        void (*visit_impl)(const void *context,
                           const NodeBuilder &owner,
                           void *visitor_context,
                           ChildGraphVisitor visitor){&child_graph_inspection_detail::visit_none};
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_CHILD_GRAPH_INSPECTION_H
