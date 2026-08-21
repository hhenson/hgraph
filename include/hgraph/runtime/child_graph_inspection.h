#ifndef HGRAPH_RUNTIME_CHILD_GRAPH_INSPECTION_H
#define HGRAPH_RUNTIME_CHILD_GRAPH_INSPECTION_H

#include <hgraph/hgraph_export.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace hgraph
{
    class GraphBuilder;
    class NodeBuilder;

    /** A path to a time-series endpoint inside a compiled child graph. */
    struct HGRAPH_EXPORT NestedGraphEndpoint
    {
        std::size_t              node{0};
        std::vector<std::size_t> path{};
    };

    /** The endpoint exposed by a compiled child through its owning node. */
    struct HGRAPH_EXPORT NestedGraphOutputBinding
    {
        enum class Kind : std::uint8_t
        {
            ChildOutput,
            ParentInput,
        };

        Kind                     kind{Kind::ChildOutput};
        NestedGraphEndpoint      source{};
        std::vector<std::size_t> parent_source_path{};
        std::vector<std::size_t> target_path{};
    };

    /** Borrowed, data-only view of one immediate compiled child graph. */
    struct HGRAPH_EXPORT ChildGraphInspectionView
    {
        const GraphBuilder             *graph{nullptr};
        const NestedGraphOutputBinding *output_binding{nullptr};
    };

    /** Borrowed callback used to inspect one immediate compiled child graph.
     *
     * The graph and output-binding pointers are valid only for the duration of
     * the callback. A null output binding denotes a sink child.
     * Inspection is a wiring/build-time operation; runtime evaluation never
     * dispatches this contract.
     */
    using ChildGraphVisitor = void (*)(void *visitor_context, ChildGraphInspectionView child);

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
