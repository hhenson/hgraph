#ifndef HGRAPH_RUNTIME_ORDERED_REDUCE_NODE_H
#define HGRAPH_RUNTIME_ORDERED_REDUCE_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/nested_graph_node.h>

#include <cstddef>

namespace hgraph
{
    struct HGRAPH_EXPORT OrderedReduceNodeSpec
    {
        /** Compiled ``(accumulator, element) -> accumulator`` child graph. */
        SingleNestedGraphNodeSpec child{};
    };

    class HGRAPH_EXPORT OrderedReduceNodeView
    {
      public:
        [[nodiscard]] static const void *node_view_type_id() noexcept;
        [[nodiscard]] static OrderedReduceNodeView from_node(NodeView view, const void *context);

        [[nodiscard]] const NodeView &node() const noexcept;
        [[nodiscard]] std::size_t child_graph_count() const noexcept;
        [[nodiscard]] bool child_graphs_use_in_place_storage() const noexcept;

        [[nodiscard]] const void *internal_context() const noexcept { return context_; }
        [[nodiscard]] void *internal_storage() const noexcept { return storage_; }

      private:
        OrderedReduceNodeView(NodeView view, const void *context, void *storage) noexcept;

        NodeView view_{};
        const void *context_{nullptr};
        void *storage_{nullptr};
    };

    /**
     * Build a left-to-right reduction over a contiguous ``TSD[int, E]``.
     * Child ``i`` receives the live ``zero`` input (``i == 0``) or child
     * ``i - 1``'s output as its lhs, and dictionary element ``i`` as its rhs.
     * Structural changes rebuild into the inactive in-place bank, publish the
     * new tail, then stop and retain the previous chain for one engine cycle.
     *
     * Outer inputs: ``[ts (TSD[int, E]), zero (A)]``. Output: ``A``.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder ordered_reduce_node(
        NodeTypeMetaData meta,
        OrderedReduceNodeSpec spec);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_ORDERED_REDUCE_NODE_H
