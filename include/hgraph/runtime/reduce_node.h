#ifndef HGRAPH_RUNTIME_REDUCE_NODE_H
#define HGRAPH_RUNTIME_REDUCE_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/nested_graph_node.h>   // SingleNestedGraphNodeSpec (the combiner template shape)

#include <cstddef>

namespace hgraph
{
    struct LiftedKernel;

    struct HGRAPH_EXPORT ReduceNodeSpec
    {
        /**
         * The compiled binary combiner template; binding ``source_path[0]`` is
         * the boundary arg ordinal (0 = lhs, 1 = rhs).
         */
        SingleNestedGraphNodeSpec child{};
        /**
         * Optional scalar-kernel capability resolved at wiring time. The
         * generic child graph remains available as the correctness fallback;
         * compatible native associative reducers can bypass its scheduler.
         */
        const LiftedKernel *lifted_kernel{nullptr};
        /** Whether the optional outer ``zero`` input is present. */
        bool has_zero{false};
    };

    /** Typed extension view exposed by ``reduce_node`` (runtime inspection surface). */
    class HGRAPH_EXPORT ReduceNodeView
    {
      public:
        [[nodiscard]] static const void *node_view_type_id() noexcept;
        [[nodiscard]] static ReduceNodeView from_node(NodeView view, const void *context);

        [[nodiscard]] const NodeView &node() const noexcept;
        /** Live key (dense leaf) count. */
        [[nodiscard]] std::size_t leaf_count() const noexcept;
        /** Live combiner count (``n - 1`` normally; one for explicit-zero singleton). */
        [[nodiscard]] std::size_t combiner_count() const noexcept;
        /** True when every live combiner graph resides in one of the two stable banks. */
        [[nodiscard]] bool        child_graphs_use_in_place_storage() const noexcept;

        /** Internal (reduce_node implementation) — the registered context / storage. */
        [[nodiscard]] const void *internal_context() const noexcept { return context_; }
        [[nodiscard]] void       *internal_storage() const noexcept { return storage_; }

      private:
        ReduceNodeView(NodeView view, const void *context, void *storage) noexcept;

        NodeView    view_{};
        const void *context_{nullptr};
        void       *storage_{nullptr};
    };

    /**
     * Build the associative ``reduce`` node over a multiplexed TSD or TSL
     * input: a balanced binary tree whose **leaves alias the live source
     * elements** and whose internal combine points own combiner child graphs
     * — instantiated when both subtrees are non-empty, plus the root when a
     * supplied zero must combine with a single live value. The node's
     * forwarding output publishes the root aggregate: no live values publish
     * the supplied zero or remain invalid when it is omitted; one live value
     * aliases that value when zero is omitted and combines it with zero when
     * supplied. The live key set is reconciled against the current
     * collection input when it modifies, re-points, becomes invalid, or is
     * first observed. See *Nested Graphs > Associative reduce runtime*.
     *
     * Outer inputs: ``[ts (TSD or dynamic TSL)]`` with an optional trailing
     * ``zero (element)``.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder reduce_node(NodeTypeMetaData meta, ReduceNodeSpec spec);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_REDUCE_NODE_H
