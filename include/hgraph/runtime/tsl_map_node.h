#ifndef HGRAPH_RUNTIME_TSL_MAP_NODE_H
#define HGRAPH_RUNTIME_TSL_MAP_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/nested_graph_node.h>

#include <cstddef>
#include <vector>

namespace hgraph
{
    struct HGRAPH_EXPORT TslMapNodeSpec
    {
        /** One child template instantiated once for every observed list index. */
        SingleNestedGraphNodeSpec child{};
        /** Per child-boundary argument: index element, outer broadcast, or ``ndx``.
         */
        std::vector<MapArgSource> args{};
        /** Outer-input indices of the dynamic TSLs whose maximum size drives growth.
         */
        std::vector<std::size_t> multiplexed_inputs{};
        /** Entry-owned ``TS<int64>`` used when the child accepts ``ndx``. */
        const TSValueTypeMetaData *index_output_schema{nullptr};
        /** Whether the child terminal writes into the list element or the
            element preserves and forwards to a child-local terminal. */
        MapOutputBindingMode output_binding_mode{
            MapOutputBindingMode::ChildTerminalWritesElement};
    };

    /** Typed inspection surface for a dynamic-TSL arbitrary-function map. */
    class HGRAPH_EXPORT TslMapNodeView
    {
      public:
        [[nodiscard]] static const void    *node_view_type_id() noexcept;
        [[nodiscard]] static TslMapNodeView from_node(NodeView view, const void *context);

        [[nodiscard]] const NodeView &node() const noexcept;
        [[nodiscard]] std::size_t     active_count() const noexcept;
        [[nodiscard]] std::size_t     child_graph_count() const noexcept;
        [[nodiscard]] std::size_t     child_slot_block_count() const noexcept;
        [[nodiscard]] bool            child_graphs_use_in_place_storage() const noexcept;

        [[nodiscard]] const void *internal_context() const noexcept { return context_; }
        [[nodiscard]] void       *internal_storage() const noexcept { return storage_; }

      private:
        TslMapNodeView(NodeView view, const void *context, void *storage) noexcept;

        NodeView    view_{};
        const void *context_{nullptr};
        void       *storage_{nullptr};
    };

    /**
     * Build a nested node that maps one compiled child graph over grow-only
     * dynamic TSL inputs. The maximum current input size determines how many
     * stable child slots exist; shorter peer lists supply unbound phantom
     * elements until they grow. Child terminals write directly into stable
     * elements of the owned dynamic-TSL output.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder tsl_map_node(NodeTypeMetaData meta, TslMapNodeSpec spec);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_TSL_MAP_NODE_H
