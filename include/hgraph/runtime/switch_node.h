#ifndef HGRAPH_RUNTIME_SWITCH_NODE_H
#define HGRAPH_RUNTIME_SWITCH_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/nested_graph_node.h>   // SingleNestedGraphNodeSpec (per-branch spec shape)
#include <hgraph/types/value/value.h>

#include <cstddef>
#include <optional>
#include <vector>

namespace hgraph
{
    /**
     * One ``switch_`` branch: the key value that selects it, plus the compiled
     * child graph in the single-nested spec shape (child builder + boundary
     * bindings). Binding ``source_path``s are over the **outer** node's input
     * root: ``{0}`` is the key input, ``{1..}`` the time-series arguments — a
     * key-consuming branch simply binds outer input ``0``.
     */
    struct HGRAPH_EXPORT SwitchBranch
    {
        Value                     key{};
        SingleNestedGraphNodeSpec spec{};
    };

    struct HGRAPH_EXPORT SwitchNodeSpec
    {
        std::vector<SwitchBranch>                branches{};
        std::optional<SingleNestedGraphNodeSpec> default_branch{};
        bool                                     reload_on_ticked{false};
        /**
         * Preserve branch terminals that own forwarding or structural endpoint
         * topology. The switch output then forwards to the active terminal
         * instead of re-homing that terminal onto switch-owned storage.
         */
        bool output_forwards_to_child_terminal{false};
    };

    /**
     * Typed extension view exposed by ``switch_node`` (the runtime inspection
     * surface, mirroring ``SingleNestedGraphNodeView``).
     */
    class HGRAPH_EXPORT SwitchNodeView
    {
      public:
        [[nodiscard]] static const void *node_view_type_id() noexcept;
        [[nodiscard]] static SwitchNodeView from_node(NodeView view, const void *context);

        [[nodiscard]] const NodeView &node() const noexcept;
        [[nodiscard]] bool            has_active_branch() const noexcept;
        [[nodiscard]] GraphValue     &active_graph_value() const noexcept;
        [[nodiscard]] const Value    &active_key() const noexcept;
        /** Number of constructed active/previous branch graphs in the two fixed slots. */
        [[nodiscard]] std::size_t     stored_graph_count() const noexcept;
        /** True when every constructed branch graph resides in its fixed node-storage slot. */
        [[nodiscard]] bool            child_graphs_use_in_place_storage() const noexcept;

        /** Internal (switch_node implementation) — the registered context / storage. */
        [[nodiscard]] const void *internal_context() const noexcept { return context_; }
        [[nodiscard]] void       *internal_storage() const noexcept { return storage_; }

      private:
        SwitchNodeView(NodeView view, const void *context, void *storage) noexcept;

        NodeView    view_{};
        const void *context_{nullptr};
        void       *storage_{nullptr};
    };

    /**
     * Build a node with two fixed child-graph memory slots, selected by its first
     * (``key``) input per ``switch_`` semantics: on a key change (or any key
     * tick with ``reload_on_ticked``) the inactive slot is reused for the new
     * branch and the old active child is stopped. That stopped child remains in
     * the previous slot until the following switch, when its destructor runs
     * before the slot is reused. The switch node normally owns one fixed output
     * and branches write into it directly. If a branch terminal already owns a
     * forwarding or structural endpoint, the switch preserves that topology and
     * its output forwards to the active terminal instead. A VALUE branch under a
     * REF-shaped switch owns its output in its graph slot and the switch publishes
     * a reference to that terminal.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder switch_node(NodeTypeMetaData meta, SwitchNodeSpec spec);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_SWITCH_NODE_H
