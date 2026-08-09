#ifndef HGRAPH_RUNTIME_MAP_NODE_H
#define HGRAPH_RUNTIME_MAP_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/nested_graph_node.h>   // SingleNestedGraphNodeSpec (the child template shape)
#include <hgraph/types/value/value.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace hgraph
{
    /** How one child boundary argument of a ``map_`` child graph is sourced. */
    enum class MapArgSourceKind : std::uint8_t
    {
        Key,         ///< the per-key constant (an entry-owned ``TS<K>`` output)
        Element,     ///< the multiplexed TSD/TSL child output at the entry's key/index
        OuterInput,  ///< an outer (broadcast) input, bound whole
    };

    enum class MapOutputBindingMode : std::uint8_t
    {
        /** Child terminal writes through to the map output element. */
        ChildTerminalWritesElement,
        /** Map output element forwards through the child terminal's existing forwarding source. */
        OutputElementForwardsToChildTerminal,
        /** Map output element forwards through the child's returned parent-input source. */
        OutputElementForwardsToParentSource,
    };

    struct HGRAPH_EXPORT MapArgSource
    {
        MapArgSourceKind kind{MapArgSourceKind::OuterInput};
        /** ``Element`` / ``OuterInput``: index of the source within the outer input root. */
        std::size_t      outer_index{0};
    };

    struct HGRAPH_EXPORT MapNodeSpec
    {
        /** The child template; binding ``source_path[0]`` is the boundary arg ordinal. */
        SingleNestedGraphNodeSpec child{};
        /** Per boundary arg ordinal: how the child argument is sourced. */
        std::vector<MapArgSource> args{};
        /**
         * Outer-input indices of ALL multiplexed TSDs. The live key set is
         * their **union** (Python parity): a key builds a child when it
         * appears in any of them and the child (plus its output entry, when
         * present) is destroyed only when it has left all of them — unless an
         * explicit ``__keys__`` set is wired (below), which then drives the
         * lifecycle alone.
         */
        std::vector<std::size_t> multiplexed_inputs{};
        /**
         * Outer-input index of the explicit ``__keys__`` ``TSS[K]`` (Python's
         * ``__keys__`` argument), when supplied: children exist exactly for
         * the set's members; the multiplexed dicts only feed elements
         * (absent keys stay phantom/invalid).
         */
        std::optional<std::size_t> keys_input_index{};
        /** ``TS<K>`` for the entry-owned key outputs (when any arg sources ``Key``). */
        const TSValueTypeMetaData *key_output_schema{nullptr};
        /** Child-terminal connection direction; ignored for sink maps. */
        MapOutputBindingMode output_binding_mode{MapOutputBindingMode::ChildTerminalWritesElement};
    };

    /** Typed extension view exposed by ``map_node`` (runtime inspection surface). */
    class HGRAPH_EXPORT MapNodeView
    {
      public:
        [[nodiscard]] static const void *node_view_type_id() noexcept;
        [[nodiscard]] static MapNodeView from_node(NodeView view, const void *context);

        [[nodiscard]] const NodeView &node() const noexcept;
        [[nodiscard]] std::size_t     active_count() const noexcept;
        /** Number of constructed per-key child graphs, including stopped entries pending erase. */
        [[nodiscard]] std::size_t     child_graph_count() const noexcept;
        /** True when every constructed child graph resides in its stable entry slot. */
        [[nodiscard]] bool            child_graphs_use_in_place_storage() const noexcept;

        /** Internal (map_node implementation) — the registered context / storage. */
        [[nodiscard]] const void *internal_context() const noexcept { return context_; }
        [[nodiscard]] void       *internal_storage() const noexcept { return storage_; }

      private:
        MapNodeView(NodeView view, const void *context, void *storage) noexcept;

        NodeView    view_{};
        const void *context_{nullptr};
        void       *storage_{nullptr};
    };

    /**
     * Build a node owning **one child graph per key** of its ``__keys__``
     * input. Wiring derives that key set from the multiplexed TSD inputs when
     * the caller does not supply it explicitly. A new key builds, binds and
     * starts a fresh child instance. For an output map it also instantiates a
     * real element in the owned TSD and binds the child's terminal
     * **forwarding output onto that element**, so the child writes the parent's
     * storage directly (no copy). A sink map omits the parent output entirely.
     * A missing key stops the child and removes any output element; the source
     * slot's later erase destroys the child in place (see *Nested Graphs*).
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder map_node(NodeTypeMetaData meta, MapNodeSpec spec);

    /**
     * Rebuild an existing TSD ``map_`` node with a sparse per-key error
     * output. ``error_element_schema`` is the error series for one child
     * (normally ``TS<NodeError>``); the hidden node output is
     * ``TSD<K, error_element_schema>`` using the map's existing key type.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder map_node_with_error_capture(
        const NodeBuilder &builder,
        const TSValueTypeMetaData *error_element_schema,
        ErrorCaptureOptions options = {});
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_MAP_NODE_H
