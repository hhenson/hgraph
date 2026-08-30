#ifndef HGRAPH_RUNTIME_MESH_NODE_H
#define HGRAPH_RUNTIME_MESH_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/map_node.h>   // MapArgSource / MapNodeSpec child shape (reused)
#include <hgraph/types/value/value.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace hgraph
{
    /**
     * ``mesh_`` runtime node — ``map_`` over a ``TSD`` whose per-key instances
     * may read each other's outputs by key (``mesh_(func)[k]``), create instances
     * on demand when an absent key is referenced, and are evaluated from a
     * sparse, dependency-ranked worklist (a cyclic dependency is a runtime error).
     *
     * It reuses the ``map_`` per-key entry model (``MapArgSource`` for the child
     * boundary args, the child terminal forwarding into the owned ``TSD``
     * element). Beyond ``map_`` it carries:
     *
     * - a **per-instance key** output (``key_output_schema`` is always set);
     * - a **dependency graph + ranks** maintained at runtime (see ``mesh.rst``).
     *
     * See the developer guide *Mesh*.
     */
    struct HGRAPH_CLASS_EXPORT MeshNodeSpec
    {
        /** The child template (same shape as ``map_``). */
        SingleNestedGraphNodeSpec child{};
        /** Per boundary-arg ordinal: how the child argument is sourced. */
        std::vector<MapArgSource> args{};
        /** Outer-input indices of the multiplexed TSD inputs. */
        std::vector<std::size_t> multiplexed_inputs{};
        /** Outer-input index of the explicit ``__keys__`` ``TSS[K]``. */
        std::size_t keys_input_index{0};
        /** ``TS<K>`` schema for the entry-owned key outputs (always present). */
        const TSValueTypeMetaData *key_output_schema{nullptr};
        /** Direction used when connecting the child output to the mesh output element. */
        MapOutputBindingMode output_binding_mode{MapOutputBindingMode::ChildTerminalWritesElement};
    };

    /**
     * Typed extension view exposed by ``mesh_node``. ``mesh_subscribe`` reaches
     * it via the ``parent_node()`` walk to register cross-instance dependencies
     * and trigger on-demand instance creation.
     */
    class HGRAPH_CLASS_EXPORT MeshNodeView
    {
      public:
        [[nodiscard]] static const void *node_view_type_id() noexcept;
        [[nodiscard]] static MeshNodeView from_node(NodeView view, const void *context);

        [[nodiscard]] const NodeView &node() const noexcept;
        [[nodiscard]] std::size_t     active_count() const noexcept;
        /** Number of constructed instance graphs, including stopped entries pending erase. */
        [[nodiscard]] std::size_t     child_graph_count() const noexcept;
        /** True when every constructed instance graph resides in its stable key slot. */
        [[nodiscard]] bool            child_graphs_use_in_place_storage() const noexcept;

        /**
         * The key of the instance whose child graph is currently being evaluated.
         * A ``mesh_subscribe`` inside that instance reads this as its requester
         * (``my_key``) when registering a dependency. Empty outside instance eval.
         */
        [[nodiscard]] ValueView current_key() const;

        /**
         * Register that the instance for ``key`` depends on the instance for
         * ``depends_on``. Creates ``depends_on`` on demand if absent. Returns
         * true when ``depends_on``'s output is already available to ``key`` this
         * cycle (it exists and is ranked below ``key``); false when the caller
         * should wait one cycle (reschedule).
         */
        [[nodiscard]] bool add_dependency(const ValueView &key, const ValueView &depends_on) const;

        /** Drop a dependency edge (key no longer reads depends_on). */
        void remove_dependency(const ValueView &key, const ValueView &depends_on) const;

        [[nodiscard]] const void *internal_context() const noexcept { return context_; }
        [[nodiscard]] void       *internal_storage() const noexcept { return storage_; }

      private:
        MeshNodeView(NodeView view, const void *context, void *storage) noexcept;

        NodeView    view_{};
        const void *context_{nullptr};
        void       *storage_{nullptr};
    };

    /** Build a ``mesh_`` runtime node. */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder mesh_node(NodeTypeMetaData meta, MeshNodeSpec spec);

    /**
     * Build the ``mesh_subscribe`` node — wired inside a mesh instance for
     * ``mesh_(func)[item]``. Its input is a ``TSB`` ``{item: TS<K>, value:
     * OUT}``, where ``value`` is a dynamic scheduling link seeded by
     * ``nothing<OUT>``. On evaluation it locates the mesh node
     * (``parent_node()`` walk), registers ``add_dependency(my_key, item)``
     * (creating ``item`` on demand), and publishes a forwarding output bound to
     * the sibling element ``self[item]``. See *Mesh*.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder mesh_subscribe_node(NodeTypeMetaData meta);

    /**
     * Build the ``mesh_key_set`` node — wired inside a mesh instance to expose
     * the enclosing mesh's key set as a forwarding ``TSS<K>`` output.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder mesh_key_set_node(NodeTypeMetaData meta);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_MESH_NODE_H
