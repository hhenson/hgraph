#ifndef HGRAPH_RUNTIME_NESTED_GRAPH_NODE_H
#define HGRAPH_RUNTIME_NESTED_GRAPH_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/graph.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace hgraph
{
    /**
     * A path to a time-series endpoint inside a node output.
     *
     * ``node`` selects a node in the child graph. ``path`` then walks through
     * indexed structural output children (TSB field index or TSL index).
     */
    struct HGRAPH_EXPORT NestedGraphEndpoint
    {
        std::size_t              node{0};
        std::vector<std::size_t> path{};
    };

    /**
     * Bind one outer node input position into a child graph input position.
     *
     * ``source_path`` walks from the outer node input root to a peered input
     * endpoint. ``target`` selects the child node input endpoint that should
     * receive the same upstream output binding.
     */
    struct HGRAPH_EXPORT NestedGraphInputBinding
    {
        std::vector<std::size_t> source_path{};
        NestedGraphEndpoint      target{};
    };

    /**
     * Forward a time-series through an outer node output position.
     *
     * ``ChildOutput`` forwards a child node's output endpoint. ``ParentInput``
     * aliases the upstream output the outer node's own input is bound to — the
     * pass-through mode (the 2603 RFC's ``alias_parent_input``), produced when a
     * sub-graph returns a boundary input directly.
     */
    struct HGRAPH_EXPORT NestedGraphOutputBinding
    {
        enum class Kind : std::uint8_t
        {
            ChildOutput,
            ParentInput,
        };

        Kind                     kind{Kind::ChildOutput};
        NestedGraphEndpoint      source{};              // ChildOutput: child node output endpoint
        std::vector<std::size_t> parent_source_path{};  // ParentInput: path within the outer input root
        std::vector<std::size_t> target_path{};
    };

    struct HGRAPH_EXPORT SingleNestedGraphNodeSpec
    {
        GraphBuilder                            graph_builder{};
        std::vector<NestedGraphInputBinding>    input_bindings{};
        std::optional<NestedGraphOutputBinding> output_binding{};
    };

    struct HGRAPH_EXPORT SingleNestedGraphNodeOptions
    {
        bool start_child_on_start{true};
        bool stop_child_on_stop{true};
        bool propagate_child_schedule{true};
        // When set, the descriptor records ``spec.output_binding`` for the
        // owning node to consult but does NOT set up output forwarding (the node
        // owns its output and writes it from its own callbacks). The
        // default-forwarding helpers (``single_nested_graph_bind_output``) are
        // then not used.
        bool manage_output_externally{false};
    };

    struct HGRAPH_EXPORT SingleNestedGraphNodeContext
    {
        SingleNestedGraphNodeSpec    spec{};
        std::size_t                  graph_storage_offset{0};
        std::size_t                  graph_memory_offset{0};
        MemoryUtils::StorageLayout   graph_memory_layout{};
        SingleNestedGraphNodeOptions options{};
    };

    /**
     * Typed extension view exposed by ``single_nested_graph_node``.
     *
     * Policy wrappers such as try/catch or delayed component/context nodes can
     * build on this view and supply their own callbacks while reusing the same
     * storage and child graph binding model.
     */
    class HGRAPH_EXPORT SingleNestedGraphNodeView
    {
      public:
        [[nodiscard]] static const void *node_view_type_id() noexcept;
        [[nodiscard]] static SingleNestedGraphNodeView from_node(NodeView view, const void *context);

        [[nodiscard]] const NodeView &node() const noexcept;
        [[nodiscard]] const SingleNestedGraphNodeContext &context() const noexcept;
        [[nodiscard]] GraphValue &child_graph_value() const noexcept;
        [[nodiscard]] GraphView child_graph() const;

        void ensure_child_graph() const;

      private:
        SingleNestedGraphNodeView(NodeView view,
                                  const SingleNestedGraphNodeContext &context,
                                  GraphValue &child_graph) noexcept;

        NodeView                            view_{};
        const SingleNestedGraphNodeContext *context_{nullptr};
        GraphValue                         *child_graph_{nullptr};
    };

    /**
     * Build the generic single-child-graph node descriptor. Callers can adjust
     * callbacks or ops before passing it to ``NodeBuilder::from_descriptor``.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeTypeDescriptor single_nested_graph_node_descriptor(
        NodeTypeMetaData meta,
        SingleNestedGraphNodeSpec spec,
        SingleNestedGraphNodeOptions options = {});

    /** Build a node that owns and evaluates exactly one child graph. */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder single_nested_graph_node(
        NodeTypeMetaData meta,
        SingleNestedGraphNodeSpec spec,
        SingleNestedGraphNodeOptions options = {});

    HGRAPH_EXPORT void single_nested_graph_start(const NodeView &view, DateTime evaluation_time);
    HGRAPH_EXPORT void single_nested_graph_stop(const NodeView &view, DateTime evaluation_time);
    /// Evaluates the single child graph; returns false if the child paused (the pause
    /// propagates to the caller, which re-evaluates to resume — re-binding is idempotent).
    HGRAPH_EXPORT bool single_nested_graph_evaluate(const NodeView &view, DateTime evaluation_time);
    HGRAPH_EXPORT void single_nested_graph_bind_inputs(const SingleNestedGraphNodeView &nested,
                                                       DateTime evaluation_time);
    HGRAPH_EXPORT void single_nested_graph_bind_output(const SingleNestedGraphNodeView &nested,
                                                       DateTime evaluation_time);
    HGRAPH_EXPORT void single_nested_graph_clear_output_binding(const SingleNestedGraphNodeView &nested,
                                                                DateTime evaluation_time);
    HGRAPH_EXPORT void single_nested_graph_propagate_schedule(const SingleNestedGraphNodeView &nested);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NESTED_GRAPH_NODE_H
