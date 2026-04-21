#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/child_graph.h>
#include <hgraph/types/node_builder.h>
#include <hgraph/types/time_series/value/value.h>

namespace hgraph
{
    /**
     * Builder state for a nested graph node, stored in NodeBuilder::m_type_state.
     *
     * Contains a pointer to the registry-owned ChildGraphTemplate that all
     * instances share.
     */
    struct NestedNodeBuilderState
    {
        const ChildGraphTemplate *child_template{nullptr};
    };

    struct MapNodeBuilderState
    {
        const ChildGraphTemplate *child_template{nullptr};
        std::string key_arg;
        std::string keys_arg;
        std::vector<std::string> multiplexed_args;
    };

    struct ReduceNodeBuilderState
    {
        const ChildGraphTemplate *child_template{nullptr};
    };

    struct NonAssociativeReduceNodeBuilderState
    {
        const ChildGraphTemplate *child_template{nullptr};
    };

    struct SwitchBranchTemplate
    {
        Value selector_value;
        const ChildGraphTemplate *child_template{nullptr};
        bool is_default{false};
    };

    /**
     * Per-instance runtime data for a nested graph node, placed in the slab chunk.
     *
     * Contains the standard input/output pointers plus the child graph instance
     * and any nested-operator-specific state.
     */
    struct NestedNodeRuntimeData
    {
        TSInput           *input{nullptr};
        TSOutput          *output{nullptr};
        TSOutput          *error_output{nullptr};
        TSOutput          *recordable_state{nullptr};
        const ChildGraphTemplate *child_template{nullptr};
        ChildGraphInstance child_instance;
        bool               bound{false};
    };

    struct MapNodeRuntimeData
    {
        TSInput           *input{nullptr};
        TSOutput          *output{nullptr};
        TSOutput          *error_output{nullptr};
        TSOutput          *recordable_state{nullptr};
        const ChildGraphTemplate *child_template{nullptr};
        std::string        key_arg;
        std::string        keys_arg;
        std::vector<std::string> multiplexed_args;
        int64_t            next_child_graph_id{1};
        bool               slot_store_initialized{false};
    };

    /**
     * Configure a NodeBuilder for a simple nested graph operator.
     *
     * This creates a node that:
     * - On start: initialises and starts a child graph from the template
     * - On eval: binds child inputs (if not yet bound), evaluates the child graph,
     *            and forwards the child's output to the parent's output
     * - On stop: stops and disposes the child graph
     *
     * The caller must set input_schema and output_schema on the builder before
     * calling this method. The child template must be registry-owned and must
     * outlive the builder and all nodes built from it.
     */
    HGRAPH_EXPORT NodeBuilder &nested_graph_implementation(NodeBuilder &builder, const ChildGraphTemplate *child_template);

    /**
     * Configure a NodeBuilder for a try_except nested graph operator.
     *
     * Same as nested_graph_implementation but wraps child evaluation in
     * try/catch. On exception the error is written to the node's public
     * output (`TS[NodeError]` for sink graphs, or the `.exception` field
     * of the `TryExceptResult` bundle) and the child graph is stopped.
     *
     * For graph outputs, the output schema should be a TSB with `exception`
     * and `out` members. For sink graphs, the output schema should be
     * `TS[NodeError]`.
     */
    HGRAPH_EXPORT NodeBuilder &try_except_graph_implementation(NodeBuilder &builder, const ChildGraphTemplate *child_template);

    HGRAPH_EXPORT NodeBuilder &map_graph_implementation(NodeBuilder &builder,
                                                        const ChildGraphTemplate *child_template,
                                                        std::string key_arg,
                                                        std::string keys_arg,
                                                        std::vector<std::string> multiplexed_args);

    HGRAPH_EXPORT NodeBuilder &reduce_graph_implementation(NodeBuilder &builder, const ChildGraphTemplate *child_template);

    HGRAPH_EXPORT NodeBuilder &non_associative_reduce_graph_implementation(NodeBuilder &builder,
                                                                           const ChildGraphTemplate *child_template);

    HGRAPH_EXPORT NodeBuilder &switch_graph_implementation(NodeBuilder &builder,
                                                           std::vector<SwitchBranchTemplate> branches,
                                                           bool reload_on_ticked);

}  // namespace hgraph
