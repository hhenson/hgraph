//
// Created by Howard Henson on 26/12/2024.
//

#ifndef NODE_BUILDER_H
#define NODE_BUILDER_H

#include <hgraph/builders/builder.h>

namespace hgraph {
    struct NodeBuilder : Builder {
        NodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                    std::optional<input_builder_ptr> input_builder_ = std::nullopt,
                    std::optional<output_builder_ptr> output_builder_ = std::nullopt,
                    std::optional<output_builder_ptr> error_builder_ = std::nullopt,
                    std::optional<output_builder_ptr> recordable_state_builder_ = std::nullopt);

        // Explicitly define move operations to avoid leaving Python-visible instances in a moved-from (null) state.
        NodeBuilder(NodeBuilder &&other) noexcept;

        NodeBuilder &operator=(NodeBuilder &&other) noexcept;

        // Default copy is fine (nb::ref increases refcount)
        NodeBuilder(const NodeBuilder &) = default;

        NodeBuilder &operator=(const NodeBuilder &) = default;

        /**
         * Construct a node instance. If buffer is provided, uses arena allocation (in-place construction).
         * Otherwise, uses heap allocation (legacy path for extending graphs).
         * @param owning_graph_id The graph id that owns this node
         * @param node_ndx The index of this node
         * @param buffer Pointer to the arena buffer (nullptr for heap allocation)
         * @param offset Current offset in the buffer (will be updated, ignored if buffer is nullptr)
         * @return shared_ptr to the constructed Node
         */
        virtual node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx,
                                       void* buffer = nullptr, size_t* offset = nullptr) const = 0;

        virtual void release_instance(node_ptr &item) const;

        [[nodiscard]] size_t memory_size() const override;

        static void register_with_nanobind(nb::module_ &m);

    protected:
        /**
         * Helper method to calculate the total size of all time-series builders.
         * Used by memory_size() implementations.
         */
        [[nodiscard]] size_t _calculate_time_series_builders_size() const;

    public:
        node_signature_ptr signature;
        nb::dict scalars;
        std::optional<input_builder_ptr> input_builder;
        std::optional<output_builder_ptr> output_builder;
        std::optional<output_builder_ptr> error_builder;
        std::optional<output_builder_ptr> recordable_state_builder;
    };

    struct BaseNodeBuilder : NodeBuilder {
        using NodeBuilder::NodeBuilder;

    protected:
        /**
         * Build inputs and outputs for a node. If buffer is provided, uses arena allocation (in-place construction).
         * Otherwise, uses heap allocation (legacy path).
         * @param node The node to build inputs/outputs for
         * @param buffer Pointer to the arena buffer (nullptr for heap allocation)
         * @param offset Current offset in the buffer (will be updated, ignored if buffer is nullptr)
         */
        void _build_inputs_and_outputs(node_ptr node, void* buffer = nullptr, size_t* offset = nullptr) const;

        /**
         * Helper method to calculate memory size for a node builder.
         * Takes the node type size and adds sizes for all time-series builders.
         * @param node_size Size of the node type (e.g., sizeof(PythonNode))
         * @return Total memory size including node, canary, and all time-series builders
         */
        [[nodiscard]] size_t _calculate_memory_size(size_t node_size) const;
    };
} // namespace hgraph

#endif  // NODE_BUILDER_H