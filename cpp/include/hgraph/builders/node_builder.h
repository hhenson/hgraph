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

        virtual node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const = 0;

        virtual void release_instance(node_ptr &item) const;

        static void register_with_nanobind(nb::module_ &m);

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
        void _build_inputs_and_outputs(node_ptr node) const;
    };
} // namespace hgraph

#endif  // NODE_BUILDER_H