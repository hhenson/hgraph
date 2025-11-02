//
// Created by Howard Henson on 26/12/2024.
//

#ifndef REDUCE_NODE_BUILDER_H
#define REDUCE_NODE_BUILDER_H

#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/graph_builder.h>
#include <tuple>

namespace hgraph {
    struct BaseReduceNodeBuilder : BaseNodeBuilder {
        BaseReduceNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                              std::optional<input_builder_ptr> input_builder_ = std::nullopt,
                              std::optional<output_builder_ptr> output_builder_ = std::nullopt,
                              std::optional<output_builder_ptr> error_builder_ = std::nullopt,
                              std::optional<output_builder_ptr> recordable_state_builder_ = std::nullopt,
                              graph_builder_ptr nested_graph_builder = {},
                              const std::tuple<int64_t, int64_t> &input_node_ids = {}, int64_t output_node_id = -1);

        graph_builder_ptr nested_graph_builder;
        std::tuple<int64_t, int64_t> input_node_ids;
        int64_t output_node_id;
    };

    template<typename T>
    struct ReduceNodeBuilder : BaseReduceNodeBuilder {
        using BaseReduceNodeBuilder::BaseReduceNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    void reduce_node_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // REDUCE_NODE_BUILDER_H