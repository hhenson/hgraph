//
// Created by Howard Henson on 26/12/2024.
//

#ifndef MESH_NODE_BUILDER_H
#define MESH_NODE_BUILDER_H

#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/graph_builder.h>
#include <unordered_map>
#include <unordered_set>

namespace hgraph {
    struct BaseMeshNodeBuilder : BaseNodeBuilder {
        BaseMeshNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                            std::optional<input_builder_ptr> input_builder_ = std::nullopt,
                            std::optional<output_builder_ptr> output_builder_ = std::nullopt,
                            std::optional<output_builder_ptr> error_builder_ = std::nullopt,
                            std::optional<output_builder_ptr> recordable_state_builder_ = std::nullopt,
                            graph_builder_ptr nested_graph_builder = {},
                            const std::unordered_map<std::string, int64_t> &input_node_ids = {},
                            int64_t output_node_id = -1,
                            const std::unordered_set<std::string> &multiplexed_args = {},
                            const std::string &key_arg = {},
                            const std::string &context_path = {});

        graph_builder_ptr nested_graph_builder;
        const std::unordered_map<std::string, int64_t> input_node_ids;
        int64_t output_node_id;
        const std::unordered_set<std::string> multiplexed_args;
        const std::string key_arg;
        const std::string context_path;
    };

    template<typename T>
    struct MeshNodeBuilder : BaseMeshNodeBuilder {
        using BaseMeshNodeBuilder::BaseMeshNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    void mesh_node_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // MESH_NODE_BUILDER_H