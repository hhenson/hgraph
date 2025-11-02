//
// Created by Howard Henson on 26/12/2024.
//

#ifndef SWITCH_NODE_BUILDER_H
#define SWITCH_NODE_BUILDER_H

#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/graph_builder.h>
#include <unordered_map>

namespace hgraph {
    struct BaseSwitchNodeBuilder : BaseNodeBuilder {
        using BaseNodeBuilder::BaseNodeBuilder;
    };

    template<typename K>
    struct SwitchNodeBuilder : BaseSwitchNodeBuilder {
        using key_type = K;

        SwitchNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                          std::optional<input_builder_ptr> input_builder_ = std::nullopt,
                          std::optional<output_builder_ptr> output_builder_ = std::nullopt,
                          std::optional<output_builder_ptr> error_builder_ = std::nullopt,
                          std::optional<output_builder_ptr> recordable_state_builder_ = std::nullopt,
                          const std::unordered_map<K, graph_builder_ptr> &nested_graph_builders = {},
                          const std::unordered_map<K, std::unordered_map<std::string, int> > &input_node_ids = {},
                          const std::unordered_map<K, int> &output_node_ids = {},
                          bool reload_on_ticked = false,
                          graph_builder_ptr default_graph_builder = nullptr,
                          const std::unordered_map<std::string, int> &default_input_node_ids = {},
                          int default_output_node_id = -1);

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;

        const std::unordered_map<K, graph_builder_ptr> nested_graph_builders;
        const std::unordered_map<K, std::unordered_map<std::string, int> > input_node_ids;
        const std::unordered_map<K, int> output_node_ids;
        bool reload_on_ticked;
        graph_builder_ptr default_graph_builder;
        const std::unordered_map<std::string, int> default_input_node_ids;
        int default_output_node_id;
    };

    void switch_node_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // SWITCH_NODE_BUILDER_H