//
// Created by Howard Henson on 26/12/2024.
//

#ifndef TSD_MAP_NODE_BUILDER_H
#define TSD_MAP_NODE_BUILDER_H

#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/graph_builder.h>
#include <unordered_map>
#include <unordered_set>

namespace hgraph {
    /**
     * Non-templated TsdMapNodeBuilder.
     * The key type is handled dynamically via the keys input.
     */
    struct TsdMapNodeBuilder : BaseNodeBuilder {
        TsdMapNodeBuilder(node_signature_s_ptr signature_, nb::dict scalars_,
                          std::optional<input_builder_s_ptr> input_builder_ = std::nullopt,
                          std::optional<output_builder_s_ptr> output_builder_ = std::nullopt,
                          std::optional<output_builder_s_ptr> error_builder_ = std::nullopt,
                          std::optional<output_builder_s_ptr> recordable_state_builder_ = std::nullopt,
                          graph_builder_s_ptr nested_graph_builder = {},
                          const std::unordered_map<std::string, int64_t> &input_node_ids = {},
                          int64_t output_node_id = -1,
                          const std::unordered_set<std::string> &multiplexed_args = {},
                          const std::string &key_arg = {});

        node_s_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;

        graph_builder_s_ptr nested_graph_builder;
        const std::unordered_map<std::string, int64_t> input_node_ids;
        int64_t output_node_id;
        const std::unordered_set<std::string> multiplexed_args;
        const std::string key_arg;
    };

    void tsd_map_node_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TSD_MAP_NODE_BUILDER_H
