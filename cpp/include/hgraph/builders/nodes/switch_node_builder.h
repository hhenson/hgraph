//
// Created by Howard Henson on 26/12/2024.
//

#ifndef SWITCH_NODE_BUILDER_H
#define SWITCH_NODE_BUILDER_H

#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/feature_extension.h>
#include <unordered_map>

namespace hgraph {

    /**
     * @brief Non-templated SwitchNodeBuilder using Value-based key storage.
     *
     * This builder creates SwitchNode instances with type-erased key storage.
     * Keys are stored as PlainValue with heterogeneous lookup support.
     * Maps are stored as shared_ptr and shared between builder and nodes.
     */
    struct SwitchNodeBuilder final : BaseNodeBuilder {
        using BaseNodeBuilder::BaseNodeBuilder;

        // Map types using PlainValue keys with heterogeneous lookup
        using graph_builders_map = std::unordered_map<value::PlainValue, graph_builder_s_ptr,
                                                       PlainValueHash, PlainValueEqual>;
        using input_node_ids_map = std::unordered_map<value::PlainValue, std::unordered_map<std::string, int>,
                                                       PlainValueHash, PlainValueEqual>;
        using output_node_ids_map = std::unordered_map<value::PlainValue, int,
                                                        PlainValueHash, PlainValueEqual>;

        // Shared pointer types for maps (shared between builder and node instances)
        using graph_builders_map_ptr = std::shared_ptr<graph_builders_map>;
        using input_node_ids_map_ptr = std::shared_ptr<input_node_ids_map>;
        using output_node_ids_map_ptr = std::shared_ptr<output_node_ids_map>;

        SwitchNodeBuilder(node_signature_s_ptr signature_, nb::dict scalars_,
                          std::optional<input_builder_s_ptr> input_builder_ = std::nullopt,
                          std::optional<output_builder_s_ptr> output_builder_ = std::nullopt,
                          std::optional<output_builder_s_ptr> error_builder_ = std::nullopt,
                          std::optional<output_builder_s_ptr> recordable_state_builder_ = std::nullopt,
                          const value::TypeMeta* key_type = nullptr,
                          graph_builders_map_ptr nested_graph_builders = nullptr,
                          input_node_ids_map_ptr input_node_ids = nullptr,
                          output_node_ids_map_ptr output_node_ids = nullptr,
                          bool reload_on_ticked = false,
                          graph_builder_s_ptr default_graph_builder = nullptr,
                          std::unordered_map<std::string, int> default_input_node_ids = {},
                          int default_output_node_id = -1);

        node_s_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;

        [[nodiscard]] const value::TypeMeta* key_type() const { return _key_type; }

        const value::TypeMeta* _key_type;
        graph_builders_map_ptr _nested_graph_builders;
        input_node_ids_map_ptr _input_node_ids;
        output_node_ids_map_ptr _output_node_ids;
        bool _reload_on_ticked;
        graph_builder_s_ptr _default_graph_builder;
        std::unordered_map<std::string, int> _default_input_node_ids;
        int _default_output_node_id;
    };

    void switch_node_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // SWITCH_NODE_BUILDER_H
