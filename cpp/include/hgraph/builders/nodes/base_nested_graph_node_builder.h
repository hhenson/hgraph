//
// Created by Howard Henson on 26/12/2024.
//

#ifndef BASE_NESTED_GRAPH_NODE_BUILDER_H
#define BASE_NESTED_GRAPH_NODE_BUILDER_H

#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/graph_builder.h>
#include <unordered_map>

namespace hgraph {
    struct BaseNestedGraphNodeBuilder : BaseNodeBuilder {
        BaseNestedGraphNodeBuilder(node_signature_s_ptr signature_, nb::dict scalars_,
                                   graph_builder_s_ptr nested_graph_builder = {},
                                   const std::unordered_map<std::string, int> &input_node_ids = {},
                                   int output_node_id = -1);

        graph_builder_s_ptr nested_graph_builder;
        const std::unordered_map<std::string, int> input_node_ids;
        int output_node_id;
    };

    void base_nested_graph_node_builder_register_with_nanobind(nb::module_ & m);

    // Helper template function for creating nested graph node builders
    template<typename T>
    auto create_nested_graph_node_builder(T *self, const nb::args &args) {
        // Signature (positional): (signature, scalars, nested_graph, input_node_ids, output_node_id)
        if (args.size() != 5) {
            throw nb::type_error("NestedGraphNodeBuilder expects 5 positional arguments: "
                "(signature, scalars, nested_graph, input_node_ids, output_node_id)");
        }

        auto signature_ = nb::cast<node_signature_s_ptr>(args[0]);
        auto scalars_ = nb::cast<nb::dict>(args[1]);
        const auto nested_graph_builder = nb::cast<graph_builder_s_ptr>(args[2]);
        const auto input_node_ids = nb::cast<std::unordered_map<std::string, int>>(args[3]);
        const auto output_node_id = nb::cast<int>(args[4]);

        return new(self) T(std::move(signature_), std::move(scalars_), std::move(nested_graph_builder),
                           std::move(input_node_ids), std::move(output_node_id));
    }
} // namespace hgraph

#endif  // BASE_NESTED_GRAPH_NODE_BUILDER_H
