//
// Created by Howard Henson on 26/12/2024.
//

#ifndef BASE_NESTED_GRAPH_NODE_BUILDER_H
#define BASE_NESTED_GRAPH_NODE_BUILDER_H

#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <unordered_map>

namespace hgraph {
    struct BaseNestedGraphNodeBuilder : BaseNodeBuilder {
        BaseNestedGraphNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                   std::optional<input_builder_ptr> input_builder_ = std::nullopt,
                                   std::optional<output_builder_ptr> output_builder_ = std::nullopt,
                                   std::optional<output_builder_ptr> error_builder_ = std::nullopt,
                                   std::optional<output_builder_ptr> recordable_state_builder_ = std::nullopt,
                                   graph_builder_ptr nested_graph_builder = {},
                                   const std::unordered_map<std::string, int> &input_node_ids = {},
                                   int output_node_id = -1);

        graph_builder_ptr nested_graph_builder;
        const std::unordered_map<std::string, int> input_node_ids;
        int output_node_id;
    };

    void base_nested_graph_node_builder_register_with_nanobind(nb::module_ & m);

    // Helper template function for creating nested graph node builders
    template<typename T>
    auto create_nested_graph_node_builder(T *self, const nb::args &args) {
        // Expected Python signature (positional):
        // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
        //  nested_graph, input_node_ids, output_node_id)
        if (args.size() != 9) {
            throw nb::type_error("NestedGraphNodeBuilder expects 9 positional arguments: "
                "(signature, scalars, input_builder, output_builder, error_builder, "
                "recordable_state_builder, nested_graph, input_node_ids, output_node_id)");
        }

        auto signature_ = nb::cast<node_signature_ptr>(args[0]);
        auto scalars_ = nb::cast<nb::dict>(args[1]);
        std::optional<input_builder_ptr> input_builder_ =
                args[2].is_none()
                    ? std::nullopt
                    : std::optional<input_builder_ptr>(nb::cast<input_builder_ptr>(args[2]));
        std::optional<output_builder_ptr> output_builder_ =
                args[3].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[3]));
        std::optional<output_builder_ptr> error_builder_ =
                args[4].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[4]));
        std::optional<output_builder_ptr> recordable_state_builder_ =
                args[5].is_none()
                    ? std::nullopt
                    : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[5]));
        auto nested_graph_builder = nb::cast<graph_builder_ptr>(args[6]);
        auto input_node_ids = nb::cast<std::unordered_map<std::string, int> >(args[7]);
        auto output_node_id = nb::cast<int>(args[8]);

        return new(self) T(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                           std::move(output_builder_),
                           std::move(error_builder_), std::move(recordable_state_builder_),
                           std::move(nested_graph_builder),
                           std::move(input_node_ids), std::move(output_node_id));
    }
} // namespace hgraph

#endif  // BASE_NESTED_GRAPH_NODE_BUILDER_H