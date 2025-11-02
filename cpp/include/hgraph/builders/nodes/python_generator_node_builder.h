//
// Created by Howard Henson on 26/12/2024.
//

#ifndef PYTHON_GENERATOR_NODE_BUILDER_H
#define PYTHON_GENERATOR_NODE_BUILDER_H

#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>

namespace hgraph {
    struct PythonGeneratorNodeBuilder : BaseNodeBuilder {
        PythonGeneratorNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                   std::optional<input_builder_ptr> input_builder_,
                                   std::optional<output_builder_ptr> output_builder_,
                                   std::optional<output_builder_ptr> error_builder_, nb::callable eval_fn);

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;

        nb::callable eval_fn;
    };

    void python_generator_node_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // PYTHON_GENERATOR_NODE_BUILDER_H