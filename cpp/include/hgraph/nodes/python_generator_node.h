//
// Created by Howard Henson on 19/10/2025.
//

#ifndef HGRAPH_CPP_ENGINE_PYTHON_GENERATOR_NODE_H
#define HGRAPH_CPP_ENGINE_PYTHON_GENERATOR_NODE_H

#include <hgraph/nodes/base_python_node.h>

namespace hgraph {
    struct PythonGeneratorNode final : BasePythonNode {
        // Legacy constructor
        PythonGeneratorNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                            nb::dict scalars, nb::callable compute_fn, nb::callable start_fn, nb::callable stop_fn)
            : BasePythonNode(node_ndx, std::move(owning_graph_id), std::move(signature),
                             std::move(scalars), std::move(compute_fn), std::move(start_fn), std::move(stop_fn)) {}

        // Constructor with TSMeta
        PythonGeneratorNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                            nb::dict scalars, nb::callable compute_fn, nb::callable start_fn, nb::callable stop_fn,
                            const TSMeta* input_meta, const TSMeta* output_meta,
                            const TSMeta* error_output_meta = nullptr, const TSMeta* recordable_state_meta = nullptr)
            : BasePythonNode(node_ndx, std::move(owning_graph_id), std::move(signature),
                             std::move(scalars), std::move(compute_fn), std::move(start_fn), std::move(stop_fn),
                             input_meta, output_meta, error_output_meta, recordable_state_meta) {}

        nb::iterator generator{};
        nb::object next_value{};

        VISITOR_SUPPORT()

    protected:
        void do_eval() override;

        void start() override;
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_PYTHON_GENERATOR_NODE_H
