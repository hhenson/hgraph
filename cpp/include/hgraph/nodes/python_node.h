//
// Created by Howard Henson on 24/10/2025.
//

#ifndef HGRAPH_CPP_ENGINE_PYTHON_NODE_H
#define HGRAPH_CPP_ENGINE_PYTHON_NODE_H

#include <hgraph/nodes/base_python_node.h>

namespace hgraph {
    /**
     * PythonNode - Standard Python compute node
     *
     * Simple wrapper around BasePythonNode that provides access to the eval function.
     * Most functionality is inherited from BasePythonNode.
     */
    struct PythonNode final : BasePythonNode {
        // Legacy constructor
        PythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                   nb::dict scalars, nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn)
            : BasePythonNode(node_ndx, std::move(owning_graph_id), std::move(signature),
                             std::move(scalars), std::move(eval_fn), std::move(start_fn), std::move(stop_fn)) {}

        // Constructor with TSMeta
        PythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                   nb::dict scalars, nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn,
                   const TSMeta* input_meta, const TSMeta* output_meta,
                   const TSMeta* error_output_meta = nullptr, const TSMeta* recordable_state_meta = nullptr)
            : BasePythonNode(node_ndx, std::move(owning_graph_id), std::move(signature),
                             std::move(scalars), std::move(eval_fn), std::move(start_fn), std::move(stop_fn),
                             input_meta, output_meta, error_output_meta, recordable_state_meta) {}

        const nb::callable &eval_fn();

        VISITOR_SUPPORT()
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_PYTHON_NODE_H