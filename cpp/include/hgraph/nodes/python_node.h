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
    struct PythonNode : BasePythonNode {
        using BasePythonNode::BasePythonNode;

        const nb::callable &eval_fn();
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_PYTHON_NODE_H