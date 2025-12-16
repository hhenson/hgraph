//
// Created by Howard Henson on 19/10/2025.
//

#ifndef HGRAPH_CPP_ENGINE_PYTHON_GENERATOR_NODE_H
#define HGRAPH_CPP_ENGINE_PYTHON_GENERATOR_NODE_H

#include <hgraph/nodes/base_python_node.h>

namespace hgraph {
    struct PythonGeneratorNode final : BasePythonNode {
        using BasePythonNode::BasePythonNode;
        nb::iterator generator{};
        nb::object next_value{};

        VISITOR_SUPPORT()

    protected:
        void do_eval() override;

        void start() override;
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_PYTHON_GENERATOR_NODE_H