//
// Created by Howard Henson on 19/10/2025.
//

#ifndef HGRAPH_CPP_ENGINE_PYTHON_GENERATOR_NODE_H
#define HGRAPH_CPP_ENGINE_PYTHON_GENERATOR_NODE_H

#include <hgraph/nodes/base_python_node.h>

namespace hgraph {
    struct PythonGeneratorNode : BasePythonNode {
        using BasePythonNode::BasePythonNode;
        nb::iterator generator{};
        nb::object next_value{};

        static void register_with_nanobind(nb::module_ &m);

    protected:
        void do_eval() override;

        void start() override;
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_PYTHON_GENERATOR_NODE_H