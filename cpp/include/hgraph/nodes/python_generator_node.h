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

    private:
        // Helper methods for TSOutput/legacy output compatibility
        [[nodiscard]] engine_time_t _get_last_modified_time();
        void _apply_output_result(const nb::object& result);
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_PYTHON_GENERATOR_NODE_H