#ifndef TRY_EXCEPT_NODE_H
#define TRY_EXCEPT_NODE_H

#include <hgraph/nodes/nest_graph_node.h>

namespace hgraph {
    /**
     * C++ implementation of PythonTryExceptNodeImpl.
     * Extends NestedGraphNode to wrap graph evaluation with exception handling.
     */
    struct TryExceptNode final : NestedGraphNode {
        using NestedGraphNode::NestedGraphNode;

        void do_eval() override;

        VISITOR_SUPPORT()

    protected:
        void wire_outputs() override;
    };
} // namespace hgraph

#endif  // TRY_EXCEPT_NODE_H