#ifndef TRY_EXCEPT_NODE_H
#define TRY_EXCEPT_NODE_H

#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/hgraph_forward_declarations.h>

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

    private:
        // Scratch time for the forwarded_target. The inner output stub writes
        // to this instead of the outer "out" field's real time.
        engine_time_t scratch_time_{MIN_DT};

        // Pointer to the outer "out" field's actual time storage.
        engine_time_t* out_field_time_ptr_{nullptr};
    };
} // namespace hgraph

#endif  // TRY_EXCEPT_NODE_H
