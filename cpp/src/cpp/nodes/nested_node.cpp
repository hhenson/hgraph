#include <hgraph/nodes/nested_node.h>
#include <hgraph/types/graph.h>

namespace hgraph {
    void NestedNode::start() {
        // NestedNode needs to initialize inputs like BasePythonNode does
        // This ensures inputs are made active based on the node signature
        _initialise_inputs();
        // Now call parent Node::start() which calls do_start()
        Node::start();
    }

    engine_time_t NestedNode::last_evaluation_time() const { return _last_evaluation_time; }

    void NestedNode::mark_evaluated() { _last_evaluation_time = graph()->evaluation_clock()->evaluation_time(); }

    void NestedNode::register_with_nanobind(nb::module_ &m) {
        nb::class_<NestedNode, Node>(m, "NestedNode")
                .def_prop_ro("last_evaluation_time", &NestedNode::last_evaluation_time);
    }
} // namespace hgraph