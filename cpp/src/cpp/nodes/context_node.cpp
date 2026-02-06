#include <hgraph/nodes/context_node.h>
#include <nanobind/nanobind.h>
#include <stdexcept>

namespace hgraph {
    void ContextStubSourceNode::do_start() {
        _subscribed_output = nullptr;
        notify();
    }

    void ContextStubSourceNode::do_stop() {
        _subscribed_output = nullptr;
    }

    void ContextStubSourceNode::do_eval() {
        throw std::runtime_error("not implemented: ContextStubSourceNode::do_eval");
    }

    void register_context_node_with_nanobind(nb::module_ &m) {
        nb::class_<ContextStubSourceNode, Node>(m, "ContextStubSourceNode");
    }
} // namespace hgraph