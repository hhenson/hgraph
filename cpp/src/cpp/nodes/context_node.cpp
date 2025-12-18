#include <hgraph/api/python/wrapper_factory.h>

#include <fmt/format.h>
#include <hgraph/api/python/py_ref.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>
#include <nanobind/nanobind.h>

namespace hgraph {
    void ContextStubSourceNode::do_start() {
        // TODO: V2 implementation - context nodes need V2 REF support
        notify();
    }

    void ContextStubSourceNode::do_stop() {
        // TODO: V2 implementation - context nodes need V2 REF support
    }

    void ContextStubSourceNode::do_eval() {
        // TODO: V2 implementation requires REF types and V2-compatible APIs
        // For now, stub out the implementation - context nodes need V2 REF support
        throw std::runtime_error("ContextStubSourceNode::do_eval not yet implemented for V2");
    }

    void register_context_node_with_nanobind(nb::module_ &m) {
        nb::class_<ContextStubSourceNode, Node>(m, "ContextStubSourceNode");
    }
} // namespace hgraph