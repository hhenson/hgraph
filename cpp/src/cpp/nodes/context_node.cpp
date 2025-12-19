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
        // TODO: Context nodes need REF support
        notify();
    }

    void ContextStubSourceNode::do_stop() {
        // TODO: Context nodes need REF support
    }

    void ContextStubSourceNode::do_eval() {
        // TODO: Context nodes require REF types - not yet implemented
        throw std::runtime_error("ContextStubSourceNode::do_eval not yet implemented");
    }

    void register_context_node_with_nanobind(nb::module_ &m) {
        nb::class_<ContextStubSourceNode, Node>(m, "ContextStubSourceNode");
    }
} // namespace hgraph