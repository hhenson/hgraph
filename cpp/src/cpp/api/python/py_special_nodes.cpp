#include <hgraph/api/python/py_special_nodes.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/api/python/wrapper_factory.h>

namespace hgraph
{

    void PyLastValuePullNode::apply_value(const nb::object &new_value) {
        impl()->apply_value(new_value);
    }

    void PyLastValuePullNode::copy_from_input(const nb::handle &input) {
        // Stub - copy_from_input requires view-based unwrap migration
        throw std::runtime_error("PyLastValuePullNode::copy_from_input not yet implemented for view-based wrappers");
    }

    void PyLastValuePullNode::copy_from_output(const nb::handle &output) {
        // Stub - copy_from_output requires view-based unwrap migration
        throw std::runtime_error("PyLastValuePullNode::copy_from_output not yet implemented for view-based wrappers");
    }

    LastValuePullNode *PyLastValuePullNode::impl() {
        return this->static_cast_impl<LastValuePullNode>();
    }

    void register_special_nodes_with_nanobind(nb::module_ &m) {
        nb::class_<PyLastValuePullNode, PyNode>(m, "LastValuePullNode")
        .def("apply_value", &PyLastValuePullNode::apply_value)
        .def("copy_from_input", &PyLastValuePullNode::copy_from_input)
        .def("copy_from_output", &PyLastValuePullNode::copy_from_output);
    }
}  // namespace hgraph
