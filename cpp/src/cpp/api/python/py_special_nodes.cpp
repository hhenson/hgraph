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
        auto input_ts{unwrap_input(input)};
        if (!input_ts) {throw std::runtime_error("Invalid input type for LastValuePullNode");}
        impl()->copy_from_input(*input_ts);
    }

    void PyLastValuePullNode::copy_from_output(const nb::handle &output) {
        auto output_ts{unwrap_output(output)};
        if (!output_ts) {throw std::runtime_error("Invalid output type for LastValuePullNode");}
        impl()->copy_from_output(*output_ts);
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
