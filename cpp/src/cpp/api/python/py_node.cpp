//
// PyNode implementation - Python API wrapper for Node
//

#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>  // For TimeSeriesBundleInput
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <hgraph/nodes/last_value_pull_node.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::api {
    
    PyNode::PyNode(Node* impl, control_block_ptr control_block)
        : _impl(impl, std::move(control_block)) {}
    
    int64_t PyNode::node_ndx() const {
        return _impl->node_ndx();
    }
    
    nb::tuple PyNode::owning_graph_id() const {
        const auto& vec = _impl->owning_graph_id();
        nb::list py_list;
        for (const auto& id : vec) {
            py_list.append(id);
        }
        return nb::tuple(py_list);
    }
    
    nb::tuple PyNode::node_id() const {
        const auto vec = _impl->node_id();
        nb::list py_list;
        for (const auto& id : vec) {
            py_list.append(id);
        }
        return nb::tuple(py_list);
    }
    
    nb::object PyNode::signature() const {
        // TODO: Wrap NodeSignature
        return nb::cast(&_impl->signature());
    }
    
    const nb::dict& PyNode::scalars() const {
        return _impl->scalars();
    }
    
    nb::object PyNode::graph() const {
        return wrap_graph(_impl->graph(), _impl.control_block());
    }
    
    nb::object PyNode::input() const {
        auto tsb_ref = _impl->input();
        return wrap_input(tsb_ref.get(), _impl.control_block());
    }
    
    nb::dict PyNode::inputs() const {
        nb::dict d;
        auto inp = *_impl->input();
        for (const auto& key : inp.schema().keys()) {
            // Wrap each input - factory returns cached wrapper if available
            d[key.c_str()] = wrap_input(inp[key], _impl.control_block());
        }
        return d;
    }
    
    nb::object PyNode::output() const {
        // Factory returns cached wrapper if available
        return wrap_output(_impl->output(), _impl.control_block());
    }
    
    nb::object PyNode::recordable_state() const {
        if (!_impl->has_recordable_state()) {
            return nb::none();
        }
        auto state = _impl->recordable_state();
        if (!state) {
            return nb::none();
        }
        return wrap_output(state.get(), _impl.control_block());
    }
    
    nb::object PyNode::scheduler() const {
        return wrap_node_scheduler(_impl->scheduler(), _impl.control_block());
    }
    
    void PyNode::notify(engine_time_t modified_time) {
        _impl->notify(modified_time);
    }
    
    std::string PyNode::str() const {
        return _impl->str();
    }
    
    std::string PyNode::repr() const {
        return _impl->repr();
    }
    
    void PyNode::register_with_nanobind(nb::module_& m) {
        nb::class_<PyNode>(m, "Node")
            .def_prop_ro("node_ndx", &PyNode::node_ndx)
            .def_prop_ro("owning_graph_id", &PyNode::owning_graph_id)
            .def_prop_ro("node_id", &PyNode::node_id)
            .def_prop_ro("signature", &PyNode::signature)
            .def_prop_ro("scalars", &PyNode::scalars)
            .def_prop_ro("graph", &PyNode::graph)
            .def_prop_ro("input", &PyNode::input)
            .def_prop_ro("inputs", &PyNode::inputs)
            .def_prop_ro("output", &PyNode::output)
            .def_prop_ro("recordable_state", &PyNode::recordable_state)
            .def_prop_ro("scheduler", &PyNode::scheduler)
            .def("notify", &PyNode::notify, "modified_time"_a)
            .def("__str__", &PyNode::str)
            .def("__repr__", &PyNode::repr);
    }
    
    PyLastValuePullNode::PyLastValuePullNode(LastValuePullNode* impl, control_block_ptr control_block)
        : PyNode(impl, control_block)
        , _impl_last_value(impl, std::move(control_block)) {}
    
    void PyLastValuePullNode::apply_value(const nb::object& new_value) {
        _impl_last_value->apply_value(new_value);
    }
    
    void PyLastValuePullNode::register_with_nanobind(nb::module_& m) {
        nb::class_<PyLastValuePullNode, PyNode>(m, "LastValuePullNode")
            .def("apply_value", &PyLastValuePullNode::apply_value);
    }
    
} // namespace hgraph::api

