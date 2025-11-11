//
// PyNode implementation - Python API wrapper for Node
//

#include <hgraph/api/python/py_node.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series_type.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

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
        // TODO: Wrap in PyGraph
        return nb::cast(_impl->graph());
    }
    
    void PyNode::set_graph(nb::object graph) {
        // TODO: Unwrap PyGraph
        _impl->set_graph(nb::cast<graph_ptr>(graph));
    }
    
    nb::object PyNode::input() const {
        // TODO: Wrap in PyTimeSeriesBundleInput
        return nb::cast(_impl->input());
    }
    
    void PyNode::set_input(nb::object input) {
        // TODO: Unwrap PyTimeSeriesBundleInput
        _impl->set_input(nb::cast<time_series_bundle_input_ptr>(input));
    }
    
    nb::dict PyNode::inputs() const {
        nb::dict d;
        auto inp = *_impl->input();
        for (const auto& key : inp.schema().keys()) {
            // TODO: Wrap each input in appropriate PyTimeSeriesInput type
            d[key.c_str()] = inp[key];
        }
        return d;
    }
    
    nb::list PyNode::start_inputs() const {
        // TODO: Wrap each input in appropriate PyTimeSeriesInput type
        return nb::list();  // Stub
    }
    
    nb::object PyNode::output() const {
        // TODO: Wrap in appropriate PyTimeSeriesOutput type
        return nb::cast(_impl->output());
    }
    
    void PyNode::set_output(nb::object output) {
        // TODO: Unwrap PyTimeSeriesOutput
        _impl->set_output(nb::cast<time_series_output_ptr>(output));
    }
    
    nb::object PyNode::recordable_state() const {
        // TODO: Wrap in PyTimeSeriesBundleOutput
        return nb::cast(_impl->recordable_state());
    }
    
    void PyNode::set_recordable_state(nb::object state) {
        // TODO: Unwrap PyTimeSeriesBundleOutput
        _impl->set_recordable_state(nb::cast<time_series_bundle_output_ptr>(state));
    }
    
    nb::object PyNode::scheduler() const {
        // TODO: Wrap in PyNodeScheduler
        return nb::cast(_impl->scheduler());
    }
    
    void PyNode::eval() {
        _impl->eval();
    }
    
    void PyNode::notify() {
        _impl->notify();
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
            .def_prop_rw("graph", &PyNode::graph, &PyNode::set_graph)
            .def_prop_rw("input", &PyNode::input, &PyNode::set_input)
            .def_prop_ro("inputs", &PyNode::inputs)
            .def_prop_ro("start_inputs", &PyNode::start_inputs)
            .def_prop_rw("output", &PyNode::output, &PyNode::set_output)
            .def_prop_rw("recordable_state", &PyNode::recordable_state, &PyNode::set_recordable_state)
            .def_prop_ro("scheduler", &PyNode::scheduler)
            .def("eval", &PyNode::eval)
            .def("notify", nb::overload_cast<>(&PyNode::notify))
            .def("notify", nb::overload_cast<engine_time_t>(&PyNode::notify))
            .def("__str__", &PyNode::str)
            .def("__repr__", &PyNode::repr);
    }
    
} // namespace hgraph::api

