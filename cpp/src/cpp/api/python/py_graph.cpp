//
// PyGraph implementation - Python API wrapper for Graph
//

#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/graph.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::api {
    
    PyGraph::PyGraph(Graph* impl, control_block_ptr control_block)
        : _impl(impl, std::move(control_block)) {}
    
    nb::tuple PyGraph::graph_id() const {
        const auto& vec = _impl->graph_id();
        nb::list py_list;
        for (const auto& id : vec) {
            py_list.append(id);
        }
        return nb::tuple(py_list);
    }
    
    nb::tuple PyGraph::nodes() const {
        const auto& vec = _impl->nodes();
        nb::list py_list;
        for (const auto& node_ref : vec) {
            py_list.append(wrap_node(node_ref.get(), _impl.control_block()));
        }
        return nb::tuple(py_list);
    }
    
    nb::object PyGraph::parent_node() const {
        auto node_ref = _impl->parent_node();
        if (node_ref) {
            return wrap_node(node_ref.get(), _impl.control_block());
        }
        return nb::none();
    }
    
    std::string PyGraph::label() const {
        auto lbl = _impl->label();
        return lbl.value_or("");
    }
    
    nb::object PyGraph::evaluation_engine_api() const {
        auto api = _impl->evaluation_engine_api();
        return wrap_evaluation_engine_api(api.get(), _impl.control_block());
    }
    
    nb::object PyGraph::evaluation_clock() const {
        auto clock = _impl->evaluation_clock();
        return wrap_evaluation_clock(clock.get(), _impl.control_block());
    }
    
    nb::object PyGraph::engine_evaluation_clock() const {
        if (!_impl.is_graph_alive()) {
            return nb::none();
        }
        auto clock = _impl->evaluation_engine_clock();
        if (clock.get() == nullptr) {
            return nb::none();
        }
        return wrap_evaluation_clock(clock.get(), _impl.control_block());
    }
    
    nb::object PyGraph::traits() const {
        // Return raw traits reference - this is a member of graph, not a separate object
        // Wrapping it causes issues during graph teardown
        return nb::cast(&_impl->traits());
    }
    
    std::string PyGraph::str() const {
        return fmt::format("Graph@{:p}[id={}, nodes={}]",
                         static_cast<const void*>(_impl.get()),
                         fmt::join(_impl->graph_id(), ","),
                         _impl->nodes().size());
    }
    
    std::string PyGraph::repr() const {
        return str();
    }
    
    void PyGraph::register_with_nanobind(nb::module_& m) {
        nb::class_<PyGraph>(m, "Graph")
            .def_prop_ro("graph_id", &PyGraph::graph_id)
            .def_prop_ro("nodes", &PyGraph::nodes)
            .def_prop_ro("parent_node", &PyGraph::parent_node)
            .def_prop_ro("label", &PyGraph::label)
            .def_prop_ro("evaluation_engine_api", &PyGraph::evaluation_engine_api)
            .def_prop_ro("evaluation_clock", &PyGraph::evaluation_clock)
            .def_prop_ro("engine_evaluation_clock", &PyGraph::engine_evaluation_clock)
            .def_prop_ro("traits", &PyGraph::traits)
            .def("__str__", &PyGraph::str)
            .def("__repr__", &PyGraph::repr);
    }
    
} // namespace hgraph::api

