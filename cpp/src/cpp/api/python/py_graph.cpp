//
// PyGraph implementation - Python API wrapper for Graph
//

#include <hgraph/api/python/py_graph.h>
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
        for (const auto& node : vec) {
            // TODO: Wrap in PyNode
            py_list.append(node);
        }
        return nb::tuple(py_list);
    }
    
    nb::object PyGraph::parent_node() const {
        // TODO: Wrap in PyNode
        auto node = _impl->parent_node();
        if (node) {
            return nb::cast(node);
        }
        return nb::none();
    }
    
    std::string PyGraph::label() const {
        auto lbl = _impl->label();
        return lbl.value_or("");
    }
    
    nb::object PyGraph::evaluation_engine_api() const {
        // TODO: Wrap EvaluationEngineApi
        return nb::cast(_impl->evaluation_engine_api());
    }
    
    nb::object PyGraph::evaluation_clock() const {
        // TODO: Wrap EvaluationClock
        return nb::cast(_impl->evaluation_clock());
    }
    
    nb::object PyGraph::engine_evaluation_clock() const {
        // TODO: Wrap EngineEvaluationClock
        return nb::cast(_impl->evaluation_engine_clock());
    }
    
    nb::object PyGraph::evaluation_engine() const {
        // TODO: Wrap EvaluationEngine
        return nb::cast(_impl->evaluation_engine());
    }
    
    void PyGraph::set_evaluation_engine(nb::object engine) {
        // TODO: Unwrap EvaluationEngine
        _impl->set_evaluation_engine(nb::cast<EvaluationEngine::ptr>(engine));
    }
    
    int64_t PyGraph::push_source_nodes_end() const {
        return _impl->push_source_nodes_end();
    }
    
    void PyGraph::schedule_node(int64_t node_ndx, engine_time_t when, bool force_set) {
        _impl->schedule_node(node_ndx, when, force_set);
    }
    
    nb::object PyGraph::schedule() const {
        // TODO: Return schedule vector
        return nb::cast(&_impl->schedule());
    }
    
    void PyGraph::evaluate_graph() {
        _impl->evaluate_graph();
    }
    
    nb::object PyGraph::copy_with(nb::tuple nodes) const {
        // TODO: Unwrap PyNode from tuple, wrap result in PyGraph
        std::vector<node_ptr> node_vec;
        for (auto node : nodes) {
            node_vec.push_back(nb::cast<node_ptr>(node));
        }
        return nb::cast(_impl->copy_with(node_vec));
    }
    
    nb::object PyGraph::traits() const {
        // TODO: Wrap Traits
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
            .def_prop_rw("evaluation_engine", &PyGraph::evaluation_engine, &PyGraph::set_evaluation_engine)
            .def_prop_ro("push_source_nodes_end", &PyGraph::push_source_nodes_end)
            .def("schedule_node", &PyGraph::schedule_node, "node_ndx"_a, "when"_a, "force_set"_a = false)
            .def_prop_ro("schedule", &PyGraph::schedule)
            .def("evaluate_graph", &PyGraph::evaluate_graph)
            .def("copy_with", &PyGraph::copy_with, "nodes"_a)
            .def_prop_ro("traits", &PyGraph::traits)
            .def("__str__", &PyGraph::str)
            .def("__repr__", &PyGraph::repr);
    }
    
} // namespace hgraph::api

