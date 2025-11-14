#include <fmt/format.h>
#include <hgraph/api/python/py_graph.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>

namespace hgraph
{
    PyGraph::PyGraph(ApiPtr<Graph> graph) : _impl{std::move(graph)} {}

    nb::tuple PyGraph::graph_id() const { return nb::make_tuple(_impl->graph_id()); }

    nb::tuple PyGraph::nodes() const { return nb::make_tuple(_impl->nodes()); }

    node_ptr PyGraph::parent_node() const { return _impl->parent_node(); }

    nb::object PyGraph::label() const {
        auto lbl{_impl->label()};
        return lbl.has_value() ? nb::cast(*lbl) : nb::none();
    }

    nb::ref<EvaluationEngineApi> PyGraph::evaluation_engine_api() { return _impl->evaluation_engine_api(); }

    EvaluationClock::ptr PyGraph::evaluation_clock() const { return _impl->evaluation_clock(); }

    nb::int_ PyGraph::push_source_nodes_end() const { return nb::int_(_impl->push_source_nodes_end()); }

    void PyGraph::schedule_node(int64_t node_ndx, engine_time_t when, bool force_set) {
        _impl->schedule_node(node_ndx, when, force_set);
    }

    nb::tuple PyGraph::schedule() { return nb::make_tuple(_impl->schedule()); }

    PyGraph PyGraph::copy_with(nb::object nodes) {
        auto nodes_{nb::cast<std::vector<node_ptr>>(nodes)};
        auto g{_impl->copy_with(nodes_)};
        return PyGraph(ApiPtr<Graph>(g.get(), g->control_block()));
    }

    const Traits &PyGraph::traits() const {
        return _impl->traits();
    }

    SenderReceiverState &PyGraph::receiver() {
        return _impl->receiver();
    }

    void PyGraph::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyGraph>(m, "Graph")
            .def_prop_ro("graph_id", &PyGraph::graph_id)
            .def_prop_ro("nodes", &PyGraph::nodes)
            .def_prop_ro("parent_node", &PyGraph::parent_node)
            .def_prop_ro("label", &PyGraph::label)
            .def_prop_ro("evaluation_engine_api", &PyGraph::evaluation_engine_api)
            .def_prop_ro("evaluation_clock", static_cast<EvaluationClock::ptr (PyGraph::*)() const>(&PyGraph::evaluation_clock))
            .def_prop_ro("push_source_nodes_end", &PyGraph::push_source_nodes_end)
            .def("schedule_node", static_cast<void (PyGraph::*)(int64_t, engine_time_t, bool)>(&PyGraph::schedule_node),
                 "node_ndx"_a, "when"_a, "force_set"_a = false)
            .def_prop_ro("schedule", &PyGraph::schedule)
            .def("copy_with", &PyGraph::copy_with, "nodes"_a)
            .def_prop_ro("traits", &PyGraph::traits)
            .def("__str__",
                 [](const PyGraph &self) {
                     return nb::str("Graph@{}[id={}, nodes={}]")
                         .format(fmt::format("{:p}", static_cast<const void *>(&self)), self.graph_id(), self.nodes().size());
                 })
            .def("__repr__", [](const PyGraph &self) {
                return nb::str("Graph@{}[id={}, nodes={}]")
                    .format(fmt::format("{:p}", static_cast<const void *>(&self)), self.graph_id(), self.nodes().size());
            });
        ;
    }

}  // namespace hgraph
