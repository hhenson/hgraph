#include "hgraph/api/python/wrapper_factory.h"

#include <fmt/format.h>
#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_evaluation_engine.h>
#include <hgraph/api/python/py_graph.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>

#include <utility>

namespace hgraph
{
    PyTraits::PyTraits(api_ptr traits) : _impl{std::move(traits)} {}

    void PyTraits::set_traits(nb::kwargs traits) { _impl->set_traits(std::move(traits)); }

    void PyTraits::set_trait(const std::string &trait_name, nb::object value) const {
        _impl->set_trait(trait_name, std::move(value));
    }

    nb::object PyTraits::get_trait(const std::string &trait_name) const { return _impl->get_trait(trait_name); }

    nb::object PyTraits::get_trait_or(const std::string &trait_name, nb::object def_value) const {
        return _impl->get_trait_or(trait_name, std::move(def_value));
    }

    void PyTraits::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTraits>(m, "Traits")
            .def("get_trait", &PyTraits::get_trait, "trait_name"_a)
            .def("set_traits", &PyTraits::set_traits)
            .def("get_trait_or", &PyTraits::get_trait_or, "trait_name"_a, "def_value"_a = nb::none());
    }

    PyGraph::PyGraph(ApiPtr<Graph> graph) : _impl{std::move(graph)} {}

    nb::tuple PyGraph::graph_id() const {
        nb::list l{};
        for (auto &id : _impl->graph_id()) { l.append(nb::int_(id)); }
        return nb::tuple(l);
    }

    nb::tuple PyGraph::nodes() const {
        // TODO: This really should be cached
        nb::list l{};
        for (const auto &node : _impl->nodes()) { l.append(wrap_node(node)); }
        return nb::tuple(l);
    }

    nb::tuple PyGraph::node_info(size_t idx) const {
        if (idx >= _impl->nodes().size()) {
            throw std::out_of_range("Node index out of range");
        }
        return nb::make_tuple(wrap_node(_impl->nodes()[idx]),
                         nb::cast(_impl->schedule()[idx]));
    }

    nb::object PyGraph::parent_node() const {
        auto *pn = _impl->parent_node();
        return pn ? wrap_node(pn->shared_from_this()) : nb::none();
    }

    nb::object PyGraph::label() const {
        auto lbl{_impl->label()};
        return lbl.has_value() ? nb::cast(*lbl) : nb::none();
    }

    PyEvaluationEngineApi PyGraph::evaluation_engine_api()
    {
        const auto& engine = _impl->evaluation_engine();
        if (!engine) {
            throw std::runtime_error("Graph::evaluation_engine() returned null");
        }
        return PyEvaluationEngineApi(ApiPtr<EvaluationEngineApi>(engine));
    }

    PyEvaluationClock PyGraph::evaluation_clock() const
    {
        auto clock = _impl->evaluation_clock();
        if (!clock) {
            throw std::runtime_error("Graph::evaluation_clock() returned null");
        }
        return PyEvaluationClock(ApiPtr<EvaluationClock>(clock));
    }

    nb::int_ PyGraph::push_source_nodes_end() const { return nb::int_(_impl->push_source_nodes_end()); }

    void PyGraph::schedule_node(int64_t node_ndx, engine_time_t when, bool force_set) {
        _impl->schedule_node(node_ndx, when, force_set);
    }

    nb::tuple PyGraph::schedule() { return nb::make_tuple(_impl->schedule()); }

    PyGraph PyGraph::copy_with(nb::object nodes) {
        auto nodes_{nb::cast<std::vector<node_s_ptr>>(nodes)};
        auto g{_impl->copy_with(nodes_)};
        return PyGraph(ApiPtr<Graph>(g));
    }

    nb::object PyGraph::traits() const { return wrap_traits(&_impl->traits(), _impl.control_block()); }

    SenderReceiverState &PyGraph::receiver() { return _impl->receiver(); }

    void PyGraph::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyGraph>(m, "Graph")
            .def_prop_ro("graph_id", &PyGraph::graph_id)
            .def_prop_ro("nodes", &PyGraph::nodes)
            .def("node_info", &PyGraph::node_info, "idx"_a)
            .def_prop_ro("parent_node", &PyGraph::parent_node)
            .def_prop_ro("label", &PyGraph::label)
            .def_prop_ro("evaluation_engine_api", &PyGraph::evaluation_engine_api)
            .def_prop_ro("evaluation_clock", &PyGraph::evaluation_clock)
            .def_prop_ro("push_source_nodes_end", &PyGraph::push_source_nodes_end)
            .def("schedule_node", static_cast<void (PyGraph::*)(int64_t, engine_time_t, bool)>(&PyGraph::schedule_node),
                 "node_ndx"_a, "when"_a, "force_set"_a = false)
            .def_prop_ro("schedule", &PyGraph::schedule)
            .def("copy_with", &PyGraph::copy_with, "nodes"_a)
            .def_prop_ro("traits", &PyGraph::traits)
            .def("__str__",
                 [](const PyGraph &self) {
                     return nb::str("Graph@{}[id={}, nodes={}]")
                         .format(fmt::format("{:p}", static_cast<const void *>(&self)), self.graph_id(), nb::len(self.nodes()));
                 })
            .def("__repr__", [](const PyGraph &self) {
                return nb::str("Graph@{}[id={}, nodes={}]")
                    .format(fmt::format("{:p}", static_cast<const void *>(&self)), self.graph_id(), nb::len(self.nodes()));
            });
        ;
    }

}  // namespace hgraph
