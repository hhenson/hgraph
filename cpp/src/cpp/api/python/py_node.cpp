#include "hgraph/nodes/push_queue_node.h"

#include <hgraph/api/python/wrapper_factory.h>

#include <hgraph/nodes/nested_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_meta.h>

#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/py_node.h>

namespace hgraph
{

    PyNodeScheduler::PyNodeScheduler(api_ptr scheduler) : _impl{std::move(scheduler)} {}

    engine_time_t PyNodeScheduler::next_scheduled_time() const { return _impl->next_scheduled_time(); }

    nb::bool_ PyNodeScheduler::requires_scheduling() const { return nb::bool_(_impl->requires_scheduling()); }

    nb::bool_ PyNodeScheduler::is_scheduled() const { return nb::bool_(_impl->is_scheduled()); }

    nb::bool_ PyNodeScheduler::is_scheduled_now() const { return nb::bool_(_impl->is_scheduled_now()); }

    nb::bool_ PyNodeScheduler::has_tag(const std::string &tag) const { return nb::bool_(_impl->has_tag(tag)); }

    engine_time_t PyNodeScheduler::pop_tag(const std::string &tag) const { return _impl->pop_tag(tag); }

    engine_time_t PyNodeScheduler::pop_tag(const std::string &tag, engine_time_t default_time) const {
        return _impl->pop_tag(tag, default_time);
    }

    void PyNodeScheduler::schedule(engine_time_t when, std::optional<std::string> tag, bool on_wall_clock) const {
        _impl->schedule(when, std::move(tag), on_wall_clock);
    }

    void PyNodeScheduler::schedule(engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock) const {
        _impl->schedule(when, std::move(tag), on_wall_clock);
    }

    void PyNodeScheduler::un_schedule(const std::string &tag) const { _impl->un_schedule(tag); }

    void PyNodeScheduler::un_schedule() const { _impl->un_schedule(); }

    void PyNodeScheduler::reset() { _impl->reset(); }

    void PyNodeScheduler::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyNodeScheduler>(m, "NodeScheduler")
            .def_prop_ro("next_scheduled_time", &PyNodeScheduler::next_scheduled_time)
            .def_prop_ro("is_scheduled", &PyNodeScheduler::is_scheduled)
            .def_prop_ro("is_scheduled_now", &PyNodeScheduler::is_scheduled_now)
            .def_prop_ro("has_tag", &PyNodeScheduler::has_tag)
            .def(
                "pop_tag", [](PyNodeScheduler &self, const std::string &tag) { return self.pop_tag(tag); }, "tag"_a)
            .def(
                "pop_tag",
                [](PyNodeScheduler &self, const std::string &tag, engine_time_t default_time) {
                    return self.pop_tag(tag, default_time);
                },
                "tag"_a, "default_time"_a)
            .def(
                "schedule",
                [](PyNodeScheduler &self, engine_time_t when, std::optional<std::string> tag, bool on_wall_clock) {
                    self.schedule(when, std::move(tag), on_wall_clock);
                },
                "when"_a, "tag"_a = nb::none(), "on_wall_clock"_a = false)
            .def(
                "schedule",
                [](PyNodeScheduler &self, engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock) {
                    self.schedule(when, std::move(tag), on_wall_clock);
                },
                "when"_a, "tag"_a = nb::none(), "on_wall_clock"_a = false)
            .def("un_schedule", static_cast<void (PyNodeScheduler::*)(const std::string &) const>(&PyNodeScheduler::un_schedule),
                 "tag"_a)
            .def("un_schedule", static_cast<void (PyNodeScheduler::*)() const>(&PyNodeScheduler::un_schedule))
            .def("reset", &PyNodeScheduler::reset)
            .def("__str__",
                 [](const PyNodeScheduler &self) {
                     auto &ns{*self._impl};
                     return fmt::format("NodeScheduler@{:p}[scheduled={}]", static_cast<const void *>(&ns), ns.is_scheduled());
                 })
            .def("__repr__", [](const PyNodeScheduler &self) {
                auto &ns{*self._impl};
                return fmt::format("NodeScheduler@{:p}[scheduled={}]", static_cast<const void *>(&ns), ns.is_scheduled());
            });
    }

    PyNode::PyNode(api_ptr node) : _impl{std::move(node)} {}

    void PyNode::notify(const nb::handle &dt) const {
        if (!dt.is_valid() || dt.is_none()) {
            _impl->notify();
            return;
        }
        _impl->notify(nb::cast<engine_time_t>(dt));
    }

    nb::int_ PyNode::node_ndx() const { return nb::int_(_impl->node_ndx()); }

    nb::tuple PyNode::owning_graph_id() const {
        // Convert internal vector<int64_t> to a proper Python tuple of ints
        // (nb::make_tuple(vec) would create a 1-element tuple containing the vector)
        nb::list ids;
        for (auto id : _impl->owning_graph_id()) { ids.append(id); }
        return nb::tuple(ids);
    }

    nb::tuple PyNode::node_id() const { return nb::tuple(nb::cast(_impl->node_id())); }

    const NodeSignature &PyNode::signature() const { return _impl->signature(); }

    const nb::dict &PyNode::scalars() const { return _impl->scalars(); }

    PyGraph PyNode::graph() const {
        auto graph{_impl->graph()};
        return PyGraph(ApiPtr<Graph>(graph->shared_from_this()));
    }

    nb::object PyNode::input() const {
        // Use view-based wrapping via Node's input_view()
        if (!_impl->has_input()) { return nb::none(); }
        TSBView input_bv = _impl->input_view();
        return wrap_bundle_input_view(input_bv);
    }

    nb::dict PyNode::inputs() const {
        nb::dict d;
        if (!_impl->has_input()) { return d; }
        TSBView input_bv = _impl->input_view();
        const TSBTypeMeta* meta = input_bv.bundle_meta();
        for (size_t i = 0; i < meta->field_count(); ++i) {
            TSView field = input_bv.field(i);
            d[meta->field(i).name.c_str()] = wrap_input_view(field);
        }
        return d;
    }

    nb::tuple PyNode::start_inputs() const { return nb::tuple(nb::cast(_impl->start_inputs())); }

    nb::object PyNode::output() {
        // Use view-based wrapping via Node's output_view()
        if (!_impl->has_output()) { return nb::none(); }
        TSMutableView output_mv = _impl->output_view();
        return wrap_output_view(output_mv);
    }

    nb::object PyNode::recordable_state() {
        // Use view-based wrapping via Node's state_view()
        TSMutableView state_mv = _impl->state_view();
        return state_mv.valid() ? wrap_output_view(state_mv) : nb::none();
    }

    nb::bool_ PyNode::has_recordable_state() const { return nb::bool_(_impl->has_recordable_state()); }

    nb::object PyNode::scheduler() const { return wrap_node_scheduler(_impl->scheduler()); }

    nb::bool_ PyNode::has_scheduler() const { return nb::bool_(_impl->has_scheduler()); }

    nb::object PyNode::error_output() {
        // Use view-based wrapping via Node's error_output_view()
        TSMutableView error_mv = _impl->error_output_view();
        return error_mv.valid() ? wrap_output_view(error_mv) : nb::none();
    }

    nb::bool_ PyNode::has_input() const { return nb::bool_(_impl->has_input()); }

    nb::bool_ PyNode::has_output() const { return nb::bool_(_impl->has_output()); }

    nb::str PyNode::repr() const { return nb::str(_impl->repr().c_str()); }

    nb::str PyNode::str() const { return nb::str(_impl->str().c_str()); }

    void PyNode::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyNode>(m, "Node")
            .def_prop_ro("node_ndx", &PyNode::node_ndx)
            .def_prop_ro("owning_graph_id", &PyNode::owning_graph_id)
            .def_prop_ro("node_id", &PyNode::node_id)
            .def_prop_ro("signature", &PyNode::signature)
            .def_prop_ro("scalars", &PyNode::scalars)
            .def_prop_ro("graph", &PyNode::graph)
            .def_prop_ro("input", &PyNode::input)
            .def_prop_ro("inputs", &PyNode::inputs)
            .def_prop_ro("start_inputs", &PyNode::start_inputs)
            .def_prop_ro("output", &PyNode::output)
            .def_prop_ro("recordable_state", &PyNode::recordable_state)
            .def_prop_ro("scheduler", &PyNode::scheduler)
            .def("notify", &PyNode::notify, "modified_time"_a = nb::none())
            // .def("notify_next_cycle", &PyNode::notify_next_cycle)
            .def_prop_ro("error_output", &PyNode::error_output)
            // owning_node returns self - needed for compatibility with Python's PythonTimeSeriesOutput
            // which checks _parent_or_node.owning_node to navigate to the owning node
            .def_prop_ro("owning_node", [](PyNode &self) -> nb::object {
                // Return self as nb::object
                return nb::cast(&self, nb::rv_policy::reference);
            })
            // mark_child_modified - needed when C++ Node is used as _parent_or_node in PythonTimeSeriesOutput
            // Python checks isinstance(_parent_or_node, Node) but C++ Node fails that check, so Python
            // thinks it's a parent output and calls mark_child_modified. This is a no-op for Nodes.
            .def("mark_child_modified", [](PyNode &self, nb::object child, nb::object modified_time) {
                // No-op: Nodes don't propagate child modification like parent outputs do
            }, "child"_a, "modified_time"_a)
            .def("__repr__", &PyNode::repr)
            .def("__str__", &PyNode::str)
            .def("__eq__", [](const PyNode &self, const nb::object &other) {
                // Compare the underlying C++ pointers to determine equality
                if (!nb::isinstance<PyNode>(other)) return false;
                const auto &other_node = nb::cast<const PyNode &>(other);
                return self._impl.get() == other_node._impl.get();
            })
            .def("__hash__", [](const PyNode &self) {
                // Hash based on the underlying C++ pointer
                return std::hash<const void *>{}(self._impl.get());
            });
    }

    control_block_ptr PyNode::control_block() const { return _impl.control_block(); }

    nb::int_ PyPushQueueNode::messages_in_queue() const {
        return nb::int_(this->static_cast_impl<PushQueueNode>()->messages_in_queue());
    }

    void PyPushQueueNode::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyPushQueueNode, PyNode>(m, "PushQueueNode")
            .def_prop_ro("messages_in_queue", &PyPushQueueNode::messages_in_queue);
    }

    engine_time_t PyNestedNode::last_evaluation_time() const {
        return this->static_cast_impl<NestedNode>()->last_evaluation_time();
    }

    nb::dict PyNestedNode::nested_graphs() const {
        nb::dict d;
        static_cast_impl<NestedNode>()->enumerate_nested_graphs([&d](const graph_s_ptr &graph) {
            if (graph && graph->label() && !graph->label()->empty()) {
                d[graph->label()->c_str()] = wrap_graph(graph);
            } else {
                d["default"] = wrap_graph(graph);
            }
        });
        return d;
    }

    void PyNestedNode::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyNestedNode, PyNode>(m, "NestedNode")
            .def_prop_ro("last_evaluation_time", &PyNestedNode::last_evaluation_time)
            .def_prop_ro("nested_graphs", &PyNestedNode::nested_graphs)
            ;
    }

    void PyMapNestedNode::register_with_nanobind(nb::module_ &m) { nb::class_<PyMapNestedNode, PyNestedNode>(m, "MapNestedNode"); }

    void PyMeshNestedNode::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyMeshNestedNode, PyNestedNode>(m, "MeshNestedNode")
            .def("_add_graph_dependency", &PyMeshNestedNode::add_graph_dependency, "key"_a, "depends_on"_a)
            .def("_remove_graph_dependency", &PyMeshNestedNode::remove_graph_dependency, "key"_a, "depends_on"_a);
    }

} // namespace hgraph