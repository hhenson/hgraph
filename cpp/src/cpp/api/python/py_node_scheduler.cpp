//
// PyNodeScheduler implementation - Python API wrapper for NodeScheduler
//

#include <hgraph/api/python/py_node_scheduler.h>
#include <hgraph/types/node.h>
#include <nanobind/stl/string.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::api {
    
    PyNodeScheduler::PyNodeScheduler(NodeScheduler* impl, control_block_ptr control_block)
        : _impl(impl, std::move(control_block)) {}
    
    void PyNodeScheduler::schedule(engine_time_t when) {
        _impl->schedule(when);
    }
    
    void PyNodeScheduler::schedule(engine_time_t when, bool force_set) {
        _impl->schedule(when, force_set);
    }
    
    bool PyNodeScheduler::is_scheduled() const {
        return _impl->is_scheduled();
    }
    
    engine_time_t PyNodeScheduler::last_scheduled_time() const {
        return _impl->last_scheduled_time();
    }
    
    engine_time_t PyNodeScheduler::next_scheduled_time() const {
        return _impl->next_scheduled_time();
    }
    
    void PyNodeScheduler::pop_tag(const std::string& tag) {
        _impl->pop_tag(tag);
    }
    
    void PyNodeScheduler::schedule_with_tag(engine_time_t when, const std::string& tag) {
        _impl->schedule_with_tag(when, tag);
    }
    
    bool PyNodeScheduler::has_tag(const std::string& tag) const {
        return _impl->has_tag(tag);
    }
    
    void PyNodeScheduler::set_alarm(engine_time_t when, const std::string& tag) {
        _impl->set_alarm(when, tag);
    }
    
    std::string PyNodeScheduler::str() const {
        return fmt::format("NodeScheduler@{:p}", static_cast<const void*>(_impl.get()));
    }
    
    std::string PyNodeScheduler::repr() const {
        return str();
    }
    
    void PyNodeScheduler::register_with_nanobind(nb::module_& m) {
        nb::class_<PyNodeScheduler>(m, "NodeScheduler")
            .def("schedule", nb::overload_cast<engine_time_t>(&PyNodeScheduler::schedule), "when"_a)
            .def("schedule", nb::overload_cast<engine_time_t, bool>(&PyNodeScheduler::schedule), "when"_a, "force_set"_a)
            .def_prop_ro("is_scheduled", &PyNodeScheduler::is_scheduled)
            .def_prop_ro("last_scheduled_time", &PyNodeScheduler::last_scheduled_time)
            .def_prop_ro("next_scheduled_time", &PyNodeScheduler::next_scheduled_time)
            .def("pop_tag", &PyNodeScheduler::pop_tag, "tag"_a)
            .def("schedule_with_tag", &PyNodeScheduler::schedule_with_tag, "when"_a, "tag"_a)
            .def("has_tag", &PyNodeScheduler::has_tag, "tag"_a)
            .def("set_alarm", &PyNodeScheduler::set_alarm, "when"_a, "tag"_a)
            .def("__str__", &PyNodeScheduler::str)
            .def("__repr__", &PyNodeScheduler::repr);
    }
    
} // namespace hgraph::api

