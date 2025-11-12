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
    
    void PyNodeScheduler::schedule(engine_time_t when, std::optional<std::string> tag, bool on_wall_clock) {
        _impl->schedule(when, std::move(tag), on_wall_clock);
    }
    
    bool PyNodeScheduler::is_scheduled() const {
        return _impl->is_scheduled();
    }
    
    bool PyNodeScheduler::is_scheduled_now() const {
        return _impl->is_scheduled_now();
    }
    
    engine_time_t PyNodeScheduler::next_scheduled_time() const {
        return _impl->next_scheduled_time();
    }
    
    engine_time_t PyNodeScheduler::pop_tag(const std::string& tag) {
        return _impl->pop_tag(tag);
    }
    
    engine_time_t PyNodeScheduler::pop_tag(const std::string& tag, engine_time_t default_time) {
        return _impl->pop_tag(tag, default_time);
    }
    
    bool PyNodeScheduler::has_tag(const std::string& tag) const {
        return _impl->has_tag(tag);
    }
    
    void PyNodeScheduler::un_schedule() {
        _impl->un_schedule();
    }
    
    void PyNodeScheduler::un_schedule(const std::string& tag) {
        _impl->un_schedule(tag);
    }
    
    std::string PyNodeScheduler::str() const {
        return fmt::format("NodeScheduler@{:p}", static_cast<const void*>(_impl.get()));
    }
    
    std::string PyNodeScheduler::repr() const {
        return str();
    }
    
    void PyNodeScheduler::register_with_nanobind(nb::module_& m) {
        nb::class_<PyNodeScheduler>(m, "NodeScheduler")
            .def("schedule", &PyNodeScheduler::schedule, "when"_a, "tag"_a = nb::none(), "on_wall_clock"_a = false)
            .def_prop_ro("is_scheduled", &PyNodeScheduler::is_scheduled)
            .def_prop_ro("is_scheduled_now", &PyNodeScheduler::is_scheduled_now)
            .def_prop_ro("next_scheduled_time", &PyNodeScheduler::next_scheduled_time)
            .def("pop_tag", nb::overload_cast<const std::string&>(&PyNodeScheduler::pop_tag), "tag"_a)
            .def("pop_tag", nb::overload_cast<const std::string&, engine_time_t>(&PyNodeScheduler::pop_tag), "tag"_a, "default_time"_a)
            .def("has_tag", &PyNodeScheduler::has_tag, "tag"_a)
            .def("un_schedule", nb::overload_cast<>(&PyNodeScheduler::un_schedule))
            .def("un_schedule", nb::overload_cast<const std::string&>(&PyNodeScheduler::un_schedule), "tag"_a)
            .def("__str__", &PyNodeScheduler::str)
            .def("__repr__", &PyNodeScheduler::repr);
    }
    
} // namespace hgraph::api

