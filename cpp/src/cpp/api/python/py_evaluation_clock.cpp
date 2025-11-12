//
// PyEvaluationClock Implementation
//

#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <fmt/format.h>

namespace hgraph::api {
    
    PyEvaluationClock::PyEvaluationClock(EvaluationClock* impl, control_block_ptr control_block)
        : _impl(impl, std::move(control_block)) {
    }
    
    nb::object PyEvaluationClock::evaluation_time() const {
        return nb::cast(_impl->evaluation_time());
    }
    
    nb::object PyEvaluationClock::now() const {
        return nb::cast(_impl->now());
    }
    
    nb::object PyEvaluationClock::next_cycle_evaluation_time() const {
        return nb::cast(_impl->next_cycle_evaluation_time());
    }
    
    nb::object PyEvaluationClock::cycle_time() const {
        return nb::cast(_impl->cycle_time());
    }
    
    std::string PyEvaluationClock::str() const {
        return fmt::format("EvaluationClock@{:p}[eval_time={}]",
                         static_cast<const void*>(_impl.get()),
                         _impl->evaluation_time().time_since_epoch().count());
    }
    
    std::string PyEvaluationClock::repr() const {
        return str();
    }
    
    void PyEvaluationClock::register_with_nanobind(nb::module_& m) {
        nb::class_<PyEvaluationClock>(m, "EvaluationClock",
            "Python wrapper for EvaluationClock - provides access to evaluation clock functionality")
            .def_prop_ro("evaluation_time", &PyEvaluationClock::evaluation_time)
            .def_prop_ro("now", &PyEvaluationClock::now)
            .def_prop_ro("next_cycle_evaluation_time", &PyEvaluationClock::next_cycle_evaluation_time)
            .def_prop_ro("cycle_time", &PyEvaluationClock::cycle_time)
            .def("is_valid", &PyEvaluationClock::is_valid,
                "Check if this wrapper is valid (graph is alive and wrapper has value)")
            .def("__str__", &PyEvaluationClock::str)
            .def("__repr__", &PyEvaluationClock::repr);
    }
    
} // namespace hgraph::api

