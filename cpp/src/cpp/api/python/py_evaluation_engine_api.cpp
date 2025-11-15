//
// PyEvaluationEngineApi Implementation
//

#include <hgraph/api/python/py_evaluation_engine_api.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <fmt/format.h>

namespace hgraph::api {
    
    PyEvaluationEngineApi::PyEvaluationEngineApi(EvaluationEngineApi* impl, control_block_ptr control_block)
        : _impl(impl, std::move(control_block)) {
    }
    
    nb::object PyEvaluationEngineApi::evaluation_mode() const {
        return nb::cast(_impl->evaluation_mode());
    }
    
    nb::object PyEvaluationEngineApi::start_time() const {
        return nb::cast(_impl->start_time());
    }
    
    nb::object PyEvaluationEngineApi::end_time() const {
        return nb::cast(_impl->end_time());
    }
    
    nb::object PyEvaluationEngineApi::evaluation_clock() const {
        return wrap_evaluation_clock(_impl->evaluation_clock().get(), _impl.control_block());
    }
    
    void PyEvaluationEngineApi::request_engine_stop() {
        _impl->request_engine_stop();
    }
    
    bool PyEvaluationEngineApi::is_stop_requested() const {
        return _impl->is_stop_requested();
    }
    
    void PyEvaluationEngineApi::add_before_evaluation_notification(nb::callable fn) {
        // Increment reference count to keep callable alive
        fn.inc_ref();
        _impl->add_before_evaluation_notification([fn]() mutable {
            fn();
        });
    }
    
    void PyEvaluationEngineApi::add_after_evaluation_notification(nb::callable fn) {
        // Increment reference count to keep callable alive
        fn.inc_ref();
        _impl->add_after_evaluation_notification([fn]() mutable {
            fn();
        });
    }
    
    void PyEvaluationEngineApi::add_life_cycle_observer(nb::object observer) {
        auto obs = nb::cast<EvaluationLifeCycleObserver::ptr>(observer);
        _impl->add_life_cycle_observer(obs);
    }
    
    void PyEvaluationEngineApi::remove_life_cycle_observer(nb::object observer) {
        auto obs = nb::cast<EvaluationLifeCycleObserver::ptr>(observer);
        _impl->remove_life_cycle_observer(obs);
    }
    
    std::string PyEvaluationEngineApi::str() const {
        return fmt::format("EvaluationEngineApi@{:p}", static_cast<const void*>(_impl.get()));
    }
    
    std::string PyEvaluationEngineApi::repr() const {
        return str();
    }
    
    void PyEvaluationEngineApi::register_with_nanobind(nb::module_& m) {
        nb::class_<PyEvaluationEngineApi>(m, "EvaluationEngineApi",
            "Python wrapper for EvaluationEngineApi - provides access to evaluation engine functionality")
            .def_prop_ro("evaluation_mode", &PyEvaluationEngineApi::evaluation_mode)
            .def_prop_ro("start_time", &PyEvaluationEngineApi::start_time)
            .def_prop_ro("end_time", &PyEvaluationEngineApi::end_time)
            .def_prop_ro("evaluation_clock", &PyEvaluationEngineApi::evaluation_clock)
            .def("request_engine_stop", &PyEvaluationEngineApi::request_engine_stop)
            .def_prop_ro("is_stop_requested", &PyEvaluationEngineApi::is_stop_requested)
            .def("add_before_evaluation_notification", &PyEvaluationEngineApi::add_before_evaluation_notification, "fn"_a)
            .def("add_after_evaluation_notification", &PyEvaluationEngineApi::add_after_evaluation_notification, "fn"_a)
            .def("add_life_cycle_observer", &PyEvaluationEngineApi::add_life_cycle_observer, "observer"_a)
            .def("remove_life_cycle_observer", &PyEvaluationEngineApi::remove_life_cycle_observer, "observer"_a)
            .def("is_valid", &PyEvaluationEngineApi::is_valid,
                "Check if this wrapper is valid (graph is alive and wrapper has value)")
            .def("__str__", &PyEvaluationEngineApi::str)
            .def("__repr__", &PyEvaluationEngineApi::repr);
    }
    
} // namespace hgraph::api

