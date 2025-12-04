#include <fmt/format.h>
#include <hgraph/api/python/py_evaluation_engine.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph
{

    PyEvaluationEngineApi::PyEvaluationEngineApi(api_ptr engine) : _impl{std::move(engine)} {}

    EvaluationMode PyEvaluationEngineApi::evaluation_mode() const { return _impl->evaluation_mode(); }

    engine_time_t PyEvaluationEngineApi::start_time() const { return _impl->start_time(); }

    engine_time_t PyEvaluationEngineApi::end_time() const { return _impl->end_time(); }

    PyEvaluationClock PyEvaluationEngineApi::evaluation_clock() const
    {
        auto clock = _impl->evaluation_clock();
        if (!clock) {
            throw std::runtime_error("EvaluationEngineApi::evaluation_clock() returned null");
        }
        return PyEvaluationClock(ApiPtr<EvaluationClock>(clock));
    }

    void PyEvaluationEngineApi::request_engine_stop() const { _impl->request_engine_stop(); }

    nb::bool_ PyEvaluationEngineApi::is_stop_requested() const { return nb::bool_(_impl->is_stop_requested()); }

    void PyEvaluationEngineApi::add_before_evaluation_notification(nb::callable fn) const
    {
        _impl->add_before_evaluation_notification([fn = std::move(fn)]() { fn(); });
    }

    void PyEvaluationEngineApi::add_after_evaluation_notification(nb::callable fn) const
    {
        _impl->add_after_evaluation_notification([fn = std::move(fn)]() { fn(); });
    }

    void PyEvaluationEngineApi::add_life_cycle_observer(nb::object observer) const
    {
        auto obs_ptr = nb::cast<EvaluationLifeCycleObserver::ptr>(observer);
        _impl->add_life_cycle_observer(std::move(obs_ptr));
    }

    void PyEvaluationEngineApi::remove_life_cycle_observer(nb::object observer) const
    {
        auto obs_ptr = nb::cast<EvaluationLifeCycleObserver::ptr>(observer);
        _impl->remove_life_cycle_observer(std::move(obs_ptr));
    }

    nb::bool_ PyEvaluationEngineApi::is_started() const { return nb::bool_(_impl->is_started()); }

    nb::bool_ PyEvaluationEngineApi::is_starting() const { return nb::bool_(_impl->is_starting()); }

    nb::bool_ PyEvaluationEngineApi::is_stopping() const { return nb::bool_(_impl->is_stopping()); }

    nb::str PyEvaluationEngineApi::str() const
    {
        auto s = fmt::format("EvaluationEngineApi@{:p}", static_cast<const void *>(_impl.get()));
        return nb::str(s.c_str());
    }

    nb::str PyEvaluationEngineApi::repr() const
    {
        auto s = fmt::format("EvaluationEngineApi@{:p}", static_cast<const void *>(_impl.get()));
        return nb::str(s.c_str());
    }

    void PyEvaluationEngineApi::register_with_nanobind(nb::module_ &m)
    {
        // Register wrapper as EvaluationEngineApi - this is the only Python API for the engine
        nb::class_<PyEvaluationEngineApi>(m, "EvaluationEngineApi")
            .def_prop_ro("evaluation_mode", &PyEvaluationEngineApi::evaluation_mode)
            .def_prop_ro("start_time", &PyEvaluationEngineApi::start_time)
            .def_prop_ro("end_time", &PyEvaluationEngineApi::end_time)
            .def_prop_ro("evaluation_clock", &PyEvaluationEngineApi::evaluation_clock)
            .def("request_engine_stop", &PyEvaluationEngineApi::request_engine_stop)
            .def_prop_ro("is_stop_requested", &PyEvaluationEngineApi::is_stop_requested)
            .def("add_before_evaluation_notification", &PyEvaluationEngineApi::add_before_evaluation_notification,
                 "fn"_a)
            .def("add_after_evaluation_notification", &PyEvaluationEngineApi::add_after_evaluation_notification, "fn"_a)
            .def("add_life_cycle_observer", &PyEvaluationEngineApi::add_life_cycle_observer, "observer"_a)
            .def("remove_life_cycle_observer", &PyEvaluationEngineApi::remove_life_cycle_observer, "observer"_a)
            .def_prop_ro("is_started", &PyEvaluationEngineApi::is_started)
            .def_prop_ro("is_starting", &PyEvaluationEngineApi::is_starting)
            .def_prop_ro("is_stopping", &PyEvaluationEngineApi::is_stopping)
            .def("__str__", &PyEvaluationEngineApi::str)
            .def("__repr__", &PyEvaluationEngineApi::repr);
    }

}  // namespace hgraph

