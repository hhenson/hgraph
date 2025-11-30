#include <fmt/format.h>
#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/runtime/evaluation_engine.h>

namespace hgraph
{

    PyEvaluationClock::PyEvaluationClock(api_ptr clock) : _impl{std::move(clock)} {}

    engine_time_t PyEvaluationClock::evaluation_time() const { return _impl->evaluation_time(); }

    engine_time_t PyEvaluationClock::now() const { return _impl->now(); }

    engine_time_t PyEvaluationClock::next_cycle_evaluation_time() const { return _impl->next_cycle_evaluation_time(); }

    engine_time_delta_t PyEvaluationClock::cycle_time() const { return _impl->cycle_time(); }

    nb::object PyEvaluationClock::key() const { return _impl->py_key(); }

    nb::str PyEvaluationClock::str() const
    {
        auto &clock{*_impl};
        auto s = fmt::format("{}@{:p}[eval_time={}]", typeid(clock).name(), static_cast<const void *>(&clock),
                             clock.evaluation_time().time_since_epoch().count());
        return nb::str(s.c_str());
    }

    nb::str PyEvaluationClock::repr() const
    {
        auto &clock{*_impl};
        auto s = fmt::format("{}@{:p}[eval_time={}]", typeid(clock).name(), static_cast<const void *>(&clock),
                             clock.evaluation_time().time_since_epoch().count());
        return nb::str(s.c_str());
    }

    void PyEvaluationClock::register_with_nanobind(nb::module_ &m)
    {
        // Register wrapper as EvaluationClock - this is the only Python API for clocks
        nb::class_<PyEvaluationClock>(m, "EvaluationClock")
            .def_prop_ro("evaluation_time", &PyEvaluationClock::evaluation_time)
            .def_prop_ro("now", &PyEvaluationClock::now)
            .def_prop_ro("next_cycle_evaluation_time", &PyEvaluationClock::next_cycle_evaluation_time)
            .def_prop_ro("cycle_time", &PyEvaluationClock::cycle_time)
            .def_prop_ro("key", &PyEvaluationClock::key)
            .def("__str__", &PyEvaluationClock::str)
            .def("__repr__", &PyEvaluationClock::repr);
    }

}  // namespace hgraph

