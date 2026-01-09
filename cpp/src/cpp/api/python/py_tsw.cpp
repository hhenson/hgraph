#include <hgraph/api/python/py_tsw.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph
{
    // ========== PyTimeSeriesFixedWindowOutput ==========

    PyTimeSeriesFixedWindowOutput::PyTimeSeriesFixedWindowOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    nb::object PyTimeSeriesFixedWindowOutput::value_times() const {
        throw std::runtime_error("PyTimeSeriesFixedWindowOutput::value_times not yet implemented for view-based wrappers");
    }

    engine_time_t PyTimeSeriesFixedWindowOutput::first_modified_time() const {
        throw std::runtime_error("PyTimeSeriesFixedWindowOutput::first_modified_time not yet implemented for view-based wrappers");
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::size() const {
        throw std::runtime_error("PyTimeSeriesFixedWindowOutput::size not yet implemented for view-based wrappers");
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::min_size() const {
        throw std::runtime_error("PyTimeSeriesFixedWindowOutput::min_size not yet implemented for view-based wrappers");
    }

    nb::bool_ PyTimeSeriesFixedWindowOutput::has_removed_value() const {
        throw std::runtime_error("PyTimeSeriesFixedWindowOutput::has_removed_value not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesFixedWindowOutput::removed_value() const {
        throw std::runtime_error("PyTimeSeriesFixedWindowOutput::removed_value not yet implemented for view-based wrappers");
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::len() const {
        throw std::runtime_error("PyTimeSeriesFixedWindowOutput::len not yet implemented for view-based wrappers");
    }

    // ========== PyTimeSeriesTimeWindowOutput ==========

    PyTimeSeriesTimeWindowOutput::PyTimeSeriesTimeWindowOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    nb::object PyTimeSeriesTimeWindowOutput::value_times() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::value_times not yet implemented for view-based wrappers");
    }

    engine_time_t PyTimeSeriesTimeWindowOutput::first_modified_time() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::first_modified_time not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesTimeWindowOutput::size() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::size not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesTimeWindowOutput::min_size() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::min_size not yet implemented for view-based wrappers");
    }

    nb::bool_ PyTimeSeriesTimeWindowOutput::has_removed_value() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::has_removed_value not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesTimeWindowOutput::removed_value() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::removed_value not yet implemented for view-based wrappers");
    }

    nb::int_ PyTimeSeriesTimeWindowOutput::len() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::len not yet implemented for view-based wrappers");
    }

    // ========== PyTimeSeriesWindowInput ==========

    PyTimeSeriesWindowInput::PyTimeSeriesWindowInput(TSView view)
        : PyTimeSeriesInput(view) {}

    nb::object PyTimeSeriesWindowInput::value_times() const {
        throw std::runtime_error("PyTimeSeriesWindowInput::value_times not yet implemented for view-based wrappers");
    }

    engine_time_t PyTimeSeriesWindowInput::first_modified_time() const {
        throw std::runtime_error("PyTimeSeriesWindowInput::first_modified_time not yet implemented for view-based wrappers");
    }

    nb::bool_ PyTimeSeriesWindowInput::has_removed_value() const {
        throw std::runtime_error("PyTimeSeriesWindowInput::has_removed_value not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesWindowInput::removed_value() const {
        throw std::runtime_error("PyTimeSeriesWindowInput::removed_value not yet implemented for view-based wrappers");
    }

    nb::int_ PyTimeSeriesWindowInput::len() const {
        throw std::runtime_error("PyTimeSeriesWindowInput::len not yet implemented for view-based wrappers");
    }

    // ========== Nanobind Registration ==========

    void tsw_register_with_nanobind(nb::module_ &m) {
        // No need to re-register value property - base class handles it via TSView

        // Fixed-size window output
        nb::class_<PyTimeSeriesFixedWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesFixedWindowOutput")
            .def_prop_ro("value_times", &PyTimeSeriesFixedWindowOutput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesFixedWindowOutput::first_modified_time)
            .def_prop_ro("size", &PyTimeSeriesFixedWindowOutput::size)
            .def_prop_ro("min_size", &PyTimeSeriesFixedWindowOutput::min_size)
            .def_prop_ro("has_removed_value", &PyTimeSeriesFixedWindowOutput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesFixedWindowOutput::removed_value)
            .def("__len__", &PyTimeSeriesFixedWindowOutput::len);

        // Time-based window output
        nb::class_<PyTimeSeriesTimeWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesTimeWindowOutput")
            .def_prop_ro("value_times", &PyTimeSeriesTimeWindowOutput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesTimeWindowOutput::first_modified_time)
            .def_prop_ro("size", &PyTimeSeriesTimeWindowOutput::size)
            .def_prop_ro("min_size", &PyTimeSeriesTimeWindowOutput::min_size)
            .def_prop_ro("has_removed_value", &PyTimeSeriesTimeWindowOutput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesTimeWindowOutput::removed_value)
            .def("__len__", &PyTimeSeriesTimeWindowOutput::len);

        // Window input (works with both fixed and time-based outputs)
        nb::class_<PyTimeSeriesWindowInput, PyTimeSeriesInput>(m, "TimeSeriesWindowInput")
            .def_prop_ro("value_times", &PyTimeSeriesWindowInput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesWindowInput::first_modified_time)
            .def_prop_ro("has_removed_value", &PyTimeSeriesWindowInput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesWindowInput::removed_value)
            .def("__len__", &PyTimeSeriesWindowInput::len);
    }

}  // namespace hgraph
