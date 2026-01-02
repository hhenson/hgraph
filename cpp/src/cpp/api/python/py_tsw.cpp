#include <hgraph/api/python/py_tsw.h>

namespace hgraph
{
    // ========== PyTimeSeriesFixedWindowOutput ==========

    PyTimeSeriesFixedWindowOutput::PyTimeSeriesFixedWindowOutput(api_ptr impl)
        : PyTimeSeriesOutput(std::move(impl)) {}

    TimeSeriesFixedWindowOutput* PyTimeSeriesFixedWindowOutput::impl() const {
        return this->template static_cast_impl<TimeSeriesFixedWindowOutput>();
    }

    nb::object PyTimeSeriesFixedWindowOutput::value_times() const {
        return impl()->py_value_times();
    }

    engine_time_t PyTimeSeriesFixedWindowOutput::first_modified_time() const {
        return impl()->first_modified_time();
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::size() const {
        return nb::int_(impl()->size());
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::min_size() const {
        return nb::int_(impl()->min_size());
    }

    nb::bool_ PyTimeSeriesFixedWindowOutput::has_removed_value() const {
        return nb::bool_(impl()->has_removed_value());
    }

    nb::object PyTimeSeriesFixedWindowOutput::removed_value() const {
        return impl()->py_removed_value();
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::len() const {
        return nb::int_(impl()->len());
    }

    // ========== PyTimeSeriesTimeWindowOutput ==========

    PyTimeSeriesTimeWindowOutput::PyTimeSeriesTimeWindowOutput(api_ptr impl)
        : PyTimeSeriesOutput(std::move(impl)) {}

    TimeSeriesTimeWindowOutput* PyTimeSeriesTimeWindowOutput::impl() const {
        return this->template static_cast_impl<TimeSeriesTimeWindowOutput>();
    }

    nb::object PyTimeSeriesTimeWindowOutput::value_times() const {
        return impl()->py_value_times();
    }

    engine_time_t PyTimeSeriesTimeWindowOutput::first_modified_time() const {
        return impl()->first_modified_time();
    }

    nb::object PyTimeSeriesTimeWindowOutput::size() const {
        return nb::cast(impl()->size());
    }

    nb::object PyTimeSeriesTimeWindowOutput::min_size() const {
        return nb::cast(impl()->min_size());
    }

    nb::bool_ PyTimeSeriesTimeWindowOutput::has_removed_value() const {
        return nb::bool_(impl()->has_removed_value());
    }

    nb::object PyTimeSeriesTimeWindowOutput::removed_value() const {
        return impl()->py_removed_value();
    }

    nb::int_ PyTimeSeriesTimeWindowOutput::len() const {
        return nb::int_(impl()->len());
    }

    // ========== PyTimeSeriesWindowInput ==========

    PyTimeSeriesWindowInput::PyTimeSeriesWindowInput(api_ptr impl)
        : PyTimeSeriesInput(std::move(impl)) {}

    TimeSeriesWindowInput* PyTimeSeriesWindowInput::impl() const {
        return this->template static_cast_impl<TimeSeriesWindowInput>();
    }

    nb::object PyTimeSeriesWindowInput::value_times() const {
        return impl()->py_value_times();
    }

    engine_time_t PyTimeSeriesWindowInput::first_modified_time() const {
        return impl()->first_modified_time();
    }

    nb::bool_ PyTimeSeriesWindowInput::has_removed_value() const {
        return nb::bool_(impl()->has_removed_value());
    }

    nb::object PyTimeSeriesWindowInput::removed_value() const {
        return impl()->py_removed_value();
    }

    nb::int_ PyTimeSeriesWindowInput::len() const {
        return nb::int_(impl()->len());
    }

    // ========== Nanobind Registration ==========

    void tsw_register_with_nanobind(nb::module_ &m) {
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
