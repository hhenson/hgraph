#include <hgraph/api/python/py_tsw.h>

namespace hgraph
{
    // PyTimeSeriesWindowOutput implementations
    nb::object PyTimeSeriesWindowOutput::value_times() const {
        // TODO: Implement via view
        return nb::none();
    }

    engine_time_t PyTimeSeriesWindowOutput::first_modified_time() const {
        // TODO: Implement via view
        return MIN_DT;
    }

    nb::object PyTimeSeriesWindowOutput::window_size() const {
        // TODO: Implement via view
        return nb::none();
    }

    nb::object PyTimeSeriesWindowOutput::min_size() const {
        // TODO: Implement via view
        return nb::none();
    }

    nb::bool_ PyTimeSeriesWindowOutput::has_removed_value() const {
        // TODO: Implement via view
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesWindowOutput::removed_value() const {
        // TODO: Implement via view
        return nb::none();
    }

    nb::int_ PyTimeSeriesWindowOutput::len() const {
        return nb::int_(view().window_size());
    }

    nb::str PyTimeSeriesWindowOutput::py_str() const {
        return nb::str("TSW[...]");
    }

    nb::str PyTimeSeriesWindowOutput::py_repr() const {
        return py_str();
    }

    // PyTimeSeriesWindowInput implementations
    nb::object PyTimeSeriesWindowInput::value_times() const {
        // TODO: Implement via view
        return nb::none();
    }

    engine_time_t PyTimeSeriesWindowInput::first_modified_time() const {
        // TODO: Implement via view
        return MIN_DT;
    }

    nb::bool_ PyTimeSeriesWindowInput::has_removed_value() const {
        // TODO: Implement via view
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesWindowInput::removed_value() const {
        // TODO: Implement via view
        return nb::none();
    }

    nb::int_ PyTimeSeriesWindowInput::len() const {
        return nb::int_(view().window_size());
    }

    nb::str PyTimeSeriesWindowInput::py_str() const {
        return nb::str("TSW[...]");
    }

    nb::str PyTimeSeriesWindowInput::py_repr() const {
        return py_str();
    }

    void tsw_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesWindowOutput")
            .def_prop_ro("value_times", &PyTimeSeriesWindowOutput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesWindowOutput::first_modified_time)
            .def_prop_ro("size", &PyTimeSeriesWindowOutput::window_size)
            .def_prop_ro("min_size", &PyTimeSeriesWindowOutput::min_size)
            .def_prop_ro("has_removed_value", &PyTimeSeriesWindowOutput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesWindowOutput::removed_value)
            .def("__len__", &PyTimeSeriesWindowOutput::len)
            .def("__str__", &PyTimeSeriesWindowOutput::py_str)
            .def("__repr__", &PyTimeSeriesWindowOutput::py_repr);

        nb::class_<PyTimeSeriesWindowInput, PyTimeSeriesInput>(m, "TimeSeriesWindowInput")
            .def_prop_ro("value_times", &PyTimeSeriesWindowInput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesWindowInput::first_modified_time)
            .def_prop_ro("has_removed_value", &PyTimeSeriesWindowInput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesWindowInput::removed_value)
            .def("__len__", &PyTimeSeriesWindowInput::len)
            .def("__str__", &PyTimeSeriesWindowInput::py_str)
            .def("__repr__", &PyTimeSeriesWindowInput::py_repr);
    }

}  // namespace hgraph
