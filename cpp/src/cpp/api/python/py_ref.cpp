#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/ref.h>

namespace hgraph
{

    // TimeSeriesReference Python bindings
    static void register_time_series_reference(nb::module_ &m) {
        nb::class_<TimeSeriesReference>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__repr__", &TimeSeriesReference::to_string)
            // Use std::optional with nb::arg().none() to allow None as input
            .def("__eq__", [](const TimeSeriesReference &self, std::optional<TimeSeriesReference> other) {
                if (!other.has_value()) {
                    return false;  // None comparison
                }
                return self == other.value();
            }, nb::arg("other").none())
            .def("__ne__", [](const TimeSeriesReference &self, std::optional<TimeSeriesReference> other) {
                if (!other.has_value()) {
                    return true;  // None comparison
                }
                return !(self == other.value());
            }, nb::arg("other").none())
            .def(
                "bind_input",
                [](TimeSeriesReference &self, PyTimeSeriesInput &ts_input) {
                    // TODO: bind_input requires input implementation
                    throw std::runtime_error("TimeSeriesReference::bind_input not yet implemented");
                },
                "input_"_a)
            .def_prop_ro("has_output", &TimeSeriesReference::has_output)
            .def_prop_ro("is_empty", &TimeSeriesReference::is_empty)
            .def_prop_ro("is_bound", &TimeSeriesReference::is_bound)
            .def_prop_ro("is_unbound", &TimeSeriesReference::is_unbound)
            .def_prop_ro("is_valid", &TimeSeriesReference::is_valid)
            .def_prop_ro("output", [](TimeSeriesReference &self) -> nb::object {
                // TODO: requires wrap_output
                if (!self.has_output()) return nb::none();
                throw std::runtime_error("TimeSeriesReference::output not yet implemented");
            })
            .def_prop_ro("items", &TimeSeriesReference::items)
            .def("__getitem__", &TimeSeriesReference::operator[])
            .def_static(
                "make",
                [](nb::object ts, nb::object items) -> TimeSeriesReference {
                    if (!ts.is_none()) {
                        if (nb::isinstance<PyTimeSeriesOutput>(ts)) {
                            // TODO: requires unwrap_output
                            throw std::runtime_error("TimeSeriesReference::make from output not yet implemented");
                        }
                        if (nb::isinstance<PyTimeSeriesReferenceInput>(ts)) {
                            // TODO: requires reference input value access
                            throw std::runtime_error("TimeSeriesReference::make from ref input not yet implemented");
                        }
                        if (nb::isinstance<PyTimeSeriesInput>(ts)) {
                            // TODO: requires input handling
                            throw std::runtime_error("TimeSeriesReference::make from input not yet implemented");
                        }
                    } else if (!items.is_none()) {
                        auto items_list = nb::cast<std::vector<TimeSeriesReference>>(items);
                        return TimeSeriesReference::make(items_list);
                    }
                    return TimeSeriesReference::make();
                },
                "ts"_a = nb::none(), "from_items"_a = nb::none());
    }

    // PyTimeSeriesReferenceOutput implementations
    nb::str PyTimeSeriesReferenceOutput::to_string() const {
        return nb::str("REF(output)");
    }

    nb::str PyTimeSeriesReferenceOutput::to_repr() const {
        return to_string();
    }

    void PyTimeSeriesReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReferenceOutput, PyTimeSeriesOutput>(m, "TimeSeriesReferenceOutput")
            .def("__str__", &PyTimeSeriesReferenceOutput::to_string)
            .def("__repr__", &PyTimeSeriesReferenceOutput::to_repr);
    }

    // PyTimeSeriesReferenceInput implementations
    nb::str PyTimeSeriesReferenceInput::to_string() const {
        return nb::str("REF(input)");
    }

    nb::str PyTimeSeriesReferenceInput::to_repr() const {
        return to_string();
    }

    void PyTimeSeriesReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput")
            .def("__str__", &PyTimeSeriesReferenceInput::to_string)
            .def("__repr__", &PyTimeSeriesReferenceInput::to_repr);
    }

    // Specialized reference wrapper implementations
    void PyTimeSeriesValueReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesValueReferenceOutput");
    }

    void PyTimeSeriesValueReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesValueReferenceInput");
    }

    nb::int_ PyTimeSeriesListReferenceOutput::size() const {
        return nb::int_(0);
    }

    void PyTimeSeriesListReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesListReferenceOutput")
            .def("__len__", &PyTimeSeriesListReferenceOutput::size);
    }

    size_t PyTimeSeriesListReferenceInput::size() const {
        return 0;
    }

    void PyTimeSeriesListReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesListReferenceInput")
            .def("__len__", &PyTimeSeriesListReferenceInput::size);
    }

    nb::int_ PyTimeSeriesBundleReferenceOutput::size() const {
        return nb::int_(0);
    }

    void PyTimeSeriesBundleReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesBundleReferenceOutput")
            .def("__len__", &PyTimeSeriesBundleReferenceOutput::size);
    }

    nb::int_ PyTimeSeriesBundleReferenceInput::size() const {
        return nb::int_(0);
    }

    void PyTimeSeriesBundleReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesBundleReferenceInput")
            .def("__len__", &PyTimeSeriesBundleReferenceInput::size);
    }

    void PyTimeSeriesDictReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesDictReferenceOutput");
    }

    void PyTimeSeriesDictReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesDictReferenceInput");
    }

    void PyTimeSeriesSetReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesSetReferenceOutput");
    }

    void PyTimeSeriesSetReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesSetReferenceInput");
    }

    void PyTimeSeriesWindowReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesWindowReferenceOutput");
    }

    void PyTimeSeriesWindowReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesWindowReferenceInput");
    }

    void ref_register_with_nanobind(nb::module_ &m) {
        // Register the TimeSeriesReference value type first
        register_time_series_reference(m);

        // Then register the input/output wrapper types
        PyTimeSeriesReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesReferenceInput::register_with_nanobind(m);
        PyTimeSeriesValueReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesValueReferenceInput::register_with_nanobind(m);
        PyTimeSeriesListReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesListReferenceInput::register_with_nanobind(m);
        PyTimeSeriesBundleReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesBundleReferenceInput::register_with_nanobind(m);
        PyTimeSeriesDictReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesDictReferenceInput::register_with_nanobind(m);
        PyTimeSeriesSetReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesSetReferenceInput::register_with_nanobind(m);
        PyTimeSeriesWindowReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesWindowReferenceInput::register_with_nanobind(m);
    }

}  // namespace hgraph
