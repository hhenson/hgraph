#include <hgraph/api/python/py_tss.h>

namespace hgraph
{
    // PyTimeSeriesSetOutput implementations
    bool PyTimeSeriesSetOutput::contains(const nb::object &item) const {
        // TODO: Implement via view
        return false;
    }

    size_t PyTimeSeriesSetOutput::size() const {
        return view().set_size();
    }

    nb::bool_ PyTimeSeriesSetOutput::empty() const {
        return nb::bool_(view().set_size() == 0);
    }

    void PyTimeSeriesSetOutput::add(const nb::object &item) {
        // TODO: Implement via view with proper type conversion
    }

    void PyTimeSeriesSetOutput::remove(const nb::object &item) {
        // TODO: Implement via view with proper type conversion
    }

    nb::object PyTimeSeriesSetOutput::values() const {
        return value();
    }

    nb::object PyTimeSeriesSetOutput::added() const {
        // TODO: Implement delta tracking
        return nb::frozenset();
    }

    nb::object PyTimeSeriesSetOutput::removed() const {
        // TODO: Implement delta tracking
        return nb::frozenset();
    }

    nb::bool_ PyTimeSeriesSetOutput::was_added(const nb::object &item) const {
        return nb::bool_(false);
    }

    nb::bool_ PyTimeSeriesSetOutput::was_removed(const nb::object &item) const {
        return nb::bool_(false);
    }

    nb::str PyTimeSeriesSetOutput::py_str() const {
        return nb::str("TSS{...}");
    }

    nb::str PyTimeSeriesSetOutput::py_repr() const {
        return py_str();
    }

    // PyTimeSeriesSetInput implementations
    bool PyTimeSeriesSetInput::contains(const nb::object &item) const {
        // TODO: Implement via view
        return false;
    }

    size_t PyTimeSeriesSetInput::size() const {
        return view().set_size();
    }

    nb::bool_ PyTimeSeriesSetInput::empty() const {
        return nb::bool_(view().set_size() == 0);
    }

    nb::object PyTimeSeriesSetInput::values() const {
        return value();
    }

    nb::object PyTimeSeriesSetInput::added() const {
        // TODO: Implement delta tracking
        return nb::frozenset();
    }

    nb::object PyTimeSeriesSetInput::removed() const {
        // TODO: Implement delta tracking
        return nb::frozenset();
    }

    nb::bool_ PyTimeSeriesSetInput::was_added(const nb::object &item) const {
        return nb::bool_(false);
    }

    nb::bool_ PyTimeSeriesSetInput::was_removed(const nb::object &item) const {
        return nb::bool_(false);
    }

    nb::str PyTimeSeriesSetInput::py_str() const {
        return nb::str("TSS{...}");
    }

    nb::str PyTimeSeriesSetInput::py_repr() const {
        return py_str();
    }

    void tss_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput")
            .def("__contains__", &PyTimeSeriesSetOutput::contains)
            .def("__len__", [](const PyTimeSeriesSetOutput& self) { return self.size(); })
            .def_prop_ro("empty", &PyTimeSeriesSetOutput::empty)
            .def("add", &PyTimeSeriesSetOutput::add)
            .def("remove", &PyTimeSeriesSetOutput::remove)
            .def_prop_ro("values", &PyTimeSeriesSetOutput::values)
            .def_prop_ro("added", &PyTimeSeriesSetOutput::added)
            .def_prop_ro("removed", &PyTimeSeriesSetOutput::removed)
            .def("was_added", &PyTimeSeriesSetOutput::was_added)
            .def("was_removed", &PyTimeSeriesSetOutput::was_removed)
            .def("__str__", &PyTimeSeriesSetOutput::py_str)
            .def("__repr__", &PyTimeSeriesSetOutput::py_repr);

        nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput")
            .def("__contains__", &PyTimeSeriesSetInput::contains)
            .def("__len__", [](const PyTimeSeriesSetInput& self) { return self.size(); })
            .def_prop_ro("empty", &PyTimeSeriesSetInput::empty)
            .def_prop_ro("values", &PyTimeSeriesSetInput::values)
            .def_prop_ro("added", &PyTimeSeriesSetInput::added)
            .def_prop_ro("removed", &PyTimeSeriesSetInput::removed)
            .def("was_added", &PyTimeSeriesSetInput::was_added)
            .def("was_removed", &PyTimeSeriesSetInput::was_removed)
            .def("__str__", &PyTimeSeriesSetInput::py_str)
            .def("__repr__", &PyTimeSeriesSetInput::py_repr);
    }

}  // namespace hgraph
