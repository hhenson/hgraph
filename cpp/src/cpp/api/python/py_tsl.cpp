#include <hgraph/api/python/py_tsl.h>

namespace hgraph
{
    // PyTimeSeriesListOutput implementations
    nb::object PyTimeSeriesListOutput::get_item(const nb::handle &key) const {
        // TODO: Implement via view
        return nb::none();
    }

    nb::object PyTimeSeriesListOutput::iter() const {
        return nb::iter(values());
    }

    nb::object PyTimeSeriesListOutput::keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListOutput::values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListOutput::items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListOutput::valid_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListOutput::valid_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListOutput::valid_items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListOutput::modified_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListOutput::modified_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListOutput::modified_items() const {
        return nb::list();
    }

    nb::int_ PyTimeSeriesListOutput::len() const {
        return nb::int_(view().list_size());
    }

    bool PyTimeSeriesListOutput::empty() const {
        return view().list_size() == 0;
    }

    void PyTimeSeriesListOutput::clear() {
        // TODO: Implement via view
    }

    nb::str PyTimeSeriesListOutput::py_str() const {
        return nb::str("TSL[...]");
    }

    nb::str PyTimeSeriesListOutput::py_repr() const {
        return py_str();
    }

    // PyTimeSeriesListInput implementations
    nb::object PyTimeSeriesListInput::get_item(const nb::handle &key) const {
        return nb::none();
    }

    nb::object PyTimeSeriesListInput::iter() const {
        return nb::iter(values());
    }

    nb::object PyTimeSeriesListInput::keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListInput::values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListInput::items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListInput::valid_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListInput::valid_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListInput::valid_items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListInput::modified_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListInput::modified_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesListInput::modified_items() const {
        return nb::list();
    }

    nb::int_ PyTimeSeriesListInput::len() const {
        return nb::int_(view().list_size());
    }

    bool PyTimeSeriesListInput::empty() const {
        return view().list_size() == 0;
    }

    nb::str PyTimeSeriesListInput::py_str() const {
        return nb::str("TSL[...]");
    }

    nb::str PyTimeSeriesListInput::py_repr() const {
        return py_str();
    }

    void tsl_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListOutput, PyTimeSeriesOutput>(m, "TimeSeriesListOutput")
            .def("__getitem__", &PyTimeSeriesListOutput::get_item)
            .def("__iter__", &PyTimeSeriesListOutput::iter)
            .def("__len__", &PyTimeSeriesListOutput::len)
            .def_prop_ro("empty", &PyTimeSeriesListOutput::empty)
            .def("keys", &PyTimeSeriesListOutput::keys)
            .def("values", &PyTimeSeriesListOutput::values)
            .def("items", &PyTimeSeriesListOutput::items)
            .def("valid_keys", &PyTimeSeriesListOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesListOutput::valid_values)
            .def("valid_items", &PyTimeSeriesListOutput::valid_items)
            .def("modified_keys", &PyTimeSeriesListOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesListOutput::modified_values)
            .def("modified_items", &PyTimeSeriesListOutput::modified_items)
            .def("clear", &PyTimeSeriesListOutput::clear)
            .def("__str__", &PyTimeSeriesListOutput::py_str)
            .def("__repr__", &PyTimeSeriesListOutput::py_repr);

        nb::class_<PyTimeSeriesListInput, PyTimeSeriesInput>(m, "TimeSeriesListInput")
            .def("__getitem__", &PyTimeSeriesListInput::get_item)
            .def("__iter__", &PyTimeSeriesListInput::iter)
            .def("__len__", &PyTimeSeriesListInput::len)
            .def_prop_ro("empty", &PyTimeSeriesListInput::empty)
            .def("keys", &PyTimeSeriesListInput::keys)
            .def("values", &PyTimeSeriesListInput::values)
            .def("items", &PyTimeSeriesListInput::items)
            .def("valid_keys", &PyTimeSeriesListInput::valid_keys)
            .def("valid_values", &PyTimeSeriesListInput::valid_values)
            .def("valid_items", &PyTimeSeriesListInput::valid_items)
            .def("modified_keys", &PyTimeSeriesListInput::modified_keys)
            .def("modified_values", &PyTimeSeriesListInput::modified_values)
            .def("modified_items", &PyTimeSeriesListInput::modified_items)
            .def("__str__", &PyTimeSeriesListInput::py_str)
            .def("__repr__", &PyTimeSeriesListInput::py_repr);
    }

}  // namespace hgraph
