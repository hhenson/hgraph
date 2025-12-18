#include <hgraph/api/python/py_tsb.h>

namespace hgraph
{
    // PyTimeSeriesBundleOutput implementations
    nb::object PyTimeSeriesBundleOutput::get_item(const nb::handle &key) const {
        // TODO: Implement via view field navigation
        return nb::none();
    }

    nb::object PyTimeSeriesBundleOutput::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    nb::bool_ PyTimeSeriesBundleOutput::contains(const nb::handle &key) const {
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesBundleOutput::iter() const {
        return nb::iter(keys());
    }

    nb::object PyTimeSeriesBundleOutput::keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::valid_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::valid_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::valid_items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::modified_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::modified_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::modified_items() const {
        return nb::list();
    }

    nb::int_ PyTimeSeriesBundleOutput::len() const {
        return nb::int_(view().field_count());
    }

    nb::bool_ PyTimeSeriesBundleOutput::empty() const {
        return nb::bool_(view().field_count() == 0);
    }

    nb::str PyTimeSeriesBundleOutput::py_str() const {
        return nb::str("TSB{...}");
    }

    nb::str PyTimeSeriesBundleOutput::py_repr() const {
        return py_str();
    }

    // PyTimeSeriesBundleInput implementations
    nb::object PyTimeSeriesBundleInput::get_item(const nb::handle &key) const {
        return nb::none();
    }

    nb::object PyTimeSeriesBundleInput::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    nb::bool_ PyTimeSeriesBundleInput::contains(const nb::handle &key) const {
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesBundleInput::iter() const {
        return nb::iter(keys());
    }

    nb::object PyTimeSeriesBundleInput::keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::valid_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::valid_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::valid_items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::modified_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::modified_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::modified_items() const {
        return nb::list();
    }

    nb::int_ PyTimeSeriesBundleInput::len() const {
        return nb::int_(view().field_count());
    }

    nb::bool_ PyTimeSeriesBundleInput::empty() const {
        return nb::bool_(view().field_count() == 0);
    }

    nb::str PyTimeSeriesBundleInput::py_str() const {
        return nb::str("TSB{...}");
    }

    nb::str PyTimeSeriesBundleInput::py_repr() const {
        return py_str();
    }

    void tsb_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleOutput, PyTimeSeriesOutput>(m, "TimeSeriesBundleOutput")
            .def("__getitem__", &PyTimeSeriesBundleOutput::get_item)
            .def("__getattr__", &PyTimeSeriesBundleOutput::get_attr)
            .def("__iter__", &PyTimeSeriesBundleOutput::iter)
            .def("__len__", &PyTimeSeriesBundleOutput::len)
            .def("__contains__", &PyTimeSeriesBundleOutput::contains)
            .def("keys", &PyTimeSeriesBundleOutput::keys)
            .def("values", &PyTimeSeriesBundleOutput::values)
            .def("items", &PyTimeSeriesBundleOutput::items)
            .def("valid_keys", &PyTimeSeriesBundleOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesBundleOutput::valid_values)
            .def("valid_items", &PyTimeSeriesBundleOutput::valid_items)
            .def("modified_keys", &PyTimeSeriesBundleOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesBundleOutput::modified_values)
            .def("modified_items", &PyTimeSeriesBundleOutput::modified_items)
            .def_prop_ro("empty", &PyTimeSeriesBundleOutput::empty)
            .def("__str__", &PyTimeSeriesBundleOutput::py_str)
            .def("__repr__", &PyTimeSeriesBundleOutput::py_repr);

        nb::class_<PyTimeSeriesBundleInput, PyTimeSeriesInput>(m, "TimeSeriesBundleInput")
            .def("__getitem__", &PyTimeSeriesBundleInput::get_item)
            .def("__getattr__", &PyTimeSeriesBundleInput::get_attr)
            .def("__iter__", &PyTimeSeriesBundleInput::iter)
            .def("__len__", &PyTimeSeriesBundleInput::len)
            .def("__contains__", &PyTimeSeriesBundleInput::contains)
            .def("keys", &PyTimeSeriesBundleInput::keys)
            .def("values", &PyTimeSeriesBundleInput::values)
            .def("items", &PyTimeSeriesBundleInput::items)
            .def("valid_keys", &PyTimeSeriesBundleInput::valid_keys)
            .def("valid_values", &PyTimeSeriesBundleInput::valid_values)
            .def("valid_items", &PyTimeSeriesBundleInput::valid_items)
            .def("modified_keys", &PyTimeSeriesBundleInput::modified_keys)
            .def("modified_values", &PyTimeSeriesBundleInput::modified_values)
            .def("modified_items", &PyTimeSeriesBundleInput::modified_items)
            .def_prop_ro("empty", &PyTimeSeriesBundleInput::empty)
            .def("__str__", &PyTimeSeriesBundleInput::py_str)
            .def("__repr__", &PyTimeSeriesBundleInput::py_repr);
    }

}  // namespace hgraph
