#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_bundle_view.h>

namespace hgraph
{

    template <typename T_TS>
    PyTimeSeriesBundle<T_TS>::PyTimeSeriesBundle(view_type view) : T_TS(std::move(view)) {}

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::iter() const {
        // View-based: iterate over field TSViews
        auto bundle_view = this->view().as_bundle();
        nb::list result;
        for (auto it = bundle_view.items().begin(); it != bundle_view.items().end(); ++it) {
            TSView ts_view = *it;
            if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
            } else {
                result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
            }
        }
        return nb::iter(result);
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::get_item(const nb::handle &key) const {
        // View-based: get field view
        auto bundle_view = this->view().as_bundle();
        TSView field_view;
        if (nb::isinstance<nb::str>(key)) {
            field_view = bundle_view.field(nb::cast<std::string>(key));
        } else if (nb::isinstance<nb::int_>(key)) {
            field_view = bundle_view.field(nb::cast<size_t>(key));
        } else {
            throw std::runtime_error("Invalid key type for TimeSeriesBundle");
        }
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
            return wrap_output_view(TSOutputView(field_view, nullptr));
        } else {
            return wrap_input_view(TSInputView(field_view, nullptr));
        }
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    template <typename T_TS>
    nb::bool_ PyTimeSeriesBundle<T_TS>::contains(const nb::handle &key) const {
        // View-based: check if field name exists in schema
        if (nb::isinstance<nb::str>(key)) {
            const auto* meta = this->view().ts_meta();
            if (meta && meta->field_count > 0 && meta->fields) {
                auto name = nb::cast<std::string>(key);
                for (size_t i = 0; i < meta->field_count; ++i) {
                    if (meta->fields[i].name == name) {
                        return nb::bool_(true);
                    }
                }
            }
        }
        return nb::bool_(false);
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::schema() const {
        // TimeSeriesSchema is a Python-only type (subclass of AbstractSchema).
        // For now, throw - this may need rework to return the Python schema object.
        throw std::runtime_error("not implemented: PyTimeSeriesBundle::schema");
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::key_from_value(const nb::handle &value) const {
        // Not supported in view mode
        throw std::runtime_error("not implemented: PyTimeSeriesBundle::key_from_value");
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::keys() const {
        // View-based: return field names from meta
        const auto* meta = this->view().ts_meta();
        nb::list result;
        if (meta && meta->field_count > 0 && meta->fields) {
            for (size_t i = 0; i < meta->field_count; ++i) {
                result.append(nb::str(meta->fields[i].name));
            }
        }
        return result;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::values() const {
        // View-based: iterate over field TSViews
        auto bundle_view = this->view().as_bundle();
        nb::list result;
        for (auto it = bundle_view.items().begin(); it != bundle_view.items().end(); ++it) {
            TSView ts_view = *it;
            if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
            } else {
                result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
            }
        }
        return result;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::valid_keys() const {
        // View-based: return field names for valid fields
        auto bundle_view = this->view().as_bundle();
        nb::list result;
        for (auto it = bundle_view.valid_items().begin(); it != bundle_view.valid_items().end(); ++it) {
            result.append(nb::str(it.name()));
        }
        return result;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::valid_values() const {
        // View-based: iterate over valid field TSViews
        auto bundle_view = this->view().as_bundle();
        nb::list result;
        for (auto it = bundle_view.valid_items().begin(); it != bundle_view.valid_items().end(); ++it) {
            TSView ts_view = *it;
            if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
            } else {
                result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
            }
        }
        return result;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::modified_keys() const {
        // View-based: return field names for modified fields
        auto bundle_view = this->view().as_bundle();
        nb::list result;
        for (auto it = bundle_view.modified_items().begin(); it != bundle_view.modified_items().end(); ++it) {
            result.append(nb::str(it.name()));
        }
        return result;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::modified_values() const {
        // View-based: iterate over modified field TSViews
        auto bundle_view = this->view().as_bundle();
        nb::list result;
        for (auto it = bundle_view.modified_items().begin(); it != bundle_view.modified_items().end(); ++it) {
            TSView ts_view = *it;
            if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
            } else {
                result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
            }
        }
        return result;
    }

    template <typename T_TS>
    nb::int_ PyTimeSeriesBundle<T_TS>::len() const {
        auto bundle_view = this->view().as_bundle();
        return nb::int_(bundle_view.field_count());
    }

    template <typename T_TS>
    nb::bool_ PyTimeSeriesBundle<T_TS>::empty() const {
        auto bundle_view = this->view().as_bundle();
        return nb::bool_(bundle_view.field_count() == 0);
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::items() const {
        // View-based: return list of (key, value) tuples
        auto bundle_view = this->view().as_bundle();
        nb::list result;
        for (auto it = bundle_view.items().begin(); it != bundle_view.items().end(); ++it) {
            TSView ts_view = *it;
            nb::object wrapped_value;
            if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            } else {
                wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            }
            result.append(nb::make_tuple(nb::str(it.name()), wrapped_value));
        }
        return result;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::valid_items() const {
        // View-based: return list of (key, value) tuples for valid fields
        auto bundle_view = this->view().as_bundle();
        nb::list result;
        for (auto it = bundle_view.valid_items().begin(); it != bundle_view.valid_items().end(); ++it) {
            TSView ts_view = *it;
            nb::object wrapped_value;
            if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            } else {
                wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            }
            result.append(nb::make_tuple(nb::str(it.name()), wrapped_value));
        }
        return result;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::modified_items() const {
        // View-based: return list of (key, value) tuples for modified fields
        auto bundle_view = this->view().as_bundle();
        nb::list result;
        for (auto it = bundle_view.modified_items().begin(); it != bundle_view.modified_items().end(); ++it) {
            TSView ts_view = *it;
            nb::object wrapped_value;
            if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            } else {
                wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            }
            result.append(nb::make_tuple(nb::str(it.name()), wrapped_value));
        }
        return result;
    }

    template <typename T_TS> constexpr const char *get_bundle_type_name() {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesInput>) {
            return "TimeSeriesBundleInput@{:p}[keys={}, valid={}]";
        } else {
            return "TimeSeriesBundleOutput@{:p}[keys={}, valid={}]";
        }
    }

    template <typename T_TS>
    nb::str PyTimeSeriesBundle<T_TS>::py_str() {
        // View-based: format from view data
        auto bundle_view = this->view().as_bundle();
        constexpr const char *name = get_bundle_type_name<T_TS>();
        auto str = fmt::format(name, static_cast<const void *>(&bundle_view), bundle_view.field_count(), bundle_view.valid());
        return nb::str(str.c_str());
    }

    template <typename T_TS>
    nb::str PyTimeSeriesBundle<T_TS>::py_repr() {
        return py_str();
    }

    template struct PyTimeSeriesBundle<PyTimeSeriesOutput>;
    template struct PyTimeSeriesBundle<PyTimeSeriesInput>;

    template <typename T_TS> void _register_tsb_with_nanobind(nb::module_ &m) {
        using PyTS_Type = PyTimeSeriesBundle<T_TS>;
        auto name{std::is_same_v<T_TS, PyTimeSeriesInput> ? "TimeSeriesBundleInput" : "TimeSeriesBundleOutput"};
        nb::class_<PyTS_Type, T_TS>(m, name)
            .def("__getitem__", &PyTS_Type::get_item)
            .def("__getattr__", &PyTS_Type::get_attr)
            .def("__iter__", &PyTS_Type::iter)
            .def("__len__", &PyTS_Type::len)
            .def("__contains__", &PyTS_Type::contains)
            .def("keys", &PyTS_Type::keys)
            .def("items", &PyTS_Type::items)
            .def("values", &PyTS_Type::values)
            .def("valid_keys", &PyTS_Type::valid_keys)
            .def("valid_values", &PyTS_Type::valid_values)
            .def("valid_items", &PyTS_Type::valid_items)
            .def("modified_keys", &PyTS_Type::modified_keys)
            .def("modified_values", &PyTS_Type::modified_values)
            .def("modified_items", &PyTS_Type::modified_items)
            .def("key_from_value", &PyTS_Type::key_from_value)
            .def_prop_ro("empty", &PyTS_Type::empty)
            .def_prop_ro("__schema__", &PyTS_Type::schema)
            .def_prop_ro("as_schema", [](nb::handle self) { return self; })
            .def("__str__", &PyTS_Type::py_str)
            .def("__repr__", &PyTS_Type::py_repr);
    }

    void tsb_register_with_nanobind(nb::module_ &m) {
        _register_tsb_with_nanobind<PyTimeSeriesOutput>(m);
        _register_tsb_with_nanobind<PyTimeSeriesInput>(m);
    }
}  // namespace hgraph
