#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/time_series/ts_bundle_view.h>

namespace hgraph
{

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    PyTimeSeriesBundle<T_TS, T_U>::PyTimeSeriesBundle(api_ptr impl) : T_TS(std::move(impl)) {}

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    PyTimeSeriesBundle<T_TS, T_U>::PyTimeSeriesBundle(view_type view) : T_TS(std::move(view)) {}

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::iter() const {
        if (this->has_view()) {
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
        return nb::iter(list_to_list(impl()->values()));
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::get_item(const nb::handle &key) const {
        if (this->has_view()) {
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
        if (nb::isinstance<nb::str>(key)) {
            return wrap_time_series(impl()->operator[](nb::cast<std::string>(key)));
        }
        if (nb::isinstance<nb::int_>(key)) {
            return wrap_time_series(impl()->operator[](nb::cast<size_t>(key)));
        }
        throw std::runtime_error("Invalid key type for TimeSeriesBundle");
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::bool_ PyTimeSeriesBundle<T_TS, T_U>::contains(const nb::handle &key) const {
        if (this->has_view()) {
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
        if (nb::isinstance<nb::str>(key)) { return nb::bool_(impl()->contains(nb::cast<std::string>(key))); }
        return nb::bool_(false);
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    const TimeSeriesSchema &PyTimeSeriesBundle<T_TS, T_U>::schema() const {
        if (this->has_view()) {
            // View-based: need to get schema from meta
            // For now, throw - this may need rework
            throw std::runtime_error("schema() not yet implemented for view-based TSB");
        }
        return impl()->schema();
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::key_from_value(const nb::handle &value) const {
        if (this->has_view()) {
            // View-based: not yet implemented
            throw std::runtime_error("key_from_value not yet implemented for view-based TSB");
        }
        constexpr auto is_input = std::is_same_v<T_U, TimeSeriesBundleInput>;
        typedef std::conditional_t<is_input, time_series_input_s_ptr, time_series_output_s_ptr> TS_SPtr;
        TS_SPtr p;
        if constexpr (is_input) {
            p = unwrap_input(value);
        } else {
            p = unwrap_output(value);
        }
        if (!p) {
            throw std::runtime_error("Value is not a valid TimeSeries");
        }
        try {
            auto key = impl()->key_from_value(p);
            return nb::str(key.c_str());
        } catch (const std::exception &e) {
            return nb::none();
        }
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::keys() const {
        if (this->has_view()) {
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
        return set_to_list(impl()->keys());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::values() const {
        if (this->has_view()) {
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
        return list_to_list(impl()->values());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::valid_keys() const {
        if (this->has_view()) {
            // View-based: return field names for valid fields
            auto bundle_view = this->view().as_bundle();
            nb::list result;
            for (auto it = bundle_view.valid_items().begin(); it != bundle_view.valid_items().end(); ++it) {
                result.append(nb::str(it.name()));
            }
            return result;
        }
        return set_to_list(impl()->valid_keys());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::valid_values() const {
        if (this->has_view()) {
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
        return list_to_list(impl()->valid_values());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::modified_keys() const {
        if (this->has_view()) {
            // View-based: return field names for modified fields
            auto bundle_view = this->view().as_bundle();
            nb::list result;
            for (auto it = bundle_view.modified_items().begin(); it != bundle_view.modified_items().end(); ++it) {
                result.append(nb::str(it.name()));
            }
            return result;
        }
        return set_to_list(impl()->modified_keys());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::modified_values() const {
        if (this->has_view()) {
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
        return list_to_list(impl()->modified_values());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::int_ PyTimeSeriesBundle<T_TS, T_U>::len() const {
        if (this->has_view()) {
            auto bundle_view = this->view().as_bundle();
            return nb::int_(bundle_view.field_count());
        }
        return nb::int_(impl()->size());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::bool_ PyTimeSeriesBundle<T_TS, T_U>::empty() const {
        if (this->has_view()) {
            auto bundle_view = this->view().as_bundle();
            return nb::bool_(bundle_view.field_count() == 0);
        }
        return nb::bool_(impl()->empty());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::items() const {
        if (this->has_view()) {
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
        return items_to_list(impl()->items());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::valid_items() const {
        if (this->has_view()) {
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
        return items_to_list(impl()->valid_items());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::modified_items() const {
        if (this->has_view()) {
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
        return items_to_list(impl()->modified_items());
    }

    template <typename T_TS, typename T_U> constexpr const char *get_bundle_type_name() {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesInput>) {
            return "TimeSeriesBundleInput@{:p}[keys={}, valid={}]";
        } else {
            return "TimeSeriesBundleOutput@{:p}[keys={}, valid={}]";
        }
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::str PyTimeSeriesBundle<T_TS, T_U>::py_str() {
        if (this->has_view()) {
            // View-based: format from view data
            auto bundle_view = this->view().as_bundle();
            constexpr const char *name = get_bundle_type_name<T_TS, T_U>();
            auto str = fmt::format(name, static_cast<const void *>(&bundle_view), bundle_view.field_count(), bundle_view.valid());
            return nb::str(str.c_str());
        }
        auto                  self = impl();
        constexpr const char *name = get_bundle_type_name<T_TS, T_U>();
        auto                  str  = fmt::format(name, static_cast<const void *>(self), self->keys().size(), self->valid());
        return nb::str(str.c_str());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::str PyTimeSeriesBundle<T_TS, T_U>::py_repr() {
        return py_str();
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    T_U *PyTimeSeriesBundle<T_TS, T_U>::impl() const {
        return this->template static_cast_impl<T_U>();
    }

    template struct PyTimeSeriesBundle<PyTimeSeriesOutput, TimeSeriesBundleOutput>;
    template struct PyTimeSeriesBundle<PyTimeSeriesInput, TimeSeriesBundleInput>;

    template <typename T_TS, typename T_U> void _register_tsb_with_nanobind(nb::module_ &m) {
        using PyTS_Type = PyTimeSeriesBundle<T_TS, T_U>;
        auto name{std::is_same_v<T_U, TimeSeriesBundleInput> ? "TimeSeriesBundleInput" : "TimeSeriesBundleOutput"};
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
        _register_tsb_with_nanobind<PyTimeSeriesOutput, TimeSeriesBundleOutput>(m);
        _register_tsb_with_nanobind<PyTimeSeriesInput, TimeSeriesBundleInput>(m);
    }
}  // namespace hgraph
