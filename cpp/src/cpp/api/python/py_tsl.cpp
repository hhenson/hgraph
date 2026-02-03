#include <hgraph/api/python/py_tsl.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/time_series/ts_list_view.h>

namespace hgraph
{

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    PyTimeSeriesList<T_TS, T_U>::PyTimeSeriesList(api_ptr impl) : T_TS(std::move(impl)) {}

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    PyTimeSeriesList<T_TS, T_U>::PyTimeSeriesList(view_type view) : T_TS(std::move(view)) {}

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::iter() const {
        if (this->has_view()) {
            // View-based: iterate over element TSViews
            auto list_view = this->view().as_list();
            nb::list result;
            for (auto it = list_view.values().begin(); it != list_view.values().end(); ++it) {
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
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::get_item(const nb::handle &key) const {
        if (this->has_view()) {
            // View-based: get element view
            if (nb::isinstance<nb::int_>(key)) {
                auto list_view = this->view().as_list();
                TSView elem_view = list_view.at(nb::cast<size_t>(key));
                if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                    return wrap_output_view(TSOutputView(elem_view, nullptr));
                } else {
                    return wrap_input_view(TSInputView(elem_view, nullptr));
                }
            }
            throw std::runtime_error("Invalid key type for TimeSeriesList");
        }
        if (nb::isinstance<nb::int_>(key)) {
            return wrap_time_series(impl()->operator[](nb::cast<size_t>(key)));
        }
        throw std::runtime_error("Invalid key type for TimeSeriesList");
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::keys() const {
        if (this->has_view()) {
            // View-based: return list of indices
            auto list_view = this->view().as_list();
            nb::list result;
            for (size_t i = 0; i < list_view.size(); ++i) {
                result.append(nb::int_(i));
            }
            return result;
        }
        return set_to_list(impl()->keys());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::values() const {
        if (this->has_view()) {
            // View-based: iterate over element TSViews
            auto list_view = this->view().as_list();
            nb::list result;
            for (auto it = list_view.values().begin(); it != list_view.values().end(); ++it) {
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
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::valid_keys() const {
        if (this->has_view()) {
            // View-based: return indices of valid elements
            auto list_view = this->view().as_list();
            nb::list result;
            for (auto it = list_view.valid_values().begin(); it != list_view.valid_values().end(); ++it) {
                result.append(nb::int_(it.index()));
            }
            return result;
        }
        return set_to_list(impl()->valid_keys());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::modified_keys() const {
        if (this->has_view()) {
            // View-based: return indices of modified elements
            auto list_view = this->view().as_list();
            nb::list result;
            for (auto it = list_view.modified_values().begin(); it != list_view.modified_values().end(); ++it) {
                result.append(nb::int_(it.index()));
            }
            return result;
        }
        return set_to_list(impl()->modified_keys());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::int_ PyTimeSeriesList<T_TS, T_U>::len() const {
        if (this->has_view()) {
            auto list_view = this->view().as_list();
            return nb::int_(list_view.size());
        }
        return nb::int_(impl()->size());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::items() const {
        if (this->has_view()) {
            // View-based: return list of (index, value) tuples
            auto list_view = this->view().as_list();
            nb::list result;
            for (auto it = list_view.items().begin(); it != list_view.items().end(); ++it) {
                TSView ts_view = *it;
                nb::object wrapped_value;
                if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                    wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
                } else {
                    wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
                }
                result.append(nb::make_tuple(nb::int_(it.index()), wrapped_value));
            }
            return result;
        }
        return items_to_list(impl()->items());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::valid_values() const {
        if (this->has_view()) {
            // View-based: iterate over valid element TSViews
            auto list_view = this->view().as_list();
            nb::list result;
            for (auto it = list_view.valid_values().begin(); it != list_view.valid_values().end(); ++it) {
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
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::valid_items() const {
        if (this->has_view()) {
            // View-based: return list of (index, value) tuples for valid elements
            auto list_view = this->view().as_list();
            nb::list result;
            for (auto it = list_view.valid_items().begin(); it != list_view.valid_items().end(); ++it) {
                TSView ts_view = *it;
                nb::object wrapped_value;
                if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                    wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
                } else {
                    wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
                }
                result.append(nb::make_tuple(nb::int_(it.index()), wrapped_value));
            }
            return result;
        }
        return items_to_list(impl()->valid_items());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::modified_values() const {
        if (this->has_view()) {
            // View-based: iterate over modified element TSViews
            auto list_view = this->view().as_list();
            nb::list result;
            for (auto it = list_view.modified_values().begin(); it != list_view.modified_values().end(); ++it) {
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
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::modified_items() const {
        if (this->has_view()) {
            // View-based: return list of (index, value) tuples for modified elements
            auto list_view = this->view().as_list();
            nb::list result;
            for (auto it = list_view.modified_items().begin(); it != list_view.modified_items().end(); ++it) {
                TSView ts_view = *it;
                nb::object wrapped_value;
                if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
                    wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
                } else {
                    wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
                }
                result.append(nb::make_tuple(nb::int_(it.index()), wrapped_value));
            }
            return result;
        }
        return items_to_list(impl()->modified_items());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    bool PyTimeSeriesList<T_TS, T_U>::empty() const {
        if (this->has_view()) {
            auto list_view = this->view().as_list();
            return list_view.size() == 0;
        }
        return impl()->empty();
    }

    template <typename T_TS, typename T_U> constexpr const char *get_list_type_name() {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesInput>) {
            return "TimeSeriesListInput@{:p}[keys={}, valid={}]";
        } else {
            return "TimeSeriesListOutput@{:p}[keys={}, valid={}]";
        }
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::str PyTimeSeriesList<T_TS, T_U>::py_str() {
        if (this->has_view()) {
            // View-based: format from view data
            auto list_view = this->view().as_list();
            constexpr const char *name = get_list_type_name<T_TS, T_U>();
            auto str = fmt::format(name, static_cast<const void *>(&list_view), list_view.size(), list_view.valid());
            return nb::str(str.c_str());
        }
        auto                  self = impl();
        constexpr const char *name = get_list_type_name<T_TS, T_U>();
        auto                  str  = fmt::format(name, static_cast<const void *>(self), self->keys().size(), self->valid());
        return nb::str(str.c_str());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::str PyTimeSeriesList<T_TS, T_U>::py_repr() {
        return py_str();
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    T_U *PyTimeSeriesList<T_TS, T_U>::impl() const {
        return this->template static_cast_impl<T_U>();
    }

    // Explicit template instantiations
    template struct PyTimeSeriesList<PyTimeSeriesOutput, TimeSeriesListOutput>;
    template struct PyTimeSeriesList<PyTimeSeriesInput, TimeSeriesListInput>;

    template <typename T_TS, typename T_U> void _register_tsl_with_nanobind(nb::module_ &m) {
        using PyTS_Type = PyTimeSeriesList<T_TS, T_U>;

        nb::class_<PyTS_Type, T_TS>(m, std::is_same_v<T_TS, PyTimeSeriesInput> ? "TimeSeriesListInput" : "TimeSeriesListOutput")
            .def("__getitem__", &PyTS_Type::get_item)
            .def("__iter__", &PyTS_Type::iter)
            .def("__len__", &PyTS_Type::len)
            .def_prop_ro("empty", &PyTS_Type::empty)
            .def("values", &PyTS_Type::values)
            .def("valid_values", &PyTS_Type::valid_values)
            .def("modified_values", &PyTS_Type::modified_values)
            .def("keys", &PyTS_Type::keys)
            .def("items", &PyTS_Type::items)
            .def("valid_keys", &PyTS_Type::valid_keys)
            .def("valid_items", &PyTS_Type::valid_items)
            .def("modified_keys", &PyTS_Type::modified_keys)
            .def("modified_items", &PyTS_Type::modified_items)
            .def("__str__", &PyTS_Type::py_str)
            .def("__repr__", &PyTS_Type::py_repr);
    }

    void tsl_register_with_nanobind(nb::module_ &m) {
        _register_tsl_with_nanobind<PyTimeSeriesOutput, TimeSeriesListOutput>(m);
        _register_tsl_with_nanobind<PyTimeSeriesInput, TimeSeriesListInput>(m);
    }
}  // namespace hgraph
