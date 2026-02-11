#include <hgraph/api/python/py_tsl.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_list_view.h>

namespace hgraph
{

    template <typename T_TS>
    PyTimeSeriesList<T_TS>::PyTimeSeriesList(view_type view) : T_TS(std::move(view)) {}

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::iter() const {
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

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::get_item(const nb::handle &key) const {
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

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::keys() const {
        // View-based: return list of indices
        auto list_view = this->view().as_list();
        nb::list result;
        for (size_t i = 0; i < list_view.size(); ++i) {
            result.append(nb::int_(i));
        }
        return result;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::values() const {
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

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::valid_keys() const {
        // View-based: return indices of valid elements
        auto list_view = this->view().as_list();
        nb::list result;
        for (auto it = list_view.valid_values().begin(); it != list_view.valid_values().end(); ++it) {
            result.append(nb::int_(it.index()));
        }
        return result;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::modified_keys() const {
        // View-based: return indices of modified elements
        auto list_view = this->view().as_list();
        nb::list result;
        for (auto it = list_view.modified_values().begin(); it != list_view.modified_values().end(); ++it) {
            result.append(nb::int_(it.index()));
        }
        return result;
    }

    template <typename T_TS>
    nb::int_ PyTimeSeriesList<T_TS>::len() const {
        auto list_view = this->view().as_list();
        return nb::int_(list_view.size());
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::items() const {
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

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::valid_values() const {
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

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::valid_items() const {
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

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::modified_values() const {
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

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::modified_items() const {
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

    template <typename T_TS>
    bool PyTimeSeriesList<T_TS>::empty() const {
        auto list_view = this->view().as_list();
        return list_view.size() == 0;
    }

    template <typename T_TS> constexpr const char *get_list_type_name() {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesInput>) {
            return "TimeSeriesListInput@{:p}[keys={}, valid={}]";
        } else {
            return "TimeSeriesListOutput@{:p}[keys={}, valid={}]";
        }
    }

    template <typename T_TS>
    nb::str PyTimeSeriesList<T_TS>::py_str() {
        // View-based: format from view data
        auto list_view = this->view().as_list();
        constexpr const char *name = get_list_type_name<T_TS>();
        auto str = fmt::format(name, static_cast<const void *>(&list_view), list_view.size(), list_view.valid());
        return nb::str(str.c_str());
    }

    template <typename T_TS>
    nb::str PyTimeSeriesList<T_TS>::py_repr() {
        return py_str();
    }

    // Explicit template instantiations
    template struct PyTimeSeriesList<PyTimeSeriesOutput>;
    template struct PyTimeSeriesList<PyTimeSeriesInput>;

    template <typename T_TS> void _register_tsl_with_nanobind(nb::module_ &m) {
        using PyTS_Type = PyTimeSeriesList<T_TS>;

        auto cls = nb::class_<PyTS_Type, T_TS>(m, std::is_same_v<T_TS, PyTimeSeriesInput> ? "TimeSeriesListInput" : "TimeSeriesListOutput")
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

        // Register copy_from_input/output for Output variant only
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
            cls.def("copy_from_input", [](PyTS_Type &self, const PyTimeSeriesInput &input) {
                // Python logic: for each child, copy from corresponding input child
                auto self_values = nb::cast<nb::list>(self.values());
                // Try to get the input as a TSL
                auto* input_as_tsl = dynamic_cast<const PyTimeSeriesListInput*>(&input);
                if (!input_as_tsl) {
                    // Fallback to base class
                    self.PyTimeSeriesOutput::copy_from_input(input);
                    return;
                }
                auto input_values = nb::cast<nb::list>(input_as_tsl->values());
                auto n = std::min(nb::len(self_values), nb::len(input_values));
                for (size_t i = 0; i < n; ++i) {
                    nb::object self_child = self_values[i];
                    nb::object input_child = input_values[i];
                    self_child.attr("copy_from_input")(input_child);
                }
            });
            cls.def("copy_from_output", [](PyTS_Type &self, const PyTimeSeriesOutput &output) {
                auto self_values = nb::cast<nb::list>(self.values());
                auto* output_as_tsl = dynamic_cast<const PyTimeSeriesListOutput*>(&output);
                if (!output_as_tsl) {
                    self.PyTimeSeriesOutput::copy_from_output(output);
                    return;
                }
                auto output_values = nb::cast<nb::list>(output_as_tsl->values());
                auto n = std::min(nb::len(self_values), nb::len(output_values));
                for (size_t i = 0; i < n; ++i) {
                    nb::object self_child = self_values[i];
                    nb::object output_child = output_values[i];
                    self_child.attr("copy_from_output")(output_child);
                }
            });
        }
    }

    void tsl_register_with_nanobind(nb::module_ &m) {
        _register_tsl_with_nanobind<PyTimeSeriesOutput>(m);
        _register_tsl_with_nanobind<PyTimeSeriesInput>(m);
    }
}  // namespace hgraph
