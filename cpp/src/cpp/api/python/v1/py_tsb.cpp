#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tsb.h>

namespace hgraph
{

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    PyTimeSeriesBundle<T_TS, T_U>::PyTimeSeriesBundle(api_ptr impl) : T_TS(std::move(impl)) {}

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::iter() const {
        return nb::iter(list_to_list(impl()->values()));
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::get_item(const nb::handle &key) const {
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
        if (nb::isinstance<nb::str>(key)) { return nb::bool_(impl()->contains(nb::cast<std::string>(key))); }
        return nb::bool_(false);
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    const TimeSeriesSchema &PyTimeSeriesBundle<T_TS, T_U>::schema() const {
        return impl()->schema();
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::key_from_value(const nb::handle &value) const {
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
        return set_to_list(impl()->keys());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::values() const {
        return list_to_list(impl()->values());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::valid_keys() const {
        return set_to_list(impl()->valid_keys());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::valid_values() const {
        return list_to_list(impl()->valid_values());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::modified_keys() const {
        return set_to_list(impl()->modified_keys());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::modified_values() const {
        return list_to_list(impl()->modified_values());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::int_ PyTimeSeriesBundle<T_TS, T_U>::len() const {
        return nb::int_(impl()->size());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::bool_ PyTimeSeriesBundle<T_TS, T_U>::empty() const {
        return nb::bool_(impl()->empty());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::items() const {
        return items_to_list(impl()->items());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::valid_items() const {
        return items_to_list(impl()->valid_items());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::modified_items() const {
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
