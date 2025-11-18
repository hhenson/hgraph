#include <hgraph/api/python/py_tsb.h>
#include <hgraph/types/tsb.h>
#include <hgraph/api/python/wrapper_factory.h>

namespace hgraph
{

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::iter() const {
        auto     impl_{impl()};
        nb::list values;
        for (size_t i = 0; i < impl_->size(); ++i) { values.append(impl_->operator[](i)); }
        return values.attr("__iter__")();
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::get_item(const nb::handle &key) const {
        typename T_U::ts_type *v;
        if (nb::isinstance<nb::str>(key)) {
            v = impl()->operator[](nb::cast<std::string>(key)).get();
        } else if (nb::isinstance<nb::int_>(key)) {
            v = impl()->operator[](nb::cast<size_t>(key)).get();
        } else {
            throw std::runtime_error("Invalid key type for TimeSeriesBundle");
        }
        if constexpr (std::is_same_v<typename T_U::ts_type, TimeSeriesOutput>) { return wrap_output(v, this->control_block()); }
        else if constexpr (std::is_same_v<typename T_U::ts_type, TimeSeriesInput>) { return wrap_input(v, this->control_block()); }
        else { throw std::runtime_error("Invalid TimeSeries type"); }
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
    nb::object PyTimeSeriesBundle<T_TS, T_U>::keys() const {
        return nb::make_tuple(impl()->keys().begin(), impl()->keys().end());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::valid_keys() const {
        return nb::make_tuple(impl()->valid_keys().begin(), impl()->valid_keys().end());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::modified_keys() const {
        return nb::make_tuple(impl()->modified_keys().begin(), impl()->modified_keys().end());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::int_ PyTimeSeriesBundle<T_TS, T_U>::len() const {
        return nb::int_(impl()->size());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::items() const {
        return nb::make_tuple(impl()->items().begin(), impl()->items().end());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::valid_items() const {
        return nb::make_tuple(impl()->valid_items().begin(), impl()->valid_items().end());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::modified_items() const {
        return nb::make_tuple(impl()->modified_items().begin(), impl()->modified_items().end());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    T_U *PyTimeSeriesBundle<T_TS, T_U>::impl() const {
        return this->template static_cast_impl<T_U>();
    }

    template <> struct PyTimeSeriesBundle<PyTimeSeriesOutput, TimeSeriesBundleOutput>;

    template <> struct PyTimeSeriesBundle<PyTimeSeriesInput, TimeSeriesBundleInput>;

    template <typename T_TS, typename T_U> void _register_tsb_with_nanobind(nb::module_ &m) {
        using PyTS_Type = PyTimeSeriesBundle<T_TS, T_U>;

        nb::class_<PyTS_Type, T_TS>(m, std::is_same_v<T_TS, TimeSeriesBundleInput> ? "TimeSeriesBundleInput"
                                                                                   : "TimeSeriesBundleOutput")
            .def("__getitem__", &PyTS_Type::get_item)
            .def("__iter__", &PyTS_Type::iter)
            .def("__contains__", &PyTS_Type::contains)
            .def("keys", &PyTS_Type::keys)
            .def("items", &PyTS_Type::items)
            .def("valid_keys", &PyTS_Type::valid_keys)
            .def("valid_items", &PyTS_Type::valid_items)
            .def("modified_keys", &PyTS_Type::modified_keys)
            .def("modified_items", &PyTS_Type::modified_items)
            .def_prop_ro("__schema__", &PyTS_Type::schema)
            .def_prop_ro("as_schema", [](nb::handle self) { return self; });
    }

    void register_tsb_with_nanobind(nb::module_ &m) {
        _register_tsb_with_nanobind<PyTimeSeriesOutput, TimeSeriesBundleOutput>(m);
        _register_tsb_with_nanobind<PyTimeSeriesInput, TimeSeriesBundleInput>(m);
    }
}  // namespace hgraph
