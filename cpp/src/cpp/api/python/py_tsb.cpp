#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tsb.h>

namespace hgraph
{

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    PyTimeSeriesBundle<T_TS, T_U>::PyTimeSeriesBundle(underlying_type *impl, const control_block_ptr &cb) : T_TS(impl, cb) {}

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    PyTimeSeriesBundle<T_TS, T_U>::PyTimeSeriesBundle(underlying_type *impl) : T_TS(impl) {}

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::iter() const {
        auto     impl_{impl()};
        nb::list values;
        for (size_t i = 0; i < impl_->size(); ++i) {
            values.append(wrap_time_series(impl_->operator[](i).get(), this->control_block()));
        }
        return values.attr("__iter__")();
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::object PyTimeSeriesBundle<T_TS, T_U>::get_item(const nb::handle &key) const {
        if (nb::isinstance<nb::str>(key)) {
            return wrap_time_series(impl()->operator[](nb::cast<std::string>(key)).get(), this->control_block());
        }
        if (nb::isinstance<nb::int_>(key)) {
            return wrap_time_series(impl()->operator[](nb::cast<size_t>(key)).get(), this->control_block());
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
    auto PyTimeSeriesBundle<T_TS, T_U>::keys() const {
        const auto &keys_{impl()->keys()};
        return nb::make_iterator(nb::type<PyTimeSeriesBundle<T_TS, T_U>>(), "TSBKeyIterator", keys_.begin(), keys_.end());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    auto PyTimeSeriesBundle<T_TS, T_U>::values() const {
        auto self{impl()};
        auto items = self->values();  // Copy the collection to ensure lifetime
        return make_time_series_iterator(nb::type<PyTimeSeriesBundle<T_TS, T_U>>(), "TSBValuesIterator", std::move(items),
                                         this->control_block());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    auto PyTimeSeriesBundle<T_TS, T_U>::valid_keys() const {
        const auto &keys_{impl()->valid_keys()};
        return nb::make_iterator(nb::type<PyTimeSeriesBundle<T_TS, T_U>>(), "TSBValidKeyIterator", keys_.begin(), keys_.end());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    auto PyTimeSeriesBundle<T_TS, T_U>::valid_values() const {
        auto self{impl()};
        auto items = self->valid_values();  // Copy the collection to ensure lifetime
        return make_time_series_iterator(nb::type<PyTimeSeriesBundle<T_TS, T_U>>(), "TSBValidValuesIterator", std::move(items),
                                         this->control_block());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    auto PyTimeSeriesBundle<T_TS, T_U>::modified_keys() const {
        const auto &keys_{impl()->modified_keys()};
        return nb::make_iterator(nb::type<PyTimeSeriesBundle<T_TS, T_U>>(), "TSBValidKeyIterator", keys_.begin(), keys_.end());

    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    auto PyTimeSeriesBundle<T_TS, T_U>::modified_values() const {
        auto self{impl()};
        auto items = self->modified_values();  // Copy the collection to ensure lifetime
        return make_time_series_iterator(nb::type<PyTimeSeriesBundle<T_TS, T_U>>(), "TSBModifiedValuesIterator", std::move(items),
                                         this->control_block());
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
    auto PyTimeSeriesBundle<T_TS, T_U>::items() const {
        auto self{impl()};
        auto items = self->items();  // Copy the collection to ensure lifetime
        return make_time_series_items_iterator(nb::type<PyTimeSeriesBundle<T_TS, T_U>>(), "TSBItemsIterator",
                                               std::move(items), this->control_block());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    auto PyTimeSeriesBundle<T_TS, T_U>::valid_items() const {
        auto self{impl()};
        auto items = self->valid_items();  // Copy the collection to ensure lifetime
        return make_time_series_items_iterator(nb::type<PyTimeSeriesBundle<T_TS, T_U>>(), "TSBValidItemsIterator",
                                               std::move(items), this->control_block());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    auto PyTimeSeriesBundle<T_TS, T_U>::modified_items() const {
        auto self{impl()};
        auto items = self->modified_items();  // Copy the collection to ensure lifetime
        return make_time_series_items_iterator(nb::type<PyTimeSeriesBundle<T_TS, T_U>>(), "TSBModifiedItemsIterator",
                                               std::move(items), this->control_block());
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
        auto                  str  = fmt::format(name, static_cast<const void *>(self.get()), self->keys().size(), self->valid());
        return nb::str(str.c_str());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    nb::str PyTimeSeriesBundle<T_TS, T_U>::py_repr() {
        return py_str();
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    std::shared_ptr<typename PyTimeSeriesBundle<T_TS, T_U>::underlying_type> PyTimeSeriesBundle<T_TS, T_U>::impl() const {
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
            .def("keys", &PyTS_Type::keys, nb::keep_alive<0, 1>())
            .def("items", &PyTS_Type::items, nb::keep_alive<0, 1>())
            .def("values", &PyTS_Type::values, nb::keep_alive<0, 1>())
            .def("valid_keys", &PyTS_Type::valid_keys, nb::keep_alive<0, 1>())
            .def("valid_values", &PyTS_Type::valid_values, nb::keep_alive<0, 1>())
            .def("valid_items", &PyTS_Type::valid_items, nb::keep_alive<0, 1>())
            .def("modified_keys", &PyTS_Type::modified_keys, nb::keep_alive<0, 1>())
            .def("modified_values", &PyTS_Type::modified_values, nb::keep_alive<0, 1>())
            .def("modified_items", &PyTS_Type::modified_items, nb::keep_alive<0, 1>())
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
