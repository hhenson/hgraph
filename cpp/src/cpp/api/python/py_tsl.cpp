#include <hgraph/api/python/py_tsl.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tsl.h>

namespace hgraph
{

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    PyTimeSeriesList<T_TS, T_U>::PyTimeSeriesList(underlying_type *impl, const control_block_ptr &cb) : T_TS(impl, cb) {}

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    PyTimeSeriesList<T_TS, T_U>::PyTimeSeriesList(underlying_type *impl) : T_TS(impl) {}

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::iter() const {
        auto     impl_{impl()};
        nb::list values;
        for (size_t i = 0; i < impl_->size(); ++i) {
            values.append(wrap_time_series(impl_->operator[](i).get(), this->control_block()));
        }
        return values.attr("__iter__")();
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::object PyTimeSeriesList<T_TS, T_U>::get_item(const nb::handle &key) const {
        if (nb::isinstance<nb::int_>(key)) {
            return wrap_time_series(impl()->operator[](nb::cast<size_t>(key)).get(), this->control_block());
        }
        throw std::runtime_error("Invalid key type for TimeSeriesList");
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    auto PyTimeSeriesList<T_TS, T_U>::keys() const {
        const auto &keys_{impl()->keys()};
        return nb::make_iterator(nb::type<PyTimeSeriesList<T_TS, T_U>>(), "TSLKeyIterator", keys_);
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    auto PyTimeSeriesList<T_TS, T_U>::values() const {
        auto        self{impl()};
        auto items = self->values();  // Copy the collection to ensure lifetime
        return make_time_series_iterator(nb::type<typename T_U::collection_type>(), "TSLValuesIterator",
                                               std::move(items), this->control_block());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    auto PyTimeSeriesList<T_TS, T_U>::valid_keys() const {
        const auto &keys_{impl()->valid_keys()};
        return nb::make_iterator(nb::type<PyTimeSeriesList<T_TS, T_U>>(), "TSLValidKeyIterator", keys_);
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    auto PyTimeSeriesList<T_TS, T_U>::modified_keys() const {
        auto keys_{impl()->modified_keys()};
        return nb::make_iterator(nb::type<PyTimeSeriesList<T_TS, T_U>>(), "TSLModifiedKeyIterator", keys_);
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::int_ PyTimeSeriesList<T_TS, T_U>::len() const {
        return nb::int_(impl()->size());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    auto PyTimeSeriesList<T_TS, T_U>::items() const {
        auto        self{impl()};
        auto items = self->items();  // Copy the collection to ensure lifetime
        return make_time_series_items_iterator(nb::type<typename T_U::enumerated_collection_type>(), "ItemsIterator",
                                               std::move(items), this->control_block());
    }
    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    auto PyTimeSeriesList<T_TS, T_U>::valid_values() const {
        auto        self{impl()};
        auto items = self->valid_values();  // Copy the collection to ensure lifetime
        return make_time_series_iterator(nb::type<typename T_U::collection_type>(), "ValidValuesIterator",
                                               std::move(items), this->control_block());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    auto PyTimeSeriesList<T_TS, T_U>::valid_items() const {
        auto        self{impl()};
        auto items = self->valid_items();  // Copy the collection to ensure lifetime
        return make_time_series_items_iterator(nb::type<typename T_U::enumerated_collection_type>(), "ValidItemsIterator",
                                               std::move(items), this->control_block());
    }
    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    auto PyTimeSeriesList<T_TS, T_U>::modified_values() const {
        auto        self{impl()};
        auto items = self->modified_values();  // Copy the collection to ensure lifetime
        return make_time_series_iterator(nb::type<typename T_U::collection_type>(), "ModifiedValuesIterator",
                                               std::move(items), this->control_block());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    auto PyTimeSeriesList<T_TS, T_U>::modified_items() const {
        auto        self{impl()};
        auto items = self->modified_items();  // Copy the collection to ensure lifetime
        return make_time_series_items_iterator(nb::type<typename T_U::enumerated_collection_type>(), "ModifiedItemsIterator",
                                               std::move(items), this->control_block());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    bool PyTimeSeriesList<T_TS, T_U>::empty() const {
        return impl()->empty();
    }

    template <typename T_TS, typename T_U> constexpr const char *get_bundle_type_name() {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesInput>) {
            return "TimeSeriesListInput@{:p}[keys={}, valid={}]";
        } else {
            return "TimeSeriesListOutput@{:p}[keys={}, valid={}]";
        }
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::str PyTimeSeriesList<T_TS, T_U>::py_str() {
        auto                  self = impl();
        constexpr const char *name = get_bundle_type_name<T_TS, T_U>();
        auto                  str  = fmt::format(name, static_cast<const void *>(self.get()), self->keys().size(), self->valid());
        return nb::str(str.c_str());
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    nb::str PyTimeSeriesList<T_TS, T_U>::py_repr() {
        return py_str();
    }

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    std::shared_ptr<typename PyTimeSeriesList<T_TS, T_U>::underlying_type> PyTimeSeriesList<T_TS, T_U>::impl() const {
        return this->template static_cast_impl<T_U>();
    }

    // Explicit template instantiations for constructors
    template PyTimeSeriesList<PyTimeSeriesOutput, TimeSeriesListOutput>::PyTimeSeriesList(TimeSeriesListOutput *,
                                                                                          const control_block_ptr &);
    template PyTimeSeriesList<PyTimeSeriesOutput, TimeSeriesListOutput>::PyTimeSeriesList(TimeSeriesListOutput *);
    template PyTimeSeriesList<PyTimeSeriesInput, TimeSeriesListInput>::PyTimeSeriesList(TimeSeriesListInput *,
                                                                                        const control_block_ptr &);
    template PyTimeSeriesList<PyTimeSeriesInput, TimeSeriesListInput>::PyTimeSeriesList(TimeSeriesListInput *);

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
