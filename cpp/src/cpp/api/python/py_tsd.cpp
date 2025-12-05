#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{

    // ===== Template impl for PyTimeSeriesDict =====
    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    size_t PyTimeSeriesDict<T_TS, T_U>::size() const {
        return impl()->size();
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::get_item(const nb::object &item) const {
        auto self{impl()};
        if (get_key_set_id().is(item)) { return key_set(); }
        return wrap_time_series(self->operator[](nb::cast<typename T_U::key_type>(item)));
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::get(const nb::object &item, const nb::object &default_value) const {
        auto self{impl()};
        auto key{nb::cast<typename T_U::key_type>(item)};
        if (self->contains(key)) { return wrap_time_series(self->operator[](key)); }
        return default_value;
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::get_or_create(const nb::object &key) {
        return wrap_time_series(impl()->get_or_create(nb::cast<typename T_U::key_type>(key)));
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    void PyTimeSeriesDict<T_TS, T_U>::create(const nb::object &item) {
        impl()->create(nb::cast<typename T_U::key_type>(item));
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::iter() const {
        return nb::iter(keys());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    bool PyTimeSeriesDict<T_TS, T_U>::contains(const nb::object &item) const {
        return impl()->contains(nb::cast<typename T_U::key_type>(item));
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::key_set() const {
        return wrap_time_series(impl()->key_set().shared_from_this());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::keys() const {
        auto self{impl()};
        return keys_to_list(self->begin(), self->end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::values() const {
        auto self{impl()};
        return values_to_list(self->begin(), self->end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::items() const {
        auto self{impl()};
        return items_to_list(self->begin(), self->end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::modified_keys() const {
        auto        self{impl()};
        const auto &items = self->modified_items();
        return keys_to_list(items.begin(), items.end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::modified_values() const {
        auto        self{impl()};
        const auto &items = self->modified_items();
        return values_to_list(items.begin(), items.end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::modified_items() const {
        auto        self{impl()};
        const auto &items = self->modified_items();
        return items_to_list(items.begin(), items.end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    bool PyTimeSeriesDict<T_TS, T_U>::was_modified(const nb::object &key) const {
        return impl()->was_modified(nb::cast<typename T_U::key_type>(key));
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::valid_keys() const {
        auto self{impl()};
        auto items = self->valid_items();  // views must be non-const
        return keys_to_list(items.begin(), items.end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::valid_values() const {
        auto self{impl()};
        auto items = self->valid_items();  // views must be non-const
        return values_to_list(items.begin(), items.end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::valid_items() const {
        auto self{impl()};
        auto items = self->valid_items();  // views must be non-const
        return items_to_list(items.begin(), items.end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::added_keys() const {
        auto self{impl()};
        return set_to_list(self->added_keys());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::added_values() const {
        auto self{impl()};
        // transform_view iterators yield pairs directly, use range overload
        return values_to_list(self->added_items());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::added_items() const {
        auto self{impl()};
        // transform_view iterators yield pairs directly, use range overload
        return items_to_list(self->added_items());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    bool PyTimeSeriesDict<T_TS, T_U>::has_added() const {
        return impl()->has_added();
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    bool PyTimeSeriesDict<T_TS, T_U>::was_added(const nb::object &key) const {
        return impl()->was_added(nb::cast<typename T_U::key_type>(key));
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::removed_keys() const {
        auto        self{impl()};
        const auto &items = self->removed_items();
        return keys_to_list(items.begin(), items.end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::removed_values() const {
        auto        self{impl()};
        const auto &items = self->removed_items();
        return values_to_list(items.begin(), items.end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::removed_items() const {
        auto        self{impl()};
        const auto &items = self->removed_items();
        return items_to_list(items.begin(), items.end());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    bool PyTimeSeriesDict<T_TS, T_U>::has_removed() const {
        return impl()->has_removed();
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    bool PyTimeSeriesDict<T_TS, T_U>::was_removed(const nb::object &key) const {
        return impl()->was_removed(nb::cast<typename T_U::key_type>(key));
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::object PyTimeSeriesDict<T_TS, T_U>::key_from_value(const nb::object &value) const {
        constexpr auto is_input = std::is_base_of_v<TimeSeriesDictInput, T_U>;
        typedef std::conditional_t<is_input, TimeSeriesInput*, TimeSeriesOutput*> TS_Ptr;
        TS_Ptr p;
        if constexpr (is_input) {
            p = unwrap_input(value).get();
        } else {
            p = unwrap_output(value).get();
        }
        if (p == nullptr) {
            throw std::runtime_error("Value is not a valid TimeSeries");
        }
        try {
            auto key = impl()->key_from_value(p);
            return nb::cast(key);
        } catch (const std::exception &e) {
            return nb::none();
        }
    }

    namespace
    {
        template <typename T_TS> constexpr const char *get_tsd_type_name() {
            if constexpr (std::is_same_v<T_TS, PyTimeSeriesInput>) {
                return "TimeSeriesDictInput@{:p}[size={}, valid={}]";
            } else {
                return "TimeSeriesDictOutput@{:p}[size={}, valid={}]";
            }
        }
    }  // namespace

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::str PyTimeSeriesDict<T_TS, T_U>::py_str() {
        auto                  self = impl();
        constexpr const char *name = get_tsd_type_name<T_TS>();
        auto                  str  = fmt::format(name, static_cast<const void *>(self), self->size(), self->valid());
        return nb::str(str.c_str());
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    nb::str PyTimeSeriesDict<T_TS, T_U>::py_repr() {
        return py_str();
    }

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    T_U *PyTimeSeriesDict<T_TS, T_U>::impl() const {
        return this->template static_cast_impl<T_U>();
    }

    // ===== PyTimeSeriesDictOutput =====
    template <typename T_U>
        requires std::is_base_of_v<TimeSeriesDictOutput, T_U>
    PyTimeSeriesDictOutput_T<T_U>::PyTimeSeriesDictOutput_T(api_ptr impl)
        : PyTimeSeriesDict<PyTimeSeriesDictOutput, T_U>(std::move(impl)) {}

    template <typename T_U>
        requires std::is_base_of_v<TimeSeriesDictOutput, T_U>
    void PyTimeSeriesDictOutput_T<T_U>::set_item(const nb::object &key, const nb::object &value) {
        this->impl()->py_set_item(key, value);
    }

    template <typename T_U>
        requires std::is_base_of_v<TimeSeriesDictOutput, T_U>
    void PyTimeSeriesDictOutput_T<T_U>::del_item(const nb::object &key) {
        this->impl()->py_del_item(key);
    }

    template <typename T_U>
        requires std::is_base_of_v<TimeSeriesDictOutput, T_U>
    nb::object PyTimeSeriesDictOutput_T<T_U>::pop(const nb::object &key, const nb::object &default_value) {
        return this->impl()->py_pop(key, default_value);
    }

    template <typename T_U>
        requires std::is_base_of_v<TimeSeriesDictOutput, T_U>
    nb::object PyTimeSeriesDictOutput_T<T_U>::get_ref(const nb::object &key, const nb::object &requester) {
        return this->impl()->py_get_ref(key, requester);
    }

    template <typename T_U>
        requires std::is_base_of_v<TimeSeriesDictOutput, T_U>
    void PyTimeSeriesDictOutput_T<T_U>::release_ref(const nb::object &key, const nb::object &requester) {
        this->impl()->py_release_ref(key, requester);
    }

    template <typename T_U>
        requires std::is_base_of_v<TimeSeriesDictInput, T_U>
    void PyTimeSeriesDictInput_T<T_U>::on_key_added(const nb::object &key) {
        this->impl()->on_key_added(nb::cast<typename T_U::key_type>(key));
    }

    template <typename T_U>
        requires std::is_base_of_v<TimeSeriesDictInput, T_U>
    void PyTimeSeriesDictInput_T<T_U>::on_key_removed(const nb::object &key) {
        this->impl()->on_key_removed(nb::cast<typename T_U::key_type>(key));
    }

    // ===== PyTimeSeriesDictInput =====
    template <typename T_U>
        requires std::is_base_of_v<TimeSeriesDictInput, T_U>
    PyTimeSeriesDictInput_T<T_U>::PyTimeSeriesDictInput_T(api_ptr impl)
        : PyTimeSeriesDict<PyTimeSeriesDictInput, T_U>(std::move(impl)) {}

    // TODO: Make this a template
    template <typename T_Key> void _tsd_register_with_nanobind(nb::module_ &m, std::string name) {
        using TSD_OUT          = PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<T_Key>>;
        std::string class_name = fmt::format("TimeSeriesDictOutput_{}", name);

        auto output = nb::class_<TSD_OUT, PyTimeSeriesDictOutput>(m, class_name.c_str());

        output.def("__contains__", &TSD_OUT::contains, "key"_a)
            .def("__getitem__", &TSD_OUT::get_item, "key"_a)
            .def("__setitem__", &TSD_OUT::set_item, "key"_a, "value"_a)
            .def("__delitem__", &TSD_OUT::del_item, "key"_a)
            .def("__len__", &TSD_OUT::size)
            .def("pop", &TSD_OUT::pop, "key"_a, "default"_a = nb::none())
            .def("get", &TSD_OUT::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &TSD_OUT::get_or_create, "key"_a)
            .def("create", &TSD_OUT::create, "key"_a)
            .def("clear", &TSD_OUT::clear)
            .def("__iter__", &TSD_OUT::iter)
            .def("keys", &TSD_OUT::keys)
            .def("values", &TSD_OUT::values)
            .def("items", &TSD_OUT::items)
            .def("valid_keys", &TSD_OUT::valid_keys)
            .def("valid_values", &TSD_OUT::valid_values)
            .def("valid_items", &TSD_OUT::valid_items)
            .def("added_keys", &TSD_OUT::added_keys)
            .def("added_values", &TSD_OUT::added_values)
            .def("added_items", &TSD_OUT::added_items)
            .def("was_added", &TSD_OUT::was_added, "key"_a)
            .def_prop_ro("has_added", &TSD_OUT::has_added)
            .def("modified_keys", &TSD_OUT::modified_keys)
            .def("modified_values", &TSD_OUT::modified_values)
            .def("modified_items", &TSD_OUT::modified_items)
            .def("was_modified", &TSD_OUT::was_modified, "key"_a)
            .def("removed_keys", &TSD_OUT::removed_keys)
            .def("removed_values", &TSD_OUT::removed_values)
            .def("removed_items", &TSD_OUT::removed_items)
            .def("was_removed", &TSD_OUT::was_removed, "key"_a)
            .def("key_from_value", &TSD_OUT::key_from_value, "value"_a)
            .def_prop_ro("has_removed", &TSD_OUT::has_removed)
            .def(
                "get_ref",
                [](TSD_OUT &self, const nb::object &key, const nb::object &requester) { return self.get_ref(key, requester); },
                "key"_a, "requester"_a)
            .def(
                "release_ref",
                [](TSD_OUT &self, const nb::object &key, const nb::object &requester) { self.release_ref(key, requester); },
                "key"_a, "requester"_a)
            .def_prop_ro("key_set", &TSD_OUT::key_set)
            .def("__str__",
                 [](const TSD_OUT &self) {
                     return fmt::format("TSD_OUT@{:p}[size={}, valid={}]", static_cast<const void *>(&self), self.size(),
                                        static_cast<bool>(self.valid()));
                 })
            .def("__repr__", [](const TSD_OUT &self) {
                return fmt::format("TSD_OUT@{:p}[size={}, valid={}]", static_cast<const void *>(&self), self.size(),
                                   static_cast<bool>(self.valid()));
            });

        using TSD_IN = PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<T_Key>>;

        auto input_ = nb::class_<TSD_IN, PyTimeSeriesDictInput>(m, fmt::format("TimeSeriesDictInput_{}", name).c_str());

        input_.def("__contains__", &TSD_IN::contains, "key"_a)
            .def("__getitem__", &TSD_IN::get_item, "key"_a)
            .def(
                "get",
                [](TSD_IN &self, const nb::object &key, const nb::object &default_value) {
                    return self.contains(key) ? self.get_item(key) : default_value;
                },
                "key"_a, "default"_a = nb::none())
            .def("__len__", &TSD_IN::size)
            .def("__iter__", &TSD_IN::iter)
            .def("get", &TSD_IN::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &TSD_IN::get_or_create, "key"_a)
            .def("create", &TSD_IN::create, "key"_a)
            .def("keys", &TSD_IN::keys)
            .def("values", &TSD_IN::values)
            .def("items", &TSD_IN::items)
            .def("valid_keys", &TSD_IN::valid_keys)
            .def("valid_values", &TSD_IN::valid_values)
            .def("valid_items", &TSD_IN::valid_items)
            .def("added_keys", &TSD_IN::added_keys)
            .def("added_values", &TSD_IN::added_values)
            .def("added_items", &TSD_IN::added_items)
            .def("was_added", &TSD_IN::was_added, "key"_a)
            .def_prop_ro("has_added", &TSD_IN::has_added)
            .def("modified_keys", &TSD_IN::modified_keys)
            .def("modified_values", &TSD_IN::modified_values)
            .def("modified_items", &TSD_IN::modified_items)
            .def("was_modified", &TSD_IN::was_modified, "key"_a)
            .def("removed_keys", &TSD_IN::removed_keys)
            .def("removed_values", &TSD_IN::removed_values)
            .def("removed_items", &TSD_IN::removed_items)
            .def("was_removed", &TSD_IN::was_removed, "key"_a)
            .def("on_key_added", &TSD_IN::on_key_added, "key"_a)
            .def("on_key_removed", &TSD_IN::on_key_removed, "key"_a)
            .def("key_from_value", &TSD_IN::key_from_value, "value"_a)
            .def_prop_ro("has_removed", &TSD_IN::has_removed)
            .def_prop_ro("key_set", &TSD_IN::key_set)
            .def("__str__",
                 [](const TSD_IN &self) {
                     return fmt::format("TSD_IN@{:p}[size={}, valid={}]", static_cast<const void *>(&self), self.size(),
                                        static_cast<bool>(self.valid()));
                 })
            .def("__repr__", [](const TSD_IN &self) {
                return fmt::format("TSD_IN@{:p}[size={}, valid={}]", static_cast<const void *>(&self), self.size(),
                                   static_cast<bool>(self.valid()));
            });
    }

    template struct PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<bool>>;
    template struct PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<int64_t>>;
    template struct PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<double>>;
    template struct PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<engine_date_t>>;
    template struct PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<engine_time_t>>;
    template struct PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<engine_time_delta_t>>;
    template struct PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<nb::object>>;

    // Explicit instantiations for input types
    template struct PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<bool>>;
    template struct PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<int64_t>>;
    template struct PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<double>>;
    template struct PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<engine_date_t>>;
    template struct PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<engine_time_t>>;
    template struct PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<engine_time_delta_t>>;
    template struct PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<nb::object>>;

    void tsd_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictOutput, PyTimeSeriesOutput>(m, "TimeSeriesDictOutput");
        nb::class_<PyTimeSeriesDictInput, PyTimeSeriesInput>(m, "TimeSeriesDictInput");
        _tsd_register_with_nanobind<bool>(m, "Bool");
        _tsd_register_with_nanobind<int64_t>(m, "Int");
        _tsd_register_with_nanobind<double>(m, "Float");
        _tsd_register_with_nanobind<engine_date_t>(m, "Date");
        _tsd_register_with_nanobind<engine_time_t>(m, "DateTime");
        _tsd_register_with_nanobind<engine_time_delta_t>(m, "TimeDelta");
        _tsd_register_with_nanobind<nb::object>(m, "Object");
    }
}  // namespace hgraph
