#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tss.h>

namespace hgraph
{

    template <typename T_U>
    PyTimeSeriesSetOutput_T<T_U>::PyTimeSeriesSetOutput_T(api_ptr impl)
        : PyTimeSeriesSet<PyTimeSeriesSetOutput, T_U>(std::move(impl)) {}

    template <typename T_U> void PyTimeSeriesSetOutput_T<T_U>::remove(const nb::object &key) const {
        if (key.is_none()) { return; }
        this->impl()->remove(nb::cast<typename T_U::element_type>(key));
    }

    template <typename T_U> void PyTimeSeriesSetOutput_T<T_U>::add(const nb::object &key) const {
        if (key.is_none()) { return; }
        this->impl()->add(nb::cast<typename T_U::element_type>(key));
    }

    template <typename T_U>
    nb::object PyTimeSeriesSetOutput_T<T_U>::get_contains_output(const nb::object &item, const nb::object &requester) const {
        return wrap_output(this->impl()->get_contains_output(item, requester));
    }

    template <typename T_U>
    void PyTimeSeriesSetOutput_T<T_U>::release_contains_output(const nb::object &item, const nb::object &requester) const {
        this->impl()->release_contains_output(item, requester);
    }

    template <typename T_U> nb::object PyTimeSeriesSetOutput_T<T_U>::is_empty_output() const {
        return wrap_output(this->impl()->is_empty_output());
    }

    template <typename T_U> nb::str PyTimeSeriesSetOutput_T<T_U>::py_str() const {
        auto self{this->impl()};
        auto s{fmt::format("TimeSeriesSetOutput@{:p}[size={}, valid={}]", static_cast<const void *>(self), self->size(),
                           self->valid())};
        return nb::str(s.c_str());
    }

    template <typename T_U> nb::str PyTimeSeriesSetOutput_T<T_U>::py_repr() const { return py_str(); }

    template <typename T_U>
    PyTimeSeriesSetInput_T<T_U>::PyTimeSeriesSetInput_T(api_ptr impl)
        : PyTimeSeriesSet<PyTimeSeriesSetInput, T_U>(std::move(impl)) {}

    template <typename T_U> nb::str PyTimeSeriesSetInput_T<T_U>::py_str() const {
        auto self{this->impl()};
        auto s =
            fmt::format("TimeSeriesSetInput@{:p}[size={}, valid={}]", static_cast<const void *>(self), self->size(), self->valid());
        return nb::str(s.c_str());
    }

    template <typename T_U> nb::str PyTimeSeriesSetInput_T<T_U>::py_repr() const { return py_str(); }

    template <typename T, typename... U> void _add_base_methods_to_tss(nb::class_<T, U...> &cls) {
        cls.def("__contains__", &T::contains)
            .def("__len__", &T::size)
            .def("empty", &T::empty)
            .def("values", &T::values)
            .def("added", &T::added)
            .def("removed", &T::removed)
            .def("was_added", &T::was_added)
            .def("was_removed", &T::was_removed)
            .def("__str__", &T::py_str)
            .def("__repr__", &T::py_repr);
    }

    template <typename T> void _tss_register_with_nanobind(nb::module_ &m, const std::string &name) {
        using TSS_I = PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<T> >;
        using TSS_O = PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<T> >;
        auto tss_i  = nb::class_<TSS_I, PyTimeSeriesSetInput>(m, ("TimeSeriesSetInput_" + name).c_str());
        _add_base_methods_to_tss(tss_i);

        auto tss_o = nb::class_<TSS_O, PyTimeSeriesSetOutput>(m, ("TimeSeriesSetOutput_" + name).c_str());
        _add_base_methods_to_tss(tss_o);
        tss_o.def("add", &TSS_O::add)
            .def("remove", &TSS_O::remove, "key"_a)
            .def("is_empty_output", &TSS_O::is_empty_output)
            .def("get_contains_output", &TSS_O::get_contains_output)
            .def("release_contains_output", &TSS_O::release_contains_output);
    }

    void tss_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput");
        nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput");
        _tss_register_with_nanobind<bool>(m, "bool");
        _tss_register_with_nanobind<int64_t>(m, "int");
        _tss_register_with_nanobind<double>(m, "float");
        _tss_register_with_nanobind<engine_date_t>(m, "date");
        _tss_register_with_nanobind<engine_time_t>(m, "date_time");
        _tss_register_with_nanobind<engine_time_delta_t>(m, "time_delta");
        _tss_register_with_nanobind<nb::object>(m, "object");
    }

    // Explicit instantiations for the concrete interface types we bind

    template struct PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<bool> >;
    template struct PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<int64_t> >;
    template struct PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<double> >;
    template struct PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<engine_date_t> >;
    template struct PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<engine_time_t> >;
    template struct PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<engine_time_delta_t> >;
    template struct PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<nb::object> >;

    // Explicit instantiations for input types
    template struct PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<bool> >;
    template struct PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<int64_t> >;
    template struct PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<double> >;
    template struct PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<engine_date_t> >;
    template struct PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<engine_time_t> >;
    template struct PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<engine_time_delta_t> >;
    template struct PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<nb::object> >;

} // namespace hgraph