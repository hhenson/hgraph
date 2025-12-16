#include <hgraph/api/python/py_tsw.h>

namespace hgraph
{
    template <typename T_U>
        requires is_tsw_output<T_U>
    PyTimeSeriesWindowOutput<T_U>::PyTimeSeriesWindowOutput(api_ptr impl) : PyTimeSeriesOutput(std::move(impl)) {}


    template <typename T_U>
        requires is_tsw_output<T_U>
    nb::object PyTimeSeriesWindowOutput<T_U>::value_times() const {
        return impl()->py_value_times();
    }

    template <typename T_U>
        requires is_tsw_output<T_U>
    engine_time_t PyTimeSeriesWindowOutput<T_U>::first_modified_time() const {
        return impl()->first_modified_time();
    }

    template <typename T_U>
        requires is_tsw_output<T_U>
    nb::object PyTimeSeriesWindowOutput<T_U>::size() const {
        return nb::cast(impl()->size());
    }

    template <typename T_U>
        requires is_tsw_output<T_U>
    nb::object PyTimeSeriesWindowOutput<T_U>::min_size() const {
        return nb::cast(impl()->min_size());
    }

    template <typename T_U>
        requires is_tsw_output<T_U>
    nb::bool_ PyTimeSeriesWindowOutput<T_U>::has_removed_value() const {
        return nb::bool_(impl()->has_removed_value());
    }

    template <typename T_U>
        requires is_tsw_output<T_U>
    nb::object PyTimeSeriesWindowOutput<T_U>::removed_value() const {
        return nb::cast(impl()->removed_value());
    }

    template <typename T_U>
        requires is_tsw_output<T_U>
    nb::int_ PyTimeSeriesWindowOutput<T_U>::len() const {
        return nb::int_(impl()->len());
    }

    template <typename T_U>
        requires is_tsw_output<T_U>
    auto PyTimeSeriesWindowOutput<T_U>::impl() const -> T_U * {
        return this->template static_cast_impl<T_U>();
    }

    template <typename T>
    PyTimeSeriesWindowInput<T>::PyTimeSeriesWindowInput(api_ptr impl) : PyTimeSeriesInput(std::move(impl)) {}

    template <typename T> nb::object PyTimeSeriesWindowInput<T>::value_times() const { return nb::cast(impl()->py_value_times()); }

    template <typename T> engine_time_t PyTimeSeriesWindowInput<T>::first_modified_time() const {
        return impl()->first_modified_time();
    }

    template <typename T> nb::bool_ PyTimeSeriesWindowInput<T>::has_removed_value() const {
        return nb::bool_(impl()->has_removed_value());
    }

    template <typename T> nb::object PyTimeSeriesWindowInput<T>::removed_value() const { return nb::cast(impl()->removed_value()); }

    template <typename T> nb::int_ PyTimeSeriesWindowInput<T>::len() const {
        // TODO: This is an example of how bad this is.
        auto self{impl()};
        if (auto *f = self->as_fixed_output()) return nb::int_(f->len());
        if (auto *t = self->as_time_output()) return nb::int_(t->len());
        return nb::int_(0);
    }

    template <typename T> auto PyTimeSeriesWindowInput<T>::impl() const -> TimeSeriesWindowInput<T> * {
        return this->template static_cast_impl<TimeSeriesWindowInput<T>>();
    }

    template <typename T> static void bind_tsw_for_type(nb::module_ &m, const char *suffix) {
        using Out_U = TimeSeriesFixedWindowOutput<T>;
        using Out   = PyTimeSeriesWindowOutput<Out_U>;
        using In    = PyTimeSeriesWindowInput<T>;

        auto out_cls =
            nb::class_<Out, PyTimeSeriesOutput>(m, (std::string("TimeSeriesFixedWindowOutput_") + suffix).c_str())
                .def_prop_ro("value_times", &Out::value_times)
                .def_prop_ro("first_modified_time", &Out::first_modified_time)
                .def_prop_ro("size", &Out::size)
                .def_prop_ro("min_size", &Out::min_size)
                .def_prop_ro("has_removed_value", &Out::has_removed_value)
                .def_prop_ro("removed_value",
                             [](const Out &o) { return o.has_removed_value() ? nb::cast(o.removed_value()) : nb::none(); })
                .def("__len__", &Out::len);

        auto in_cls = nb::class_<In, PyTimeSeriesInput>(m, (std::string("TimeSeriesWindowInput_") + suffix).c_str())
                          .def_prop_ro("value_times", &In::value_times)
                          .def_prop_ro("first_modified_time", &In::first_modified_time)
                          .def_prop_ro("has_removed_value", &In::has_removed_value)
                          .def_prop_ro("removed_value", &In::removed_value)
                          .def("__len__", &In::len);

        (void)out_cls;
        (void)in_cls;
    }

    // Binding functions for time-based windows
    template <typename T> static void bind_time_tsw_for_type(nb::module_ &m, const char *suffix) {
        using Out = PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<T>>;

        auto out_cls = nb::class_<Out, PyTimeSeriesOutput>(m, (std::string("TimeSeriesTimeWindowOutput_") + suffix).c_str())
                           .def_prop_ro("value_times", &Out::value_times)
                           .def_prop_ro("first_modified_time", &Out::first_modified_time)
                           .def_prop_ro("size", &Out::size)
                           .def_prop_ro("min_size", &Out::min_size)
                           .def_prop_ro("has_removed_value", &Out::has_removed_value)
                           .def_prop_ro("removed_value", &Out::removed_value)
                           .def("__len__", &Out::len);

        (void)out_cls;
    }

    void tsw_register_with_nanobind(nb::module_ &m) {
        // Fixed-size (tick-based) windows
        bind_tsw_for_type<bool>(m, "bool");
        bind_tsw_for_type<int64_t>(m, "int");
        bind_tsw_for_type<double>(m, "float");
        bind_tsw_for_type<engine_date_t>(m, "date");
        bind_tsw_for_type<engine_time_t>(m, "date_time");
        bind_tsw_for_type<engine_time_delta_t>(m, "time_delta");
        bind_tsw_for_type<nb::object>(m, "object");

        // Time-based (timedelta) windows
        bind_time_tsw_for_type<bool>(m, "bool");
        bind_time_tsw_for_type<int64_t>(m, "int");
        bind_time_tsw_for_type<double>(m, "float");
        bind_time_tsw_for_type<engine_date_t>(m, "date");
        bind_time_tsw_for_type<engine_time_t>(m, "date_time");
        bind_time_tsw_for_type<engine_time_delta_t>(m, "time_delta");
        bind_time_tsw_for_type<nb::object>(m, "object");
    }

    // Explicit template instantiations for constructors used by wrapper factory
    // Fixed window outputs - all registered types (ApiPtr version)
    template PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<bool>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesFixedWindowOutput<bool>>);
    template PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<int64_t>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesFixedWindowOutput<int64_t>>);
    template PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<double>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesFixedWindowOutput<double>>);
    template PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<engine_date_t>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesFixedWindowOutput<engine_date_t>>);
    template PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<engine_time_t>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesFixedWindowOutput<engine_time_t>>);
    template PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<engine_time_delta_t>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesFixedWindowOutput<engine_time_delta_t>>);
    template PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<nb::object>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesFixedWindowOutput<nb::object>>);

    // Time window outputs - all registered types (ApiPtr version)
    template PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<bool>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesTimeWindowOutput<bool>>);
    template PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<int64_t>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesTimeWindowOutput<int64_t>>);
    template PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<double>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesTimeWindowOutput<double>>);
    template PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<engine_date_t>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesTimeWindowOutput<engine_date_t>>);
    template PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<engine_time_t>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesTimeWindowOutput<engine_time_t>>);
    template PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<engine_time_delta_t>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesTimeWindowOutput<engine_time_delta_t>>);
    template PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<nb::object>>::PyTimeSeriesWindowOutput(ApiPtr<TimeSeriesTimeWindowOutput<nb::object>>);

    // Window inputs - all registered types (ApiPtr version)
    template PyTimeSeriesWindowInput<bool>::PyTimeSeriesWindowInput(ApiPtr<TimeSeriesWindowInput<bool>>);
    template PyTimeSeriesWindowInput<int64_t>::PyTimeSeriesWindowInput(ApiPtr<TimeSeriesWindowInput<int64_t>>);
    template PyTimeSeriesWindowInput<double>::PyTimeSeriesWindowInput(ApiPtr<TimeSeriesWindowInput<double>>);
    template PyTimeSeriesWindowInput<engine_date_t>::PyTimeSeriesWindowInput(ApiPtr<TimeSeriesWindowInput<engine_date_t>>);
    template PyTimeSeriesWindowInput<engine_time_t>::PyTimeSeriesWindowInput(ApiPtr<TimeSeriesWindowInput<engine_time_t>>);
    template PyTimeSeriesWindowInput<engine_time_delta_t>::PyTimeSeriesWindowInput(ApiPtr<TimeSeriesWindowInput<engine_time_delta_t>>);
    template PyTimeSeriesWindowInput<nb::object>::PyTimeSeriesWindowInput(ApiPtr<TimeSeriesWindowInput<nb::object>>);

}  // namespace hgraph
