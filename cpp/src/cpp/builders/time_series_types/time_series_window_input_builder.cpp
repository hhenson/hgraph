#include <hgraph/builders/time_series_types/time_series_window_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsw.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    // Unified window input builder implementations
    // Creates unified input that dynamically works with both fixed-size and timedelta outputs
    template<typename T>
    time_series_input_s_ptr TimeSeriesWindowInputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesWindowInput<T>, TimeSeriesInput>(owning_node);
    }

    template<typename T>
    time_series_input_s_ptr TimeSeriesWindowInputBuilder_T<T>::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesWindowInput<T>, TimeSeriesInput>(owning_input);
    }

    template<typename T>
    size_t TimeSeriesWindowInputBuilder_T<T>::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesWindowInput<T>));
    }

    void time_series_window_input_builder_register_with_nanobind(nb::module_ &m) {
        // Unified window input builders
        using InputBuilder_TSW_Bool = TimeSeriesWindowInputBuilder_T<bool>;
        using InputBuilder_TSW_Int = TimeSeriesWindowInputBuilder_T<int64_t>;
        using InputBuilder_TSW_Float = TimeSeriesWindowInputBuilder_T<double>;
        using InputBuilder_TSW_Date = TimeSeriesWindowInputBuilder_T<engine_date_t>;
        using InputBuilder_TSW_DateTime = TimeSeriesWindowInputBuilder_T<engine_time_t>;
        using InputBuilder_TSW_TimeDelta = TimeSeriesWindowInputBuilder_T<engine_time_delta_t>;
        using InputBuilder_TSW_Object = TimeSeriesWindowInputBuilder_T<nb::object>;

        nb::class_<InputBuilder_TSW_Bool, InputBuilder>(m, "InputBuilder_TSW_Bool")
                .def(nb::init<>());
        nb::class_<InputBuilder_TSW_Int, InputBuilder>(m, "InputBuilder_TSW_Int")
                .def(nb::init<>());
        nb::class_<InputBuilder_TSW_Float, InputBuilder>(m, "InputBuilder_TSW_Float")
                .def(nb::init<>());
        nb::class_<InputBuilder_TSW_Date, InputBuilder>(m, "InputBuilder_TSW_Date")
                .def(nb::init<>());
        nb::class_<InputBuilder_TSW_DateTime, InputBuilder>(m, "InputBuilder_TSW_DateTime")
                .def(nb::init<>());
        nb::class_<InputBuilder_TSW_TimeDelta, InputBuilder>(m, "InputBuilder_TSW_TimeDelta")
                .def(nb::init<>());
        nb::class_<InputBuilder_TSW_Object, InputBuilder>(m, "InputBuilder_TSW_Object")
                .def(nb::init<>());
    }

    // Template instantiations for unified window builders
    template struct TimeSeriesWindowInputBuilder_T<bool>;
    template struct TimeSeriesWindowInputBuilder_T<int64_t>;
    template struct TimeSeriesWindowInputBuilder_T<double>;
    template struct TimeSeriesWindowInputBuilder_T<engine_date_t>;
    template struct TimeSeriesWindowInputBuilder_T<engine_time_t>;
    template struct TimeSeriesWindowInputBuilder_T<engine_time_delta_t>;
    template struct TimeSeriesWindowInputBuilder_T<nb::object>;
} // namespace hgraph