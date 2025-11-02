#include <hgraph/builders/time_series_types/time_series_value_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>

namespace hgraph {
    template<typename T>
    time_series_input_ptr TimeSeriesValueInputBuilder<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesValueInput<T>(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    template<typename T>
    time_series_input_ptr TimeSeriesValueInputBuilder<T>::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesValueInput<T>(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    void time_series_value_input_builder_register_with_nanobind(nb::module_ &m) {
        using InputBuilder_TS_Bool = TimeSeriesValueInputBuilder<bool>;
        using InputBuilder_TS_Int = TimeSeriesValueInputBuilder<int64_t>;
        using InputBuilder_TS_Float = TimeSeriesValueInputBuilder<double>;
        using InputBuilder_TS_Date = TimeSeriesValueInputBuilder<engine_date_t>;
        using InputBuilder_TS_DateTime = TimeSeriesValueInputBuilder<engine_time_t>;
        using InputBuilder_TS_TimeDelta = TimeSeriesValueInputBuilder<engine_time_delta_t>;
        using InputBuilder_TS_Object = TimeSeriesValueInputBuilder<nb::object>;

        nb::class_<InputBuilder_TS_Bool, InputBuilder>(m, "InputBuilder_TS_Bool").def(nb::init<>());
        nb::class_<InputBuilder_TS_Int, InputBuilder>(m, "InputBuilder_TS_Int").def(nb::init<>());
        nb::class_<InputBuilder_TS_Float, InputBuilder>(m, "InputBuilder_TS_Float").def(nb::init<>());
        nb::class_<InputBuilder_TS_Date, InputBuilder>(m, "InputBuilder_TS_Date").def(nb::init<>());
        nb::class_<InputBuilder_TS_DateTime, InputBuilder>(m, "InputBuilder_TS_DateTime").def(nb::init<>());
        nb::class_<InputBuilder_TS_TimeDelta, InputBuilder>(m, "InputBuilder_TS_TimeDelta").def(nb::init<>());
        nb::class_<InputBuilder_TS_Object, InputBuilder>(m, "InputBuilder_TS_Object").def(nb::init<>());
    }

    // Template instantiations
    template struct TimeSeriesValueInputBuilder<bool>;
    template struct TimeSeriesValueInputBuilder<int64_t>;
    template struct TimeSeriesValueInputBuilder<double>;
    template struct TimeSeriesValueInputBuilder<engine_date_t>;
    template struct TimeSeriesValueInputBuilder<engine_time_t>;
    template struct TimeSeriesValueInputBuilder<engine_time_delta_t>;
    template struct TimeSeriesValueInputBuilder<nb::object>;
} // namespace hgraph