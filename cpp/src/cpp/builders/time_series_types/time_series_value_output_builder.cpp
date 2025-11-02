#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>

namespace hgraph {
    template<typename T>
    time_series_output_ptr TimeSeriesValueOutputBuilder<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesValueOutput<T>(owning_node)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    template<typename T>
    time_series_output_ptr TimeSeriesValueOutputBuilder<T>::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesValueOutput<T>(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    template<typename T>
    void TimeSeriesValueOutputBuilder<T>::release_instance(time_series_output_ptr item) const {
        OutputBuilder::release_instance(item);
        auto ts = dynamic_cast<TimeSeriesValueOutput<T> *>(item.get());
        if (ts) { ts->reset_value(); }
    }

    void time_series_value_output_builder_register_with_nanobind(nb::module_ &m) {
        using OutputBuilder_TS_Bool = TimeSeriesValueOutputBuilder<bool>;
        using OutputBuilder_TS_Int = TimeSeriesValueOutputBuilder<int64_t>;
        using OutputBuilder_TS_Float = TimeSeriesValueOutputBuilder<double>;
        using OutputBuilder_TS_Date = TimeSeriesValueOutputBuilder<engine_date_t>;
        using OutputBuilder_TS_DateTime = TimeSeriesValueOutputBuilder<engine_time_t>;
        using OutputBuilder_TS_TimeDelta = TimeSeriesValueOutputBuilder<engine_time_delta_t>;
        using OutputBuilder_TS_Object = TimeSeriesValueOutputBuilder<nb::object>;

        nb::class_<OutputBuilder_TS_Bool, OutputBuilder>(m, "OutputBuilder_TS_Bool").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Int, OutputBuilder>(m, "OutputBuilder_TS_Int").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Float, OutputBuilder>(m, "OutputBuilder_TS_Float").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Date, OutputBuilder>(m, "OutputBuilder_TS_Date").def(nb::init<>());
        nb::class_<OutputBuilder_TS_DateTime, OutputBuilder>(m, "OutputBuilder_TS_DateTime").def(nb::init<>());
        nb::class_<OutputBuilder_TS_TimeDelta, OutputBuilder>(m, "OutputBuilder_TS_TimeDelta").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Object, OutputBuilder>(m, "OutputBuilder_TS_Object").def(nb::init<>());
    }

    // Template instantiations
    template struct TimeSeriesValueOutputBuilder<bool>;
    template struct TimeSeriesValueOutputBuilder<int64_t>;
    template struct TimeSeriesValueOutputBuilder<double>;
    template struct TimeSeriesValueOutputBuilder<engine_date_t>;
    template struct TimeSeriesValueOutputBuilder<engine_time_t>;
    template struct TimeSeriesValueOutputBuilder<engine_time_delta_t>;
    template struct TimeSeriesValueOutputBuilder<nb::object>;
} // namespace hgraph