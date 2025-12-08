#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    template<typename T>
    time_series_output_s_ptr TimeSeriesValueOutputBuilder<T>::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesValueOutput<T>, TimeSeriesOutput>(owning_node);
    }

    template<typename T>
    time_series_output_s_ptr TimeSeriesValueOutputBuilder<T>::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesValueOutput<T>, TimeSeriesOutput>(owning_output);
    }

    template<typename T>
    void TimeSeriesValueOutputBuilder<T>::release_instance(time_series_output_ptr item) const {
        OutputBuilder::release_instance(item);
        auto ts = dynamic_cast<TimeSeriesValueOutput<T> *>(item);
        if (ts == nullptr) {
            throw std::runtime_error("TimeSeriesValueOutputBuilder::release_instance: expected TimeSeriesValueOutput but got different type");
        }
        ts->reset_value();
    }

    template<typename T>
    size_t TimeSeriesValueOutputBuilder<T>::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesValueOutput<T>));
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
