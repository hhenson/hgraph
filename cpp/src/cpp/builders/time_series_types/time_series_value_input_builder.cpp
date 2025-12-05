#include <hgraph/builders/time_series_types/time_series_value_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    template<typename T>
    time_series_input_s_ptr TimeSeriesValueInputBuilder<T>::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesValueInput<T>, TimeSeriesInput>(owning_node);
    }

    template<typename T>
    time_series_input_s_ptr TimeSeriesValueInputBuilder<T>::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesValueInput<T>, TimeSeriesInput>(owning_input);
    }

    template<typename T>
    size_t TimeSeriesValueInputBuilder<T>::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesValueInput<T>));
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
