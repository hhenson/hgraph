#include <hgraph/builders/time_series_types/time_series_set_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tss.h>

namespace hgraph {
    template<typename T>
    time_series_output_ptr TimeSeriesSetOutputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesSetOutput_T<T>(owning_node)};
        return v;
    }

    template<typename T>
    time_series_output_ptr TimeSeriesSetOutputBuilder_T<T>::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesSetOutput_T<T>{dynamic_cast_ref<TimeSeriesType>(owning_output)}};
        return v;
    }

    template<typename T>
    void TimeSeriesSetOutputBuilder_T<T>::release_instance(time_series_output_ptr item) const {
        TimeSeriesSetOutputBuilder::release_instance(item);
        auto set = dynamic_cast<TimeSeriesSetOutput_T<T> *>(item.get());
        if (set) { set->_reset_value(); }
    }

    void time_series_set_output_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSS");
        nb::class_<TimeSeriesSetOutputBuilder_T<bool>, TimeSeriesSetOutputBuilder>(m, "OutputBuilder_TSS_Bool").def(
            nb::init<>());
        nb::class_<TimeSeriesSetOutputBuilder_T<int64_t>, TimeSeriesSetOutputBuilder>(m, "OutputBuilder_TSS_Int").def(
            nb::init<>());
        nb::class_<TimeSeriesSetOutputBuilder_T<double>, TimeSeriesSetOutputBuilder>(m, "OutputBuilder_TSS_Float")
                .def(nb::init<>());
        nb::class_<TimeSeriesSetOutputBuilder_T<engine_date_t>, TimeSeriesSetOutputBuilder>(m, "OutputBuilder_TSS_Date")
                .def(nb::init<>());
        nb::class_<TimeSeriesSetOutputBuilder_T<engine_time_t>, TimeSeriesSetOutputBuilder>(
                    m, "OutputBuilder_TSS_DateTime")
                .def(nb::init<>());
        nb::class_<TimeSeriesSetOutputBuilder_T<engine_time_delta_t>, TimeSeriesSetOutputBuilder>(
                    m, "OutputBuilder_TSS_TimeDelta")
                .def(nb::init<>());
        nb::class_<TimeSeriesSetOutputBuilder_T<nb::object>, TimeSeriesSetOutputBuilder>(m, "OutputBuilder_TSS_Object")
                .def(nb::init<>());
    }

    // Template instantiations
    template struct TimeSeriesSetOutputBuilder_T<bool>;
    template struct TimeSeriesSetOutputBuilder_T<int64_t>;
    template struct TimeSeriesSetOutputBuilder_T<double>;
    template struct TimeSeriesSetOutputBuilder_T<engine_date_t>;
    template struct TimeSeriesSetOutputBuilder_T<engine_time_t>;
    template struct TimeSeriesSetOutputBuilder_T<engine_time_delta_t>;
    template struct TimeSeriesSetOutputBuilder_T<nb::object>;
} // namespace hgraph