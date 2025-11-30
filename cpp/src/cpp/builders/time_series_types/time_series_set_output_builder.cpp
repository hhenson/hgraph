#include <hgraph/builders/time_series_types/time_series_set_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tss.h>

namespace hgraph {
    template<typename T>
    time_series_output_ptr TimeSeriesSetOutputBuilder_T<T>::make_instance(node_ptr owning_node, std::shared_ptr<void> buffer, size_t* offset) const {
        return make_instance_impl<TimeSeriesSetOutput_T<T>, TimeSeriesOutput>(
            buffer, offset, "TimeSeriesSetOutput", owning_node);
    }

    template<typename T>
    time_series_output_ptr TimeSeriesSetOutputBuilder_T<T>::make_instance(time_series_output_ptr owning_output, std::shared_ptr<void> buffer, size_t* offset) const {
        // Convert owning_output to TimeSeriesType shared_ptr
        auto owning_ts = std::dynamic_pointer_cast<TimeSeriesType>(owning_output);
        if (!owning_ts) {
            throw std::runtime_error("TimeSeriesSetOutputBuilder: owning_output must be a TimeSeriesType");
        }
        return make_instance_impl<TimeSeriesSetOutput_T<T>, TimeSeriesOutput>(
            buffer, offset, "TimeSeriesSetOutput", owning_ts);
    }

    template<typename T>
    void TimeSeriesSetOutputBuilder_T<T>::release_instance(time_series_output_ptr item) const {
        TimeSeriesSetOutputBuilder::release_instance(item);
        auto set = dynamic_cast<TimeSeriesSetOutput_T<T> *>(item.get());
        if (set) { set->_reset_value(); }
    }

    template<typename T>
    size_t TimeSeriesSetOutputBuilder_T<T>::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesSetOutput_T<T>));
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