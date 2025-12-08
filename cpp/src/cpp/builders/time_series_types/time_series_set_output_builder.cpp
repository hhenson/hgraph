#include <hgraph/builders/time_series_types/time_series_set_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tss.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    template<typename T>
    time_series_output_s_ptr TimeSeriesSetOutputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesSetOutput_T<T>, TimeSeriesOutput>(owning_node);
    }

    template<typename T>
    time_series_output_s_ptr TimeSeriesSetOutputBuilder_T<T>::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesSetOutput_T<T>, TimeSeriesOutput>(owning_output);
    }

    template<typename T>
    void TimeSeriesSetOutputBuilder_T<T>::release_instance(time_series_output_ptr item) const {
        TimeSeriesSetOutputBuilder::release_instance(item);
        auto set = dynamic_cast<TimeSeriesSetOutput_T<T> *>(item);
        if (set == nullptr) {
            throw std::runtime_error("TimeSeriesSetOutputBuilder_T::release_instance: expected TimeSeriesSetOutput_T but got different type");
        }
        set->_reset_value();
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