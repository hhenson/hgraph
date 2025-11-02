#include <hgraph/builders/time_series_types/time_series_set_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tss.h>

namespace hgraph {
    template<typename T>
    time_series_input_ptr TimeSeriesSetInputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesSetInput_T<T>{owning_node}};
        return v;
    }

    template<typename T>
    time_series_input_ptr TimeSeriesSetInputBuilder_T<T>::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesSetInput_T<T>{dynamic_cast_ref<TimeSeriesType>(owning_input)}};
        return v;
    }

    void time_series_set_input_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetInputBuilder, InputBuilder>(m, "InputBuilder_TSS");

        nb::class_<TimeSeriesSetInputBuilder_T<bool>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_Bool").def(
            nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<int64_t>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_Int").def(
            nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<double>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_Float").def(
            nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<engine_date_t>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_Date")
                .def(nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<engine_time_t>, TimeSeriesSetInputBuilder>(
                    m, "InputBuilder_TSS_DateTime")
                .def(nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<engine_time_delta_t>, TimeSeriesSetInputBuilder>(
                    m, "InputBuilder_TSS_TimeDelta")
                .def(nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<nb::object>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_Object")
                .def(nb::init<>());
    }

    // Template instantiations
    template struct TimeSeriesSetInputBuilder_T<bool>;
    template struct TimeSeriesSetInputBuilder_T<int64_t>;
    template struct TimeSeriesSetInputBuilder_T<double>;
    template struct TimeSeriesSetInputBuilder_T<engine_date_t>;
    template struct TimeSeriesSetInputBuilder_T<engine_time_t>;
    template struct TimeSeriesSetInputBuilder_T<engine_time_delta_t>;
    template struct TimeSeriesSetInputBuilder_T<nb::object>;
} // namespace hgraph