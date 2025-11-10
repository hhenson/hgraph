#include <hgraph/builders/time_series_types/specialized_ref_builders.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>

namespace hgraph {
    // ============================================================
    // Specialized Reference Input Builders
    // ============================================================

    // TimeSeriesValueRefInputBuilder - REF[TS[...]]
    time_series_input_ptr TimeSeriesValueRefInputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_input_ptr{new TimeSeriesValueReferenceInput(owning_node)};
    }

    time_series_input_ptr TimeSeriesValueRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return time_series_input_ptr{new TimeSeriesValueReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
    }

    void TimeSeriesValueRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueRefInputBuilder, InputBuilder>(m, "InputBuilder_TS_Value_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesListRefInputBuilder - REF[TSL[...]]
    time_series_input_ptr TimeSeriesListRefInputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_input_ptr{new TimeSeriesListReferenceInput(owning_node)};
    }

    time_series_input_ptr TimeSeriesListRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return time_series_input_ptr{new TimeSeriesListReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
    }

    void TimeSeriesListRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListRefInputBuilder, InputBuilder>(m, "InputBuilder_TSL_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesBundleRefInputBuilder - REF[TSB[...]]
    time_series_input_ptr TimeSeriesBundleRefInputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_input_ptr{new TimeSeriesBundleReferenceInput(owning_node)};
    }

    time_series_input_ptr TimeSeriesBundleRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return time_series_input_ptr{new TimeSeriesBundleReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
    }

    void TimeSeriesBundleRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleRefInputBuilder, InputBuilder>(m, "InputBuilder_TSB_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesDictRefInputBuilder - REF[TSD[...]]
    time_series_input_ptr TimeSeriesDictRefInputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_input_ptr{new TimeSeriesDictReferenceInput(owning_node)};
    }

    time_series_input_ptr TimeSeriesDictRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return time_series_input_ptr{new TimeSeriesDictReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
    }

    void TimeSeriesDictRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictRefInputBuilder, InputBuilder>(m, "InputBuilder_TSD_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesSetRefInputBuilder - REF[TSS[...]]
    time_series_input_ptr TimeSeriesSetRefInputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_input_ptr{new TimeSeriesSetReferenceInput(owning_node)};
    }

    time_series_input_ptr TimeSeriesSetRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return time_series_input_ptr{new TimeSeriesSetReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
    }

    void TimeSeriesSetRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetRefInputBuilder, InputBuilder>(m, "InputBuilder_TSS_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesWindowRefInputBuilder - REF[TSW[...]]
    time_series_input_ptr TimeSeriesWindowRefInputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_input_ptr{new TimeSeriesWindowReferenceInput(owning_node)};
    }

    time_series_input_ptr TimeSeriesWindowRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return time_series_input_ptr{new TimeSeriesWindowReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
    }

    void TimeSeriesWindowRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesWindowRefInputBuilder, InputBuilder>(m, "InputBuilder_TSW_Ref")
                .def(nb::init<>());
    }

    // ============================================================
    // Specialized Reference Output Builders
    // ============================================================

    // TimeSeriesValueRefOutputBuilder - REF[TS[...]]
    time_series_output_ptr TimeSeriesValueRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_output_ptr{new TimeSeriesValueReferenceOutput(owning_node)};
    }

    time_series_output_ptr TimeSeriesValueRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return time_series_output_ptr{new TimeSeriesValueReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
    }

    void TimeSeriesValueRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TS_Value_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesListRefOutputBuilder - REF[TSL[...]]
    time_series_output_ptr TimeSeriesListRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_output_ptr{new TimeSeriesListReferenceOutput(owning_node)};
    }

    time_series_output_ptr TimeSeriesListRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return time_series_output_ptr{new TimeSeriesListReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
    }

    void TimeSeriesListRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSL_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesBundleRefOutputBuilder - REF[TSB[...]]
    time_series_output_ptr TimeSeriesBundleRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_output_ptr{new TimeSeriesBundleReferenceOutput(owning_node)};
    }

    time_series_output_ptr TimeSeriesBundleRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return time_series_output_ptr{new TimeSeriesBundleReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
    }

    void TimeSeriesBundleRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSB_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesDictRefOutputBuilder - REF[TSD[...]]
    time_series_output_ptr TimeSeriesDictRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_output_ptr{new TimeSeriesDictReferenceOutput(owning_node)};
    }

    time_series_output_ptr TimeSeriesDictRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return time_series_output_ptr{new TimeSeriesDictReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
    }

    void TimeSeriesDictRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSD_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesSetRefOutputBuilder - REF[TSS[...]]
    time_series_output_ptr TimeSeriesSetRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_output_ptr{new TimeSeriesSetReferenceOutput(owning_node)};
    }

    time_series_output_ptr TimeSeriesSetRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return time_series_output_ptr{new TimeSeriesSetReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
    }

    void TimeSeriesSetRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSS_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesWindowRefOutputBuilder - REF[TSW[...]]
    time_series_output_ptr TimeSeriesWindowRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return time_series_output_ptr{new TimeSeriesWindowReferenceOutput(owning_node)};
    }

    time_series_output_ptr TimeSeriesWindowRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return time_series_output_ptr{new TimeSeriesWindowReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
    }

    void TimeSeriesWindowRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesWindowRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSW_Ref")
                .def(nb::init<>());
    }

} // namespace hgraph

