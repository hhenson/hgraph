#include <hgraph/builders/time_series_types/specialized_ref_builders.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>

namespace hgraph {
    // ============================================================
    // Specialized Reference Input Builders
    // ============================================================

    // TimeSeriesValueRefInputBuilder - REF[TS[...]]
    time_series_input_ptr TimeSeriesValueRefInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesValueReferenceInput(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesValueRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesValueReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    void TimeSeriesValueRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueRefInputBuilder, InputBuilder>(m, "InputBuilder_TS_Value_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesListRefInputBuilder - REF[TSL[...]]
    time_series_input_ptr TimeSeriesListRefInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesListReferenceInput(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesListRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesListReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    void TimeSeriesListRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListRefInputBuilder, InputBuilder>(m, "InputBuilder_TSL_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesBundleRefInputBuilder - REF[TSB[...]]
    time_series_input_ptr TimeSeriesBundleRefInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesBundleReferenceInput(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesBundleRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesBundleReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    void TimeSeriesBundleRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleRefInputBuilder, InputBuilder>(m, "InputBuilder_TSB_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesDictRefInputBuilder - REF[TSD[...]]
    time_series_input_ptr TimeSeriesDictRefInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesDictReferenceInput(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesDictRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesDictReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    void TimeSeriesDictRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictRefInputBuilder, InputBuilder>(m, "InputBuilder_TSD_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesSetRefInputBuilder - REF[TSS[...]]
    time_series_input_ptr TimeSeriesSetRefInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesSetReferenceInput(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesSetRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesSetReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    void TimeSeriesSetRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetRefInputBuilder, InputBuilder>(m, "InputBuilder_TSS_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesWindowRefInputBuilder - REF[TSW[...]]
    time_series_input_ptr TimeSeriesWindowRefInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesWindowReferenceInput(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesWindowRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesWindowReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
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
        auto v{new TimeSeriesValueReferenceOutput(owning_node)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    time_series_output_ptr TimeSeriesValueRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesValueReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    void TimeSeriesValueRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TS_Value_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesListRefOutputBuilder - REF[TSL[...]]
    time_series_output_ptr TimeSeriesListRefOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesListReferenceOutput(owning_node)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    time_series_output_ptr TimeSeriesListRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesListReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    void TimeSeriesListRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSL_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesBundleRefOutputBuilder - REF[TSB[...]]
    time_series_output_ptr TimeSeriesBundleRefOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesBundleReferenceOutput(owning_node)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    time_series_output_ptr TimeSeriesBundleRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesBundleReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    void TimeSeriesBundleRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSB_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesDictRefOutputBuilder - REF[TSD[...]]
    time_series_output_ptr TimeSeriesDictRefOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesDictReferenceOutput(owning_node)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    time_series_output_ptr TimeSeriesDictRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesDictReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    void TimeSeriesDictRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSD_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesSetRefOutputBuilder - REF[TSS[...]]
    time_series_output_ptr TimeSeriesSetRefOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesSetReferenceOutput(owning_node)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    time_series_output_ptr TimeSeriesSetRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesSetReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    void TimeSeriesSetRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSS_Ref")
                .def(nb::init<>());
    }

    // TimeSeriesWindowRefOutputBuilder - REF[TSW[...]]
    time_series_output_ptr TimeSeriesWindowRefOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesWindowReferenceOutput(owning_node)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    time_series_output_ptr TimeSeriesWindowRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesWindowReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    void TimeSeriesWindowRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesWindowRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSW_Ref")
                .def(nb::init<>());
    }

} // namespace hgraph

