//
// Created by Claude on 15/12/2025.
//
// Unified time-series builders using TSMeta.
//
// The TSMeta itself acts as the builder - it knows how to
// efficiently construct instances of the time-series type it represents.
// This file provides thin wrappers for integration with the existing
// builder infrastructure.
//

#include <hgraph/builders/time_series_types/cpp_time_series_builder.h>
#include <hgraph/types/node.h>

#include <nanobind/nanobind.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph {

// ============================================================================
// CppTimeSeriesOutputBuilder - Wrapper that delegates to TypeMeta
// ============================================================================

time_series_output_s_ptr CppTimeSeriesOutputBuilder::make_instance(node_ptr owning_node) const {
    return ts_type_meta->make_output(owning_node);
}

time_series_output_s_ptr CppTimeSeriesOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
    // For nested outputs, delegate to TypeMeta's parent-based method
    return ts_type_meta->make_output(owning_output);
}

void CppTimeSeriesOutputBuilder::release_instance(time_series_output_ptr item) const {
    OutputBuilder::release_instance(item);
}

bool CppTimeSeriesOutputBuilder::has_reference() const {
    return ts_type_meta->is_reference();
}

size_t CppTimeSeriesOutputBuilder::memory_size() const {
    return ts_type_meta->output_memory_size();
}

// ============================================================================
// CppTimeSeriesInputBuilder - Wrapper that delegates to TypeMeta
// ============================================================================

time_series_input_s_ptr CppTimeSeriesInputBuilder::make_instance(node_ptr owning_node) const {
    return ts_type_meta->make_input(owning_node);
}

time_series_input_s_ptr CppTimeSeriesInputBuilder::make_instance(time_series_input_ptr owning_input) const {
    // For nested inputs, delegate to TypeMeta's parent-based method
    return ts_type_meta->make_input(owning_input);
}

void CppTimeSeriesInputBuilder::release_instance(time_series_input_ptr item) const {
    InputBuilder::release_instance(item);
}

bool CppTimeSeriesInputBuilder::has_reference() const {
    return ts_type_meta->is_reference();
}

size_t CppTimeSeriesInputBuilder::memory_size() const {
    return ts_type_meta->input_memory_size();
}

// ============================================================================
// Nanobind registration
// ============================================================================

void cpp_time_series_output_builder_register_with_nanobind(nb::module_ &m) {
    nb::class_<CppTimeSeriesOutputBuilder, OutputBuilder>(m, "CppTimeSeriesOutputBuilder")
        .def(nb::init<const TSMeta*>(), "ts_type_meta"_a)
        .def("has_reference", &CppTimeSeriesOutputBuilder::has_reference);

    // Expose make_output directly on TSMeta for convenience
    m.def("make_output_builder", [](const TSMeta* meta) -> OutputBuilder::ptr {
        return OutputBuilder::ptr(new CppTimeSeriesOutputBuilder(meta));
    }, nb::rv_policy::reference, "ts_type_meta"_a,
       "Create an output builder from TSMeta");
}

void cpp_time_series_input_builder_register_with_nanobind(nb::module_ &m) {
    nb::class_<CppTimeSeriesInputBuilder, InputBuilder>(m, "CppTimeSeriesInputBuilder")
        .def(nb::init<const TSMeta*>(), "ts_type_meta"_a)
        .def("has_reference", &CppTimeSeriesInputBuilder::has_reference);

    // Expose make_input directly on TSMeta for convenience
    m.def("make_input_builder", [](const TSMeta* meta) -> InputBuilder::ptr {
        return InputBuilder::ptr(new CppTimeSeriesInputBuilder(meta));
    }, nb::rv_policy::reference, "ts_type_meta"_a,
       "Create an input builder from TSMeta");
}

} // namespace hgraph
