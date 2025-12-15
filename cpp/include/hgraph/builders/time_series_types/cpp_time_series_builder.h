//
// Created by Claude on 15/12/2025.
//
// Unified time-series builders that use TimeSeriesTypeMeta for type-driven construction.
// These builders dispatch to the appropriate specialized builder based on TimeSeriesKind.
//

#ifndef CPP_TIME_SERIES_BUILDER_H
#define CPP_TIME_SERIES_BUILDER_H

#include <hgraph/builders/output_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/types/time_series/ts_type_meta.h>

namespace hgraph {

/**
 * CppTimeSeriesOutputBuilder - Unified output builder using TimeSeriesTypeMeta
 *
 * This builder takes a TimeSeriesTypeMeta pointer and creates the appropriate
 * output time-series type based on the ts_kind field.
 */
struct HGRAPH_EXPORT CppTimeSeriesOutputBuilder : OutputBuilder {
    const TimeSeriesTypeMeta* ts_type_meta;

    explicit CppTimeSeriesOutputBuilder(const TimeSeriesTypeMeta* meta)
        : ts_type_meta(meta) {}

    time_series_output_s_ptr make_instance(node_ptr owning_node) const override;
    time_series_output_s_ptr make_instance(time_series_output_ptr owning_output) const override;
    void release_instance(time_series_output_ptr item) const override;
    [[nodiscard]] bool has_reference() const override;
    [[nodiscard]] size_t memory_size() const override;
};

/**
 * CppTimeSeriesInputBuilder - Unified input builder using TimeSeriesTypeMeta
 *
 * This builder takes a TimeSeriesTypeMeta pointer and creates the appropriate
 * input time-series type based on the ts_kind field.
 */
struct HGRAPH_EXPORT CppTimeSeriesInputBuilder : InputBuilder {
    const TimeSeriesTypeMeta* ts_type_meta;

    explicit CppTimeSeriesInputBuilder(const TimeSeriesTypeMeta* meta)
        : ts_type_meta(meta) {}

    time_series_input_s_ptr make_instance(node_ptr owning_node) const override;
    time_series_input_s_ptr make_instance(time_series_input_ptr owning_input) const override;
    void release_instance(time_series_input_ptr item) const override;
    [[nodiscard]] bool has_reference() const override;
    [[nodiscard]] size_t memory_size() const override;
};

/**
 * Register CppTimeSeriesOutputBuilder with nanobind.
 * Called from OutputBuilder::register_with_nanobind.
 */
void cpp_time_series_output_builder_register_with_nanobind(nb::module_ &m);

/**
 * Register CppTimeSeriesInputBuilder with nanobind.
 * Called from InputBuilder::register_with_nanobind.
 */
void cpp_time_series_input_builder_register_with_nanobind(nb::module_ &m);

} // namespace hgraph

#endif // CPP_TIME_SERIES_BUILDER_H
