//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_WINDOW_OUTPUT_BUILDER_H
#define TIME_SERIES_WINDOW_OUTPUT_BUILDER_H

#include <hgraph/builders/output_builder.h>

namespace hgraph {
    // TimeSeriesWindow (TSW) output builder for fixed-size windows
    template<typename T>
    struct HGRAPH_EXPORT TimeSeriesWindowOutputBuilder_T : OutputBuilder {
        using ptr = nb::ref<TimeSeriesWindowOutputBuilder_T<T> >;
        size_t size;
        size_t min_size;

        TimeSeriesWindowOutputBuilder_T(size_t size, size_t min_size);

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_output_ptr item) const override;
    };

    // TimeSeriesWindow (TSW) output builder for timedelta-based windows
    template<typename T>
    struct HGRAPH_EXPORT TimeSeriesTimeWindowOutputBuilder_T : OutputBuilder {
        using ptr = nb::ref<TimeSeriesTimeWindowOutputBuilder_T<T> >;
        engine_time_delta_t size;
        engine_time_delta_t min_size;

        TimeSeriesTimeWindowOutputBuilder_T(engine_time_delta_t size, engine_time_delta_t min_size);

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_output_ptr item) const override;
    };

    void time_series_window_output_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_WINDOW_OUTPUT_BUILDER_H