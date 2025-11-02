//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_LIST_OUTPUT_BUILDER_H
#define TIME_SERIES_LIST_OUTPUT_BUILDER_H

#include <hgraph/builders/output_builder.h>

namespace hgraph {
    struct HGRAPH_EXPORT TimeSeriesListOutputBuilder : OutputBuilder {
        using ptr = nb::ref<TimeSeriesListOutputBuilder>;

        TimeSeriesListOutputBuilder(OutputBuilder::ptr output_builder, size_t size);

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        bool has_reference() const override { return output_builder->has_reference(); }

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_output_ptr item) const override;

        static void register_with_nanobind(nb::module_ &m);

    private:
        time_series_output_ptr make_and_set_outputs(TimeSeriesListOutput *output) const;

        OutputBuilder::ptr output_builder;
        size_t size;
    };
} // namespace hgraph

#endif  // TIME_SERIES_LIST_OUTPUT_BUILDER_H