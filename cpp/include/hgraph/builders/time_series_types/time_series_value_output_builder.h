//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_VALUE_OUTPUT_BUILDER_H
#define TIME_SERIES_VALUE_OUTPUT_BUILDER_H

#include <hgraph/builders/output_builder.h>

namespace hgraph {
    template<typename T>
    struct HGRAPH_EXPORT TimeSeriesValueOutputBuilder : OutputBuilder {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        void release_instance(time_series_output_ptr item) const override;
    };

    void time_series_value_output_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_VALUE_OUTPUT_BUILDER_H