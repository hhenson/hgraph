//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_REF_OUTPUT_BUILDER_H
#define TIME_SERIES_REF_OUTPUT_BUILDER_H

#include <hgraph/builders/output_builder.h>

namespace hgraph {
    struct HGRAPH_EXPORT TimeSeriesRefOutputBuilder : OutputBuilder {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        bool has_reference() const override { return true; }

        static void register_with_nanobind(nb::module_ &m);
    };
} // namespace hgraph

#endif  // TIME_SERIES_REF_OUTPUT_BUILDER_H