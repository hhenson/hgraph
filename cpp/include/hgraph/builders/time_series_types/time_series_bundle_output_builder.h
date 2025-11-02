//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_BUNDLE_OUTPUT_BUILDER_H
#define TIME_SERIES_BUNDLE_OUTPUT_BUILDER_H

#include <hgraph/builders/output_builder.h>
#include <vector>

namespace hgraph {
    struct HGRAPH_EXPORT TimeSeriesBundleOutputBuilder : OutputBuilder {
        TimeSeriesBundleOutputBuilder(time_series_schema_ptr schema, std::vector<OutputBuilder::ptr> output_builders);

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        bool has_reference() const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_output_ptr item) const override;

        static void register_with_nanobind(nb::module_ &m);

    private:
        time_series_output_ptr make_and_set_outputs(TimeSeriesBundleOutput *output) const;

        time_series_schema_ptr schema;
        std::vector<OutputBuilder::ptr> output_builders;
    };
} // namespace hgraph

#endif  // TIME_SERIES_BUNDLE_OUTPUT_BUILDER_H