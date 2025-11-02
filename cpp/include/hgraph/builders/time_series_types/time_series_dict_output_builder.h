//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_DICT_OUTPUT_BUILDER_H
#define TIME_SERIES_DICT_OUTPUT_BUILDER_H

#include <hgraph/builders/output_builder.h>

namespace hgraph {
    struct HGRAPH_EXPORT TimeSeriesDictOutputBuilder : OutputBuilder {
        output_builder_ptr ts_builder;
        output_builder_ptr ts_ref_builder;

        TimeSeriesDictOutputBuilder(output_builder_ptr ts_builder, output_builder_ptr ts_ref_builder);

        bool has_reference() const override { return ts_builder->has_reference(); }
    };

    template<typename T>
    struct HGRAPH_EXPORT TimeSeriesDictOutputBuilder_T : TimeSeriesDictOutputBuilder {
        using TimeSeriesDictOutputBuilder::TimeSeriesDictOutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_output_ptr item) const override;
    };

    void time_series_dict_output_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_DICT_OUTPUT_BUILDER_H