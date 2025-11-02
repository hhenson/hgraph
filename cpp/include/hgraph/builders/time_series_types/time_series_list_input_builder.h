//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_LIST_INPUT_BUILDER_H
#define TIME_SERIES_LIST_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>
#include <vector>

namespace hgraph {
    struct HGRAPH_EXPORT TimeSeriesListInputBuilder : InputBuilder {
        using ptr = nb::ref<TimeSeriesListInputBuilder>;

        TimeSeriesListInputBuilder(InputBuilder::ptr input_builder, size_t size);

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        bool has_reference() const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_input_ptr item) const override;

        static void register_with_nanobind(nb::module_ &m);

    private:
        time_series_input_ptr make_and_set_inputs(TimeSeriesListInput *input) const;

        InputBuilder::ptr input_builder;
        size_t size;
    };
} // namespace hgraph

#endif  // TIME_SERIES_LIST_INPUT_BUILDER_H