//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_VALUE_INPUT_BUILDER_H
#define TIME_SERIES_VALUE_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>

namespace hgraph {
    template<typename T>
    struct HGRAPH_EXPORT TimeSeriesValueInputBuilder : InputBuilder {
        using ptr = nb::ref<TimeSeriesValueInputBuilder<T> >;
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
    };

    void time_series_value_input_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_VALUE_INPUT_BUILDER_H