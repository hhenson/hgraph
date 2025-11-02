//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_SIGNAL_INPUT_BUILDER_H
#define TIME_SERIES_SIGNAL_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>

namespace hgraph {
    struct TimeSeriesSignalInput;

    struct HGRAPH_EXPORT TimeSeriesSignalInputBuilder : InputBuilder {
        using ptr = nb::ref<TimeSeriesSignalInputBuilder>;
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        void release_instance(time_series_input_ptr item) const override;

        void release_instance(TimeSeriesSignalInput *item) const;

        static void register_with_nanobind(nb::module_ &m);
    };
} // namespace hgraph

#endif  // TIME_SERIES_SIGNAL_INPUT_BUILDER_H