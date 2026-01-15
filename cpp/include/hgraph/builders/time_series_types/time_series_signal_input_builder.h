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

        [[nodiscard]] size_t memory_size() const override;

        [[nodiscard]] size_t type_alignment() const override;

        static void register_with_nanobind(nb::module_ &m);
    };
} // namespace hgraph

#endif  // TIME_SERIES_SIGNAL_INPUT_BUILDER_H