//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_WINDOW_INPUT_BUILDER_H
#define TIME_SERIES_WINDOW_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>

namespace hgraph {
    // Unified window input builder - creates unified input that works with both window types
    template<typename T>
    struct HGRAPH_EXPORT TimeSeriesWindowInputBuilder_T : InputBuilder {
        using ptr = nb::ref<TimeSeriesWindowInputBuilder_T<T> >;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override {
            return dynamic_cast<const TimeSeriesWindowInputBuilder_T<T> *>(&other) != nullptr;
        }
    };

    void time_series_window_input_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_WINDOW_INPUT_BUILDER_H