//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_VALUE_INPUT_BUILDER_H
#define TIME_SERIES_VALUE_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>
#include <hgraph/types/value/type_meta.h>

namespace hgraph {

    /**
     * @brief Non-templated builder for TimeSeriesValueInput.
     *
     * Creates TimeSeriesValueInput instances. The schema is not needed
     * for input since it delegates to its bound output.
     */
    struct HGRAPH_EXPORT TimeSeriesValueInputBuilder : InputBuilder {
        using ptr = nb::ref<TimeSeriesValueInputBuilder>;
        using InputBuilder::InputBuilder;

        time_series_input_s_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_s_ptr make_instance(time_series_input_ptr owning_input) const override;
        [[nodiscard]] size_t memory_size() const override;

        [[nodiscard]] size_t type_alignment() const override;
    };

    void time_series_value_input_builder_register_with_nanobind(nb::module_& m);

} // namespace hgraph

#endif  // TIME_SERIES_VALUE_INPUT_BUILDER_H
