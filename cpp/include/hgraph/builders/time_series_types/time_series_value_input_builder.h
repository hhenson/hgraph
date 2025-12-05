//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_VALUE_INPUT_BUILDER_H
#define TIME_SERIES_VALUE_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>
#include <typeinfo>

namespace hgraph {
    /**
     * Non-templated builder for TimeSeriesValueInput.
     * Takes type_info at construction time.
     */
    struct HGRAPH_EXPORT TimeSeriesValueInputBuilder : InputBuilder {
        explicit TimeSeriesValueInputBuilder(const std::type_info& value_type);

        time_series_input_s_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_s_ptr make_instance(time_series_input_ptr owning_input) const override;
        [[nodiscard]] size_t memory_size() const override;

    private:
        const std::type_info& _value_type;
    };

    void time_series_value_input_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_VALUE_INPUT_BUILDER_H
