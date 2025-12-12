//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_SET_INPUT_BUILDER_H
#define TIME_SERIES_SET_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>
#include <typeinfo>

namespace hgraph {
    /**
     * Non-templated builder for TimeSeriesSetInput.
     * Takes element type_info at construction time.
     */
    struct HGRAPH_EXPORT TimeSeriesSetInputBuilder : InputBuilder {
        explicit TimeSeriesSetInputBuilder(const std::type_info& element_type);

        time_series_input_s_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_s_ptr make_instance(time_series_input_ptr owning_input) const override;
        void release_instance(time_series_input_ptr item) const override;
        [[nodiscard]] size_t memory_size() const override;

    private:
        const std::type_info& _element_type;
    };

    void time_series_set_input_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_SET_INPUT_BUILDER_H
