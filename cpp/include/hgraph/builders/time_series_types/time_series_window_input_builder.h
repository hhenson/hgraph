//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_WINDOW_INPUT_BUILDER_H
#define TIME_SERIES_WINDOW_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>
#include <hgraph/types/value/type_meta.h>

namespace hgraph {
    /**
     * @brief Non-templated builder for TimeSeriesWindowInput.
     *
     * Takes a TypeMeta* for the element type at construction time so the
     * element type is available before the input binds to an output.
     */
    struct HGRAPH_EXPORT TimeSeriesWindowInputBuilder : InputBuilder {
        using ptr = nb::ref<TimeSeriesWindowInputBuilder>;

        explicit TimeSeriesWindowInputBuilder(const value::TypeMeta* element_type = nullptr);

        time_series_input_s_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_s_ptr make_instance(time_series_input_ptr owning_input) const override;

        [[nodiscard]] size_t memory_size() const override;
        [[nodiscard]] size_t type_alignment() const override;

    private:
        const value::TypeMeta* _element_type{nullptr};
    };

    void time_series_window_input_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_WINDOW_INPUT_BUILDER_H
