//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_SET_OUTPUT_BUILDER_H
#define TIME_SERIES_SET_OUTPUT_BUILDER_H

#include <hgraph/builders/output_builder.h>
#include <hgraph/types/value/type_meta.h>

namespace hgraph {

    /**
     * @brief Non-templated builder for TimeSeriesSetOutput.
     *
     * Takes a TypeMeta* for the element type at construction time and creates
     * TimeSeriesSetOutput instances with that element type.
     */
    struct HGRAPH_EXPORT TimeSeriesSetOutputBuilder : OutputBuilder {
        explicit TimeSeriesSetOutputBuilder(const value::TypeMeta* element_type);

        time_series_output_s_ptr make_instance(node_ptr owning_node) const override;
        time_series_output_s_ptr make_instance(time_series_output_ptr owning_output) const override;
        void release_instance(time_series_output_ptr item) const override;
        [[nodiscard]] size_t memory_size() const override;
        [[nodiscard]] size_t type_alignment() const override;

    private:
        const value::TypeMeta* _element_type;
    };

    void time_series_set_output_builder_register_with_nanobind(nb::module_& m);

} // namespace hgraph

#endif  // TIME_SERIES_SET_OUTPUT_BUILDER_H
