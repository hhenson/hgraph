//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_SET_OUTPUT_BUILDER_H
#define TIME_SERIES_SET_OUTPUT_BUILDER_H

#include <hgraph/builders/output_builder.h>
#include <typeinfo>

namespace hgraph {
    /**
     * Non-templated builder for TimeSeriesSetOutput.
     * Takes element type_info at construction time.
     */
    struct HGRAPH_EXPORT TimeSeriesSetOutputBuilder : OutputBuilder {
        explicit TimeSeriesSetOutputBuilder(const std::type_info& element_type);

        time_series_output_s_ptr make_instance(node_ptr owning_node) const override;
        time_series_output_s_ptr make_instance(time_series_output_ptr owning_output) const override;
        void release_instance(time_series_output_ptr item) const override;
        [[nodiscard]] size_t memory_size() const override;

    private:
        const std::type_info& _element_type;
    };

    void time_series_set_output_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_SET_OUTPUT_BUILDER_H
