#ifndef OUTPUT_BUILDER_H
#define OUTPUT_BUILDER_H

#include <hgraph/builders/builder.h>
#include <ranges>

namespace hgraph {

    // Forward declaration
    struct TSMeta;
    struct HGRAPH_EXPORT OutputBuilder : Builder {
        using ptr = nb::ref<OutputBuilder>;
        using Builder::Builder;

        virtual time_series_output_s_ptr make_instance(node_ptr owning_node) const = 0;

        virtual time_series_output_s_ptr make_instance(time_series_output_ptr owning_output) const = 0;

        virtual void release_instance(time_series_output_ptr item) const;

        virtual bool has_reference() const { return false; }

        /**
         * Get the TSMeta schema for this output, if available.
         * Default returns nullptr; concrete builders override if they have TSMeta.
         */
        virtual const TSMeta* ts_meta() const { return nullptr; }

        static void register_with_nanobind(nb::module_ &m);
    };
} // namespace hgraph

#endif  // OUTPUT_BUILDER_H