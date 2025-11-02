#ifndef OUTPUT_BUILDER_H
#define OUTPUT_BUILDER_H

#include <hgraph/builders/builder.h>
#include <ranges>

namespace hgraph {
    struct HGRAPH_EXPORT OutputBuilder : Builder {
        using ptr = nb::ref<OutputBuilder>;
        using Builder::Builder;

        virtual time_series_output_ptr make_instance(node_ptr owning_node) const = 0;

        virtual time_series_output_ptr make_instance(time_series_output_ptr owning_output) const = 0;

        virtual void release_instance(time_series_output_ptr item) const;

        virtual bool has_reference() const { return false; }

        static void register_with_nanobind(nb::module_ &m);
    };
} // namespace hgraph

#endif  // OUTPUT_BUILDER_H