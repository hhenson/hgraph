#ifndef OUTPUT_BUILDER_H
#define OUTPUT_BUILDER_H

#include <hgraph/builders/builder.h>
#include <ranges>

namespace hgraph {
    struct HGRAPH_EXPORT OutputBuilder : Builder {
        using ptr = nb::ref<OutputBuilder>;
        using Builder::Builder;

        /**
         * Create an instance of OutputBuilder using an owning node.
         * If buffer is provided, uses arena allocation (in-place construction).
         * Otherwise, uses heap allocation (legacy path).
         */
        virtual time_series_output_ptr make_instance(node_ptr owning_node, std::shared_ptr<void> buffer = nullptr, size_t* offset = nullptr) const = 0;

        /**
         * Create an instance of OutputBuilder using an parent output.
         * If buffer is provided, uses arena allocation (in-place construction).
         * Otherwise, uses heap allocation (legacy path).
         */
        virtual time_series_output_ptr make_instance(time_series_output_ptr owning_output, std::shared_ptr<void> buffer = nullptr, size_t* offset = nullptr) const = 0;

        virtual void release_instance(time_series_output_ptr item) const;

        virtual bool has_reference() const { return false; }

        static void register_with_nanobind(nb::module_ &m);
    };
} // namespace hgraph

#endif  // OUTPUT_BUILDER_H