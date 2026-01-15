#ifndef OUTPUT_BUILDER_H
#define OUTPUT_BUILDER_H

#include <hgraph/builders/builder.h>

namespace hgraph {

    /**
     * @brief Abstract base class for building time-series outputs.
     *
     * NOTE: The TSMeta-based API via CppTimeSeriesOutputBuilder is the preferred approach.
     * The legacy make_instance methods are kept as stubs for compatibility during migration.
     */
    struct HGRAPH_EXPORT OutputBuilder : Builder {
        using ptr = nb::ref<OutputBuilder>;
        using Builder::Builder;

        /**
         * @brief Check if this builder creates REF types.
         */
        virtual bool has_reference() const { return false; }

        // Legacy stub methods - throw at runtime, kept for compilation compatibility
        virtual time_series_output_s_ptr make_instance(node_ptr owning_node);
        virtual time_series_output_s_ptr make_instance(time_series_type_ptr owning_ts);
        virtual void release_instance(time_series_output_s_ptr output) const;

        static void register_with_nanobind(nb::module_ &m);
    };

} // namespace hgraph

#endif  // OUTPUT_BUILDER_H