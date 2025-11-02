//
// Created by Howard Henson on 27/12/2024.
//

#ifndef INPUT_BUILDER_H
#define INPUT_BUILDER_H

#include <hgraph/builders/builder.h>

namespace hgraph {
    // The InputBuilder class implementation

    struct InputBuilder : Builder {
        using ptr = nb::ref<InputBuilder>;

        /**
         * Create an instance of InputBuilder using an owning node
         */
        virtual time_series_input_ptr make_instance(node_ptr owning_node) const = 0;

        /**
         * Create an instance of InputBuilder using an parent input
         */
        virtual time_series_input_ptr make_instance(time_series_input_ptr owning_input) const = 0;

        /**
         * Release an instance of the input type.
         * By default, do nothing.
         */
        virtual void release_instance(time_series_input_ptr item) const;

        virtual bool has_reference() const { return false; }

        static void register_with_nanobind(nb::module_ &m);
    };
} // namespace hgraph

#endif  // INPUT_BUILDER_H