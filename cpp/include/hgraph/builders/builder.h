//
// Created by Howard Henson on 26/12/2024.
//

#ifndef BUILDER_H
#define BUILDER_H

#include<hgraph/hgraph_base.h>

#include <typeinfo>

namespace hgraph {
    /**
     * The Builder class is responsible for constructing and initializing
     * the item type it is responsible for. It is also responsible for
     * destroying and cleaning up the resources associated with the item.
     * These can be thought of as life-cycle methods.
     *
     * This provides a guide to prepare the different builders, the actual implementations
     * will vary in terms of the make_instance parameters at least.
     */
    struct Builder : nb::intrusive_base {
        Builder() = default;

        ~Builder() override = default;

        /**
         * Create a new instance of the item.
         * Any additional attributes required for construction are passed in as arguments.
         * Actual instance of the builder will fix these arguments for all instances
         * of the builder for the type.
         */
        //virtual ITEM make_instance(/* Add parameters if needed */) = 0;

        /**
         * Release the item and its resources.
         */
        //virtual void release_instance(ITEM item) = 0;

        /**
         * Compare this builder with another to determine if they build the same nested structure/type.
         * Default implementation compares concrete builder types.
         */
        [[nodiscard]] virtual bool is_same_type(const Builder &other) const { return typeid(*this) == typeid(other); }

        static void register_with_nanobind(nb::module_ &m);
    };
}
#endif //BUILDER_H