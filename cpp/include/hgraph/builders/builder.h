//
// Created by Howard Henson on 26/12/2024.
//

#ifndef BUILDER_H
#define BUILDER_H

#include<hgraph/hgraph_base.h>

#include <typeinfo>
#include <cstddef>

namespace hgraph {
    /**
     * Helper function to calculate aligned size.
     * Rounds up the current size to the next alignment boundary.
     */
    inline size_t align_size(size_t current_size, size_t alignment) {
        if (alignment == 0) return current_size;
        size_t remainder = current_size % alignment;
        return remainder == 0 ? current_size : current_size + (alignment - remainder);
    }

    /**
     * Helper function to calculate size with alignment for a type.
     * Returns the current size rounded up to alignof(T), then adds sizeof(T).
     */
    template<typename T>
    inline size_t add_aligned_size(size_t current_size) {
        size_t aligned = align_size(current_size, alignof(T));
        return aligned + sizeof(T);
    }
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

        /**
         * Calculate the memory size required to allocate the object(s) this builder constructs.
         * For leaf builders, this is a simple sizeof calculation.
         * For complex builders with nested builders, this is a recursive computation.
         */
        [[nodiscard]] virtual size_t memory_size() const = 0;

        static void register_with_nanobind(nb::module_ &m);
    };
}
#endif //BUILDER_H