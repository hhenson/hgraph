//
// Created by Howard Henson on 26/12/2024.
//

#ifndef BUILDER_H
#define BUILDER_H

#include<hgraph/hgraph_base.h>
#include<hgraph/util/arena_enable_shared_from_this.h>

#include <typeinfo>
#include <cstddef>
#include <stdexcept>
#include <memory>
#include <utility>
#include <fmt/format.h>

namespace hgraph {
    /**
     * Helper function to construct an object either in-place (arena allocation) or on the heap.
     * This reduces duplication in make_instance methods.
     *
     * @tparam ConcreteType The concrete type to construct (e.g., TimeSeriesValueInput<T>)
     * @tparam BaseType The base type to cast to (e.g., TimeSeriesInput)
     * @tparam Args Constructor argument types
     * @param buffer Arena buffer (nullptr for heap allocation)
     * @param offset Pointer to current offset in buffer (updated on arena allocation)
     * @param type_name Name of the type (for error messages)
     * @param args Constructor arguments (perfect forwarded)
     * @return shared_ptr to BaseType
     */
    template<typename ConcreteType, typename BaseType, typename... Args>
    std::shared_ptr<BaseType> make_instance_impl(const std::shared_ptr<void> &buffer, size_t* offset, const char* type_name, Args&&... args) {
        if (buffer != nullptr && offset != nullptr) {
            // Align the offset for this object
            *offset = align_size(*offset, alignof(ConcreteType));
            // Arena allocation: construct in-place
            char* buf = static_cast<char*>(buffer.get());
            size_t obj_size = sizeof(ConcreteType);
            size_t aligned_obj_size = align_size(obj_size, alignof(size_t));
            // Set canary BEFORE construction
            if (arena_debug_mode) {
                size_t* canary_ptr = reinterpret_cast<size_t*>(buf + *offset + aligned_obj_size);
                *canary_ptr = ARENA_CANARY_PATTERN;
            }
            // Construct the object in arena memory
            ConcreteType* obj_ptr_raw = new (buf + *offset) ConcreteType(std::forward<Args>(args)...);
            // Check canary after construction
            verify_canary(obj_ptr_raw, sizeof(ConcreteType), type_name);
            *offset += add_canary_size(sizeof(ConcreteType));

            // Create shared_ptr with aliasing constructor (arena manages lifetime)
            auto sp = std::shared_ptr<ConcreteType>(buffer, obj_ptr_raw);

            // Initialize arena_enable_shared_from_this - handle inheritance chains
            // ConcreteType may inherit from arena_enable_shared_from_this<BaseType> (not ConcreteType)
            if constexpr (std::is_base_of_v<arena_enable_shared_from_this<BaseType>, ConcreteType>) {
                auto base_sp = std::static_pointer_cast<BaseType>(sp);
                _arena_init_weak_this(static_cast<arena_enable_shared_from_this<BaseType> *>(obj_ptr_raw), base_sp);
            } else if constexpr (std::is_base_of_v<arena_enable_shared_from_this<ConcreteType>, ConcreteType>) {
                _arena_init_weak_this(static_cast<arena_enable_shared_from_this<ConcreteType> *>(obj_ptr_raw), sp);
            }

            return std::static_pointer_cast<BaseType>(sp);
        } else {
            // Heap allocation - use std::make_shared then manually initialize arena_enable_shared_from_this
            auto sp = std::make_shared<ConcreteType>(std::forward<Args>(args)...);

            // Initialize arena_enable_shared_from_this - handle inheritance chains
            // ConcreteType may inherit from arena_enable_shared_from_this<BaseType> (not ConcreteType)
            if constexpr (std::is_base_of_v<arena_enable_shared_from_this<BaseType>, ConcreteType>) {
                auto base_sp = std::static_pointer_cast<BaseType>(sp);
                _arena_init_weak_this(static_cast<arena_enable_shared_from_this<BaseType> *>(sp.get()), base_sp);
            } else if constexpr (std::is_base_of_v<arena_enable_shared_from_this<ConcreteType>, ConcreteType>) {
                _arena_init_weak_this(static_cast<arena_enable_shared_from_this<ConcreteType> *>(sp.get()), sp);
            }

            return std::static_pointer_cast<BaseType>(sp);
        }
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

        /**
         * Get the alignment requirement of the type this builder constructs.
         * This is used by parent builders to correctly calculate memory layout
         * when multiple objects are allocated sequentially in an arena.
         */
        [[nodiscard]] virtual size_t type_alignment() const = 0;

        static void register_with_nanobind(nb::module_ &m);
    };
}
#endif //BUILDER_H
