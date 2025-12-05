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
    // Global debug flag for arena allocation debugging
    // When enabled, adds a canary value at the end of each object to detect buffer overruns
    extern bool arena_debug_mode;

    // Canary pattern value - a distinctive pattern that's unlikely to occur naturally
    constexpr size_t ARENA_CANARY_PATTERN = 0xDEADBEEFCAFEBABEULL;

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
     * If debug mode is enabled, also adds space for a canary value.
     */
    template<typename T>
    inline size_t add_aligned_size(size_t current_size) {
        size_t aligned = align_size(current_size, alignof(T));
        size_t total = aligned + sizeof(T);
        // Add canary size if debug mode is enabled
        if (arena_debug_mode) {
            total = align_size(total, alignof(size_t));
            total += sizeof(size_t);
        }
        return total;
    }

    /**
     * Get the size of the canary padding (0 if debug mode is off, sizeof(size_t) if on).
     */
    inline size_t get_canary_size() {
        return arena_debug_mode ? sizeof(size_t) : 0;
    }

    /**
     * Add canary size to a base size, with proper alignment.
     * Use this for simple sizeof() calculations that need canary support.
     */
    inline size_t add_canary_size(size_t base_size) {
        if (!arena_debug_mode) {
            return base_size;
        }
        // Align to size_t boundary, then add canary
        return align_size(base_size, alignof(size_t)) + sizeof(size_t);
    }

    /**
     * Set the canary value at the end of an allocated object.
     * @param ptr Pointer to the start of the object
     * @param object_size Size of the object (without canary)
     * @return Pointer to the object (for chaining)
     */
    inline void* set_canary(void* ptr, size_t object_size) {
        if (arena_debug_mode && ptr != nullptr) {
            size_t aligned_size = align_size(object_size, alignof(size_t));
            size_t* canary_ptr = reinterpret_cast<size_t*>(static_cast<char*>(ptr) + aligned_size);
            *canary_ptr = ARENA_CANARY_PATTERN;
        }
        return ptr;
    }

    /**
     * Check the canary value at the end of an allocated object.
     * @param ptr Pointer to the start of the object
     * @param object_size Size of the object (without canary)
     * @return True if canary is intact, false if it was overwritten
     */
    inline bool check_canary(const void* ptr, size_t object_size) {
        if (!arena_debug_mode || ptr == nullptr) {
            return true;  // No canary to check
        }
        size_t aligned_size = align_size(object_size, alignof(size_t));
        const size_t* canary_ptr = reinterpret_cast<const size_t*>(static_cast<const char*>(ptr) + aligned_size);
        return *canary_ptr == ARENA_CANARY_PATTERN;
    }

    /**
     * Verify canary and throw exception if it's been overwritten.
     * @param ptr Pointer to the start of the object
     * @param object_size Size of the object (without canary)
     * @param object_name Name of the object type for error message
     */
    inline void verify_canary(const void* ptr, size_t object_size, const char* object_name = "object") {
        if (!check_canary(ptr, object_size)) {
            throw std::runtime_error(
                fmt::format("Arena allocation buffer overrun detected for {} at address {:p}. "
                           "Canary value was overwritten, indicating memory corruption.",
                           object_name, ptr));
        }
    }

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

        static void register_with_nanobind(nb::module_ &m);
    };
}
#endif //BUILDER_H
