#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/value/list.h>

#include <array>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace hgraph
{

    struct ValueBuilder;

    /**
     * Owning schema-bound value.
     *
     * `Value` owns storage and delegates behavior to the schema-resolved
     * dispatch cached on its builder. The storage itself is plain data.
     *
     * Validity follows the storage model rather than being tracked separately:
     * - values stored inline in the handle are always live
     * - heap-backed values are live when the owned heap pointer is non-null
     *
     * This keeps the representation simple and matches the intended meaning of
     * invalidity in this layer: only storage-owning heap representations become
     * empty after ownership transfer. Inline representations remain valid after
     * move, with the usual moved-from payload semantics of C++ objects.
     */
    struct Value
    {
        /**
         * Bind this value to the supplied schema and default-construct storage
         * for that schema.
         */
        explicit Value(const value::TypeMeta &schema);
        /**
         * Copy the stored payload while preserving the schema selected by the
         * builder.
         */
        Value(const Value &other);
        /**
         * Move the stored payload while preserving the schema selected by the
         * builder.
         *
         * Inline-storage values remain live after move. Heap-backed values
         * transfer their owned storage and leave the source storage-empty.
         */
        Value(Value &&other) noexcept;
        /**
         * Replace the payload from another value with the same builder.
         *
         * Assignment does not permit schema rebinding. Matching builder identity
         * is the contract that guarantees the same layout and behavior.
         */
        Value &operator=(const Value &other);
        /**
         * Replace the payload by moving from another value with the same
         * builder.
         */
        Value &operator=(Value &&other);
        /**
         * Destroy any live payload owned by this value.
         */
        ~Value();

        /**
         * Return whether this value currently owns live storage.
         *
         * Inline-storage values are always live. Heap-backed values are live
         * when their owned pointer is non-null.
         */
        [[nodiscard]] bool valid() const noexcept;
        explicit operator bool() const noexcept;
        /**
         * Return the schema bound to this value.
         *
         * The schema remains available even when a heap-backed value is storage
         * empty because the builder reference is always retained.
         */
        [[nodiscard]] const value::TypeMeta *schema() const noexcept;
        /**
         * Return a mutable erased view over the stored payload.
         */
        [[nodiscard]] View view() noexcept;
        /**
         * Return a const erased view over the stored payload.
         */
        [[nodiscard]] View view() const noexcept;

      private:
        union Storage
        {
            void                                     *heap_memory;
            alignas(void *) std::array<std::byte, sizeof(void *)> inline_storage;

            Storage() noexcept : heap_memory(nullptr) {}
        };

        /**
         * Allocate storage when required and default-construct the schema-bound
         * payload into that storage.
         */
        void allocate_and_construct();
        /**
         * Destroy any live payload and release owned heap storage.
         *
         * This leaves heap-backed values storage-empty. Inline-storage values
         * do not have an externally observable invalid state.
         */
        void reset() noexcept;
        /**
         * Return the raw memory block that holds the payload.
         *
         * Depending on the builder, this may be the inline handle storage or a
         * separately allocated block.
         */
        [[nodiscard]] void *storage_memory() noexcept;
        /**
         * Return the raw memory block that holds the payload.
         */
        [[nodiscard]] const void *storage_memory() const noexcept;
        /**
         * Return the schema-bound builder.
         */
        [[nodiscard]] const ValueBuilder &builder() const noexcept;

        /**
         * Schema-bound builder describing storage layout and behavior dispatch.
         */
        const ValueBuilder *m_builder{nullptr};
        Storage             m_storage;
    };

    template <typename T> inline void View::set(T &&value)
    {
        if (!valid()) { throw std::runtime_error("View::set(T) on invalid view"); }

        using TValue = std::remove_cvref_t<T>;
        if constexpr (std::is_lvalue_reference_v<T &&>) {
            dispatch()->set_from_cpp(data(), std::addressof(value), value::scalar_type_meta<TValue>());
        } else {
            TValue moved_value = std::forward<T>(value);
            dispatch()->move_from_cpp(data(), std::addressof(moved_value), value::scalar_type_meta<TValue>());
        }
    }

    template <typename T>
    [[nodiscard]] Value value_for(T &&value)
    {
        using TValue = std::remove_cvref_t<T>;

        Value out{*value::scalar_type_meta<TValue>()};
        out.view().as_atomic().set(std::forward<T>(value));
        return out;
    }

    template <typename T>
    [[nodiscard]] Value value_for(const value::TypeMeta &schema, T &&value)
    {
        Value out{schema};
        out.view().set(std::forward<T>(value));
        return out;
    }

}  // namespace hgraph
