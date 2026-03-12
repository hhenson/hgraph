#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/tracking.h>
#include <hgraph/types/time_series/value/associative.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/value/list.h>
#include <hgraph/types/time_series/value/record.h>
#include <hgraph/types/time_series/value/sequence.h>
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
         *
         * The tracking mode selects whether this value retains mutation deltas.
         * Ordinary value storage can use `Plain`, while time-series-facing
         * storage uses `Delta`.
         */
        explicit Value(const value::TypeMeta &schema, MutationTracking tracking = MutationTracking::Delta);
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
         * Return whether this value was built with plain or delta-aware
         * storage.
         */
        [[nodiscard]] MutationTracking tracking() const noexcept;
        /**
         * Return a mutable erased view over the stored payload.
         */
        [[nodiscard]] View view() noexcept;
        /**
         * Return a const erased view over the stored payload.
         */
        [[nodiscard]] View view() const noexcept;
        /**
         * Return a mutable atomic view over the stored payload.
         *
         * This is shorthand for `view().as_atomic()` when the caller already
         * owns a `Value` and wants the typed view directly.
         */
        [[nodiscard]] AtomicView atomic_view() noexcept;
        /**
         * Return a const atomic view over the stored payload.
         */
        [[nodiscard]] AtomicView atomic_view() const noexcept;
        /**
         * Return a mutable tuple-compatible view over the stored payload.
         */
        [[nodiscard]] TupleView tuple_view() noexcept;
        /**
         * Return a const tuple-compatible view over the stored payload.
         */
        [[nodiscard]] TupleView tuple_view() const noexcept;
        /**
         * Return a mutable bundle view over the stored payload.
         */
        [[nodiscard]] BundleView bundle_view() noexcept;
        /**
         * Return a const bundle view over the stored payload.
         */
        [[nodiscard]] BundleView bundle_view() const noexcept;
        /**
         * Return a mutable list view over the stored payload.
         */
        [[nodiscard]] ListView list_view() noexcept;
        /**
         * Return a const list view over the stored payload.
         */
        [[nodiscard]] ListView list_view() const noexcept;
        /**
         * Return a mutable set view over the stored payload.
         */
        [[nodiscard]] SetView set_view() noexcept;
        /**
         * Return a const set view over the stored payload.
         */
        [[nodiscard]] SetView set_view() const noexcept;
        /**
         * Return a mutable map view over the stored payload.
         */
        [[nodiscard]] MapView map_view() noexcept;
        /**
         * Return a const map view over the stored payload.
         */
        [[nodiscard]] MapView map_view() const noexcept;
        /**
         * Return a mutable cyclic buffer view over the stored payload.
         */
        [[nodiscard]] CyclicBufferView cyclic_buffer_view() noexcept;
        /**
         * Return a const cyclic buffer view over the stored payload.
         */
        [[nodiscard]] CyclicBufferView cyclic_buffer_view() const noexcept;
        /**
         * Return a mutable queue view over the stored payload.
         */
        [[nodiscard]] QueueView queue_view() noexcept;
        /**
         * Return a const queue view over the stored payload.
         */
        [[nodiscard]] QueueView queue_view() const noexcept;

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
