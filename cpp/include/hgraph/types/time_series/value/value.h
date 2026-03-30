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
         * Construct a schema-less empty value.
         *
         * This exists only as a storage placeholder for runtime types that
         * later bind a concrete schema before use.
         */
        Value() noexcept = default;
        /**
         * Construct a schema-bound value from a schema pointer.
         *
         * This compatibility overload keeps older call sites readable while
         * still requiring a concrete schema to exist.
         */
        explicit Value(const value::TypeMeta *schema, MutationTracking tracking = MutationTracking::Plain);
        /**
         * Bind this value to the supplied schema and default-construct storage
         * for that schema.
         *
         * The tracking mode selects whether this value retains mutation deltas.
         * Plain tracking is the default for ordinary value storage so callers
         * do not pay delta costs unless they ask for them explicitly.
         * Time-series-facing storage opts into `Delta` deliberately.
         */
        explicit Value(const value::TypeMeta &schema, MutationTracking tracking = MutationTracking::Plain);
        /**
         * Construct a schema-bound value by copying the payload represented by
         * an existing view.
         */
        explicit Value(const View &view, MutationTracking tracking = MutationTracking::Plain);
        /**
         * Construct an atomic value directly from a native C++ object.
         *
         * This keeps transient scalar key/value creation compact at runtime.
         */
        template <typename T>
            requires(!std::same_as<std::remove_cvref_t<T>, Value> && !std::derived_from<std::remove_cvref_t<T>, View>)
        explicit Value(T &&value)
            : Value(*value::scalar_type_meta<std::remove_cvref_t<T>>())
        {
            view().set(std::forward<T>(value));
        }
        /**
         * Copy the stored payload while preserving the schema selected by the
         * builder.
         */
        Value(const Value &other);
        /**
         * Copy the supplied value into a new owning instance.
         *
         * This compatibility helper keeps older runtime code readable while
         * still going through the ordinary copy-construction path.
         */
        [[nodiscard]] static Value copy(const Value &other) { return Value(other); }
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
         * The value layer follows optional-style presence semantics. A value
         * either has storage or it does not; there is no separate "valid"
         * state in this API.
         */
        [[nodiscard]] bool has_value() const noexcept;
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
        [[nodiscard]] AtomicView atomic_view();
        /**
         * Return a const atomic view over the stored payload.
         */
        [[nodiscard]] AtomicView atomic_view() const;
        /**
         * Return a mutable tuple-compatible view over the stored payload.
         */
        [[nodiscard]] TupleView tuple_view();
        /**
         * Return a const tuple-compatible view over the stored payload.
         */
        [[nodiscard]] TupleView tuple_view() const;
        /**
         * Return a mutable bundle view over the stored payload.
         */
        [[nodiscard]] BundleView bundle_view();
        /**
         * Return a const bundle view over the stored payload.
         */
        [[nodiscard]] BundleView bundle_view() const;
        /**
         * Return a mutable list view over the stored payload.
         */
        [[nodiscard]] ListView list_view();
        /**
         * Return a const list view over the stored payload.
         */
        [[nodiscard]] ListView list_view() const;
        /**
         * Return a mutable set view over the stored payload.
         */
        [[nodiscard]] SetView set_view();
        /**
         * Return a const set view over the stored payload.
         */
        [[nodiscard]] SetView set_view() const;
        /**
         * Return a mutable map view over the stored payload.
         */
        [[nodiscard]] MapView map_view();
        /**
         * Return a const map view over the stored payload.
         */
        [[nodiscard]] MapView map_view() const;
        /**
         * Return a mutable cyclic buffer view over the stored payload.
         */
        [[nodiscard]] CyclicBufferView cyclic_buffer_view();
        /**
         * Return a const cyclic buffer view over the stored payload.
         */
        [[nodiscard]] CyclicBufferView cyclic_buffer_view() const;
        /**
         * Return a mutable queue view over the stored payload.
         */
        [[nodiscard]] QueueView queue_view();
        /**
         * Return a const queue view over the stored payload.
         */
        [[nodiscard]] QueueView queue_view() const;
        /**
         * Compare two owning values for equality.
         */
        [[nodiscard]] bool equals(const Value &other) const;
        /**
         * Compare this owning value to a non-owning view.
         */
        [[nodiscard]] bool equals(const View &other) const;
        /**
         * Return the hash of the current payload.
         */
        [[nodiscard]] size_t hash() const;
        /**
         * Return the string form of the current payload.
         */
        [[nodiscard]] std::string to_string() const;
        /**
         * Convert the current payload to a Python object.
         */
        [[nodiscard]] nb::object to_python() const;
        /**
         * Replace the current payload from a Python object.
         *
         * Passing `None` clears heap-backed storage and reconstructs a default
         * payload so the value remains schema-bound and settable.
         */
        void from_python(const nb::object &src);
        /**
         * Reset this value to the default payload for its schema.
         *
         * The name follows the standard C++ `reset()` convention: after the
         * call, the value remains schema-bound but its payload is restored to
         * the default state for that schema.
         */
        void reset();

      private:
        friend struct ValueBuilder;

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
        void clear_storage() noexcept;
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

    inline Value View::clone() const
    {
        return Value{*this};
    }

    template <typename T> inline void View::set(T &&value)
    {
        if (!has_value()) { throw std::runtime_error("View::set(T) on empty view"); }

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
