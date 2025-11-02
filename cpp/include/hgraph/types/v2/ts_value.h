#pragma once

#include <memory>
#include <concepts>
#include "any_value.h"
#include "ts_event.h"
#include "hgraph/hgraph_forward_declarations.h"
#include "hgraph/util/date_time.h"
#include "hgraph/types/ts_traits.h"

namespace hgraph
{
    // Concept for parent node requirements
    template <typename T>
    concept ParentNode = std::derived_from<T, Notifiable> && std::derived_from<T, CurrentTimeProvider>;

    /**
     * @brief Base implementation class for type-erased time series value storage.
     *
     * This is the core implementation (PIMPL) that holds the actual shared state
     * between TSInput and TSOutput. It uses AnyValue for type-erased
     * storage, eliminating template proliferation.
     *
     * TSValue is the virtual interface. Two implementations exist:
     * - NonBoundImpl: For unbound inputs (returns defaults, tracks active as bool)
     * - SimplePeeredImpl: For bound inputs/outputs (shared state, subscriber management)
     *
     * Design principles:
     * - Single source of truth: All state lives here, shared via shared_ptr
     * - Computed properties: valid() and last_modified_time() derived from _last_event
     * - Swappable implementations: Virtual interface allows different state machine variants
     */
    struct TSValue
    {
        // Virtual interface for variant behavior
        virtual void       apply_event(const TsEventAny &event) = 0;
        virtual TsEventAny query_event(engine_time_t t) const = 0;
        virtual void       bind_to(TSValue *other) = 0;
        virtual void       unbind() = 0;

        // Subscriber management (for active state)
        virtual void mark_active(Notifiable *subscriber) = 0;
        virtual void mark_passive(Notifiable *subscriber) = 0;
        virtual bool active(Notifiable *subscriber) const = 0;

        // State queries
        virtual bool          modified(engine_time_t t) const = 0;
        virtual bool          all_valid() const = 0;
        virtual bool          valid() const = 0;
        virtual engine_time_t last_modified_time() const = 0;

        // Value access
        virtual const AnyValue<> &value() const = 0;

        // Type information
        virtual const std::type_info& value_type() const = 0;

        // Event generation
        virtual void mark_invalid(engine_time_t t) = 0;

        // Notification
        virtual void notify_subscribers(engine_time_t t) = 0;

        virtual ~TSValue() = default;
    };

} // namespace hgraph


namespace hgraph
{
    /**
     * @brief Type-erased time series output (event generator).
     *
     * Thin wrapper around TSValue that uses AnyValue for storage.
     * Multiple inputs can bind to the same output, sharing the impl.
     *
     * This is the internal implementation used by TimeSeriesValueOutput<T>.
     */
    struct TSOutput
    {
        using impl_ptr = std::shared_ptr<TSValue>;

        // Non-template constructor (implementation in .cpp)
        explicit TSOutput(Notifiable *parent, const std::type_info& value_type);

        // Move semantics
        TSOutput(TSOutput&&) = default;
        TSOutput& operator=(TSOutput&&) = default;

        // Delete copy operations
        TSOutput(const TSOutput&) = delete;
        TSOutput& operator=(const TSOutput&) = delete;

        // Value access (returns AnyValue)
        [[nodiscard]] const AnyValue<> &value() const;

        // Set value with AnyValue
        void set_value(const AnyValue<> &v);

        // Set value with AnyValue move
        void set_value(AnyValue<> &&v);

        // Invalidate the value
        void invalidate();

        // Delegate to impl
        [[nodiscard]] bool          modified() const;
        [[nodiscard]] bool          valid() const;
        [[nodiscard]] engine_time_t last_modified_time() const;

        [[nodiscard]] TsEventAny delta_value() const;

        // Access to impl for binding
        [[nodiscard]] impl_ptr get_impl() const;

        // Current time accessor (delegates to parent)
        [[nodiscard]] engine_time_t current_time() const;

    private:
        impl_ptr    _impl;   // Shared with bound inputs
        Notifiable *_parent; // Owning node (implements both Notifiable and CurrentTimeProvider)
    };

    /**
     * @brief Type-erased time series input (event consumer).
     *
     * Thin wrapper that binds to a TSOutput by sharing its impl.
     * Provides read-only access to the value. Implements Notifiable to receive
     * notifications when the bound output changes.
     *
     * This is the internal implementation used by TimeSeriesValueInput<T>.
     */
    struct TSInput
    {
        using impl_ptr = std::shared_ptr<TSValue>;

        // Non-template constructor (implementation in .cpp)
        explicit TSInput(Notifiable *parent, const std::type_info& value_type);

        // Move semantics
        TSInput(TSInput&&) = default;
        TSInput& operator=(TSInput&&) = default;

        // Delete copy operations
        TSInput(const TSInput&) = delete;
        TSInput& operator=(const TSInput&) = delete;

        // Bind to output (shares impl) - implementation in .cpp
        void bind_output(TSOutput *output);

        // Value access (returns AnyValue)
        [[nodiscard]] const AnyValue<> &value() const;

        // Queries delegate to shared impl
        [[nodiscard]] bool modified() const;

        [[nodiscard]] bool valid() const;

        [[nodiscard]] engine_time_t last_modified_time() const;

        [[nodiscard]] TsEventAny delta_value() const;

        // Active state (computed from subscription)
        [[nodiscard]] bool active() const;

        // Mark input as active (adds to subscriber set)
        void mark_active();

        // Mark input as passive (removes from subscriber set)
        void mark_passive();

        // Notifiable interface (would be implemented if this inherited from Notifiable)
        void notify(engine_time_t t) const;

        // Current time accessor (delegates to parent)
        [[nodiscard]] engine_time_t current_time() const;

    private:
        impl_ptr    _impl;   // Shared impl
        Notifiable *_parent; // Owning node (implements both Notifiable and CurrentTimeProvider)
    };

    // Factory functions for template convenience
    template <ParentNode P, typename T>
    TSOutput make_ts_output(P *parent, const std::type_info& value_type = typeid(T)) {
        return TSOutput(static_cast<Notifiable*>(parent), value_type);
    }

    template <ParentNode P, typename T>
    TSInput make_ts_input(P *parent, const std::type_info& value_type = typeid(T)) {
        return TSInput(static_cast<Notifiable*>(parent), value_type);
    }

} // namespace hgraph
