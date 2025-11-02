#pragma once

#include <memory>
#include <concepts>
#include "ts_value_impl.h"
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
     * @brief Type-erased time series output (event generator).
     *
     * Thin wrapper around TimeSeriesValueImpl that uses AnyValue for storage.
     * Multiple inputs can bind to the same output, sharing the impl.
     */
    struct TimeSeriesValueOutput
    {
        using impl_ptr = std::shared_ptr<TimeSeriesValueImpl>;

        // Constructor with parent node and value type
        template <ParentNode P, typename T>
        explicit TimeSeriesValueOutput(P *parent, const std::type_info& value_type = typeid(T))
            : _impl(std::make_shared<SimplePeeredImpl>(value_type))
              , _parent(static_cast<Notifiable *>(parent)) { if (!_parent) { throw std::runtime_error("Parent cannot be null"); } }

        // Constructor with parent node only (for generic/dynamic typing)
        template <ParentNode P>
        explicit TimeSeriesValueOutput(P *parent, const std::type_info& value_type)
            : _impl(std::make_shared<SimplePeeredImpl>(value_type))
              , _parent(static_cast<Notifiable *>(parent)) { if (!_parent) { throw std::runtime_error("Parent cannot be null"); } }

        // Value access (returns AnyValue)
        [[nodiscard]] const AnyValue<> &value() const { return _impl->value(); }

        // Set value with AnyValue
        void set_value(const AnyValue<> &v) {
            auto event = TsEventAny::modify(current_time(), v);
            _impl->apply_event(event);
        }

        // Set value with AnyValue move
        void set_value(AnyValue<> &&v) {
            auto event = TsEventAny::modify(current_time(), std::move(v));
            _impl->apply_event(event);
        }

        // Invalidate the value
        void invalidate() { _impl->mark_invalid(current_time()); }

        // Delegate to impl
        [[nodiscard]] bool          modified() const { return _impl->modified(current_time()); }
        [[nodiscard]] bool          valid() const { return _impl->valid(); }
        [[nodiscard]] engine_time_t last_modified_time() const { return _impl->last_modified_time(); }

        [[nodiscard]] TsEventAny delta_value() const { return _impl->query_event(current_time()); }

        // Access to impl for binding
        [[nodiscard]] impl_ptr get_impl() const { return _impl; }

        // Current time accessor (delegates to parent)
        [[nodiscard]] engine_time_t current_time() const {
            auto *provider = dynamic_cast<CurrentTimeProvider *>(_parent);
            if (!provider) { throw std::runtime_error("Parent does not implement CurrentTimeProvider"); }
            return provider->current_engine_time();
        }

    private:
        impl_ptr    _impl;   // Shared with bound inputs
        Notifiable *_parent; // Owning node (implements both traits)
    };

    /**
     * @brief Type-erased time series input (event consumer).
     *
     * Thin wrapper that binds to a TimeSeriesOutput by sharing its impl.
     * Provides read-only access to the value. Implements Notifiable to receive
     * notifications when the bound output changes.
     */
    struct TimeSeriesValueInput
    {
        using impl_ptr = std::shared_ptr<TimeSeriesValueImpl>;

        // Constructor with parent node and value type
        template <ParentNode P, typename T>
        explicit TimeSeriesValueInput(P *parent, const std::type_info& value_type = typeid(T))
            : _impl(std::make_shared<NonBoundImpl>(value_type))
              , _parent(static_cast<Notifiable *>(parent)) {}

        // Constructor with parent node only (for generic/dynamic typing)
        template <ParentNode P>
        explicit TimeSeriesValueInput(P *parent, const std::type_info& value_type)
            : _impl(std::make_shared<NonBoundImpl>(value_type))
              , _parent(static_cast<Notifiable *>(parent)) {}

        // Bind to output (shares impl)
        void bind_output(TimeSeriesValueOutput *output) {
            // Type validation: ensure input and output types match
            if (_impl->value_type() != output->get_impl()->value_type()) {
                throw std::runtime_error(
                    std::string("Type mismatch in bind_output: input expects ") +
                    _impl->value_type().name() + " but output provides " +
                    output->get_impl()->value_type().name()
                );
            }

            // Get active state from current impl before switching
            bool was_active = _impl->active(reinterpret_cast<Notifiable *>(this));

            // Mark passive on old impl
            if (was_active) { _impl->mark_passive(reinterpret_cast<Notifiable *>(this)); }

            // Bind to new impl
            _impl = output->get_impl();

            // Restore active state on new impl
            if (was_active) { _impl->mark_active(reinterpret_cast<Notifiable *>(this)); }
        }

        // Value access (returns AnyValue)
        [[nodiscard]] const AnyValue<> &value() const { return _impl->value(); }

        // Queries delegate to shared impl
        [[nodiscard]] bool modified() const { return _impl ? _impl->modified(current_time()) : false; }

        [[nodiscard]] bool valid() const { return _impl ? _impl->valid() : false; }

        [[nodiscard]] engine_time_t last_modified_time() const { return _impl ? _impl->last_modified_time() : min_time(); }

        [[nodiscard]] TsEventAny delta_value() const {
            return _impl ? _impl->query_event(current_time()) : TsEventAny::none(min_time());
        }

        // Active state (computed from subscription)
        [[nodiscard]] bool active() const {
            return _impl->active(reinterpret_cast<Notifiable *>(
                const_cast<TimeSeriesValueInput *>(this)
            ));
        }

        // Mark input as active (adds to subscriber set)
        void mark_active() { if (_impl) { _impl->mark_active(reinterpret_cast<Notifiable *>(this)); } }

        // Mark input as passive (removes from subscriber set)
        void mark_passive() { if (_impl) { _impl->mark_passive(reinterpret_cast<Notifiable *>(this)); } }

        // Notifiable interface (would be implemented if this inherited from Notifiable)
        void notify(engine_time_t t) {
            // Notify parent to schedule owning node
            _parent->notify(t);
        }

        // Current time accessor (delegates to parent)
        [[nodiscard]] engine_time_t current_time() const {
            return dynamic_cast<CurrentTimeProvider *>(_parent)->current_engine_time();
        }

    private:
        impl_ptr    _impl;   // Shared impl
        Notifiable *_parent; // Owning node (provides notification and time)
    };
} // namespace hgraph
