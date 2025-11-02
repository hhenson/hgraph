#pragma once

#include <memory>
#include <unordered_set>
#include <typeinfo>
#include "any_value.h"
#include "ts_event.h"
#include "hgraph/types/time_series_type.h"
#include "hgraph/util/date_time.h"
#include "hgraph/types/ts_traits.h"

namespace hgraph
{

    /**
     * @brief Base implementation class for type-erased time series value storage.
     *
     * This is the PIMPL (Pointer to Implementation) that holds the actual shared state
     * between TimeSeriesInput and TimeSeriesOutput. It uses AnyValue for type-erased
     * storage, eliminating template proliferation.
     *
     * Design principles:
     * - Single source of truth: All state lives here, shared via shared_ptr
     * - Computed properties: valid() and last_modified_time() derived from _last_event
     * - Swappable implementations: Virtual interface allows different state machine variants
     */
    struct TimeSeriesValueImpl
    {
        // Virtual interface for variant behavior
        virtual void       apply_event(const TsEventAny &event) = 0;
        virtual TsEventAny query_event(engine_time_t t) const = 0;
        virtual void       bind_to(TimeSeriesValueImpl *other) = 0;
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

        virtual ~TimeSeriesValueImpl() = default;
    };

    /**
     * @brief Non-bound implementation for inputs not yet bound to an output.
     *
     * Tracks the active state locally as a boolean. Ignores the Notifiable* parameter
     * since non-bound inputs don't actually notify anyone.
     */
    struct NonBoundImpl : TimeSeriesValueImpl
    {
        bool       _active{false};         // Active state tracked locally
        AnyValue<> _empty_value;           // Empty value to return
        TypeId _value_type;                // Expected value type

        explicit NonBoundImpl(const std::type_info& type)
            : _value_type(TypeId{&type}) {}

        void apply_event(const TsEventAny &event) override {
            // Non-bound inputs don't receive events
            throw std::runtime_error("Cannot apply events to non-bound input");
        }

        [[nodiscard]] TsEventAny query_event(engine_time_t t) const override { return TsEventAny::none(t); }

        void bind_to(TimeSeriesValueImpl *) override {
            // No-op - binding is handled at the Input/Output level
        }

        void unbind() override {
            // No-op
        }

        void mark_active([[maybe_unused]] Notifiable *subscriber) override { _active = true; }

        void mark_passive([[maybe_unused]] Notifiable *subscriber) override { _active = false; }

        [[nodiscard]] bool active([[maybe_unused]] Notifiable *subscriber) const override {
            return _active; // Return the local active state, ignore subscriber parameter
        }

        [[nodiscard]] bool modified(engine_time_t t) const override {
            return false; // Never modified when not bound
        }

        [[nodiscard]] bool all_valid() const override {
            return false; // Never valid when not bound
        }

        [[nodiscard]] bool valid() const override {
            return false; // Never valid when not bound
        }

        [[nodiscard]] engine_time_t last_modified_time() const override {
            return min_time(); // Never modified
        }

        [[nodiscard]] const AnyValue<> &value() const override {
            return _empty_value; // Always empty
        }

        [[nodiscard]] const std::type_info& value_type() const override {
            return *_value_type.info;
        }

        void mark_invalid(engine_time_t t) override {
            // No-op - non-bound inputs don't track invalidation
        }

        void notify_subscribers(engine_time_t t) override {
            // No-op - non-bound inputs don't have subscribers
        }
    };

    /**
     * @brief Simple peered implementation - direct input-output binding, no references.
     *
     * This is the simplest and most common case:
     * - Direct value updates
     * - No reference tracking
     * - Minimal overhead
     */
    struct SimplePeeredImpl : TimeSeriesValueImpl
    {
        // Shared state (single source of truth)
        AnyValue<>                       _value;       // Current value (type-erased)
        TsEventAny                       _last_event;  // Most recent event (holds timestamp + kind + value)
        std::unordered_set<Notifiable *> _subscribers; // Notification subscribers
        TypeId                           _value_type;  // Expected value type

        explicit SimplePeeredImpl(const std::type_info& type)
            : _value_type(TypeId{&type}) {}

        void apply_event(const TsEventAny &event) override {
            // Guard: Only one event can be applied at a particular time
            if (_last_event.kind != TsEventKind::None && _last_event.time == event.time) {
                throw std::runtime_error("Cannot apply multiple events at the same time");
            }

            // Type validation: ensure event value matches expected type
            if ((event.kind == TsEventKind::Modify || event.kind == TsEventKind::Recover) && event.value.has_value()) {
                if (!(event.value.type() == _value_type)) {
                    throw std::runtime_error(
                        std::string("Type mismatch in apply_event: expected ") +
                        _value_type.info->name() + " but got " + event.value.type().info->name()
                    );
                }
            }

            // Efficient: mutate value in place when possible
            if (event.kind == TsEventKind::Modify || event.kind == TsEventKind::Recover) {
                // For now, simple copy - could optimize with move semantics or mutation
                _value = event.value;
            } else if (event.kind == TsEventKind::Invalidate) {
                // Invalid means no value - reset to empty state
                _value.reset();
            }
            _last_event = event; // Stores value, timestamp, and kind
            notify_subscribers(event.time);
        }

        [[nodiscard]] TsEventAny query_event(engine_time_t t) const override {
            // Check if last event occurred at requested time
            if (last_modified_time() == t) { return _last_event; }
            return TsEventAny::none(t);
        }

        void bind_to(TimeSeriesValueImpl *) override {
            // No-op for simple peered
        }

        void unbind() override {
            // No-op for simple peered
        }

        // Modified state (computed from _last_event.time)
        [[nodiscard]] bool modified(engine_time_t t) const override { return last_modified_time() == t; }

        [[nodiscard]] bool all_valid() const override { return valid(); }

        // Valid state (derived from _last_event)
        [[nodiscard]] bool valid() const override {
            return _last_event.kind == TsEventKind::Modify ||
                   _last_event.kind == TsEventKind::Recover;
        }

        // Last modified time (derived from _last_event)
        [[nodiscard]] engine_time_t last_modified_time() const override {
            return _last_event.kind != TsEventKind::None ? _last_event.time : min_time();
        }

        // Value access
        [[nodiscard]] const AnyValue<> &value() const override { return _value; }

        [[nodiscard]] const std::type_info& value_type() const override {
            return *_value_type.info;
        }

        void mark_invalid(engine_time_t t) override {
            auto event = TsEventAny::invalidate(t);
            apply_event(event);
        }

        void mark_active(Notifiable *subscriber) override { _subscribers.insert(subscriber); }

        void mark_passive(Notifiable *subscriber) override { _subscribers.erase(subscriber); }

        void notify_subscribers(engine_time_t t) override { for (auto *subscriber : _subscribers) { subscriber->notify(t); } }

        [[nodiscard]] bool active(Notifiable *subscriber) const override { return _subscribers.contains(subscriber); }
    };

} // namespace hgraph
