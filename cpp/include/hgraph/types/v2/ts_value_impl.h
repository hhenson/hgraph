#pragma once

#include "any_value.h"
#include "hgraph/types/time_series_type.h"
#include "hgraph/types/ts_traits.h"
#include "hgraph/util/date_time.h"
#include "ts_event.h"
#include "ts_value.h"
#include <memory>
#include <typeinfo>
#include <unordered_set>

namespace hgraph
{

    /**
     * @brief Non-bound implementation for inputs not yet bound to an output.
     *
     * Tracks the active state locally as a boolean. Ignores the Notifiable* parameter
     * since non-bound inputs don't actually notify anyone.
     */
    struct NonBoundImpl : TSValue
    {
        bool       _active{false};  // Active state tracked locally
        AnyValue<> _empty_value;    // Empty value to return
        TypeId     _value_type;     // Expected value type

        explicit NonBoundImpl(const std::type_info &type) : _value_type(TypeId{&type}) {}

        void apply_event(const TsEventAny &event) override {
            // Non-bound inputs don't receive events
            throw std::runtime_error("Cannot apply events to non-bound input");
        }

        [[nodiscard]] TsEventAny query_event(engine_time_t t) const override { return TsEventAny::none(t); }

        void bind_to(TSValue *) override {
            // No-op - binding is handled at the Input/Output level
        }

        void unbind() override {
            // No-op
        }

        void reset() override {
            // No-op
        }

        void make_active([[maybe_unused]] Notifiable *subscriber) override { _active = true; }

        void make_passive([[maybe_unused]] Notifiable *subscriber) override { _active = false; }

        [[nodiscard]] bool active([[maybe_unused]] Notifiable *subscriber) const override {
            return _active;  // Return the local active state, ignore subscriber parameter
        }

        [[nodiscard]] bool modified(engine_time_t t) const override {
            return false;  // Never modified when not bound
        }

        [[nodiscard]] bool all_valid() const override {
            return false;  // Never valid when not bound
        }

        [[nodiscard]] bool valid() const override {
            return false;  // Never valid when not bound
        }

        [[nodiscard]] engine_time_t last_modified_time() const override {
            return min_time();  // Never modified
        }

        [[nodiscard]] const AnyValue<> &value() const override {
            return _empty_value;  // Always empty
        }

        [[nodiscard]] const std::type_info &value_type() const override { return *_value_type.info; }

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
    struct SimplePeeredImpl : TSValue
    {
        // Shared state (single source of truth)
        AnyValue<>                       _value;        // Current value (type-erased)
        TsEventAny                       _last_event;   // Most recent event (holds timestamp + kind + value)
        std::unordered_set<Notifiable *> _subscribers;  // Notification subscribers
        TypeId                           _value_type;   // Expected value type

        explicit SimplePeeredImpl(const std::type_info &type) : _value_type(TypeId{&type}) {}

        void apply_event(const TsEventAny &event) override {
            // Guard: Only one event can be applied at a particular time
            if (_last_event.kind != TsEventKind::None && _last_event.time == event.time) {
                throw std::runtime_error("Cannot apply multiple events at the same time");
            }

            // Type validation: ensure event value matches expected type
            if ((event.kind == TsEventKind::Modify || event.kind == TsEventKind::Recover) && event.value.has_value()) {
                if (!(event.value.type() == _value_type)) {
                    throw std::runtime_error(std::string("Type mismatch in apply_event: expected ") + _value_type.info->name() +
                                             " but got " + event.value.type().info->name());
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
            _last_event = event;  // Stores value, timestamp, and kind
            notify_subscribers(event.time);
        }

        [[nodiscard]] TsEventAny query_event(engine_time_t t) const override {
            // Check if last event occurred at requested time
            if (last_modified_time() == t) { return _last_event; }
            return TsEventAny::none(t);
        }

        void bind_to(TSValue *) override {
            // No-op for simple peered
        }

        void unbind() override {
            // No-op for simple peered
        }

        void reset() override {
            _value.reset();
            _last_event = TsEventAny::none(min_time());
        }

        // Modified state (computed from _last_event.time)
        [[nodiscard]] bool modified(engine_time_t t) const override { return last_modified_time() == t; }

        [[nodiscard]] bool all_valid() const override { return valid(); }

        // Valid state (derived from _last_event)
        [[nodiscard]] bool valid() const override {
            return _last_event.kind == TsEventKind::Modify || _last_event.kind == TsEventKind::Recover;
        }

        // Last modified time (derived from _last_event)
        [[nodiscard]] engine_time_t last_modified_time() const override {
            return _last_event.kind != TsEventKind::None ? _last_event.time : min_time();
        }

        // Value access
        [[nodiscard]] const AnyValue<> &value() const override { return _value; }

        [[nodiscard]] const std::type_info &value_type() const override { return *_value_type.info; }

        void mark_invalid(engine_time_t t) override {
            auto event = TsEventAny::invalidate(t);
            apply_event(event);
        }

        void make_active(Notifiable *subscriber) override { _subscribers.insert(subscriber); }

        void make_passive(Notifiable *subscriber) override { _subscribers.erase(subscriber); }

        void notify_subscribers(engine_time_t t) override {
            for (auto *subscriber : _subscribers) { subscriber->notify(t); }
        }

        [[nodiscard]] bool active(Notifiable *subscriber) const override { return _subscribers.contains(subscriber); }
    };

}  // namespace hgraph
