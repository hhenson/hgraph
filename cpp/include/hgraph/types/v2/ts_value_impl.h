#pragma once

#include "any_value.h"
#include "ts_event.h"
#include "ts_value.h"
#include <hgraph/types/ts_traits.h>
#include <hgraph/util/date_time.h>
#include <memory>
#include <typeinfo>
#include <unordered_set>

namespace hgraph
{

    struct DelegateTSValue : TSValue
    {
        explicit DelegateTSValue(TSValue::s_ptr ts_value) : _ts_value(ts_value) {}

        [[nodiscard]] TsEventAny query_event(engine_time_t t) const override { return _ts_value->query_event(t); }

        void apply_event(const TsEventAny &event) override { _ts_value->apply_event(event); }
        void bind_to(TSValue *value) override { _ts_value->bind_to(value); }
        void unbind() override { _ts_value->unbind(); }
        void reset() override { _ts_value->reset(); }
        void add_subscriber(Notifiable *subscriber) override { _ts_value->add_subscriber(subscriber); }
        void remove_subscriber(Notifiable *subscriber) override { _ts_value->remove_subscriber(subscriber); }
        void mark_invalid(engine_time_t t) override { _ts_value->mark_invalid(t); }

        [[nodiscard]] bool has_subscriber(Notifiable *subscriber) const override { return _ts_value->has_subscriber(subscriber); }
        [[nodiscard]] bool modified(engine_time_t t) const override { return _ts_value->modified(t); }
        [[nodiscard]] bool all_valid() const override { return _ts_value->all_valid(); }
        [[nodiscard]] bool valid() const override { return _ts_value->valid(); }
        [[nodiscard]] engine_time_t         last_modified_time() const override { return _ts_value->last_modified_time(); }
        [[nodiscard]] const AnyValue<>     &value() const override { return _ts_value->value(); }
        [[nodiscard]] const std::type_info &value_type() const override { return _ts_value->value_type(); }
        [[nodiscard]] bool                  is_value_instanceof(const std::type_info &value_type) override {
            return _ts_value->is_value_instanceof(value_type);
        }
        void notify_subscribers(engine_time_t t) override { _ts_value->notify_subscribers(t); }

        void swap(TSValue::s_ptr other) { std::swap(_ts_value, other); }

        const TSValue::s_ptr &delegate() const { return _ts_value; }

      private:
        TSValue::s_ptr _ts_value;
    };

    struct BaseTSValue : TSValue
    {
        // Shared state (single source of truth)
        TypeId     _value_type;  // Expected value type
        AnyValue<> _value;       // Current value (type-erased)
        TsEventAny _last_event;  // Most recent event (holds timestamp + kind + value)

        explicit BaseTSValue(const std::type_info &type) : _value_type(TypeId{&type}) {}

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

        [[nodiscard]] bool is_value_instanceof(const std::type_info &value_type) override {
            return _value_type.info == &value_type;
        }
    };

    /**
     * @brief Non-bound implementation for inputs not yet bound to an output.
     *
     * This provides the ability for an input to track values if required but has no
     * peer that is producing values. As such, there is no need for a subscriber as the only
     * subscriber in this model is the input.
     *
     * We can reduce "subscription" to marking the _active flag.
     * This will still perform all other tasks.
     *
     * This also assumes that if the input is managing the state, that it is either in an active
     * state or can do it's own notification / scheduling if required.
     */
    struct NonBoundTSValue final : BaseTSValue
    {
        bool _active{false};  // Active state tracked locally

        explicit NonBoundTSValue(const std::type_info &type) : BaseTSValue(type) {}

        void add_subscriber([[maybe_unused]] Notifiable *subscriber) override { _active = true; }

        void remove_subscriber([[maybe_unused]] Notifiable *subscriber) override { _active = false; }

        [[nodiscard]] bool has_subscriber([[maybe_unused]] Notifiable *subscriber) const override {
            return _active;  // Return the local active state, ignore subscriber parameter
        }

        void notify_subscribers(engine_time_t t) override;
    };

    /**
     * @brief Simple peered implementation - direct input-output binding, no references.
     *
     * This is the simplest and most common case:
     * - Direct value updates
     * - No reference tracking
     * - Minimal overhead
     */
    struct PeeredTSValue final : BaseTSValue
    {
        // Shared state (single source of truth)
        std::unordered_set<Notifiable *> _subscribers;  // Notification subscribers

        explicit PeeredTSValue(const std::type_info &type) : BaseTSValue(type) {}

        void add_subscriber(Notifiable *subscriber) override { _subscribers.insert(subscriber); }

        void remove_subscriber(Notifiable *subscriber) override { _subscribers.erase(subscriber); }

        void notify_subscribers(engine_time_t t) override {
            for (auto *subscriber : _subscribers) { subscriber->notify(t); }
        }
        [[nodiscard]] bool has_subscriber(Notifiable *subscriber) const override { return _subscribers.contains(subscriber); }
    };

    struct SampledTSValue final : DelegateTSValue
    {
        explicit SampledTSValue(TSValue::s_ptr ts_value, engine_time_t sampled_time)
            : DelegateTSValue(ts_value), _sampled_time(sampled_time) {}

        [[nodiscard]] bool          modified(engine_time_t t) const override { return t == _sampled_time; }
        [[nodiscard]] engine_time_t last_modified_time() const override { return _sampled_time; }

      private:
        engine_time_t _sampled_time;
    };

    inline bool is_sampled(const TSValue::s_ptr &ts_value) { return dynamic_cast<SampledTSValue *>(ts_value.get()) != nullptr; }

    inline bool is_peered(const TSValue::s_ptr &ts_value) { return dynamic_cast<PeeredTSValue *>(ts_value.get()) != nullptr; }

    inline bool is_non_bound(const TSValue::s_ptr &ts_value) { return dynamic_cast<NonBoundTSValue *>(ts_value.get()) != nullptr; }
}  // namespace hgraph
