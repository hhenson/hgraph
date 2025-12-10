#include "hgraph/types/graph.h"
#include "hgraph/types/node.h"

#include <hgraph/types/v2/ts_value.h>
#include <hgraph/types/v2/ts_value_impl.h>
#include <nanobind/nanobind.h>

// Placeholder for ref_value_tp - will be replaced when we integrate references
namespace hgraph {
    struct ref_value_tp_placeholder {};
    using ref_value_tp = ref_value_tp_placeholder;

    // TSOutput constructor
    TSOutput::TSOutput(NotifiableContext *owner, const std::type_info &value_type)
        : _impl(std::make_shared<PeeredTSValue>(value_type)), _owner(owner) {
        if (!_owner) { throw std::runtime_error("Parent cannot be null"); }
    }

    const AnyValue<> &TSOutput::value() const { return _impl->value(); }

    void TSOutput::set_value(const AnyValue<> &v) {
        auto t{current_time()};
        auto event = TsEventAny::modify(t, v);
        _impl->apply_event(event);
        // Note: We do NOT call notify_parent() here because:
        // 1. For regular node outputs, the node producing the output shouldn't be scheduled
        // 2. Subscriber notification is handled by TimeSeriesValueOutput::mark_modified()
        // 3. Parent output notification is handled by BaseTimeSeriesOutput::mark_modified()
    }

    void TSOutput::set_value(AnyValue<> &&v) {
        auto t{current_time()};
        auto event = TsEventAny::modify(t, std::move(v));
        _impl->apply_event(event);
        // See comment in set_value(const AnyValue<>&) above
    }

    void TSOutput::invalidate() {
        if (!valid()) { return; }
        auto t{current_time()};
        _impl->mark_invalid(t);
        // See comment in set_value(const AnyValue<>&) above
    }

    void TSOutput::reset() {
        if (!valid()) { return; }
        _impl->reset();
    }

    bool TSOutput::modified() const { return _impl->modified(current_time()); }

    bool TSOutput::valid() const { return _impl->valid(); }

    engine_time_t TSOutput::last_modified_time() const { return _impl->last_modified_time(); }

    TsEventAny TSOutput::delta_value() const { return _impl->query_event(current_time()); }

    engine_time_t TSOutput::current_time() const {
        auto *provider = dynamic_cast<CurrentTimeProvider *>(_owner);
        if (!provider) { throw std::runtime_error("Parent does not implement CurrentTimeProvider"); }
        return provider->current_engine_time();
    }

    Notifiable *TSOutput::owner() const { return _owner; }

    void TSOutput::set_owner(NotifiableContext *owner) { _owner = owner; }

    void TSOutput::subscribe(Notifiable *notifier) { _impl->add_subscriber(notifier); }

    void TSOutput::unsubscribe(Notifiable *notifier) { _impl->remove_subscriber(notifier); }

    const std::type_info &TSOutput::value_type() const { return _impl->value_type(); }

    void TSOutput::notify_parent(engine_time_t t) const {
        if (_owner) { _owner->notify(t); }
    }

    // TSInput constructor
    TSInput::TSInput(NotifiableContext *parent, const std::type_info &value_type)
        : _impl(std::make_shared<NonBoundTSValue>(value_type)), _owner(parent) {}

    // TSInput destructor - unsubscribe from impl to avoid dangling subscriber pointers
    TSInput::~TSInput() {
        if (_impl && active()) {
            make_passive();
        }
    }

    // TSInput::bind_output implementation
    void TSInput::bind_output(TSOutput &output) { bind(output._impl); }

    void TSInput::copy_from_input(TSInput &input) { bind(input._impl); }

    void TSInput::un_bind() {
        if (!bound()) {
            // Create the effect of unbinding (reset values, etc.)
            _impl->reset();
            return;
        }

        // Get active-state from current impl before switching
        bool was_active = _impl->has_subscriber(reinterpret_cast<Notifiable *>(this));

        // Mark passive on old impl
        if (was_active) { _impl->remove_subscriber(reinterpret_cast<Notifiable *>(this)); }

        // Reset the state model to NonBoundImpl
        _impl = std::make_shared<NonBoundTSValue>(_impl->value_type());

        // Restore active state on new impl
        if (was_active) {
            _impl->add_subscriber(reinterpret_cast<Notifiable *>(this));
            mark_sampled();
        }
    }

    void TSInput::subscribe(Notifiable *notifier) {
        // If we aren't pointing to a delegate, upgrade this to be a delegate so we can benefit from
        // its local subscription tracking.
        if (auto delegate{dynamic_cast<DelegateTSValue *>(_impl.get())}; delegate == nullptr) {
            auto was_active{active()};
            if (was_active) { make_passive(); }
            _impl = std::make_shared<DelegateTSValue>(_impl);
            if (was_active) { make_active(); }
        }
        _impl->add_subscriber(notifier);
    }

    void TSInput::unsubscribe(Notifiable *notifier) {
        // We will not worry about downgrading once we have upgraded to tracking status.
        _impl->remove_subscriber(notifier);
    }

    const std::type_info &TSInput::value_type() const { return _impl->value_type(); }

    void TSInput::add_before_evaluation_notification(std::function<void()> &&fn) const {
        dynamic_cast<EvaluationScheduler *>(_owner)->add_before_evaluation_notification(std::move(fn));
    }

    void TSInput::add_after_evaluation_notification(std::function<void()> &&fn) const {
        dynamic_cast<EvaluationScheduler *>(_owner)->add_after_evaluation_notification(std::move(fn));
    }

    void TSInput::bind(impl_ptr &other) {
        // Type checking is delegated to the higher-level wrapper (e.g., TimeSeriesValueInput::bind_output)
        // which already validates type compatibility before calling this method.
        // The v2 layer is purely for value storage and binding mechanism.

        // Check if we're binding to a reference type (for special handling)
        auto is_ts_bound_to_ref{other->is_value_instanceof(typeid(ref_value_tp)) &&
                                !_impl->is_value_instanceof(typeid(ref_value_tp))};

        // Get active-state from current impl before switching
        bool                             was_active{active()};
        std::unordered_set<Notifiable *> subscriptions{};

        // Mark passive on old impl
        if (was_active) {
            make_passive();  // First, remove ourselves as we only want to see if there are external subscriptions to track
            // If we are pointing to an instance of delegate, we need to capture the subscribers.
            if (auto delegate{dynamic_cast<DelegateTSValue *>(_impl.get())}; delegate != nullptr) {
                subscriptions.insert_range(delegate->delegate_subscribers());
            }
        }

        // Bind to new impl
        if (is_ts_bound_to_ref) {
            _impl = std::make_shared<ReferencedTSValue>(other, value_type(), this->owner());
        } else {
            // If we have subscribers and the other is not a delegate, make it a delegate first!
            if (!subscriptions.empty()) {
                _impl = std::make_shared<DelegateTSValue>(other);
            } else {
                _impl = other;
            }
        }

        // Restore active state on new impl
        if (was_active) {
            if (!subscriptions.empty()) {
                for (auto *sub : subscriptions) { _impl->add_subscriber(sub); }
            }
            make_active();
            // If we are active and the newly bound value is valid, we need to sample as this is
            // equivalent to a new tick for this input.
            if (valid()) { mark_sampled(); };
        }
    }

    const AnyValue<> &TSInput::value() const { return _impl->value(); }

    void TSInput::set_value(AnyValue<> &&v) {
        if (!is_non_bound(_impl)) { throw std::runtime_error("Cannot set value on a bound input"); }
        auto t{current_time()};
        auto event = TsEventAny::modify(t, std::move(v));
        _impl->apply_event(event);
    }

    void TSInput::set_value(const AnyValue<> &v) {
        if (!is_non_bound(_impl)) { throw std::runtime_error("Cannot set value on a bound input"); }
        auto t{current_time()};
        auto event = TsEventAny::modify(t, v);
        _impl->apply_event(event);
    }

    bool TSInput::modified() const { return _impl ? _impl->modified(current_time()) : false; }

    bool TSInput::valid() const { return _impl ? _impl->valid() : false; }

    engine_time_t TSInput::last_modified_time() const { return _impl ? _impl->last_modified_time() : min_time(); }

    bool TSInput::active() const { return _impl->has_subscriber(reinterpret_cast<Notifiable *>(const_cast<TSInput *>(this))); }

    void TSInput::make_active() {
        if (_impl) { _impl->add_subscriber(reinterpret_cast<Notifiable *>(this)); }
    }

    void TSInput::make_passive() {
        if (_impl) { _impl->remove_subscriber(reinterpret_cast<Notifiable *>(this)); }
    }

    void TSInput::mark_sampled() {
        auto tm{current_time()};
        // Check if this is already marked as modified we have either already added sampled
        // or the item ticked now, in which case who cares?
        // We could also check for valid, but what if the previous value was valid
        // and this one is not. In the latter case we need to sample to ensure this gets notified.
        if (!_impl->modified(tm)) {
            // Since the code may look at the value even if it does not actively subscribe to it
            // We need to indicate that the value has changed (in this case due to a binding
            // change).
            auto sampled{std::make_shared<SampledTSValue>(_impl, tm)};
            _impl = sampled;  // Actually assign the sampled wrapper to _impl
            // Register a cleanup handler, as we don't want to keep this indefinitely
            _owner->add_after_evaluation_notification([this]() {
                // Make sure the current delegate is in fact a sampled delegate, if not...
                auto *impl = dynamic_cast<SampledTSValue *>(_impl.get());
                if (impl != nullptr) {
                    // Switch the delegate in the sample with the delegate in ourselves
                    // Currently held by the delegate.
                    _impl = impl->delegate();
                }
            });
        }

        // We will force the notification
        _owner->notify(tm);
    }

    void TSInput::notify(engine_time_t t) {
        // Notify parent to schedule owning node
        if (_owner) { _owner->notify(t); }
    }

    engine_time_t TSInput::current_time() const {
        auto *provider = dynamic_cast<CurrentTimeProvider *>(_owner);
        if (!provider) { throw std::runtime_error("Parent does not implement CurrentTimeProvider"); }
        return provider->current_engine_time();
    }

    NotifiableContext *TSInput::owner() const { return _owner; }

    void TSInput::set_owner(NotifiableContext *owner) { _owner = owner; }

    bool TSInput::bound() const {
        if (is_sampled(_impl)) { return !is_non_bound(static_cast<const SampledTSValue *>(_impl.get())->delegate()); }
        return !is_non_bound(_impl);
    }

}  // namespace hgraph