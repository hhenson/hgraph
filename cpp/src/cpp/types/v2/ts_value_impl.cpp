#include "hgraph/nodes/last_value_pull_node.h"
#include "hgraph/types/ref.h"

#include <hgraph/types/ref_value.h>
#include <hgraph/types/v2/ts_value_impl.h>

namespace hgraph
{

    // NoneTSValue implementations

    NoneTSValue::NoneTSValue(const std::type_info &type) : _value_type(TypeId(&type)), _value({}) {}

    void NoneTSValue::apply_event(const TsEventAny &event) { throw std::runtime_error("Cannot apply event to NoneTSValue"); }

    TsEventAny NoneTSValue::query_event(engine_time_t t) const { return TsEventAny::none(t); }

    void NoneTSValue::bind_to(TSValue *other) { throw std::runtime_error("Cannot bind to NoneTSValue"); }

    void NoneTSValue::unbind() { /* Nothing to do */ }

    void NoneTSValue::reset() { /* Nothing to do */ }

    void NoneTSValue::add_subscriber(Notifiable *subscriber) { throw std::runtime_error("Cannot add subscriber to NoneTSValue"); }

    void NoneTSValue::remove_subscriber(Notifiable *subscriber) {
        throw std::runtime_error("Cannot remove subscriber from NoneTSValue");
    }

    bool NoneTSValue::has_subscriber(Notifiable *subscriber) const { return false; }

    bool NoneTSValue::modified(engine_time_t t) const { return false; }

    bool NoneTSValue::all_valid() const { return false; }

    bool NoneTSValue::valid() const { return false; }

    engine_time_t NoneTSValue::last_modified_time() const { return MIN_DT; }

    const AnyValue<> &NoneTSValue::value() const { return _value; }

    const std::type_info &NoneTSValue::value_type() const { return *_value_type.info; }

    void NoneTSValue::mark_invalid(engine_time_t t) { /* Nothing to do */ }

    void NoneTSValue::notify_subscribers(engine_time_t t) { /* Nothing to do */ }

    bool NoneTSValue::is_value_instanceof(const std::type_info &value_type) { return _value_type.info == &value_type; }

    // DelegateTSValue implementations

    DelegateTSValue::DelegateTSValue(TSValue::s_ptr ts_value) : _ts_value(std::move(ts_value)) {}

    TsEventAny DelegateTSValue::query_event(engine_time_t t) const { return _ts_value->query_event(t); }

    void DelegateTSValue::apply_event(const TsEventAny &event) { _ts_value->apply_event(event); }

    void DelegateTSValue::bind_to(TSValue *value) { _ts_value->bind_to(value); }

    void DelegateTSValue::unbind() { _ts_value->unbind(); }

    void DelegateTSValue::reset() { _ts_value->reset(); }

    void DelegateTSValue::add_subscriber(Notifiable *subscriber) { _ts_value->add_subscriber(subscriber); }

    void DelegateTSValue::remove_subscriber(Notifiable *subscriber) { _ts_value->remove_subscriber(subscriber); }

    void DelegateTSValue::mark_invalid(engine_time_t t) { _ts_value->mark_invalid(t); }

    bool DelegateTSValue::has_subscriber(Notifiable *subscriber) const { return _ts_value->has_subscriber(subscriber); }

    bool DelegateTSValue::modified(engine_time_t t) const { return _ts_value->modified(t); }

    bool DelegateTSValue::all_valid() const { return _ts_value->all_valid(); }

    bool DelegateTSValue::valid() const { return _ts_value->valid(); }

    engine_time_t DelegateTSValue::last_modified_time() const { return _ts_value->last_modified_time(); }

    const AnyValue<> &DelegateTSValue::value() const { return _ts_value->value(); }

    const std::type_info &DelegateTSValue::value_type() const { return _ts_value->value_type(); }

    bool DelegateTSValue::is_value_instanceof(const std::type_info &value_type) {
        return _ts_value->is_value_instanceof(value_type);
    }

    void DelegateTSValue::notify_subscribers(engine_time_t t) { _ts_value->notify_subscribers(t); }

    // BaseTSValue implementations
    void BaseTSValue::apply_event(const TsEventAny &event) {
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

    TsEventAny BaseTSValue::query_event(engine_time_t t) const {
        // Check if last event occurred at requested time
        if (last_modified_time() == t) { return _last_event; }
        return TsEventAny::none(t);
    }

    void BaseTSValue::bind_to(TSValue *) {
        // No-op for simple peered
    }

    void BaseTSValue::unbind() {
        // No-op for simple peered
    }

    void BaseTSValue::reset() {
        _value.reset();
        _last_event = TsEventAny::none(min_time());
    }

    bool BaseTSValue::modified(engine_time_t t) const { return last_modified_time() == t; }

    bool BaseTSValue::all_valid() const { return valid(); }

    bool BaseTSValue::valid() const { return _last_event.kind == TsEventKind::Modify || _last_event.kind == TsEventKind::Recover; }

    engine_time_t BaseTSValue::last_modified_time() const {
        return _last_event.kind != TsEventKind::None ? _last_event.time : min_time();
    }

    const AnyValue<> &BaseTSValue::value() const { return _value; }

    const std::type_info &BaseTSValue::value_type() const { return *_value_type.info; }

    void BaseTSValue::mark_invalid(engine_time_t t) {
        auto event = TsEventAny::invalidate(t);
        apply_event(event);
    }

    bool BaseTSValue::is_value_instanceof(const std::type_info &value_type) { return _value_type.info == &value_type; }

    // NonBoundTSValue implementations
    void NonBoundTSValue::add_subscriber([[maybe_unused]] Notifiable *subscriber) { _active = true; }

    void NonBoundTSValue::remove_subscriber([[maybe_unused]] Notifiable *subscriber) { _active = false; }

    bool NonBoundTSValue::has_subscriber([[maybe_unused]] Notifiable *subscriber) const {
        return _active;  // Return the local active state, ignore subscriber parameter
    }

    void NonBoundTSValue::notify_subscribers(engine_time_t t) {
        // No-op: Non-bound values don't have external subscribers
    }

    // PeeredTSValue implementations
    void PeeredTSValue::add_subscriber(Notifiable *subscriber) { _subscribers.insert(subscriber); }

    void PeeredTSValue::remove_subscriber(Notifiable *subscriber) { _subscribers.erase(subscriber); }

    void PeeredTSValue::notify_subscribers(engine_time_t t) {
        for (auto *subscriber : _subscribers) { subscriber->notify(t); }
    }

    bool PeeredTSValue::has_subscriber(Notifiable *subscriber) const { return _subscribers.contains(subscriber); }

    // SampledTSValue implementations
    bool SampledTSValue::modified(engine_time_t t) const { return t == _sampled_time; }

    engine_time_t SampledTSValue::last_modified_time() const { return _sampled_time; }

    ReferencedTSValue::ReferencedTSValue(TSValue::s_ptr reference_ts_value, const std::type_info &type, NotifiableContext *context)
        : DelegateTSValue(std::make_shared<NoneTSValue>(type)), _reference_ts_value(std::move(reference_ts_value)),
          _context(context) {
        // We are always active to reference changes
        _reference_ts_value->add_subscriber(this);
        update_binding();
        if (_context == nullptr) { throw std::runtime_error("ReferencedTSValue: Cannot create with null scheduler"); }
    }

    ReferencedTSValue::~ReferencedTSValue() {
        if (_active != nullptr) {
            delegate()->remove_subscriber(_active);
        }
        _reference_ts_value->remove_subscriber(this);
    }

    void ReferencedTSValue::add_subscriber(Notifiable *subscriber) {
        if (_active == subscriber) { return; }
        if (_active != nullptr) {
            throw std::runtime_error("ReferencedTSValue::add_subscriber: Trying to bind an additional subsriber not supported");
        }
        _active = subscriber;
        if (bound()) { DelegateTSValue::add_subscriber(subscriber); }
    }

    void ReferencedTSValue::remove_subscriber(Notifiable *subscriber) {
        if (_active == subscriber) {
            _active = nullptr;
            _reference_ts_value->remove_subscriber(this);
            if (bound()) { DelegateTSValue::remove_subscriber(subscriber); }
        } else if (_active != nullptr) {
            throw std::runtime_error("ReferenceTSValue::remove_subscriber: Trying to remove a subscriber that was not subscribed");
        }
    }

    bool ReferencedTSValue::has_subscriber(Notifiable *subscriber) const { return _active != nullptr; }

    void ReferencedTSValue::notify_subscribers(engine_time_t t) { _active->notify(t); }

    void ReferencedTSValue::notify(engine_time_t et) {
        if (_reference_ts_value->modified(et)) { update_binding(); }
    }

    // ReferencedTSValue implementations
    void ReferencedTSValue::update_binding() {
        if (!_reference_ts_value->valid()) {
            // If reference is not valid yet, ensure we have a NonBoundTSValue instead of NoneTSValue
            // so that operations like set_value can work properly
            if (is_none(delegate())) {
                swap(std::make_shared<NonBoundTSValue>(value_type()));
            }
            return;
        }
        auto v(get_from_any<ref_value_tp>(_reference_ts_value->value()));
        if (v->has_output()) {
            // We can be bound to a TS value, not another reference that would not make sense.
            auto output{dynamic_cast<BoundTimeSeriesReference *>(v.get())->output()};
            // We can only deal with Bound types being TimeSeriesValueOutput (and perhaps REF, see if that is needed)
            if (auto output_v = dynamic_cast<TimeSeriesValueOutput *>(output.get()); output_v != nullptr) {
                // We have a TimeSeriesValueOutput so we can extract TSOutput (ts) get the underlying impl
                // and bind it to delegate. Using p, we should get a scoped copy
                auto p{output_v->ts()._impl};

                // Now we swap it in, clearing out anything that was.
                swap(p);
                if (is_active()) {
                    p->remove_subscriber(_active);
                    delegate()->add_subscriber(_active);
                    if (delegate()->valid()) {}
                }
            }
        }
    }

    bool ReferencedTSValue::bound() const { return is_bound(delegate()); }

    bool ReferencedTSValue::is_active() const { return _active != nullptr; }

    void ReferencedTSValue::mark_sampled() {
        // Sampled is always now, if our delegate is already sampled there there
        // is no work to do.
        if (is_sampled(delegate())) { return; }
        auto tm{current_time()};

        // Since the code may look at the value even if it does not actively subscribe to it
        // We need to indicate that the value has changed (in this case due to a binding
        // change).
        auto sampled{std::make_shared<SampledTSValue>(delegate(), tm)};
        // Register a cleanup handler, as we don't want to keep this indefinitely
        _context->add_after_evaluation_notification([&]() {
            // Make sure the current delegate is in fact a sampled delegate, if not...
            auto *impl = dynamic_cast<SampledTSValue *>(delegate().get());
            if (impl != nullptr) {
                // Switch the delegate in the sample with the delegate in ourselves
                // Currently held by the delegate.
                swap(impl->delegate());
            }
        });

        // Now, if we are active, we need to notify the subscriber
        if (is_active()) { notify_subscribers(tm); }
    }

    engine_time_t ReferencedTSValue::current_time() const {
        // Rely on the fact that the _scheduler is also a CurrentTimeProvider
        auto ctp{dynamic_cast<CurrentTimeProvider *>(_context)};
        if (ctp == nullptr) { throw std::runtime_error("ReferencedTSValue::current_time: Expected CurrentTimeProvider"); }
        return ctp->current_engine_time();
    }

}  // namespace hgraph