#include <hgraph/types/v2/ts_value_impl.h>
#include <hgraph/types/ref_value.h>

namespace hgraph
{
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

    bool BaseTSValue::valid() const {
        return _last_event.kind == TsEventKind::Modify || _last_event.kind == TsEventKind::Recover;
    }

    engine_time_t BaseTSValue::last_modified_time() const {
        return _last_event.kind != TsEventKind::None ? _last_event.time : min_time();
    }

    const AnyValue<> &BaseTSValue::value() const { return _value; }

    const std::type_info &BaseTSValue::value_type() const { return *_value_type.info; }

    void BaseTSValue::mark_invalid(engine_time_t t) {
        auto event = TsEventAny::invalidate(t);
        apply_event(event);
    }

    bool BaseTSValue::is_value_instanceof(const std::type_info &value_type) {
        return _value_type.info == &value_type;
    }

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

    // ReferencedTSValue implementations
    void ReferencedTSValue::update_binding() {
        if (!_reference_value->valid()) {
            if((dynamic_cast<const SampledTSValue *>(delegate().get()) != nullptr)) {
                DelegateTSValue::swap(std::make_shared<NonBoundTSValue>(_reference_value->value_type()));
            }
            return;
        }
        auto v(get_from_any<ref_value_tp>(_reference_value->value()));
        if (v->has_output()) {
            auto output{dynamic_cast<BoundTimeSeriesReference *>(v.get())->output()};
            // We can only deal with Bound types being TimeSeriesValueOutput (and perhaps REF, see if that is needed)

        }
    }

}