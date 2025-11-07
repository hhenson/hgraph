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

    bool NoneTSValue::is_value_instanceof(const std::type_info &value_type) {
        // Treat nanobind::object as a dynamic "any" that can bind to any concrete type
        return _value_type.info == &value_type || _value_type.info == &typeid(nanobind::object) || &value_type == &typeid(nanobind::object);
    }

    // DelegateTSValue implementations

    DelegateTSValue::DelegateTSValue(TSValue::s_ptr ts_value) : _ts_value(std::move(ts_value)) {}

    TsEventAny DelegateTSValue::query_event(engine_time_t t) const { return _ts_value->query_event(t); }

    void DelegateTSValue::apply_event(const TsEventAny &event) { _ts_value->apply_event(event); }

    void DelegateTSValue::reset() { _ts_value->reset(); }

    void DelegateTSValue::add_subscriber(Notifiable *subscriber) {
        auto result{_subscribers.insert(subscriber)};
        if (result.second) { _ts_value->add_subscriber(subscriber); }
    }

    void DelegateTSValue::remove_subscriber(Notifiable *subscriber) {
        auto result{_subscribers.erase(subscriber)};
        if (result != 0) { _ts_value->remove_subscriber(subscriber); }
    }

    void DelegateTSValue::mark_invalid(engine_time_t t) { _ts_value->mark_invalid(t); }

    bool DelegateTSValue::has_subscriber(Notifiable *subscriber) const { return _subscribers.contains(subscriber); }

    bool DelegateTSValue::modified(engine_time_t t) const { return _ts_value->modified(t); }

    bool DelegateTSValue::all_valid() const { return _ts_value->all_valid(); }

    bool DelegateTSValue::valid() const { return _ts_value->valid(); }

    engine_time_t DelegateTSValue::last_modified_time() const { return _ts_value->last_modified_time(); }

    const AnyValue<> &DelegateTSValue::value() const { return _ts_value->value(); }

    const std::type_info &DelegateTSValue::value_type() const { return _ts_value->value_type(); }

    bool DelegateTSValue::is_value_instanceof(const std::type_info &value_type) {
        return _ts_value->is_value_instanceof(value_type);
    }

    void DelegateTSValue::notify_subscribers(engine_time_t t) {
        // The delegate manages its own subscriber list, and if called directly, we should only be
        // notifying those subscribers
        for (auto subscriber : _subscribers) { subscriber->notify(t); }
    }

    void DelegateTSValue::swap(TSValue::s_ptr other) {
        if (delegate() != nullptr) {
            for (auto subscriber : _subscribers) { delegate()->remove_subscriber(subscriber); }
        }
        std::swap(_ts_value, other);
        if (delegate() != nullptr)
            for (auto subscriber : _subscribers) { delegate()->add_subscriber(subscriber); }
    }

    const TSValue::s_ptr &DelegateTSValue::delegate() const { return _ts_value; }

    const std::set<Notifiable *> &DelegateTSValue::delegate_subscribers() const { return _subscribers; }

    // BaseTSValue implementations
    static bool allow_pyobject_wildcard() {
        const char* env = std::getenv("HGRAPH_PYOBJECT_WILDCARD");
        return env && (env[0] == '1' || env[0] == 'T' || env[0] == 't' || env[0] == 'Y' || env[0] == 'y');
    }

    void BaseTSValue::apply_event(const TsEventAny &event) {
        // We need to support the possibility of multiple updates, this happens in cases such as TSD updates to KeySet as we process
        // multiple keys. Perhaps we can see how to optimise this a bit later.

        // Type validation: ensure event value matches expected type
        if ((event.kind == TsEventKind::Modify || event.kind == TsEventKind::Recover) && event.value.has_value()) {
            if (allow_pyobject_wildcard()) {
                // Allow wildcard binding when expected type is nb::object
                if (!(_value_type.info == &typeid(nanobind::object) || (event.value.type() == _value_type))) {
                    throw std::runtime_error(std::string("Type mismatch in apply_event: expected ") + _value_type.info->name() +
                                             " but got " + event.value.type().info->name());
                }
            } else {
                if (!(event.value.type() == _value_type)) {
                    throw std::runtime_error(std::string("Type mismatch in apply_event: expected ") + _value_type.info->name() +
                                             " but got " + event.value.type().info->name());
                }
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

    bool BaseTSValue::is_value_instanceof(const std::type_info &value_type) {
        if (allow_pyobject_wildcard()) {
            // Treat nanobind::object as a dynamic "any" that can bind to any concrete type
            return _value_type.info == &value_type || _value_type.info == &typeid(nanobind::object) || &value_type == &typeid(nanobind::object);
        }
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

    void SampledTSValue::add_subscriber(Notifiable *subscriber) { delegate()->add_subscriber(subscriber); }
    void SampledTSValue::remove_subscriber(Notifiable *subscriber) { delegate()->remove_subscriber(subscriber); }
    bool SampledTSValue::has_subscriber(Notifiable *subscriber) const { return delegate()->has_subscriber(subscriber); }

    void SampledTSValue::notify_subscribers(engine_time_t t) { delegate()->notify_subscribers(t); }

    ReferencedTSValue::ReferencedTSValue(TSValue::s_ptr reference_ts_value, const std::type_info &type, NotifiableContext *context)
        : DelegateTSValue(std::make_shared<NoneTSValue>(type)), _reference_ts_value(std::move(reference_ts_value)),
          _context(context) {
        // We are always active to reference changes
        _reference_ts_value->add_subscriber(this);
        update_binding();
        if (_context == nullptr) { throw std::runtime_error("ReferencedTSValue: Cannot create with null scheduler"); }
    }

    ReferencedTSValue::~ReferencedTSValue() {
        for (auto subscriber : delegate_subscribers()) { delegate()->remove_subscriber(subscriber); }
        _reference_ts_value->remove_subscriber(this);
    }

    void ReferencedTSValue::notify(engine_time_t et) {
        if (_reference_ts_value->modified(et)) { update_binding(); }
    }

    // ReferencedTSValue implementations
    void ReferencedTSValue::update_binding() {
        if (!_reference_ts_value->valid()) {
            // If reference is not valid yet, ensure we have a NonBoundTSValue instead of NoneTSValue
            // so that operations like set_value can work properly
            if (is_none(delegate())) { swap(std::make_shared<NonBoundTSValue>(value_type())); }
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
                auto was_valid{delegate()->valid()};
                // Now we swap it in, clearing out anything that was.
                swap(p);
                if (is_active() && (was_valid || delegate()->valid())) {
                    // We have just swapped this in, we should mark the value as sampled to make sure we evaluate
                    mark_sampled();
                }
            }
        }
    }

    bool ReferencedTSValue::bound() const { return is_bound(delegate()); }

    bool ReferencedTSValue::is_active() const { return !delegate_subscribers().empty(); }

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