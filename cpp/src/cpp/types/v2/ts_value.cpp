#include "hgraph/types/graph.h"
#include "hgraph/types/node.h"

#include <hgraph/types/ref_value.h>
#include <hgraph/types/v2/ts_value.h>
#include <hgraph/types/v2/ts_value_impl.h>

namespace hgraph
{

    // TSOutput constructor
    TSOutput::TSOutput(NotifiableContext *parent, const std::type_info &value_type)
        : _impl(std::make_shared<PeeredTSValue>(value_type)), _parent(parent) {
        if (!_parent) { throw std::runtime_error("Parent cannot be null"); }
    }

    const AnyValue<> &TSOutput::value() const { return _impl->value(); }

    void TSOutput::set_value(const AnyValue<> &v) {
        auto t{current_time()};
        auto event = TsEventAny::modify(t, v);
        _impl->apply_event(event);
        notify_parent(t);
    }

    void TSOutput::set_value(AnyValue<> &&v) {
        auto t{current_time()};
        auto event = TsEventAny::modify(t, std::move(v));
        _impl->apply_event(event);
        notify_parent(t);
    }

    void TSOutput::invalidate() {
        if (!valid()) { return; }
        auto t{current_time()};
        _impl->mark_invalid(t);
        notify_parent(t);
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
        auto *provider = dynamic_cast<CurrentTimeProvider *>(_parent);
        if (!provider) { throw std::runtime_error("Parent does not implement CurrentTimeProvider"); }
        return provider->current_engine_time();
    }

    Notifiable *TSOutput::parent() const { return _parent; }

    void TSOutput::set_parent(NotifiableContext *parent) { _parent = parent; }

    void TSOutput::subscribe(Notifiable *notifier) { _impl->add_subscriber(notifier); }

    void TSOutput::unsubscribe(Notifiable *notifier) { _impl->remove_subscriber(notifier); }

    const std::type_info &TSOutput::value_type() const { return _impl->value_type(); }

    void TSOutput::notify_parent(engine_time_t t) const {
        if (_parent) { _parent->notify(t); }
    }

    // TSInput constructor
    TSInput::TSInput(NotifiableContext *parent, const std::type_info &value_type)
        : _impl(std::make_shared<NonBoundTSValue>(value_type)), _parent(parent) {}

    // TSInput::bind_output implementation
    void TSInput::bind_output(TSOutput &output) {
        bind(output._impl);
    }

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

    const std::type_info &TSInput::value_type() const { return _impl->value_type(); }

    void TSInput::add_before_evaluation_notification(std::function<void()> &&fn) const {
        dynamic_cast<EvaluationScheduler *>(_parent)->add_before_evaluation_notification(std::move(fn));
    }

    void TSInput::add_after_evaluation_notification(std::function<void()> &&fn) const {
        dynamic_cast<EvaluationScheduler *>(_parent)->add_after_evaluation_notification(std::move(fn));
    }

    void TSInput::bind(impl_ptr &other) {
        // If the other is a reference and we are not ...
        auto is_ts_bound_to_ref{other->is_value_instanceof(typeid(ref_value_tp)) &&
                                !_impl->is_value_instanceof(typeid(ref_value_tp))};
        auto is_same{_impl->is_value_instanceof(other)};
        if (!(is_ts_bound_to_ref || is_same)) {
            throw std::runtime_error(std::string("Type mismatch in bind_output: input expects ") + _impl->value_type().name() +
                                     " but output provides " + other->value_type().name());
        }

        // Get active-state from current impl before switching
        bool was_active{active()};

        // Mark passive on old impl
        if (was_active) { make_passive(); }

        // Bind to new impl
        if (is_ts_bound_to_ref) {
            _impl = std::make_shared<ReferencedTSValue>(other, value_type(), this->parent());
        } else {
            _impl = other;
        }

        // Restore active state on new impl
        if (was_active) {
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
            // Register a cleanup handler, as we don't want to keep this indefinitely
            _parent->add_after_evaluation_notification([&]() {
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
        _parent->notify(tm);
    }

    void TSInput::notify(engine_time_t t) {
        // Notify parent to schedule owning node
        if (_parent) { _parent->notify(t); }
    }

    engine_time_t TSInput::current_time() const {
        auto *provider = dynamic_cast<CurrentTimeProvider *>(_parent);
        if (!provider) { throw std::runtime_error("Parent does not implement CurrentTimeProvider"); }
        return provider->current_engine_time();
    }

    NotifiableContext *TSInput::parent() const { return _parent; }

    void TSInput::set_parent(NotifiableContext *parent) { _parent = parent; }

    bool TSInput::bound() const {
        if (is_sampled(_impl)) { return !is_non_bound(static_cast<const SampledTSValue *>(_impl.get())->delegate()); }
        return !is_non_bound(_impl);
    }

}  // namespace hgraph