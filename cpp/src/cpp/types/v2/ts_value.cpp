#include <hgraph/types/v2/ts_value.h>
#include <hgraph/types/v2/ts_value_impl.h>

namespace hgraph
{
    // TSOutput constructor
    TSOutput::TSOutput(Notifiable *parent, const std::type_info &value_type)
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

    void TSOutput::set_parent(Notifiable *parent) { _parent = parent; }

    void TSOutput::subscribe(Notifiable *notifier) { _impl->add_subscriber(notifier); }

    void TSOutput::unsubscribe(Notifiable *notifier) { _impl->remove_subscriber(notifier); }

    void TSOutput::notify_parent(engine_time_t t) const {
        if (_parent) { _parent->notify(t); }
    }

    // TSInput constructor
    TSInput::TSInput(Notifiable *parent, const std::type_info &value_type)
        : _impl(std::make_shared<NonBoundTSValue>(value_type)), _parent(parent) {}

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
        if (was_active) { _impl->add_subscriber(reinterpret_cast<Notifiable *>(this)); }
    }

    void TSInput::make_sampled(bool use_active_guard) {
        // Are we already sampled?
        if (is_sampled(_impl)) {
            // If we are already sampled, we can assume it happened in this engine
            // cycle (since we have post-cycle clean-up, so just ignore
            return;
        }
        auto tm{current_time()};
        _impl = std::make_shared<SampledTSValue>(_impl, tm);
        // Register a cleanup handler, as we don't want to keep this indefinitely
        add_after_evaluation_notification([this]() {
            auto *impl = dynamic_cast<SampledTSValue *>(_impl.get());
            if (impl != nullptr) { impl->swap(_impl); }
        });

        if (!use_active_guard || active()) { notify(tm); }
    }

    void TSInput::add_before_evaluation_notification(std::function<void()> &&fn) const {
        dynamic_cast<EvaluationScheduler *>(_parent)->add_before_evaluation_notification(std::move(fn));
    }

    void TSInput::add_after_evaluation_notification(std::function<void()> &&fn) const {
        dynamic_cast<EvaluationScheduler *>(_parent)->add_after_evaluation_notification(std::move(fn));
    }

    void TSInput::bind(impl_ptr &other) {
        // If the other is a reference and we are not ...
        //if (other->is_value_instanceof(reference_value))
        // Type validation: ensure input and output types match
        if (!_impl->is_value_instanceof(other)) {
            throw std::runtime_error(std::string("Type mismatch in bind_output: input expects ") + _impl->value_type().name() +
                                     " but output provides " + other->value_type().name());
        }

        // Get active-state from current impl before switching
        bool was_active{active()};

        // Mark passive on old impl
        if (was_active) { make_passive(); }

        // Bind to new impl
        _impl = other;

        // Restore active state on new impl
        if (was_active) { make_active(); }
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

    void TSInput::notify(engine_time_t t) {
        // Notify parent to schedule owning node
        if (_parent) { _parent->notify(t); }
    }

    engine_time_t TSInput::current_time() const {
        auto *provider = dynamic_cast<CurrentTimeProvider *>(_parent);
        if (!provider) { throw std::runtime_error("Parent does not implement CurrentTimeProvider"); }
        return provider->current_engine_time();
    }

    Notifiable *TSInput::parent() const { return _parent; }

    void TSInput::set_parent(Notifiable *parent) { _parent = parent; }

    bool TSInput::bound() const {
        if (is_sampled(_impl)) { return !is_non_bound(static_cast<const SampledTSValue *>(_impl.get())->delegate()); }
        return !is_non_bound(_impl);
    }

}  // namespace hgraph