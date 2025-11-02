#include "hgraph/types/v2/ts_value.h"
#include "hgraph/types/v2/ts_value_impl.h"

namespace hgraph
{
    // TSOutput constructor
    TSOutput::TSOutput(Notifiable *parent, const std::type_info& value_type)
        : _impl(std::make_shared<SimplePeeredImpl>(value_type))
        , _parent(parent)
    {
        if (!_parent) {
            throw std::runtime_error("Parent cannot be null");
        }
    }

    const AnyValue<> & TSOutput::value() const { return _impl->value(); }

    void TSOutput::set_value(const AnyValue<> &v) {
        auto event = TsEventAny::modify(current_time(), v);
        _impl->apply_event(event);
    }

    void TSOutput::set_value(AnyValue<> &&v) {
        auto event = TsEventAny::modify(current_time(), std::move(v));
        _impl->apply_event(event);
    }

    void TSOutput::invalidate() { _impl->mark_invalid(current_time()); }

    bool TSOutput::modified() const { return _impl->modified(current_time()); }

    bool TSOutput::valid() const { return _impl->valid(); }

    engine_time_t TSOutput::last_modified_time() const { return _impl->last_modified_time(); }

    TsEventAny TSOutput::delta_value() const { return _impl->query_event(current_time()); }

    TSOutput::impl_ptr TSOutput::get_impl() const { return _impl; }

    engine_time_t TSOutput::current_time() const {
        auto *provider = dynamic_cast<CurrentTimeProvider *>(_parent);
        if (!provider) { throw std::runtime_error("Parent does not implement CurrentTimeProvider"); }
        return provider->current_engine_time();
    }

    // TSInput constructor
    TSInput::TSInput(Notifiable *parent, const std::type_info& value_type)
        : _impl(std::make_shared<NonBoundImpl>(value_type))
        , _parent(parent)
    {
    }

    // TSInput::bind_output implementation
    void TSInput::bind_output(TSOutput *output)
    {
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
        if (was_active) {
            _impl->mark_passive(reinterpret_cast<Notifiable *>(this));
        }

        // Bind to new impl
        _impl = output->get_impl();

        // Restore active state on new impl
        if (was_active) {
            _impl->mark_active(reinterpret_cast<Notifiable *>(this));
        }
    }

    const AnyValue<> & TSInput::value() const { return _impl->value(); }

    bool TSInput::modified() const { return _impl ? _impl->modified(current_time()) : false; }

    bool TSInput::valid() const { return _impl ? _impl->valid() : false; }

    engine_time_t TSInput::last_modified_time() const { return _impl ? _impl->last_modified_time() : min_time(); }

    TsEventAny TSInput::delta_value() const {
        return _impl ? _impl->query_event(current_time()) : TsEventAny::none(min_time());
    }

    bool TSInput::active() const {
        return _impl->active(reinterpret_cast<Notifiable *>(
            const_cast<TSInput *>(this)
        ));
    }

    void TSInput::mark_active() { if (_impl) { _impl->mark_active(reinterpret_cast<Notifiable *>(this)); } }

    void TSInput::mark_passive() { if (_impl) { _impl->mark_passive(reinterpret_cast<Notifiable *>(this)); } }

    void TSInput::notify(engine_time_t t) const {
        // Notify parent to schedule owning node
        _parent->notify(t);
    }

    engine_time_t TSInput::current_time() const {
        return dynamic_cast<CurrentTimeProvider *>(_parent)->current_engine_time();
    }

} // namespace hgraph
