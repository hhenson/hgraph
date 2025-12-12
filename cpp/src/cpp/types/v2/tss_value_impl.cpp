#include <hgraph/types/v2/tss_value_impl.h>
#include <stdexcept>

namespace hgraph
{

    // ============================================================================
    // BaseTSSValue Implementation
    // ============================================================================

    void BaseTSSValue::apply_event(const TsSetEventAny &event) {
        switch (event.kind) {
            case TsEventKind::Modify:
                // Apply removals first
                for (const auto &item : event.delta.removed) {
                    validate_item_type(item);
                    do_remove(item);
                }
                // Then apply additions
                for (const auto &item : event.delta.added) {
                    validate_item_type(item);
                    do_add(item);
                }
                _valid = true;
                break;

            case TsEventKind::Invalidate:
                _value.clear();
                _valid = false;
                break;

            case TsEventKind::Recover:
                // For recover, delta.added contains the full set state
                _value.clear();
                for (const auto &item : event.delta.added) {
                    validate_item_type(item);
                    _value.insert(item);
                }
                _valid = true;
                break;

            case TsEventKind::None:
                // No operation
                break;
        }

        _last_event = event;
    }

    TsSetEventAny BaseTSSValue::query_event(engine_time_t t) const {
        if (_last_event.time == t) {
            return _last_event;
        }
        return TsSetEventAny::none(t);
    }

    void BaseTSSValue::reset() {
        _value.clear();
        _last_event = TsSetEventAny{};
        _valid = false;
    }

    void BaseTSSValue::add_item(const AnyValue<> &item) {
        validate_item_type(item);
        if (do_add(item)) {
            // Update last event
            if (_last_event.kind != TsEventKind::Modify) {
                _last_event = TsSetEventAny::modify(_last_event.time);
            }
            _last_event.delta.added.push_back(item);
            _valid = true;
        }
    }

    void BaseTSSValue::remove_item(const AnyValue<> &item) {
        validate_item_type(item);
        if (do_remove(item)) {
            // Update last event
            if (_last_event.kind != TsEventKind::Modify) {
                _last_event = TsSetEventAny::modify(_last_event.time);
            }
            _last_event.delta.removed.push_back(item);
        }
    }

    void BaseTSSValue::clear_items(engine_time_t t) {
        if (!_value.empty()) {
            // Build removed list from current contents
            TsSetEventAny event = TsSetEventAny::modify(t);
            for (const auto &item : _value) {
                event.delta.removed.push_back(item);
            }
            _value.clear();
            _last_event = event;
        }
    }

    bool BaseTSSValue::contains(const AnyValue<> &item) const {
        return _value.find(item) != _value.end();
    }

    size_t BaseTSSValue::size() const {
        return _value.size();
    }

    bool BaseTSSValue::empty() const {
        return _value.empty();
    }

    bool BaseTSSValue::modified(engine_time_t t) const {
        return _last_event.time == t && _last_event.kind == TsEventKind::Modify;
    }

    bool BaseTSSValue::all_valid() const {
        return _valid;
    }

    bool BaseTSSValue::valid() const {
        return _valid;
    }

    engine_time_t BaseTSSValue::last_modified_time() const {
        return _last_event.time;
    }

    std::vector<AnyValue<>> BaseTSSValue::values() const {
        return std::vector<AnyValue<>>(_value.begin(), _value.end());
    }

    const std::vector<AnyValue<>> &BaseTSSValue::added_items() const {
        return _last_event.delta.added;
    }

    const std::vector<AnyValue<>> &BaseTSSValue::removed_items() const {
        return _last_event.delta.removed;
    }

    bool BaseTSSValue::was_added(const AnyValue<> &item) const {
        return _last_event.delta.was_added(item);
    }

    bool BaseTSSValue::was_removed(const AnyValue<> &item) const {
        return _last_event.delta.was_removed(item);
    }

    const std::type_info &BaseTSSValue::element_type() const {
        return *_element_type.info;
    }

    bool BaseTSSValue::is_element_instanceof(const std::type_info &type) const {
        return *_element_type.info == type;
    }

    void BaseTSSValue::mark_invalid(engine_time_t t) {
        _value.clear();
        _valid = false;
        _last_event = TsSetEventAny::invalidate(t);
    }

    void BaseTSSValue::validate_item_type(const AnyValue<> &item) const {
        if (item.has_value() && item.type() != _element_type) {
            throw std::runtime_error(
                "Type mismatch in TSS operation: expected " +
                std::string(_element_type.info->name()) +
                " but got " + std::string(item.type().info->name())
            );
        }
    }

    bool BaseTSSValue::do_add(const AnyValue<> &item) {
        auto [_, inserted] = _value.insert(item);
        return inserted;
    }

    bool BaseTSSValue::do_remove(const AnyValue<> &item) {
        return _value.erase(item) > 0;
    }

    // ============================================================================
    // NoneTSSValue Implementation
    // ============================================================================

    NoneTSSValue::NoneTSSValue(const std::type_info &type) : _element_type(TypeId{&type}) {}

    void NoneTSSValue::apply_event(const TsSetEventAny &) {
        throw std::runtime_error("Cannot apply event to NoneTSSValue");
    }

    TsSetEventAny NoneTSSValue::query_event(engine_time_t t) const {
        return TsSetEventAny::none(t);
    }

    void NoneTSSValue::reset() {
        // No-op for None
    }

    void NoneTSSValue::add_item(const AnyValue<> &) {
        throw std::runtime_error("Cannot add item to NoneTSSValue");
    }

    void NoneTSSValue::remove_item(const AnyValue<> &) {
        throw std::runtime_error("Cannot remove item from NoneTSSValue");
    }

    void NoneTSSValue::clear_items(engine_time_t) {
        throw std::runtime_error("Cannot clear items from NoneTSSValue");
    }

    bool NoneTSSValue::contains(const AnyValue<> &) const {
        return false;
    }

    size_t NoneTSSValue::size() const {
        return 0;
    }

    bool NoneTSSValue::empty() const {
        return true;
    }

    bool NoneTSSValue::modified(engine_time_t) const {
        return false;
    }

    bool NoneTSSValue::all_valid() const {
        return false;
    }

    bool NoneTSSValue::valid() const {
        return false;
    }

    engine_time_t NoneTSSValue::last_modified_time() const {
        return MIN_ST;
    }

    std::vector<AnyValue<>> NoneTSSValue::values() const {
        return {};
    }

    const std::vector<AnyValue<>> &NoneTSSValue::added_items() const {
        return _empty_vec;
    }

    const std::vector<AnyValue<>> &NoneTSSValue::removed_items() const {
        return _empty_vec;
    }

    bool NoneTSSValue::was_added(const AnyValue<> &) const {
        return false;
    }

    bool NoneTSSValue::was_removed(const AnyValue<> &) const {
        return false;
    }

    void NoneTSSValue::add_subscriber(Notifiable *) {
        // No-op
    }

    void NoneTSSValue::remove_subscriber(Notifiable *) {
        // No-op
    }

    bool NoneTSSValue::has_subscriber(Notifiable *) const {
        return false;
    }

    void NoneTSSValue::notify_subscribers(engine_time_t) {
        // No-op
    }

    const std::type_info &NoneTSSValue::element_type() const {
        return *_element_type.info;
    }

    bool NoneTSSValue::is_element_instanceof(const std::type_info &type) const {
        return *_element_type.info == type;
    }

    void NoneTSSValue::mark_invalid(engine_time_t) {
        // No-op for None
    }

    // ============================================================================
    // NonBoundTSSValue Implementation
    // ============================================================================

    void NonBoundTSSValue::add_subscriber(Notifiable *) {
        _active = true;
    }

    void NonBoundTSSValue::remove_subscriber(Notifiable *) {
        _active = false;
    }

    bool NonBoundTSSValue::has_subscriber(Notifiable *) const {
        return _active;
    }

    void NonBoundTSSValue::notify_subscribers(engine_time_t) {
        // Non-bound has no subscribers to notify
    }

    // ============================================================================
    // PeeredTSSValue Implementation
    // ============================================================================

    void PeeredTSSValue::add_subscriber(Notifiable *subscriber) {
        _subscribers.insert(subscriber);
    }

    void PeeredTSSValue::remove_subscriber(Notifiable *subscriber) {
        _subscribers.erase(subscriber);
    }

    bool PeeredTSSValue::has_subscriber(Notifiable *subscriber) const {
        return _subscribers.find(subscriber) != _subscribers.end();
    }

    void PeeredTSSValue::notify_subscribers(engine_time_t t) {
        for (auto *subscriber : _subscribers) {
            subscriber->notify(t);
        }
    }

    // ============================================================================
    // DelegateTSSValue Implementation
    // ============================================================================

    DelegateTSSValue::DelegateTSSValue(TSSValue::s_ptr delegate) : _delegate(std::move(delegate)) {}

    TsSetEventAny DelegateTSSValue::query_event(engine_time_t t) const {
        return _delegate->query_event(t);
    }

    void DelegateTSSValue::apply_event(const TsSetEventAny &event) {
        _delegate->apply_event(event);
    }

    void DelegateTSSValue::reset() {
        _delegate->reset();
    }

    void DelegateTSSValue::add_item(const AnyValue<> &item) {
        _delegate->add_item(item);
    }

    void DelegateTSSValue::remove_item(const AnyValue<> &item) {
        _delegate->remove_item(item);
    }

    void DelegateTSSValue::clear_items(engine_time_t t) {
        _delegate->clear_items(t);
    }

    bool DelegateTSSValue::contains(const AnyValue<> &item) const {
        return _delegate->contains(item);
    }

    size_t DelegateTSSValue::size() const {
        return _delegate->size();
    }

    bool DelegateTSSValue::empty() const {
        return _delegate->empty();
    }

    bool DelegateTSSValue::modified(engine_time_t t) const {
        return _delegate->modified(t);
    }

    bool DelegateTSSValue::all_valid() const {
        return _delegate->all_valid();
    }

    bool DelegateTSSValue::valid() const {
        return _delegate->valid();
    }

    engine_time_t DelegateTSSValue::last_modified_time() const {
        return _delegate->last_modified_time();
    }

    std::vector<AnyValue<>> DelegateTSSValue::values() const {
        return _delegate->values();
    }

    const std::vector<AnyValue<>> &DelegateTSSValue::added_items() const {
        return _delegate->added_items();
    }

    const std::vector<AnyValue<>> &DelegateTSSValue::removed_items() const {
        return _delegate->removed_items();
    }

    bool DelegateTSSValue::was_added(const AnyValue<> &item) const {
        return _delegate->was_added(item);
    }

    bool DelegateTSSValue::was_removed(const AnyValue<> &item) const {
        return _delegate->was_removed(item);
    }

    void DelegateTSSValue::add_subscriber(Notifiable *subscriber) {
        _local_subscribers.insert(subscriber);
    }

    void DelegateTSSValue::remove_subscriber(Notifiable *subscriber) {
        _local_subscribers.erase(subscriber);
    }

    bool DelegateTSSValue::has_subscriber(Notifiable *subscriber) const {
        return _local_subscribers.find(subscriber) != _local_subscribers.end();
    }

    void DelegateTSSValue::notify_subscribers(engine_time_t t) {
        for (auto *subscriber : _local_subscribers) {
            subscriber->notify(t);
        }
    }

    const std::type_info &DelegateTSSValue::element_type() const {
        return _delegate->element_type();
    }

    bool DelegateTSSValue::is_element_instanceof(const std::type_info &type) const {
        return _delegate->is_element_instanceof(type);
    }

    void DelegateTSSValue::mark_invalid(engine_time_t t) {
        _delegate->mark_invalid(t);
    }

    void DelegateTSSValue::swap(TSSValue::s_ptr other) {
        _delegate = std::move(other);
    }

    const TSSValue::s_ptr &DelegateTSSValue::delegate() const {
        return _delegate;
    }

    // ============================================================================
    // SampledTSSValue Implementation
    // ============================================================================

    bool SampledTSSValue::modified(engine_time_t t) const {
        return t == _sampled_time || DelegateTSSValue::modified(t);
    }

    engine_time_t SampledTSSValue::last_modified_time() const {
        return _sampled_time;
    }

    void SampledTSSValue::add_subscriber(Notifiable *subscriber) {
        // Sampled values don't propagate subscriptions to delegate
        _local_subscribers.insert(subscriber);
    }

    void SampledTSSValue::remove_subscriber(Notifiable *subscriber) {
        _local_subscribers.erase(subscriber);
    }

    bool SampledTSSValue::has_subscriber(Notifiable *subscriber) const {
        return _local_subscribers.find(subscriber) != _local_subscribers.end();
    }

    void SampledTSSValue::notify_subscribers(engine_time_t t) {
        for (auto *subscriber : _local_subscribers) {
            subscriber->notify(t);
        }
    }

}  // namespace hgraph
