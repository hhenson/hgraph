#include <hgraph/types/v2/tss_value.h>
#include <hgraph/types/v2/tss_value_impl.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <stdexcept>

namespace hgraph
{

    // ============================================================================
    // TSSRefOutputManager Implementation
    // ============================================================================

    TSSRefOutputManager::TSSRefOutputManager(NotifiableContext *owner, const std::type_info &)
        : _owner(owner) {}

    TSSRefOutputManager::~TSSRefOutputManager() = default;

    TSOutput *TSSRefOutputManager::get_contains_output(const AnyValue<> &item) {
        auto it = _contains_outputs.find(item);
        if (it != _contains_outputs.end()) {
            it->second.ref_count++;
            return it->second.output.get();
        }

        // Create new contains output
        auto output = std::make_unique<TSOutput>(_owner, typeid(bool));
        auto *ptr = output.get();
        _contains_outputs[item] = ContainsEntry{std::move(output), 1};
        return ptr;
    }

    void TSSRefOutputManager::release_contains_output(const AnyValue<> &item) {
        auto it = _contains_outputs.find(item);
        if (it != _contains_outputs.end()) {
            if (--it->second.ref_count == 0) {
                _contains_outputs.erase(it);
            }
        }
    }

    TSOutput &TSSRefOutputManager::is_empty_output() {
        if (!_is_empty_output) {
            _is_empty_output = std::make_unique<TSOutput>(_owner, typeid(bool));
        }
        return *_is_empty_output;
    }

    void TSSRefOutputManager::on_items_added(const std::vector<AnyValue<>> &items, engine_time_t t) {
        for (const auto &item : items) {
            auto it = _contains_outputs.find(item);
            if (it != _contains_outputs.end()) {
                // Set contains to true
                AnyValue<> val;
                val.emplace<bool>(true);
                it->second.output->set_value(std::move(val));
            }
        }
    }

    void TSSRefOutputManager::on_items_removed(const std::vector<AnyValue<>> &items, engine_time_t t) {
        for (const auto &item : items) {
            auto it = _contains_outputs.find(item);
            if (it != _contains_outputs.end()) {
                // Set contains to false
                AnyValue<> val;
                val.emplace<bool>(false);
                it->second.output->set_value(std::move(val));
            }
        }
    }

    void TSSRefOutputManager::on_cleared(engine_time_t t) {
        // All tracked items removed
        for (auto &[item, entry] : _contains_outputs) {
            AnyValue<> val;
            val.emplace<bool>(false);
            entry.output->set_value(std::move(val));
        }
    }

    void TSSRefOutputManager::on_became_non_empty(engine_time_t t) {
        if (_is_empty_output) {
            AnyValue<> val;
            val.emplace<bool>(false);
            _is_empty_output->set_value(std::move(val));
        }
    }

    void TSSRefOutputManager::on_became_empty(engine_time_t t) {
        if (_is_empty_output) {
            AnyValue<> val;
            val.emplace<bool>(true);
            _is_empty_output->set_value(std::move(val));
        }
    }

    // ============================================================================
    // TSSOutput Implementation
    // ============================================================================

    TSSOutput::TSSOutput(NotifiableContext *owner, const std::type_info &element_type)
        : _impl(std::make_shared<PeeredTSSValue>(element_type)),
          _owner(owner),
          _ref_outputs(std::make_unique<TSSRefOutputManager>(owner, typeid(bool))) {
        if (!owner) {
            throw std::runtime_error("TSSOutput requires non-null owner");
        }
    }

    TSSOutput::TSSOutput(TSSOutput &&) noexcept = default;
    TSSOutput &TSSOutput::operator=(TSSOutput &&) noexcept = default;
    TSSOutput::~TSSOutput() = default;

    void TSSOutput::add(const AnyValue<> &item) {
        bool was_empty = _impl->empty();
        _impl->add_item(item);

        // Update reference outputs
        if (_ref_outputs) {
            _ref_outputs->on_items_added({item}, current_time());
            if (was_empty && !_impl->empty()) {
                _ref_outputs->on_became_non_empty(current_time());
            }
        }

        // Notify
        _impl->notify_subscribers(current_time());
        notify_parent(current_time());
    }

    void TSSOutput::remove(const AnyValue<> &item) {
        bool was_non_empty = !_impl->empty();
        _impl->remove_item(item);

        // Update reference outputs
        if (_ref_outputs) {
            _ref_outputs->on_items_removed({item}, current_time());
            if (was_non_empty && _impl->empty()) {
                _ref_outputs->on_became_empty(current_time());
            }
        }

        // Notify
        _impl->notify_subscribers(current_time());
        notify_parent(current_time());
    }

    void TSSOutput::clear() {
        if (!_impl->empty()) {
            bool was_non_empty = !_impl->empty();
            _impl->clear_items(current_time());

            // Update reference outputs
            if (_ref_outputs) {
                _ref_outputs->on_cleared(current_time());
                if (was_non_empty) {
                    _ref_outputs->on_became_empty(current_time());
                }
            }

            // Notify
            _impl->notify_subscribers(current_time());
            notify_parent(current_time());
        }
    }

    void TSSOutput::set_delta(const std::vector<AnyValue<>> &added_items, const std::vector<AnyValue<>> &removed_items) {
        bool was_empty = _impl->empty();

        TsSetEventAny event = TsSetEventAny::modify(current_time());
        event.delta.added = added_items;
        event.delta.removed = removed_items;
        _impl->apply_event(event);

        bool is_empty = _impl->empty();

        // Update reference outputs
        if (_ref_outputs) {
            _ref_outputs->on_items_removed(removed_items, current_time());
            _ref_outputs->on_items_added(added_items, current_time());

            if (was_empty && !is_empty) {
                _ref_outputs->on_became_non_empty(current_time());
            } else if (!was_empty && is_empty) {
                _ref_outputs->on_became_empty(current_time());
            }
        }

        // Notify
        _impl->notify_subscribers(current_time());
        notify_parent(current_time());
    }

    void TSSOutput::apply_event(const TsSetEventAny &event) {
        bool was_empty = _impl->empty();
        _impl->apply_event(event);
        bool is_empty = _impl->empty();

        // Update reference outputs
        if (_ref_outputs && event.kind == TsEventKind::Modify) {
            _ref_outputs->on_items_removed(event.delta.removed, current_time());
            _ref_outputs->on_items_added(event.delta.added, current_time());

            if (was_empty && !is_empty) {
                _ref_outputs->on_became_non_empty(current_time());
            } else if (!was_empty && is_empty) {
                _ref_outputs->on_became_empty(current_time());
            }
        }

        // Notify
        _impl->notify_subscribers(current_time());
        notify_parent(current_time());
    }

    void TSSOutput::invalidate() {
        bool was_non_empty = !_impl->empty();
        _impl->mark_invalid(current_time());

        // Update reference outputs
        if (_ref_outputs) {
            _ref_outputs->on_cleared(current_time());
            if (was_non_empty) {
                _ref_outputs->on_became_empty(current_time());
            }
        }

        // Notify
        _impl->notify_subscribers(current_time());
        notify_parent(current_time());
    }

    void TSSOutput::reset() {
        _impl->reset();
    }

    bool TSSOutput::contains(const AnyValue<> &item) const {
        return _impl->contains(item);
    }

    size_t TSSOutput::size() const {
        return _impl->size();
    }

    bool TSSOutput::empty() const {
        return _impl->empty();
    }

    bool TSSOutput::modified() const {
        return _impl->modified(current_time());
    }

    bool TSSOutput::valid() const {
        return _impl->valid();
    }

    engine_time_t TSSOutput::last_modified_time() const {
        return _impl->last_modified_time();
    }

    std::vector<AnyValue<>> TSSOutput::values() const {
        return _impl->values();
    }

    const std::vector<AnyValue<>> &TSSOutput::added() const {
        return _impl->added_items();
    }

    const std::vector<AnyValue<>> &TSSOutput::removed() const {
        return _impl->removed_items();
    }

    bool TSSOutput::was_added(const AnyValue<> &item) const {
        return _impl->was_added(item);
    }

    bool TSSOutput::was_removed(const AnyValue<> &item) const {
        return _impl->was_removed(item);
    }

    TsSetEventAny TSSOutput::delta_value() const {
        return _impl->query_event(current_time());
    }

    TSOutput *TSSOutput::get_contains_output(const AnyValue<> &item) {
        return _ref_outputs ? _ref_outputs->get_contains_output(item) : nullptr;
    }

    void TSSOutput::release_contains_output(const AnyValue<> &item) {
        if (_ref_outputs) {
            _ref_outputs->release_contains_output(item);
        }
    }

    TSOutput &TSSOutput::is_empty_output() {
        if (!_ref_outputs) {
            throw std::runtime_error("Reference outputs not available");
        }
        return _ref_outputs->is_empty_output();
    }

    engine_time_t TSSOutput::current_time() const {
        return _owner ? _owner->current_engine_time() : MIN_ST;
    }

    Notifiable *TSSOutput::owner() const {
        return _owner;
    }

    void TSSOutput::set_owner(NotifiableContext *owner) {
        _owner = owner;
    }

    void TSSOutput::subscribe(Notifiable *notifier) {
        _impl->add_subscriber(notifier);
    }

    void TSSOutput::unsubscribe(Notifiable *notifier) {
        _impl->remove_subscriber(notifier);
    }

    const std::type_info &TSSOutput::element_type() const {
        return _impl->element_type();
    }

    void TSSOutput::notify_parent(engine_time_t t) const {
        if (_owner) {
            _owner->notify(t);
        }
    }

    // ============================================================================
    // TSSInput Implementation
    // ============================================================================

    TSSInput::TSSInput(NotifiableContext *owner, const std::type_info &element_type)
        : _impl(std::make_shared<NonBoundTSSValue>(element_type)),
          _owner(owner),
          _prev_impl(nullptr) {
        if (!owner) {
            throw std::runtime_error("TSSInput requires non-null owner");
        }
    }

    TSSInput::~TSSInput() {
        if (_impl && active()) {
            make_passive();
        }
    }

    TSSInput::TSSInput(TSSInput &&) noexcept = default;
    TSSInput &TSSInput::operator=(TSSInput &&) noexcept = default;

    void TSSInput::bind_output(TSSOutput &output) {
        // Validate types match
        if (_impl->element_type() != output.element_type()) {
            throw std::runtime_error(
                "Type mismatch in TSSInput::bind_output: input expects " +
                std::string(_impl->element_type().name()) +
                " but output provides " + std::string(output.element_type().name())
            );
        }

        // Capture current active state
        bool was_active = active();

        // If currently active, unsubscribe from current impl
        if (was_active) {
            _impl->remove_subscriber(this);
        }

        // Store previous impl for delta computation
        _prev_impl = _impl;

        // Share output's impl
        _impl = output.get_impl();

        // Restore active state on new impl
        if (was_active) {
            _impl->add_subscriber(this);
        }
    }

    void TSSInput::copy_from_input(TSSInput &input) {
        bind(input._impl);
    }

    void TSSInput::unbind() {
        // Capture current active state
        bool was_active = active();

        // Unsubscribe if active
        if (was_active) {
            _impl->remove_subscriber(this);
        }

        // Create new non-bound impl
        _prev_impl = _impl;
        _impl = std::make_shared<NonBoundTSSValue>(_impl->element_type());

        // Restore active state
        if (was_active) {
            _impl->add_subscriber(this);
        }
    }

    bool TSSInput::bound() const {
        return is_bound_tss(_impl);
    }

    bool TSSInput::active() const {
        return _impl->has_subscriber(reinterpret_cast<Notifiable *>(const_cast<TSSInput *>(this)));
    }

    void TSSInput::make_active() {
        if (!active()) {
            _impl->add_subscriber(reinterpret_cast<Notifiable *>(this));
        }
    }

    void TSSInput::make_passive() {
        if (active()) {
            _impl->remove_subscriber(reinterpret_cast<Notifiable *>(this));
        }
    }

    bool TSSInput::contains(const AnyValue<> &item) const {
        return _impl->contains(item);
    }

    size_t TSSInput::size() const {
        return _impl->size();
    }

    bool TSSInput::empty() const {
        return _impl->empty();
    }

    bool TSSInput::modified() const {
        return _impl->modified(current_time());
    }

    bool TSSInput::valid() const {
        return _impl->valid();
    }

    engine_time_t TSSInput::last_modified_time() const {
        return _impl->last_modified_time();
    }

    std::vector<AnyValue<>> TSSInput::values() const {
        return _impl->values();
    }

    std::vector<AnyValue<>> TSSInput::added() const {
        if (!_prev_impl) {
            return _impl->added_items();
        }

        // Compute delta from previous state
        std::vector<AnyValue<>> result;
        auto current_values = _impl->values();

        for (const auto &item : current_values) {
            // Item is "added" if it's in current but wasn't in prev
            // (accounting for prev's pending changes)
            bool was_in_prev = _prev_impl->contains(item) ||
                               _prev_impl->was_removed(item);
            was_in_prev = was_in_prev && !_prev_impl->was_added(item);

            if (!was_in_prev) {
                result.push_back(item);
            }
        }

        return result;
    }

    std::vector<AnyValue<>> TSSInput::removed() const {
        if (!_prev_impl) {
            return _impl->removed_items();
        }

        // Compute delta from previous state
        std::vector<AnyValue<>> result;
        auto prev_values = _prev_impl->values();

        for (const auto &item : prev_values) {
            // Item is "removed" if it was in prev but isn't in current
            if (!_impl->contains(item)) {
                result.push_back(item);
            }
        }

        return result;
    }

    bool TSSInput::was_added(const AnyValue<> &item) const {
        if (!_prev_impl) {
            return _impl->was_added(item);
        }

        // Check if item is now present but wasn't before
        bool is_present = _impl->contains(item);
        bool was_present = _prev_impl->contains(item) ||
                          (_prev_impl->was_removed(item) && !_prev_impl->was_added(item));

        return is_present && !was_present;
    }

    bool TSSInput::was_removed(const AnyValue<> &item) const {
        if (!_prev_impl) {
            return _impl->was_removed(item);
        }

        // Check if item was present but isn't now
        bool is_present = _impl->contains(item);
        bool was_present = _prev_impl->contains(item);

        return was_present && !is_present;
    }

    TsSetEventAny TSSInput::delta_value() const {
        TsSetEventAny event = TsSetEventAny::modify(current_time());
        event.delta.added = added();
        event.delta.removed = removed();
        return event;
    }

    void TSSInput::notify(engine_time_t et) {
        if (_owner) {
            _owner->notify(et);
        }
    }

    engine_time_t TSSInput::current_time() const {
        return _owner ? _owner->current_engine_time() : MIN_ST;
    }

    NotifiableContext *TSSInput::owner() const {
        return _owner;
    }

    void TSSInput::set_owner(NotifiableContext *owner) {
        _owner = owner;
    }

    void TSSInput::subscribe(Notifiable *notifier) {
        _impl->add_subscriber(notifier);
    }

    void TSSInput::unsubscribe(Notifiable *notifier) {
        _impl->remove_subscriber(notifier);
    }

    const std::type_info &TSSInput::element_type() const {
        return _impl->element_type();
    }

    void TSSInput::add_before_evaluation_notification(std::function<void()> &&fn) const {
        if (_owner) {
            _owner->add_before_evaluation_notification(std::move(fn));
        }
    }

    void TSSInput::add_after_evaluation_notification(std::function<void()> &&fn) const {
        if (_owner) {
            _owner->add_after_evaluation_notification(std::move(fn));
        }
    }

    void TSSInput::bind(impl_ptr &other) {
        // Capture current active state
        bool was_active = active();

        // Unsubscribe from current
        if (was_active) {
            _impl->remove_subscriber(this);
        }

        // Store prev and bind new
        _prev_impl = _impl;
        _impl = other;

        // Restore active
        if (was_active) {
            _impl->add_subscriber(this);
        }
    }

}  // namespace hgraph
