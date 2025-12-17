//
// Created by Claude on 16/12/2025.
//
// TSInput implementation - V2 time-series input with binding strategies
//

#include <hgraph/types/time_series/v2/ts_input.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/node.h>

namespace hgraph::ts {

// ============================================================================
// PeeredStrategy implementation
// ============================================================================

void PeeredStrategy::make_active() {
    if (_peer_output && _input) {
        _peer_output->subscribe(_input);
    }
}

void PeeredStrategy::make_passive() {
    if (_peer_output && _input) {
        _peer_output->unsubscribe(_input);
    }
}

void PeeredStrategy::unbind() {
    if (_input && _input->active() && _peer_output) {
        make_passive();
    }
    _peer_output = nullptr;
}

// ============================================================================
// NonPeeredStrategy implementation
// ============================================================================

bool NonPeeredStrategy::has_value() const {
    // True if any element has value
    for (auto* output : _element_outputs) {
        if (output && output->has_value()) {
            return true;
        }
    }
    return false;
}

bool NonPeeredStrategy::modified_at(engine_time_t time) const {
    // True if any element is modified
    for (auto* output : _element_outputs) {
        if (output && output->modified_at(time)) {
            return true;
        }
    }
    return false;
}

engine_time_t NonPeeredStrategy::last_modified_time() const {
    // Maximum of all elements' last_modified_time
    engine_time_t max_time = MIN_DT;
    for (auto* output : _element_outputs) {
        if (output) {
            max_time = std::max(max_time, output->last_modified_time());
        }
    }
    return max_time;
}

value::ConstValueView NonPeeredStrategy::value() const {
    // For non-peered, return value from the input's own storage
    if (_input && _input->has_storage()) {
        return _input->storage().value();
    }
    return {};
}

value::ModificationTracker NonPeeredStrategy::tracker() const {
    // For non-peered, return tracker from the input's own storage
    if (_input && _input->has_storage()) {
        return _input->storage().underlying_tracker().tracker();
    }
    return {};
}

void NonPeeredStrategy::make_active() {
    // Subscribe to all bound element outputs
    for (auto* output : _element_outputs) {
        if (output && _input) {
            output->subscribe(_input);
        }
    }
}

void NonPeeredStrategy::make_passive() {
    // Unsubscribe from all element outputs
    for (auto* output : _element_outputs) {
        if (output && _input) {
            output->unsubscribe(_input);
        }
    }
}

void NonPeeredStrategy::unbind() {
    if (_input && _input->active()) {
        make_passive();
    }
    // Clear all element bindings
    for (auto& output : _element_outputs) {
        output = nullptr;
    }
}

void NonPeeredStrategy::bind_element(size_t index, TSOutput* output) {
    if (index >= _element_outputs.size()) {
        return;
    }

    // Unsubscribe from previous output at this index
    if (_element_outputs[index] && _input && _input->active()) {
        _element_outputs[index]->unsubscribe(_input);
    }

    _element_outputs[index] = output;

    // Update storage with view into output
    if (_input && _input->has_storage() && output) {
        // Get the storage view and update the element
        auto storage_view = _input->storage().view();
        auto elem_view = storage_view.element(index);
        if (elem_view.valid()) {
            // TODO: Set element view to point to output's value
            // This requires TimeSeriesValueView to support setting element views
            // For now, the binding is tracked in _element_outputs
        }
    }

    // Subscribe if active
    if (_input && _input->active() && output) {
        output->subscribe(_input);
    }
}

void NonPeeredStrategy::unbind_element(size_t index) {
    if (index >= _element_outputs.size()) {
        return;
    }

    if (_element_outputs[index] && _input && _input->active()) {
        _element_outputs[index]->unsubscribe(_input);
    }

    _element_outputs[index] = nullptr;
}

// ============================================================================
// RefObserverStrategy implementation
// ============================================================================

void RefObserverStrategy::make_active() {
    // Always ensure we're subscribed to the ref output for reference changes
    // (this is done in subscribe_to_ref, called when strategy is set)

    // Subscribe to current target for value changes (only when active)
    if (_target_output && _input) {
        _target_output->subscribe(_input);
    }
}

void RefObserverStrategy::make_passive() {
    // Unsubscribe from current target (but remain subscribed to ref output)
    if (_target_output && _input) {
        _target_output->unsubscribe(_input);
    }
}

void RefObserverStrategy::unbind() {
    // Unsubscribe from target if active
    if (_input && _input->active() && _target_output) {
        _target_output->unsubscribe(_input);
    }

    // Always unsubscribe from ref output
    if (_ref_output && _input) {
        _ref_output->unsubscribe(_input);
    }

    _ref_output = nullptr;
    _target_output = nullptr;
    _sample_time = MIN_DT;
}

void RefObserverStrategy::on_reference_changed(TSOutput* new_target, engine_time_t time) {
    // Unsubscribe from old target (only if active)
    if (_target_output && _input && _input->active()) {
        _target_output->unsubscribe(_input);
    }

    // Bind to new target
    _target_output = new_target;
    _sample_time = time;

    // Subscribe to new target (only if active)
    if (_target_output && _input && _input->active()) {
        _target_output->subscribe(_input);
    }

    // Notify the owning node that the input changed
    if (_input) {
        _input->notify(time);
    }
}

// ============================================================================
// RefWrapperStrategy implementation
// ============================================================================

value::ConstValueView RefWrapperStrategy::value() const {
    // Return the TimeSeriesReference value from input's storage
    if (_input && _input->has_storage()) {
        return _input->storage().value();
    }
    return {};
}

value::ModificationTracker RefWrapperStrategy::tracker() const {
    // Return tracker from input's storage
    if (_input && _input->has_storage()) {
        return _input->storage().underlying_tracker().tracker();
    }
    return {};
}

void RefWrapperStrategy::unbind() {
    _wrapped_output = nullptr;
    _bind_time = MIN_DT;
}

// ============================================================================
// TSInput implementation
// ============================================================================

bool TSInput::bound() const {
    if (_strategy == &UnboundStrategy::instance()) {
        return false;
    }

    if (_strategy->has_peer()) {
        return true;  // Peered = bound
    }

    // Non-peered: check if any element output is bound
    if (auto* non_peered = dynamic_cast<NonPeeredStrategy*>(_strategy)) {
        for (size_t i = 0; i < non_peered->element_count(); ++i) {
            if (non_peered->element_output(i) != nullptr) {
                return true;
            }
        }
    }

    // RefObserver/RefWrapper are always considered bound
    if (dynamic_cast<RefObserverStrategy*>(_strategy) ||
        dynamic_cast<RefWrapperStrategy*>(_strategy)) {
        return true;
    }

    return false;
}

void TSInput::bind_output(TSOutput* output) {
    if (!output) {
        unbind_output();
        return;
    }

    // First, unbind any existing binding
    if (bound()) {
        bool was_active = _active;
        unbind_output();
        _active = was_active;  // Preserve activation state
    }

    // Determine which strategy to use based on type compatibility
    bool input_is_ref = _meta && _meta->is_reference();
    bool output_is_ref = output->ts_kind() == TimeSeriesKind::REF;

    if (output_is_ref && !input_is_ref) {
        // REF-Observer: normal TS input bound to REF output
        set_strategy_ref_observer(output);
    }
    else if (input_is_ref && !output_is_ref) {
        // REF-Wrapper: REF input bound to non-REF output
        // TODO: Get current time from evaluation context
        set_strategy_ref_wrapper(output, MIN_DT);
    }
    else {
        // Peered: standard single-output binding (including REF-to-REF)
        set_strategy_peered(output);
    }

    // If active, activate the new strategy
    if (_active) {
        _strategy->make_active();
    }
}

void TSInput::bind_element(size_t index, TSOutput* output) {
    // Only valid for collection types
    if (!_meta) return;
    TimeSeriesKind kind = _meta->ts_kind;
    if (kind != TimeSeriesKind::TSL && kind != TimeSeriesKind::TSB) {
        return;
    }

    // Ensure we're in non-peered mode
    auto* non_peered = dynamic_cast<NonPeeredStrategy*>(_strategy);
    if (!non_peered) {
        // Switch to non-peered mode
        bool was_active = _active;
        if (_strategy != &UnboundStrategy::instance()) {
            _strategy->unbind();
        }
        set_strategy_non_peered(get_element_count());
        non_peered = std::get_if<NonPeeredStrategy>(&_strategy_storage);
        if (was_active) {
            _active = true;
            _strategy->make_active();
        }
    }

    if (non_peered) {
        non_peered->bind_element(index, output);
    }
}

void TSInput::unbind_output() {
    bool was_active = _active;

    if (_strategy != &UnboundStrategy::instance()) {
        _strategy->unbind();
    }

    set_strategy_unbound();
    _active = was_active;  // Preserve the activation request (will apply on next bind)
}

void TSInput::make_active() {
    if (_active) return;
    _active = true;
    _strategy->make_active();
}

void TSInput::make_passive() {
    if (!_active) return;
    _strategy->make_passive();
    _active = false;
}

void TSInput::notify(engine_time_t time) {
    // Propagate notification to owning node if active
    if (_active && _owning_node) {
        _owning_node->notify(time);
    }
}

TSInputView TSInput::view() const {
    if (!bound()) {
        return {};
    }

    auto val = _strategy->value();
    auto trk = _strategy->tracker();

    return {
        val,
        trk,
        _meta,
        NavigationPath(nullptr)  // Input views track from input, not output
    };
}

size_t TSInput::get_element_count() const {
    if (!_meta) return 0;

    switch (_meta->ts_kind) {
        case TimeSeriesKind::TSL: {
            auto* tsl_meta = static_cast<const TSLTypeMeta*>(_meta);
            return tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : 0;
        }
        case TimeSeriesKind::TSB: {
            auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
            return tsb_meta->fields.size();
        }
        default:
            return 0;
    }
}

void TSInput::set_strategy_unbound() {
    _strategy_storage = std::monostate{};
    _strategy = &UnboundStrategy::instance();
}

void TSInput::set_strategy_peered(TSOutput* output) {
    _strategy_storage = PeeredStrategy(this, output);
    _strategy = std::get_if<PeeredStrategy>(&_strategy_storage);
}

void TSInput::set_strategy_non_peered(size_t element_count) {
    _strategy_storage = NonPeeredStrategy(this, element_count);
    _strategy = std::get_if<NonPeeredStrategy>(&_strategy_storage);

    // Create storage for non-peered mode if not already present
    if (!_storage && _meta && _meta->value_schema()) {
        _storage.emplace(_meta->value_schema());
    }
}

void TSInput::set_strategy_ref_observer(TSOutput* ref_output) {
    _strategy_storage = RefObserverStrategy(this, ref_output);
    _strategy = std::get_if<RefObserverStrategy>(&_strategy_storage);

    // Always subscribe to ref output for reference changes (regardless of active state)
    if (ref_output) {
        ref_output->subscribe(this);
    }
}

void TSInput::set_strategy_ref_wrapper(TSOutput* output, engine_time_t time) {
    _strategy_storage = RefWrapperStrategy(this, output, time);
    _strategy = std::get_if<RefWrapperStrategy>(&_strategy_storage);

    // Create storage for REF value if not already present
    if (!_storage && _meta && _meta->value_schema()) {
        _storage.emplace(_meta->value_schema());
    }
}

} // namespace hgraph::ts
