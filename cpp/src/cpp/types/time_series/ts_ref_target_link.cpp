//
// ts_ref_target_link.cpp - TSRefTargetLink implementation
//

#include <hgraph/types/time_series/ts_ref_target_link.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_path.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>

namespace hgraph {

// ============================================================================
// Construction
// ============================================================================

TSRefTargetLink::TSRefTargetLink(Node* node) noexcept
    : _ref_link(node)
    , _target_link(node)
{}

TSRefTargetLink::TSRefTargetLink(TSRefTargetLink&& other) noexcept
    : _ref_link(std::move(other._ref_link))
    , _target_link(std::move(other._target_link))
    , _rebind_delta(std::move(other._rebind_delta))
    , _ref_output_ptr(other._ref_output_ptr)
{
    other._ref_output_ptr = nullptr;
}

TSRefTargetLink& TSRefTargetLink::operator=(TSRefTargetLink&& other) noexcept {
    if (this != &other) {
        // Clean up current state
        unbind();

        // Move state
        _ref_link = std::move(other._ref_link);
        _target_link = std::move(other._target_link);
        _rebind_delta = std::move(other._rebind_delta);
        _ref_output_ptr = other._ref_output_ptr;

        other._ref_output_ptr = nullptr;
    }
    return *this;
}

TSRefTargetLink::~TSRefTargetLink() {
    unbind();
}

// ============================================================================
// Node Association
// ============================================================================

void TSRefTargetLink::set_node(Node* node) noexcept {
    _ref_link.set_node(node);
    _target_link.set_node(node);
}

// ============================================================================
// REF Binding
// ============================================================================

void TSRefTargetLink::bind_ref(const TSValue* ref_output, TimeSeriesReferenceOutput* ref_output_ptr, engine_time_t time) {
    if (_ref_link.output() == ref_output) {
        return;  // Already bound to this REF output
    }

    // Unbind from current if any
    unbind();

    // Store the REF output pointer for observer cleanup
    _ref_output_ptr = ref_output_ptr;

    // Bind control channel to REF output (always active)
    _ref_link.bind(ref_output);
    _ref_link.make_active();

    // NOTE: Initial target binding is handled by the caller via rebind_target().
    // The caller is responsible for:
    // 1. Registering as observer with ref_output_ptr->observe_reference()
    // 2. Resolving the initial target from the REF output's current value
    // 3. Calling rebind_target(initial_target, time)
}

void TSRefTargetLink::unbind() {
    // Unregister from REF output observer list
    if (_ref_output_ptr) {
        // Observer cleanup is handled at higher level (TimeSeriesReferenceOutput::stop_observing_reference)
        _ref_output_ptr = nullptr;
    }

    // Unbind both channels
    _ref_link.unbind();
    _target_link.unbind();

    // Clear delta storage
    _rebind_delta.reset();
}

// ============================================================================
// Target Management
// ============================================================================

void TSRefTargetLink::rebind_target(const TSValue* new_target, engine_time_t time) {
    const TSValue* old_target = _target_link.output();

    if (old_target == new_target) {
        return;  // No change
    }

    // Compute delta eagerly while both targets are valid
    if (old_target != nullptr || new_target != nullptr) {
        compute_delta(old_target, new_target);
    }

    // Preserve active state
    bool was_active = _target_link.active();

    // Unbind from old target
    _target_link.unbind();

    // Bind to new target
    if (new_target) {
        _target_link.bind(new_target);
    }

    // Restore active state
    if (was_active) {
        _target_link.make_active();
    }

    // Set sample time to track rebinding
    _target_link.set_sample_time(time);
}

// ============================================================================
// Subscription Control
// ============================================================================

void TSRefTargetLink::make_active() {
    // Only affect data channel - control channel is always active
    _target_link.make_active();
}

void TSRefTargetLink::make_passive() {
    // Only affect data channel - control channel stays active
    _target_link.make_passive();
}

// ============================================================================
// State Queries
// ============================================================================

bool TSRefTargetLink::valid() const {
    // Valid if target link is valid (has been set)
    return _target_link.valid();
}

bool TSRefTargetLink::modified_at(engine_time_t time) const {
    // Modified if either channel was modified at this time
    // This ensures rebinding shows as modified
    return last_modified_time() == time;
}

// ============================================================================
// View Access
// ============================================================================

TSView TSRefTargetLink::view() const {
    // Delegate to target link - user sees target data
    // If target_link is bound, use it; otherwise fall back to ref_link
    // (This handles cases where only ref_link is bound for notifications)
    if (_target_link.bound()) {
        return _target_link.view();
    }
    // Fall back to ref_link view if target is not bound
    return _ref_link.view();
}

// ============================================================================
// Private Helpers
// ============================================================================

void TSRefTargetLink::ensure_delta_storage() {
    if (!_rebind_delta) {
        _rebind_delta = std::make_unique<RebindDelta>();
    }
}

void TSRefTargetLink::compute_delta(const TSValue* old_target, const TSValue* new_target) {
    // For now, we don't compute actual delta - this requires type-specific logic
    // that will be implemented when integrating with specific collection types.
    //
    // The RebindDelta struct provides storage for:
    // - TSS: added_values, removed_values (set difference)
    // - TSD: added_keys, removed_keys (key set difference)
    // - TSL/TSB: changed_indices (indices that differ)
    //
    // Specialized inputs (TimeSeriesSetReferenceInput, etc.) will override
    // this behavior to compute actual deltas based on their type knowledge.
    //
    // For scalar types, no delta computation is needed - just the rebinding
    // itself is sufficient, and modified_at() will return true due to the
    // sample_time update.

    ensure_delta_storage();

    // Mark that a rebind occurred (delta storage exists but may be empty)
    // Specialized inputs can populate the actual delta values
}

}  // namespace hgraph
