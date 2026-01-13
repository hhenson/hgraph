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
#include <iostream>

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

    std::cerr << "[DEBUG rebind_target] old_target=" << old_target
              << " new_target=" << new_target
              << " old==new=" << (old_target == new_target)
              << std::endl;

    if (old_target == new_target) {
        return;  // No change
    }

    // Compute delta eagerly while both targets are valid
    if (old_target != nullptr || new_target != nullptr) {
        compute_delta(old_target, new_target);
    }

    // Preserve active state
    bool was_active = _target_link.active();
    std::cerr << "[DEBUG rebind_target] was_active=" << was_active << std::endl;

    // Unbind from old target
    _target_link.unbind();

    // Bind to new target
    if (new_target) {
        _target_link.bind(new_target);
        std::cerr << "[DEBUG rebind_target] after bind, _target_link.bound()=" << _target_link.bound()
                  << " _target_link.output()=" << _target_link.output()
                  << std::endl;
    }

    // Restore active state
    if (was_active) {
        _target_link.make_active();
    }

    // Set sample time to track rebinding
    _target_link.set_sample_time(time);

    std::cerr << "[DEBUG rebind_target] DONE, _target_link.bound()=" << _target_link.bound()
              << " _target_link.output()=" << _target_link.output()
              << std::endl;

    // Clear element-based binding state since we're using direct TSValue binding
    _target_container = nullptr;
    _target_elem_index = -1;
}

void TSRefTargetLink::rebind_target_element(const TSValue* container, size_t elem_index, engine_time_t time) {
    std::cerr << "[DEBUG rebind_target_element] container=" << container
              << " elem_index=" << elem_index
              << " time=" << time
              << std::endl;

    // For element-based binding, we store container + index instead of using _target_link
    // This handles TSL elements that don't have separate TSValues

    const TSValue* old_target = _target_container;
    int old_index = _target_elem_index;

    // Check if same binding
    if (old_target == container && old_index == static_cast<int>(elem_index)) {
        return;  // No change
    }

    // Unbind target_link if it was previously using direct binding
    if (_target_link.bound()) {
        _target_link.unbind();
    }

    // Store element-based binding state
    _target_container = container;
    _target_elem_index = static_cast<int>(elem_index);

    std::cerr << "[DEBUG rebind_target_element] DONE, _target_container=" << _target_container
              << " _target_elem_index=" << _target_elem_index
              << std::endl;
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
    // Valid if target link is bound OR if element-based binding is set.
    // For TSRefTargetLink, "valid" means the REF has resolved to a target.
    // We check bound() rather than valid() because valid() checks overlay modification,
    // which may be false for containers (e.g., TSL) even when elements have been set.
    return _target_link.bound() || (_target_container != nullptr && _target_elem_index >= 0);
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
    // If target_link is bound, use it; otherwise check element-based binding;
    // finally fall back to ref_link (for notifications)
    std::cerr << "[DEBUG TSRefTargetLink::view] target_bound=" << _target_link.bound()
              << " ref_bound=" << _ref_link.bound()
              << " target_output=" << _target_link.output()
              << " ref_output=" << _ref_link.output()
              << " target_container=" << _target_container
              << " target_elem_index=" << _target_elem_index
              << std::endl;

    // Check for element-based binding first (TSL elements)
    if (_target_container != nullptr && _target_elem_index >= 0) {
        // Navigate into the container at the specified element index
        TSView container_view = _target_container->view();
        std::cerr << "[DEBUG TSRefTargetLink::view] using element-based binding, container kind="
                  << (container_view.ts_meta() ? static_cast<int>(container_view.ts_meta()->kind()) : -1)
                  << std::endl;

        // Use TSLView to navigate to the element
        if (container_view.ts_meta() && container_view.ts_meta()->kind() == TSTypeKind::TSL) {
            TSLView list_view = container_view.as_list();
            TSView elem_view = list_view.element(static_cast<size_t>(_target_elem_index));
            std::cerr << "[DEBUG TSRefTargetLink::view] returning element view, kind="
                      << (elem_view.ts_meta() ? static_cast<int>(elem_view.ts_meta()->kind()) : -1)
                      << std::endl;
            return elem_view;
        }
        // For other container types, return container view (should not happen for element binding)
        return container_view;
    }

    if (_target_link.bound()) {
        TSView v = _target_link.view();
        std::cerr << "[DEBUG TSRefTargetLink::view] returning target_link view, kind="
                  << (v.ts_meta() ? static_cast<int>(v.ts_meta()->kind()) : -1) << std::endl;
        return v;
    }
    // Fall back to ref_link view if target is not bound
    TSView v = _ref_link.view();
    std::cerr << "[DEBUG TSRefTargetLink::view] returning ref_link view, kind="
              << (v.ts_meta() ? static_cast<int>(v.ts_meta()->kind()) : -1) << std::endl;
    return v;
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
