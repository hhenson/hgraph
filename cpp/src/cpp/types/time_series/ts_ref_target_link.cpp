//
// ts_ref_target_link.cpp - TSRefTargetLink implementation
//

#include <hgraph/types/time_series/ts_ref_target_link.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/time_series/ts_path.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>

namespace hgraph {

// ============================================================================
// Construction
// ============================================================================

TSRefTargetLink::TSRefTargetLink(Node* node) noexcept
    : _target_link(node)
    , _node(node)
{}

TSRefTargetLink::TSRefTargetLink(TSRefTargetLink&& other) noexcept
    : _ref_output(other._ref_output)
    , _ref_overlay(other._ref_overlay)
    , _ref_subscribed(other._ref_subscribed)
    , _target_link(std::move(other._target_link))
    , _node(other._node)
    , _rebind_delta(std::move(other._rebind_delta))
    , _target_container(other._target_container)
    , _target_elem_index(other._target_elem_index)
    , _prev_target_output(other._prev_target_output)
    , _notify_time(other._notify_time)
{
    // Transfer subscription if active
    if (_ref_subscribed && _ref_overlay) {
        _ref_overlay->unsubscribe(&other);
        _ref_overlay->subscribe(this);
    }

    // Clear moved-from object
    other._ref_output = nullptr;
    other._ref_overlay = nullptr;
    other._ref_subscribed = false;
    other._target_container = nullptr;
    other._target_elem_index = -1;
    other._prev_target_output = nullptr;
}

TSRefTargetLink& TSRefTargetLink::operator=(TSRefTargetLink&& other) noexcept {
    if (this != &other) {
        // Clean up current state
        unbind();

        // Move state
        _ref_output = other._ref_output;
        _ref_overlay = other._ref_overlay;
        _ref_subscribed = other._ref_subscribed;
        _target_link = std::move(other._target_link);
        _node = other._node;
        _rebind_delta = std::move(other._rebind_delta);
        _target_container = other._target_container;
        _target_elem_index = other._target_elem_index;
        _prev_target_output = other._prev_target_output;
        _notify_time = other._notify_time;

        // Transfer subscription
        if (_ref_subscribed && _ref_overlay) {
            _ref_overlay->unsubscribe(&other);
            _ref_overlay->subscribe(this);
        }

        // Clear moved-from
        other._ref_output = nullptr;
        other._ref_overlay = nullptr;
        other._ref_subscribed = false;
        other._target_container = nullptr;
        other._target_elem_index = -1;
        other._prev_target_output = nullptr;
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
    _node = node;
    _target_link.set_node(node);
}

// ============================================================================
// REF Binding (Control Channel)
// ============================================================================

void TSRefTargetLink::bind_ref(const TSValue* ref_output, int field_index) {
    if (_ref_output == ref_output && _field_index == field_index) {
        return;  // Already bound to this REF output
    }

    // Unbind from current if any
    unbind();

    // Store REF output reference and field index
    _ref_output = ref_output;
    _field_index = field_index;

    // Get the overlay for subscription and extract expected element type
    if (ref_output) {
        _ref_overlay = const_cast<TSOverlayStorage*>(ref_output->overlay());

        // Extract the expected element type from the REF output's metadata
        // For bundle with REF fields, we need to navigate to the specific field
        const TSMeta* ref_meta = ref_output->ts_meta();
        if (field_index >= 0 && ref_meta && ref_meta->kind() == TSTypeKind::TSB) {
            // Parent is a bundle - get the specific field's type
            auto* bundle_meta = static_cast<const TSBTypeMeta*>(ref_meta);
            if (static_cast<size_t>(field_index) < bundle_meta->field_count()) {
                const TSMeta* field_meta = bundle_meta->field(field_index).type;
                if (field_meta && field_meta->is_reference()) {
                    auto* ref_type_meta = static_cast<const REFTypeMeta*>(field_meta);
                    _expected_element_meta = ref_type_meta->referenced_type();
                }
            }
        } else if (ref_meta && ref_meta->is_reference()) {
            auto* ref_type_meta = static_cast<const REFTypeMeta*>(ref_meta);
            _expected_element_meta = ref_type_meta->referenced_type();
        }
    }

    // Subscribe immediately (control channel is always active)
    subscribe_ref();
}

void TSRefTargetLink::unbind() {
    // Unsubscribe from REF output
    unsubscribe_ref();

    // Unbind target channel
    _target_link.unbind();

    // Clear state
    _ref_output = nullptr;
    _ref_overlay = nullptr;
    _field_index = -1;
    _target_container = nullptr;
    _target_elem_index = -1;
    _rebind_delta.reset();
    _prev_target_output = nullptr;
    _expected_element_meta = nullptr;
}

// ============================================================================
// Notifiable Implementation
// ============================================================================

void TSRefTargetLink::notify(engine_time_t time) {
    // Notification deduplication
    if (_notify_time == time) {
        return;
    }

    // Check if graph is stopping
    if (is_graph_stopping()) {
        return;
    }

    // Read the TimeSeriesReference from the REF output and resolve it
    if (_ref_output) {
        try {
            // Get the TimeSeriesReference from the REF output's cache (or value)
            // REF types store the TimeSeriesReference in ref_cache, not _value
            nb::object ref_value;

            // Check if we need to navigate into a bundle field
            if (_field_index >= 0) {
                // Parent is a bundle - first try to get the field's ref_cache via child_value
                const TSValue* field_child = _ref_output->child_value(static_cast<size_t>(_field_index));

                if (field_child && field_child->has_ref_cache()) {
                    // Child TSValue has ref_cache - use it directly
                    ref_value = std::any_cast<nb::object>(field_child->ref_cache());
                } else if (field_child) {
                    // Try the child's value
                    ref_value = field_child->value().to_python();
                } else {
                    // Fallback: navigate through the bundle dict (dereferenced values)
                    nb::object bundle_value = _ref_output->value().to_python();

                    if (!bundle_value.is_none() && nb::isinstance<nb::dict>(bundle_value)) {
                        // Bundle is a dict - get field by name
                        const TSMeta* ref_meta = _ref_output->ts_meta();
                        if (ref_meta && ref_meta->kind() == TSTypeKind::TSB) {
                            auto* bundle_meta = static_cast<const TSBTypeMeta*>(ref_meta);
                            if (static_cast<size_t>(_field_index) < bundle_meta->field_count()) {
                                std::string field_name = bundle_meta->field(_field_index).name;
                                nb::dict bundle_dict = nb::cast<nb::dict>(bundle_value);
                                if (bundle_dict.contains(field_name.c_str())) {
                                    ref_value = bundle_dict[field_name.c_str()];
                                }
                            }
                        }
                    }
                }
            } else if (_ref_output->has_ref_cache()) {
                ref_value = std::any_cast<nb::object>(_ref_output->ref_cache());
            } else {
                // Fallback to value() for non-cache case
                ref_value = _ref_output->value().to_python();
            }

            if (!ref_value.is_none()) {
                // Cast to TimeSeriesReference
                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_value);

                if (ref.is_view_bound()) {
                    // View-bound reference - get target directly
                    const TSValue* target = ref.view_output();
                    int elem_index = ref.view_element_index();

                    if (elem_index >= 0 && target) {
                        // Element-based binding (e.g., into a TSL)
                        rebind_target_element(target, static_cast<size_t>(elem_index), time);
                    } else if (target) {
                        // Direct TSValue binding
                        rebind_target(target, time);
                    } else {
                        // No target - unbind
                        rebind_target(nullptr, time);
                    }
                } else {
                    // Path-bound reference - would need graph context to resolve
                    // For now, treat as no target
                    rebind_target(nullptr, time);
                }
            } else {
                // None value - unbind target
                rebind_target(nullptr, time);
            }
        } catch (...) {
            // Failed to read/resolve - unbind target
            rebind_target(nullptr, time);
        }
    }

    // Mark notification time
    _notify_time = time;

    // Notify the owning node
    if (_node) {
        _node->notify(time);
    }
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
    // Valid if target link is bound OR if element-based binding is set
    return _target_link.bound() || (_target_container != nullptr && _target_elem_index >= 0);
}

bool TSRefTargetLink::modified_at(engine_time_t time) const {
    // Modified if notified at this time (covers both REF change and target rebind)
    return _notify_time == time || _target_link.modified_at(time);
}

engine_time_t TSRefTargetLink::last_modified_time() const {
    // Return max of notification time and target's last modified time
    engine_time_t target_time = _target_link.last_modified_time();
    return std::max(_notify_time, target_time);
}

// ============================================================================
// View Access
// ============================================================================

TSView TSRefTargetLink::view() const {
    // Check for element-based binding first (TSL elements)
    if (_target_container != nullptr && _target_elem_index >= 0) {
        // Navigate into the container at the specified element index
        TSView container_view = _target_container->view();

        // Use TSLView to navigate to the element
        if (container_view.ts_meta() && container_view.ts_meta()->kind() == TSTypeKind::TSL) {
            TSLView list_view = container_view.as_list();
            return list_view.element(static_cast<size_t>(_target_elem_index));
        }
        // For other container types, return container view
        return container_view;
    }

    // Direct binding - delegate to target link
    if (_target_link.bound()) {
        return _target_link.view();
    }

    // No target bound yet - return a view with expected element meta
    // This allows wrap_input_view to create the correct wrapper type during initialization,
    // even though we don't have actual data yet. The wrapper's valid() method will
    // return false until a target is actually bound.
    if (_expected_element_meta) {
        return TSView(nullptr, _expected_element_meta, static_cast<const TSValue*>(nullptr));
    }

    // No expected meta - return invalid view
    return TSView{};
}

// ============================================================================
// Private Helpers - Subscription
// ============================================================================

void TSRefTargetLink::subscribe_ref() {
    if (_ref_overlay && !_ref_subscribed) {
        _ref_overlay->subscribe(this);
        _ref_subscribed = true;
    }
}

void TSRefTargetLink::unsubscribe_ref() {
    if (_ref_overlay && _ref_subscribed) {
        _ref_overlay->unsubscribe(this);
        _ref_subscribed = false;
    }
}

// ============================================================================
// Private Helpers - Target Rebinding
// ============================================================================

void TSRefTargetLink::rebind_target(const TSValue* new_target, engine_time_t time) {
    const TSValue* old_target = _target_link.output();
    bool was_active = _target_link.active();

    if (old_target == new_target) {
        return;  // No change
    }

    // Store the previous target for delta computation
    _prev_target_output = old_target;

    // Compute delta eagerly while both targets are valid
    if (old_target != nullptr || new_target != nullptr) {
        compute_delta(old_target, new_target);
    }

    // Unbind from old target
    _target_link.unbind();

    // Bind to new target
    if (new_target) {
        _target_link.bind(new_target);
    }

    // Restore active state
    if (was_active && new_target) {
        _target_link.make_active();
    }

    // Set sample time to track rebinding
    _target_link.set_sample_time(time);

    // Clear element-based binding state since we're using direct TSValue binding
    _target_container = nullptr;
    _target_elem_index = -1;
}

void TSRefTargetLink::rebind_target_element(const TSValue* container, size_t elem_index, engine_time_t time) {
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

    // For element-based binding, we need to subscribe to the container's overlay
    // so we get notified when the element changes
    bool was_active = _target_link.active();
    if (was_active && container) {
        // Bind target_link to container with element index
        _target_link.bind(container);
        _target_link.set_element_index(static_cast<int>(elem_index));
        _target_link.make_active();
    }
}

// ============================================================================
// Private Helpers - Delta Computation
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
    // notification time update.

    ensure_delta_storage();

    // Mark that a rebind occurred (delta storage exists but may be empty)
    // Specialized inputs can populate the actual delta values
}

bool TSRefTargetLink::is_graph_stopping() const {
    if (!_node) {
        return false;
    }
    auto* graph = _node->graph();
    return graph && graph->is_stopping();
}

}  // namespace hgraph
