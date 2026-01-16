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
#include <hgraph/api/python/py_tsd.h>

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
    , _python_output(std::move(other._python_output))
    , _tsd_overlay(other._tsd_overlay)
    , _tsd_subscribed(other._tsd_subscribed)
    , _notify_time(other._notify_time)
{
    // Transfer REF subscription if active
    if (_ref_subscribed && _ref_overlay) {
        _ref_overlay->unsubscribe(&other);
        _ref_overlay->subscribe(this);
    }

    // Transfer TSD subscription if active
    if (_tsd_subscribed && _tsd_overlay) {
        _tsd_overlay->unsubscribe(&other);
        _tsd_overlay->subscribe(this);
    }

    // Clear moved-from object
    other._ref_output = nullptr;
    other._ref_overlay = nullptr;
    other._ref_subscribed = false;
    other._target_container = nullptr;
    other._target_elem_index = -1;
    other._prev_target_output = nullptr;
    other._tsd_overlay = nullptr;
    other._tsd_subscribed = false;
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
        _python_output = std::move(other._python_output);
        _tsd_overlay = other._tsd_overlay;
        _tsd_subscribed = other._tsd_subscribed;
        _notify_time = other._notify_time;

        // Transfer REF subscription
        if (_ref_subscribed && _ref_overlay) {
            _ref_overlay->unsubscribe(&other);
            _ref_overlay->subscribe(this);
        }

        // Transfer TSD subscription
        if (_tsd_subscribed && _tsd_overlay) {
            _tsd_overlay->unsubscribe(&other);
            _tsd_overlay->subscribe(this);
        }

        // Clear moved-from
        other._ref_output = nullptr;
        other._ref_overlay = nullptr;
        other._ref_subscribed = false;
        other._target_container = nullptr;
        other._target_elem_index = -1;
        other._prev_target_output = nullptr;
        other._tsd_overlay = nullptr;
        other._tsd_subscribed = false;
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

    // Unsubscribe from TSD overlay if subscribed
    if (_tsd_subscribed && _tsd_overlay) {
        _tsd_overlay->unsubscribe(this);
        _tsd_subscribed = false;
    }
    _tsd_overlay = nullptr;

    // Unbind target channel
    _target_link.unbind();

    // Clear Python output
    _python_output.reset();

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
    // Check if graph is stopping
    if (is_graph_stopping()) {
        return;
    }

    // Notification deduplication
    if (_notify_time == time) {
        return;
    }

    // NOTE: For Python outputs with TSD subscription, we do NOT filter here based on
    // py_output.modified because the TSD values may not be updated yet when we receive
    // the notification. Instead, we let the node run and rely on view() to only mark
    // the output as modified when py_output.modified returns True (which is checked
    // after TSD values are updated during node evaluation).

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
                // First try C++ TimeSeriesReference
                if (nb::isinstance<TimeSeriesReference>(ref_value)) {
                    // Clear Python output when using C++ target
                    _python_output.reset();

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
                    // Check for Python TimeSeriesReference subclasses (BoundTimeSeriesReference, etc.)
                    auto ref_type_module = nb::module_::import_("hgraph._types._ref_type");
                    auto ts_ref_class = ref_type_module.attr("TimeSeriesReference");
                    if (nb::isinstance(ref_value, ts_ref_class)) {
                        // It's a Python TimeSeriesReference - check if it has an output
                        bool is_empty = false;
                        if (nb::hasattr(ref_value, "is_empty")) {
                            is_empty = nb::cast<bool>(ref_value.attr("is_empty"));
                        }

                        if (is_empty) {
                            // Empty reference - unbind target
                            rebind_target(nullptr, time);
                        } else if (nb::hasattr(ref_value, "output") && nb::hasattr(ref_value, "has_output")) {
                            bool has_output = nb::cast<bool>(ref_value.attr("has_output"));
                            if (has_output) {
                                // Store the Python output wrapper for later value access
                                // Must wrap accessor in nb::object for std::any assignment
                                nb::object py_output = ref_value.attr("output");
                                _python_output = nb::object(py_output);

                                // For Python outputs like CppKeySetIsEmptyOutput that are backed by
                                // a C++ TSD, we need to subscribe to the TSD overlay for notifications.
                                // We subscribe TSRefTargetLink directly (not _target_link) so we can
                                // filter notifications based on py_output.modified in notify().
                                _target_link.unbind();  // Don't use _target_link for Python outputs

                                // Unsubscribe from previous TSD overlay if any
                                if (_tsd_subscribed && _tsd_overlay) {
                                    _tsd_overlay->unsubscribe(this);
                                    _tsd_subscribed = false;
                                }
                                _tsd_overlay = nullptr;

                                if (nb::hasattr(py_output, "_tsd_output")) {
                                    nb::object tsd_output = py_output.attr("_tsd_output");
                                    try {
                                        PyTimeSeriesDictOutput* tsd_cpp = nb::cast<PyTimeSeriesDictOutput*>(tsd_output);
                                        if (tsd_cpp) {
                                            const TSValue* tsd_root = tsd_cpp->view().root();
                                            if (tsd_root && tsd_root->overlay()) {
                                                // Subscribe TSRefTargetLink directly to TSD overlay
                                                // notify() will check py_output.modified before forwarding
                                                _tsd_overlay = const_cast<TSOverlayStorage*>(tsd_root->overlay());
                                                _tsd_overlay->subscribe(this);
                                                _tsd_subscribed = true;
                                            }
                                        }
                                    } catch (...) {
                                        // Cast failed - no subscription
                                    }
                                }
                                _target_container = nullptr;
                                _target_elem_index = -1;
                            } else {
                                _python_output.reset();
                                rebind_target(nullptr, time);
                            }
                        } else {
                            rebind_target(nullptr, time);
                        }
                    } else {
                        // Unknown type - unbind
                        rebind_target(nullptr, time);
                    }
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
    // Valid if target link is bound OR if element-based binding is set OR if Python output is set
    return _target_link.bound() ||
           (_target_container != nullptr && _target_elem_index >= 0) ||
           _python_output.has_value();
}

bool TSRefTargetLink::modified_at(engine_time_t time) const {
    // For Python outputs (like CppKeySetIsEmptyOutput), we need to check the Python object's
    // modified status rather than _target_link (which may be subscribed to a parent TSD overlay).
    // This ensures we only report modified when the actual value (e.g., is_empty bool) changes,
    // not when the underlying container changes.
    if (_python_output.has_value()) {
        try {
            nb::object py_output = std::any_cast<nb::object>(_python_output);
            if (!py_output.is_none() && nb::hasattr(py_output, "modified")) {
                return nb::cast<bool>(py_output.attr("modified"));
            }
        } catch (...) {
            // Fall through to default logic
        }
    }

    // Modified if notified at this time (covers both REF change and target rebind)
    return _notify_time == time || _target_link.modified_at(time);
}

engine_time_t TSRefTargetLink::last_modified_time() const {
    // For Python outputs, query the Python object for last_modified_time
    if (_python_output.has_value()) {
        try {
            nb::object py_output = std::any_cast<nb::object>(_python_output);
            if (!py_output.is_none() && nb::hasattr(py_output, "last_modified_time")) {
                nb::object lmt = py_output.attr("last_modified_time");
                if (!lmt.is_none()) {
                    // Python datetime to engine_time_t conversion
                    // For now, return _notify_time as the last modified time
                    // when Python output exists (since we set it on notify)
                    return _notify_time;
                }
            }
        } catch (...) {
            // Fall through to default logic
        }
    }

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

    // Check for Python output FIRST - if we have one, get its value from Python
    // Note: _target_link may also be bound (for notifications), but we use _python_output for values
    if (_python_output.has_value()) {
        try {
            nb::object py_output = std::any_cast<nb::object>(_python_output);
            if (!py_output.is_none() && nb::hasattr(py_output, "value")) {
                nb::object value = py_output.attr("value");

                // Get the expected type for the TSValue
                const TSMeta* meta = _expected_element_meta;
                if (meta) {
                    // Create or update the cache TSValue
                    if (!_python_value_cache || _python_value_cache->ts_meta() != meta) {
                        _python_value_cache = std::make_unique<TSValue>(meta);
                    }
                    // Set the value from Python
                    _python_value_cache->from_python(value);

                    // Only mark modified if the Python output says it's modified
                    // This ensures is_empty only reports modified when the bool value changes,
                    // not every time the underlying TSD changes
                    bool is_modified = false;
                    if (nb::hasattr(py_output, "modified")) {
                        is_modified = nb::cast<bool>(py_output.attr("modified"));
                    }

                    if (is_modified) {
                        // Mark both the value and the overlay as modified at current time
                        _python_value_cache->notify_modified(_notify_time);
                        TSMutableView cache_view = _python_value_cache->mutable_view();
                        if (cache_view.overlay()) {
                            cache_view.overlay()->mark_modified(_notify_time);
                        }
                    }
                    return _python_value_cache->view();
                }
            }
        } catch (...) {
            // Failed to get Python value - fall through
        }
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
