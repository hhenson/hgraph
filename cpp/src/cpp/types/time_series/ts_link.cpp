//
// ts_link.cpp - TSLink implementation
//

#include <hgraph/types/time_series/ts_link.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <fmt/core.h>
#include <iostream>

namespace hgraph {

// ============================================================================
// Move Operations
// ============================================================================

TSLink::TSLink(TSLink&& other) noexcept
    : _output(other._output)
    , _output_overlay(other._output_overlay)
    , _node(other._node)
    , _active(other._active)
    , _notify_once(other._notify_once)
    , _sample_time(other._sample_time)
    , _notify_time(other._notify_time)
    , _element_index(other._element_index)
{
    // If we were subscribed, transfer the subscription
    if (_active && _output_overlay) {
        _output_overlay->unsubscribe(&other);
        _output_overlay->subscribe(this);
    }

    // Clear the moved-from object
    other._output = nullptr;
    other._output_overlay = nullptr;
    other._active = false;
}

TSLink& TSLink::operator=(TSLink&& other) noexcept {
    if (this != &other) {
        // Unsubscribe from our current output
        unsubscribe_if_needed();

        // Move state
        _output = other._output;
        _output_overlay = other._output_overlay;
        _node = other._node;
        _active = other._active;
        _notify_once = other._notify_once;
        _sample_time = other._sample_time;
        _notify_time = other._notify_time;
        _element_index = other._element_index;

        // Transfer subscription
        if (_active && _output_overlay) {
            _output_overlay->unsubscribe(&other);
            _output_overlay->subscribe(this);
        }

        // Clear moved-from object
        other._output = nullptr;
        other._output_overlay = nullptr;
        other._active = false;
    }
    return *this;
}

TSLink::~TSLink() {
    unsubscribe_if_needed();
}

// ============================================================================
// Binding
// ============================================================================

void TSLink::bind(const TSValue* output) {
    if (_output == output) {
        return;  // Already bound to this output
    }

    // Unsubscribe from current output if active
    unsubscribe_if_needed();

    // Update binding
    _output = output;

    // Determine which overlay to use
    // If element_index is set and output is a TSL, subscribe to the element's overlay
    // instead of the whole TSL overlay. This ensures we only get notified when
    // the specific element we're bound to is modified, not when any element is modified.
    _output_overlay = nullptr;
    if (output) {
        if (_element_index == KEY_SET_INDEX && output->ts_meta() &&
            output->ts_meta()->kind() == TSTypeKind::TSD) {
            // KEY_SET binding - subscribe to the TSD's overlay directly
            // The TSD's MapTSOverlay tracks key additions/removals
            _output_overlay = const_cast<TSOverlayStorage*>(output->overlay());
        } else if (_element_index >= 0 && output->ts_meta() &&
            output->ts_meta()->kind() == TSTypeKind::TSL) {
            // We're binding to a TSL element - use the element's overlay
            auto* list_overlay = dynamic_cast<ListTSOverlay*>(
                const_cast<TSOverlayStorage*>(output->overlay()));
            if (list_overlay && static_cast<size_t>(_element_index) < list_overlay->child_count()) {
                _output_overlay = list_overlay->child(static_cast<size_t>(_element_index));
            } else {
                // Fall back to whole TSL overlay
                _output_overlay = const_cast<TSOverlayStorage*>(output->overlay());
            }
        } else {
            _output_overlay = const_cast<TSOverlayStorage*>(output->overlay());
        }
    }

    // Subscribe to new output if active
    subscribe_if_needed();
}

void TSLink::unbind() {
    unsubscribe_if_needed();
    _output = nullptr;
    _output_overlay = nullptr;
    // NOTE: _active is preserved
}

void TSLink::set_element_index(int idx) {
    if (_element_index == idx) {
        return;  // No change
    }

    int old_index = _element_index;
    _element_index = idx;

    // If we're bound to a TSL and the index changed, we need to switch overlays
    // This ensures we subscribe to the element's overlay, not the whole TSL
    if (_output && _output->ts_meta() &&
        _output->ts_meta()->kind() == TSTypeKind::TSL) {
        // Unsubscribe from current overlay first
        unsubscribe_if_needed();

        // Get the new overlay - either the element's overlay or the whole TSL
        auto* base_overlay = const_cast<TSOverlayStorage*>(_output->overlay());
        auto* list_overlay = dynamic_cast<ListTSOverlay*>(base_overlay);

        if (list_overlay && idx >= 0 && static_cast<size_t>(idx) < list_overlay->child_count()) {
            // Use element overlay
            _output_overlay = list_overlay->child(static_cast<size_t>(idx));
        } else if (old_index >= 0 && idx < 0) {
            // Switching from element to whole - use base overlay
            _output_overlay = base_overlay;
        }

        // Resubscribe if we were active
        subscribe_if_needed();
    }
}

// ============================================================================
// Subscription Control
// ============================================================================

void TSLink::make_active() {
    if (!_active) {
        _active = true;
        subscribe_if_needed();

        // For REF bindings (notify_once mode), we need to ensure the owning node
        // runs on tick 1. This matches Python's PythonTimeSeriesReferenceInput.make_active()
        // behavior which calls notify() when valid. Without this, nodes like is_empty_tss
        // wouldn't run on tick 1 because the TSD's key_set hasn't been "modified" yet.
        //
        // NOTE: During graph construction, we set a flag that will be checked during
        // node startup. The actual scheduling happens in TSLink::activate_on_start()
        // which is called from Node::_initialise_inputs().
        if (_notify_once && _output) {
            _needs_startup_notify = true;
        }
    }
}

void TSLink::make_passive() {
    if (_active) {
        _active = false;
        unsubscribe_if_needed();
    }
}

void TSLink::check_startup_notify(engine_time_t start_time) {
    if (_needs_startup_notify && _active && _output) {
        _needs_startup_notify = false;  // Clear flag
        // Trigger notification which will schedule the owning node
        // The notify() method handles sample_time setup for first notification
        notify(start_time);
    }
}

// ============================================================================
// Notifiable Implementation
// ============================================================================

void TSLink::notify(engine_time_t time) {
    if (!_active) {
        return;  // Ignore notifications when passive
    }

    // Set sample_time on first notification (for REF semantics)
    // REF inputs should only report modified when the binding changes,
    // not when underlying values change. Sample_time tracks when binding took effect.
    bool first_notification = (_sample_time == MIN_DT);
    if (first_notification) {
        _sample_time = time;
    }

    // For REF links (notify_once mode), only notify on first notification
    // This matches Python's behavior where REF inputs don't subscribe to output
    // overlays and only get notified once on binding.
    // EXCEPTION: KEY_SET bindings (tsd.key_set) need continuous notification because
    // the node needs to observe key changes to update is_empty output.
    if (_notify_once && !first_notification && _element_index != KEY_SET_INDEX) {
        return;  // Skip notification for REF links after first tick
    }

    if (_notify_time != time) {
        _notify_time = time;

        // Delegate to owning node
        if (_node && !is_graph_stopping()) {
            _node->notify(time);
        }
    }
}

// ============================================================================
// View Access
// ============================================================================

TSView TSLink::view() const {
    if (!_output) {
        return TSView{};  // Invalid view if unbound
    }

    // For KEY_SET bindings (TSD->TSS), return a TSSView that reads TSD keys directly
    if (_element_index == KEY_SET_INDEX) {
        if (_output->ts_meta() && _output->ts_meta()->kind() == TSTypeKind::TSD) {
            // Get the TSD's key type and create the TSS schema
            auto* tsd_meta = static_cast<const TSDTypeMeta*>(_output->ts_meta());
            const value::TypeMeta* key_type = tsd_meta->key_type();
            const TSSTypeMeta* tss_meta = TSTypeRegistry::instance().tss(key_type);

            // Create a TSSView that reads keys from the TSD (no separate TSValue!)
            return TSSView(_output, tss_meta);
        }
        // Not a TSD - fall back to whole container
    }

    // For element bindings (TSL->TS), navigate to the specific element
    if (_element_index >= 0) {
        TSView container_view = _output->view();
        if (container_view.ts_meta() && container_view.ts_meta()->kind() == TSTypeKind::TSL) {
            return container_view.as_list().element(static_cast<size_t>(_element_index));
        }
        // Not a list - fall back to whole container
    }

    // For field bindings (TSB->field), navigate to the specific field
    if (_field_index >= 0) {
        TSView container_view = _output->view();
        if (container_view.ts_meta() && container_view.ts_meta()->kind() == TSTypeKind::TSB) {
            return container_view.as_bundle().field(static_cast<size_t>(_field_index));
        }
        // Not a bundle - fall back to whole container
    }

    return _output->view();
}

// ============================================================================
// State Queries
// ============================================================================

bool TSLink::valid() const {
    if (!_output_overlay) {
        return false;
    }

    // For REF inputs (notify_once mode), the link is valid when the binding
    // has been established (sample_time set), not when the underlying output
    // is modified. This matches Python's PythonTimeSeriesReferenceInput behavior.
    if (_notify_once) {
        return _sample_time > MIN_DT;
    }

    return _output_overlay->last_modified_time() > MIN_DT;
}

bool TSLink::modified_at(engine_time_t time) const {
    if (!_output_overlay) {
        return false;
    }
    return _output_overlay->last_modified_time() == time;
}

engine_time_t TSLink::last_modified_time() const {
    if (!_output_overlay) {
        return MIN_DT;
    }
    return _output_overlay->last_modified_time();
}

// ============================================================================
// Private Helpers
// ============================================================================

void TSLink::subscribe_if_needed() {
    if (_active && _output_overlay) {
        _output_overlay->subscribe(this);
    }
}

void TSLink::unsubscribe_if_needed() {
    if (_output_overlay && _output_overlay->is_subscribed(this)) {
        _output_overlay->unsubscribe(this);
    }
}

bool TSLink::is_graph_stopping() const {
    if (!_node) {
        return false;
    }
    auto* graph = _node->graph();
    return graph && graph->is_stopping();
}

}  // namespace hgraph
