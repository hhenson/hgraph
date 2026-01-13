//
// ts_link.cpp - TSLink implementation
//

#include <hgraph/types/time_series/ts_link.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/time_series/ts_type_meta.h>
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
        if (_element_index >= 0 && output->ts_meta() &&
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

        // Check if we should notify immediately
        // (output is valid and modified at current time)
        if (_output_overlay && _output_overlay->last_modified_time() > MIN_DT) {
            // The output has valid data - we might need to notify
            // This will be handled by the caller checking modified state
        }
    }
}

void TSLink::make_passive() {
    if (_active) {
        _active = false;
        unsubscribe_if_needed();
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
    if (_notify_once && !first_notification) {
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

    // For element bindings (TSL->TS), navigate to the specific element
    if (_element_index >= 0) {
        TSView container_view = _output->view();
        if (container_view.ts_meta() && container_view.ts_meta()->kind() == TSTypeKind::TSL) {
            return container_view.as_list().element(static_cast<size_t>(_element_index));
        }
        // Not a list - fall back to whole container
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
