//
// ts_link.cpp - TSLink implementation
//

#include <hgraph/types/time_series/ts_link.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <fmt/core.h>

namespace hgraph {

// ============================================================================
// Move Operations
// ============================================================================

TSLink::TSLink(TSLink&& other) noexcept
    : _output(other._output)
    , _output_overlay(other._output_overlay)
    , _node(other._node)
    , _active(other._active)
    , _sample_time(other._sample_time)
    , _notify_time(other._notify_time)
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
        _sample_time = other._sample_time;
        _notify_time = other._notify_time;

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
    _output_overlay = output ? const_cast<TSOverlayStorage*>(output->overlay()) : nullptr;

    // Subscribe to new output if active
    subscribe_if_needed();
}

void TSLink::unbind() {
    unsubscribe_if_needed();
    _output = nullptr;
    _output_overlay = nullptr;
    // NOTE: _active is preserved
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
