//
// Created by Claude on 16/12/2025.
//
// TSInput implementation - V2 time-series input with hierarchical access strategies
//

#include <hgraph/types/time_series/v2/ts_input.h>
#include <hgraph/types/node.h>

namespace hgraph::ts {

// ============================================================================
// TSInput implementation
// ============================================================================

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

    // Build strategy tree based on schema comparison
    auto* output_meta = output->meta();
    _strategy = build_access_strategy(_meta, output_meta, this);

    // Bind the strategy to the output
    if (_strategy) {
        _strategy->bind(output);

        // If active, activate the strategy
        if (_active) {
            _strategy->make_active();
        }
    }
}

void TSInput::unbind_output() {
    bool was_active = _active;

    if (_strategy) {
        _strategy->unbind();
        _strategy.reset();
    }

    _active = was_active;  // Preserve the activation request (will apply on next bind)
}

void TSInput::make_active() {
    if (_active) return;
    _active = true;

    if (_strategy) {
        _strategy->make_active();
    }
}

void TSInput::make_passive() {
    if (!_active) return;

    if (_strategy) {
        _strategy->make_passive();
    }
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

} // namespace hgraph::ts
