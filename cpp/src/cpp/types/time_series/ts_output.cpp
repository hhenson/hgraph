//
// Created by Claude on 16/12/2025.
//
// TSOutput implementation - Value-based time-series output
//
// Most functionality is header-only (templates and inline methods).
// This file provides implementations that require full type definitions.
//

#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/runtime/evaluation_engine.h>

namespace hgraph::ts {

void TSOutput::register_delta_reset_callback() const {
    // Only register once
    if (_delta_reset_registered) {
        return;
    }

    // Only Set and Dict types need delta reset
    if (!_meta) {
        return;
    }

    auto ts_kind = _meta->ts_kind;
    if (ts_kind != TSKind::TSS && ts_kind != TSKind::TSD) {
        return;
    }

    // Need owning node to access graph
    if (!_owning_node) {
        return;
    }

    auto* graph = _owning_node->graph();
    if (!graph) {
        return;
    }

    auto engine_api = graph->evaluation_engine_api();
    if (!engine_api) {
        return;
    }

    // Get the tracker from the underlying storage
    // Note: Using const_cast is safe here because:
    // 1. We're capturing the tracker for use in a later callback
    // 2. The callback runs after evaluation when modifications are allowed
    auto& tracker_storage = const_cast<value::ModificationTrackerStorage&>(_value.underlying_tracker());
    auto tracker = tracker_storage.tracker();

    // Register callback based on type
    if (ts_kind == TSKind::TSS) {
        // Capture tracker by value - it's a lightweight view pointing to the storage
        engine_api->add_after_evaluation_notification([tracker]() mutable {
            tracker.clear_set_delta();
        });
    } else if (ts_kind == TSKind::TSD) {
        engine_api->add_after_evaluation_notification([tracker]() mutable {
            tracker.clear_dict_delta();
        });
    }

    _delta_reset_registered = true;
}

} // namespace hgraph::ts
