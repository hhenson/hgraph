//
// Created by Claude on 16/12/2025.
//
// TSOutput implementation - Value-based time-series output
//
// Most functionality is header-only (templates and inline methods).
// This file provides implementations that require full type definitions.
//

#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_python_cache.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/runtime/evaluation_engine.h>

namespace hgraph::ts {

// ============================================================================
// TSOutput destructor and move operations
// ============================================================================

TSOutput::~TSOutput() {
    delete _python_cache;
}

TSOutput::TSOutput(TSOutput&& other) noexcept
    : _meta(other._meta)
    , _owning_node(other._owning_node)
    , _value(std::move(other._value))
    , _delta_reset_registered(other._delta_reset_registered)
    , _python_cache(other._python_cache)
{
    other._meta = nullptr;
    other._owning_node = nullptr;
    other._delta_reset_registered = false;
    other._python_cache = nullptr;
}

TSOutput& TSOutput::operator=(TSOutput&& other) noexcept {
    if (this != &other) {
        delete _python_cache;

        _meta = other._meta;
        _owning_node = other._owning_node;
        _value = std::move(other._value);
        _delta_reset_registered = other._delta_reset_registered;
        _python_cache = other._python_cache;

        other._meta = nullptr;
        other._owning_node = nullptr;
        other._delta_reset_registered = false;
        other._python_cache = nullptr;
    }
    return *this;
}

// ============================================================================
// Python cache methods
// ============================================================================

PythonCache* TSOutput::python_cache() {
    if (!_python_cache) {
        _python_cache = new PythonCache();
    }
    return _python_cache;
}

void TSOutput::clear_cached_delta() {
    if (_python_cache) {
        _python_cache->cached_delta = nb::none();
    }
}

void TSOutput::clear_cached_value() {
    if (_python_cache) {
        _python_cache->cached_value = nb::none();
        _python_cache->value_cache_time = MIN_DT;
    }
}

// ============================================================================
// Delta reset callback
// ============================================================================

void TSOutput::register_delta_reset_callback() const {
    // Only register once
    if (_delta_reset_registered) {
        return;
    }

    // Collection types need delta reset (TSS, TSD, TSL)
    if (!_meta) {
        return;
    }

    auto ts_kind = _meta->ts_kind;
    if (ts_kind != TSKind::TSS && ts_kind != TSKind::TSD && ts_kind != TSKind::TSL) {
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

    // Capture pointer to this for clearing python cache
    // const_cast is safe: callback runs after evaluation when modifications are allowed
    auto* self = const_cast<TSOutput*>(this);

    // Register callback based on type
    if (ts_kind == TSKind::TSS) {
        engine_api->add_after_evaluation_notification([tracker, self]() mutable {
            tracker.clear_set_delta();
            self->clear_cached_delta();
        });
    } else if (ts_kind == TSKind::TSD) {
        engine_api->add_after_evaluation_notification([tracker, self]() mutable {
            tracker.clear_dict_delta();
            self->clear_cached_delta();
        });
    } else if (ts_kind == TSKind::TSL) {
        // TSL only needs python cache clearing (no tracker delta)
        engine_api->add_after_evaluation_notification([self]() {
            self->clear_cached_delta();
        });
    }

    _delta_reset_registered = true;
}

} // namespace hgraph::ts
