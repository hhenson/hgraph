//
// Python helper functions for ts::TSOutput and ts::TSInput
//
// These functions provide Python-aware operations for value-based time-series types.
// They delegate conversion logic to the schema's from_python/to_python ops.
//
// Pattern:
//   apply_result(value): If None, do nothing. Otherwise call set_value(value).
//   set_value(value): If None, invalidate. Otherwise convert and set.
//

#ifndef HGRAPH_TS_PYTHON_HELPERS_H
#define HGRAPH_TS_PYTHON_HELPERS_H

#include <nanobind/nanobind.h>
#include <fmt/format.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/value/python_conversion.h>
#include <unordered_map>

namespace hgraph::ts
{

// =============================================================================
// Delta Cache for Collection Types (TSD, TSL, TSS)
// =============================================================================
//
// Collection types (TSD, TSL, TSS) don't have native C++ storage - their values
// are managed by Python. When a Python node returns a dict/list/set result,
// we need to cache it so that delta_value() can return it later for recording.
//
// The cache maps TSOutput* -> (nb::object, engine_time_t) pairs.
// Values are cleared when consumed to avoid memory leaks.

struct CachedDelta {
    nb::object value;
    engine_time_t time{MIN_DT};
};

// Thread-local cache for delta values
// Using a simple map keyed by TSOutput pointer
inline std::unordered_map<const TSOutput*, CachedDelta>& get_delta_cache() {
    static std::unordered_map<const TSOutput*, CachedDelta> cache;
    return cache;
}

/**
 * Cache a delta value for a collection type output.
 *
 * Called from set_python_value() for TSD/TSL/TSS types that don't have
 * native C++ storage.
 */
inline void cache_delta(const TSOutput* output, nb::object value, engine_time_t time) {
    if (!output) return;
    get_delta_cache()[output] = CachedDelta{std::move(value), time};
}

/**
 * Get and consume a cached delta value.
 *
 * Returns the cached value if available and was set at the given time.
 * The value is removed from the cache after retrieval.
 *
 * @param output The output to get cached delta for
 * @param time The time to check (must match cache time)
 * @return The cached Python object, or None if not available
 */
inline nb::object get_cached_delta(const TSOutput* output, engine_time_t time) {
    if (!output) return nb::none();

    auto& cache = get_delta_cache();
    auto it = cache.find(output);
    if (it == cache.end()) return nb::none();

    // Check if the cached value is from the current time
    if (it->second.time != time) {
        cache.erase(it);
        return nb::none();
    }

    // Extract and remove the cached value
    nb::object result = std::move(it->second.value);
    cache.erase(it);
    return result;
}

/**
 * Check if a delta is cached for this output at the given time.
 */
inline bool has_cached_delta(const TSOutput* output, engine_time_t time) {
    if (!output) return false;
    auto& cache = get_delta_cache();
    auto it = cache.find(output);
    if (it == cache.end()) return false;
    return it->second.time == time;
}

/**
 * Clear all cached deltas (e.g., at end of evaluation cycle).
 */
inline void clear_delta_cache() {
    get_delta_cache().clear();
}

/**
 * Set a Python value on a TSOutput, using the schema's from_python conversion.
 *
 * If py_value is None, the output is invalidated.
 * Otherwise, the value is converted using the schema's ops->from_python.
 *
 * For TSB (bundle) types, this also marks individual fields as modified.
 *
 * @param output The output to set
 * @param py_value The Python value to set
 * @param time The evaluation time for marking modification
 */
inline void set_python_value(TSOutput* output, nb::object py_value, engine_time_t time) {
    if (!output) return;

    // None means invalidate
    if (py_value.is_none()) {
        output->mark_invalid();
        return;
    }

    // view is already a TSView
    auto view = output->view();
    auto* schema = view.schema();

    if (schema && schema->ops && schema->ops->from_python) {
        // Get the underlying ValueView which has the data() method
        auto value_view = view.value_view();
        schema->ops->from_python(value_view.data(), py_value.ptr(), schema);

        // For TSB types, also mark individual fields as modified
        auto* meta = output->meta();
        if (meta && meta->ts_kind == TSKind::TSB && nb::isinstance<nb::dict>(py_value)) {
            nb::dict d = nb::cast<nb::dict>(py_value);
            auto* tsb_meta = static_cast<const TSBTypeMeta*>(meta);
            auto tracker = view.tracker();

            // Mark each field from the dict as modified
            for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
                const auto& field = tsb_meta->fields[i];
                if (d.contains(field.name.c_str())) {
                    auto field_tracker = tracker.field(i);
                    field_tracker.mark_modified(time);
                }
            }
        }

        view.mark_modified(time);
    } else {
        // For collection types without value schema (TSL, TSD, TSS),
        // we can't store the value directly in C++ storage, but we should
        // still mark as modified so subscribers (like REF inputs) get notified.
        // Cache the Python value so delta_value() can return it later.
        cache_delta(output, py_value, time);
        view.mark_modified(time);
    }
}

/**
 * Apply a Python result to a TSOutput.
 *
 * This is the main entry point for setting a value from Python.
 * If py_value is None, this does nothing (returns immediately).
 * Otherwise, it calls set_python_value to do the conversion.
 *
 * @param output The output to apply to
 * @param py_value The Python value to apply
 * @param time The evaluation time for marking modification
 */
inline void apply_python_result(TSOutput* output, nb::object py_value, engine_time_t time) {
    if (!output) return;

    // None means "no result" - do nothing
    if (py_value.is_none()) return;

    set_python_value(output, std::move(py_value), time);
}

/**
 * Check if a Python value can be applied to the output.
 *
 * For simple values this always returns true if the output is valid.
 * Collection types may override this with more specific checks.
 *
 * @param output The output to check
 * @param py_value The Python value to check
 * @return true if the value can be applied
 */
inline bool can_apply_python_result(TSOutput* output, nb::object py_value) {
    if (!output) return false;
    // For now, we can always apply if the output exists
    // More sophisticated checks could be added based on schema
    (void)py_value;
    return true;
}

/**
 * Get the Python value from a TSOutput.
 *
 * Uses the schema's to_python conversion.
 *
 * @param output The output to get from
 * @return The Python object, or None if not valid
 */
inline nb::object get_python_value(const TSOutput* output) {
    if (!output || !output->has_value()) return nb::none();

    // view is already a TSView
    auto view = const_cast<TSOutput*>(output)->view();
    auto* schema = view.schema();

    if (!view.valid() || !schema) return nb::none();

    // Get the underlying ValueView which has the data() method
    auto value_view = view.value_view();
    return value::value_to_python(value_view.data(), schema);
}

/**
 * Get the Python value from a TSInput.
 *
 * Uses the schema's to_python conversion.
 *
 * @param input The input to get from
 * @return The Python object, or None if not valid
 */
inline nb::object get_python_value(const TSInput* input) {
    if (!input || !input->has_value()) return nb::none();

    auto view = input->view();
    // TSInputView::value_view() returns a fresh ConstValueView each time (not cached)
    auto value_view = view.value_view();
    auto* schema = value_view.schema();

    if (!value_view.valid() || !schema) return nb::none();

    return value::value_to_python(value_view.data(), schema);
}

} // namespace hgraph::ts

// Include type-erased copy helpers (no nanobind dependency)
#include <hgraph/types/time_series/ts_copy_helpers.h>

#endif // HGRAPH_TS_PYTHON_HELPERS_H
