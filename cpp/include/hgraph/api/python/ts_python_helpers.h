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
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_python_cache.h>
#include <hgraph/types/value/python_conversion.h>
#include <hgraph/types/value/window_type.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/value/dict_type.h>

namespace hgraph::ts
{

// =============================================================================
// Delta/Value Cache Helper Functions
// =============================================================================
//
// Collection types (TSD, TSL, TSS) don't have native C++ storage - their values
// are managed by Python. When a Python node returns a dict/list/set result,
// we cache it on the TSOutput so that delta_value() can return it later.
//
// The cache is stored directly on TSOutput::python_cache().
// Delta values are cleared at the end of each evaluation tick via
// TSOutput::register_delta_reset_callback().

/**
 * Cache a delta value for a collection type output.
 *
 * Called from set_python_value() for TSD/TSL/TSS types that don't have
 * native C++ storage.
 */
inline void cache_delta(TSOutput* output, nb::object value) {
    if (!output) return;
    auto* cache = output->python_cache();
    cache->cached_delta = std::move(value);
}

/**
 * Get the cached delta value (non-consuming).
 *
 * Returns the cached delta if available.
 * The delta is NOT consumed - it will be cleared at tick end by the
 * after-evaluation callback.
 *
 * @param output The output to get cached delta for
 * @return The cached Python object, or None if not available
 */
inline nb::object get_cached_delta(const TSOutput* output) {
    if (!output || !output->has_python_cache()) return nb::none();
    auto* cache = const_cast<TSOutput*>(output)->python_cache();
    if (cache->cached_delta.is_none()) return nb::none();
    return cache->cached_delta;
}

/**
 * Cache a value conversion for an output.
 *
 * The cached value is valid as long as cache_time >= last_modified_time.
 */
inline void cache_value(TSOutput* output, nb::object value, engine_time_t time) {
    if (!output) return;
    auto* cache = output->python_cache();
    cache->cached_value = std::move(value);
    cache->value_cache_time = time;
}

/**
 * Get the cached value if still valid.
 *
 * @param output The output to get cached value for
 * @return The cached Python object if valid, or None if stale/unavailable
 */
inline nb::object get_cached_value(const TSOutput* output) {
    if (!output || !output->has_python_cache()) return nb::none();
    auto* cache = const_cast<TSOutput*>(output)->python_cache();

    // Check if cache is still valid:
    // - cached_value must not be None (cleared)
    // - cache_time must be >= last_modified_time (not stale)
    if (!cache->cached_value.is_none() &&
        cache->value_cache_time >= output->last_modified_time()) {
        return cache->cached_value;
    }
    return nb::none();
}

// Helper to check if an object is a SetDelta (has 'added' and 'removed' attributes)
inline bool is_set_delta(const nb::object& obj) {
    return nb::hasattr(obj, "added") && nb::hasattr(obj, "removed");
}

// Helper to check if an object is a set or frozenset
inline bool is_python_set(const nb::object& obj) {
    return nb::isinstance<nb::set>(obj) || nb::isinstance<nb::frozenset>(obj);
}

// Helper to check if an object is specifically a frozenset
inline bool is_python_frozenset(const nb::object& obj) {
    return nb::isinstance<nb::frozenset>(obj);
}

// Helper to check if an object is a Removed marker (has 'item' attribute and is from hgraph._impl._types._tss)
inline bool is_removed_marker(const nb::object& obj) {
    // Check for the 'item' attribute which Removed has
    if (!nb::hasattr(obj, "item")) return false;
    // Also verify it's the Removed class by checking the type name
    auto type_name = nb::str(obj.type().attr("__name__"));
    return std::string_view(type_name.c_str()) == "Removed";
}

// Helper to check if a set contains any Removed markers
inline bool set_contains_removed_markers(const nb::object& set_obj) {
    for (auto item : set_obj) {
        if (is_removed_marker(nb::cast<nb::object>(item))) {
            return true;
        }
    }
    return false;
}

/**
 * Set a Python value on a TSOutput, using the schema's from_python conversion.
 *
 * If py_value is None, the output is invalidated.
 * Otherwise, the value is converted using the schema's ops->from_python.
 *
 * For TSB (bundle) types, this also marks individual fields as modified.
 * For TSS types, SetDelta and plain set objects are handled specially to compute deltas.
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
        output->clear_cached_value();
        return;
    }

    // Clear value cache since we're updating the value
    output->clear_cached_value();

    auto* meta = output->meta();

    // Special handling for TSL (TimeSeriesList) types
    // TSL doesn't store values directly - it delegates to sub-outputs for each element
    if (meta && meta->ts_kind == TSKind::TSL) {
        auto view = output->view();
        size_t list_size = view.list_size();

        // Handle tuple/list input
        if (nb::isinstance<nb::tuple>(py_value) || nb::isinstance<nb::list>(py_value)) {
            nb::dict delta;  // Build delta dict with {index: value}
            size_t i = 0;
            for (auto item : py_value) {
                if (i >= list_size) break;
                nb::object item_obj = nb::cast<nb::object>(item);
                if (!item_obj.is_none()) {
                    // Navigate to element and set its value recursively
                    auto elem_view = view.element(i);
                    if (elem_view.valid()) {
                        auto* elem_schema = elem_view.value_schema();
                        if (elem_schema && elem_schema->ops && elem_schema->ops->from_python) {
                            auto elem_value_view = elem_view.value_view();
                            elem_schema->ops->from_python(elem_value_view.data(), item_obj.ptr(), elem_schema);
                            elem_view.mark_modified(time);
                        }
                    }
                    // Add to delta dict with index as key
                    delta[nb::cast(i)] = item_obj;
                }
                ++i;
            }
            // Cache the delta in proper format {index: value}
            cache_delta(output, delta);
            view.mark_modified(time);
            return;
        }

        // Handle dict input (keyed by index)
        if (nb::isinstance<nb::dict>(py_value)) {
            nb::dict d = nb::cast<nb::dict>(py_value);
            for (auto kv : d) {
                nb::object key = nb::cast<nb::object>(kv.first);
                nb::object val = nb::cast<nb::object>(kv.second);
                if (val.is_none()) continue;

                // Convert key to index
                size_t idx = nb::cast<size_t>(key);
                if (idx >= list_size) continue;

                // Navigate to element and set its value
                auto elem_view = view.element(idx);
                if (elem_view.valid()) {
                    auto* elem_schema = elem_view.value_schema();
                    if (elem_schema && elem_schema->ops && elem_schema->ops->from_python) {
                        auto elem_value_view = elem_view.value_view();
                        elem_schema->ops->from_python(elem_value_view.data(), val.ptr(), elem_schema);
                        elem_view.mark_modified(time);
                    }
                }
            }
            // Cache the delta for delta_value()
            cache_delta(output, py_value);
            view.mark_modified(time);
            return;
        }
    }

    // Special handling for TSS (TimeSeriesSet) types
    if (meta && meta->ts_kind == TSKind::TSS) {
        auto view = output->view();

        // Get current value
        nb::object current_value = nb::none();
        if (view.has_value()) {
            auto* schema = view.value_schema();
            if (schema && schema->ops && schema->ops->to_python) {
                auto vv = view.value_view();
                current_value = nb::steal(reinterpret_cast<PyObject*>(schema->ops->to_python(vv.data(), schema)));
            }
        }

        nb::set old_set;
        if (!current_value.is_none()) {
            old_set = nb::set(current_value);
        }

        // Handle SetDelta objects
        if (is_set_delta(py_value)) {
            nb::set new_set;
            if (!current_value.is_none()) {
                new_set = nb::set(current_value);
            }

            // Get added and removed from the delta
            nb::object added = py_value.attr("added");
            nb::object removed = py_value.attr("removed");

            // Build filtered added set (only elements not already in set)
            nb::set filtered_added;
            for (auto item : added) {
                nb::object item_obj = nb::cast<nb::object>(item);
                bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(item_obj));
                if (!in_set) {
                    filtered_added.add(item_obj);
                    new_set.add(item_obj);
                }
            }

            // Build filtered removed set (only elements that are in set)
            nb::set filtered_removed;
            for (auto item : removed) {
                nb::object item_obj = nb::cast<nb::object>(item);
                bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(item_obj));
                if (in_set) {
                    filtered_removed.add(item_obj);
                    new_set.discard(item_obj);
                }
            }

            // Only mark as modified if there were actual changes
            if (len(filtered_added) > 0 || len(filtered_removed) > 0 || !view.has_value()) {
                // Store the new set value
                auto* schema = view.schema();
                if (schema && schema->ops && schema->ops->from_python) {
                    auto value_view = view.value_view();
                    nb::frozenset fs(new_set);
                    schema->ops->from_python(value_view.data(), fs.ptr(), schema);
                }

                // Create and cache the filtered delta
                nb::module_ tss_module = nb::module_::import_("hgraph._impl._types._tss");
                nb::object PythonSetDelta = tss_module.attr("PythonSetDelta");
                nb::object filtered_delta = PythonSetDelta(
                    nb::frozenset(filtered_added),
                    nb::frozenset(filtered_removed)
                );

                cache_delta(output, filtered_delta);
                view.mark_modified(time);
                output->register_delta_reset_callback();

                // Update TSS contains extension if present
                auto* cache = output->python_cache();
                if (cache->tss_update_contains_for_keys) {
                    if (len(filtered_added) > 0) {
                        cache->tss_update_contains_for_keys(filtered_added);
                    }
                    if (len(filtered_removed) > 0) {
                        cache->tss_update_contains_for_keys(filtered_removed);
                    }
                }
            }
            return;
        }

        // Handle plain set/frozenset - may contain Removed markers
        if (is_python_set(py_value)) {
            nb::set added_set;
            nb::set removed_set;
            nb::set new_set;

            // Check if set contains Removed markers
            bool has_removed_markers = set_contains_removed_markers(py_value);

            if (has_removed_markers) {
                // Process set with Removed markers
                // Start with a copy of the old set
                if (!current_value.is_none()) {
                    new_set = nb::set(current_value);
                }

                for (auto item : py_value) {
                    nb::object item_obj = nb::cast<nb::object>(item);
                    if (is_removed_marker(item_obj)) {
                        // Extract the actual item from the Removed wrapper
                        nb::object actual_item = item_obj.attr("item");
                        // Only remove if it's in the current set
                        bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(actual_item));
                        if (in_set) {
                            removed_set.add(actual_item);
                            new_set.discard(actual_item);
                        }
                    } else {
                        // Regular item - add if not already in set
                        bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(item_obj));
                        if (!in_set) {
                            added_set.add(item_obj);
                            new_set.add(item_obj);
                        }
                    }
                }
            } else if (is_python_frozenset(py_value)) {
                // frozenset without Removed markers - treat as REPLACEMENT
                // Python logic for frozenset: self._value = v, added = v - old, removed = old - v
                new_set = nb::set(py_value);  // The new value IS the input set

                // Compute added = input - old (elements in input but not in old)
                for (auto item : py_value) {
                    nb::object item_obj = nb::cast<nb::object>(item);
                    bool in_old = !current_value.is_none() &&
                                  nb::cast<bool>(nb::borrow<nb::object>(current_value).attr("__contains__")(item_obj));
                    if (!in_old) {
                        added_set.add(item_obj);
                    }
                }

                // Compute removed = old - input (elements in old but not in input)
                if (!current_value.is_none()) {
                    for (auto item : current_value) {
                        nb::object item_obj = nb::cast<nb::object>(item);
                        bool in_new = nb::cast<bool>(py_value.attr("__contains__")(item_obj));
                        if (!in_new) {
                            removed_set.add(item_obj);
                        }
                    }
                }
            } else {
                // Regular set without Removed markers - treat as ADDITIONS ONLY
                // Python logic: added = elements not in current value, no removals
                // Start with current value
                if (!current_value.is_none()) {
                    new_set = nb::set(current_value);
                }

                // Add all elements that are not already in the set
                for (auto item : py_value) {
                    nb::object item_obj = nb::cast<nb::object>(item);
                    bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(item_obj));
                    if (!in_set) {
                        added_set.add(item_obj);
                        new_set.add(item_obj);
                    }
                }
                // removed_set stays empty - no removals for regular sets
            }

            // Only mark as modified if there were actual changes or first tick
            if (len(added_set) > 0 || len(removed_set) > 0 || !view.has_value()) {
                // Store the new set value
                auto* schema = view.schema();
                if (schema && schema->ops && schema->ops->from_python) {
                    auto value_view = view.value_view();
                    nb::frozenset fs(new_set);
                    schema->ops->from_python(value_view.data(), fs.ptr(), schema);
                }

                // Create and cache the delta
                nb::module_ tss_module = nb::module_::import_("hgraph._impl._types._tss");
                nb::object PythonSetDelta = tss_module.attr("PythonSetDelta");
                nb::object delta = PythonSetDelta(
                    nb::frozenset(added_set),
                    nb::frozenset(removed_set)
                );

                cache_delta(output, delta);
                view.mark_modified(time);
                output->register_delta_reset_callback();

                // Update TSS contains extension if present
                auto* cache = output->python_cache();
                if (cache->tss_update_contains_for_keys) {
                    if (len(added_set) > 0) {
                        cache->tss_update_contains_for_keys(added_set);
                    }
                    if (len(removed_set) > 0) {
                        cache->tss_update_contains_for_keys(removed_set);
                    }
                }
            }
            return;
        }
    }

    // Special handling for TSD (TimeSeriesDict) types
    // TSD values may contain REMOVE/REMOVE_IF_EXISTS sentinels that need special handling
    if (meta && meta->ts_kind == TSKind::TSD) {
        // Check if it's a dict-like object
        if (nb::isinstance<nb::dict>(py_value) || nb::hasattr(py_value, "items")) {
            auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(meta);
            if (tsd_meta) {
                auto view = output->view();
                auto* key_type = tsd_meta->key_type;
                auto* value_ts_type = tsd_meta->value_ts_type;
                auto* value_schema = value_ts_type ? value_ts_type->value_schema() : tsd_meta->dict_value_type;

                if (key_type && value_schema) {
                    // Get REMOVE and REMOVE_IF_EXISTS sentinels
                    nb::object remove_sentinel = get_remove();
                    nb::object remove_if_exists_sentinel = get_remove_if_exists();

                    // Get dict storage
                    auto* storage = static_cast<value::DictStorage*>(view.value_view().data());

                    // Iterate through the dict
                    nb::object items_obj;
                    if (nb::isinstance<nb::dict>(py_value)) {
                        items_obj = nb::cast<nb::dict>(py_value).attr("items")();
                    } else {
                        items_obj = py_value.attr("items")();
                    }

                    // Check if empty dict on first tick - still needs to mark as modified
                    // Python: "if not self.valid and not v: self.key_set.mark_modified()"
                    bool is_empty = (nb::len(items_obj) == 0);
                    if (is_empty && !view.has_value()) {
                        view.mark_modified(time);
                        cache_delta(output, py_value);
                        output->register_delta_reset_callback();
                        return;
                    }

                    // Get tracker for delta tracking
                    auto tracker = view.tracker();

                    bool modified = false;
                    for (auto item : items_obj) {
                        nb::tuple kv = nb::cast<nb::tuple>(item);
                        nb::object key = kv[0];
                        nb::object val = kv[1];

                        // Skip None values
                        if (val.is_none()) {
                            continue;
                        }

                        // Convert key to C++ storage
                        std::vector<char> key_storage(key_type->size);
                        key_type->ops->construct(key_storage.data(), key_type);
                        value::value_from_python(key_storage.data(), key, key_type);

                        // Check for REMOVE sentinels using 'is' comparison
                        if (val.is(remove_sentinel)) {
                            // REMOVE: Remove key immediately
                            auto opt_index = storage->keys().find_index(key_storage.data());
                            if (opt_index) {
                                size_t index = *opt_index;
                                bool was_added_this_tick = tracker.dict_key_added_at(index, time);
                                if (was_added_this_tick) {
                                    // Add-then-remove same tick: cancel out, don't record as removed
                                    tracker.remove_dict_entry_tracking(index);
                                } else {
                                    // Record key for delta access before removal
                                    tracker.record_dict_key_removal(key_storage.data(), time);
                                }
                                storage->remove(key_storage.data());
                                modified = true;
                            }
                        } else if (val.is(remove_if_exists_sentinel)) {
                            // REMOVE_IF_EXISTS: Only remove if key exists
                            auto opt_index = storage->keys().find_index(key_storage.data());
                            if (opt_index) {
                                size_t index = *opt_index;
                                bool was_added_this_tick = tracker.dict_key_added_at(index, time);
                                if (was_added_this_tick) {
                                    // Add-then-remove same tick: cancel out, don't record as removed
                                    tracker.remove_dict_entry_tracking(index);
                                } else {
                                    // Record key for delta access before removal
                                    tracker.record_dict_key_removal(key_storage.data(), time);
                                }
                                storage->remove(key_storage.data());
                                modified = true;
                            }
                        } else {
                            // Normal value - update or create the entry
                            std::vector<char> value_storage(value_schema->size);
                            value_schema->ops->construct(value_storage.data(), value_schema);
                            value::value_from_python(value_storage.data(), val, value_schema);

                            // Insert and track delta
                            auto [is_new_key, idx] = storage->insert(key_storage.data(), value_storage.data());
                            if (is_new_key) {
                                tracker.mark_dict_key_added(idx, time);
                            } else {
                                tracker.mark_dict_value_modified(idx, time);
                            }
                            modified = true;

                            if (value_schema->ops->destruct) {
                                value_schema->ops->destruct(value_storage.data(), value_schema);
                            }
                        }

                        // Cleanup key storage
                        if (key_type->ops->destruct) {
                            key_type->ops->destruct(key_storage.data(), key_type);
                        }
                    }

                    if (modified) {
                        cache_delta(output, py_value);
                        view.mark_modified(time);
                        output->register_delta_reset_callback();
                    }
                    return;
                }
            }
        }
    }

    // Special handling for TSW (TimeSeriesWindow) types
    // TSW stores values in a circular buffer (WindowStorage) - we push the scalar value
    if (meta && meta->ts_kind == TSKind::TSW) {
        auto view = output->view();
        auto* schema = view.value_schema();

        if (schema && schema->kind == value::TypeKind::Window) {
            auto* window_meta = static_cast<const value::WindowTypeMeta*>(schema);
            auto* elem_type = window_meta->element_type;

            if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                // Get the WindowStorage from the view
                auto value_view = view.value_view();
                auto* storage = static_cast<value::WindowStorage*>(value_view.data());

                // Convert the Python value to C++ element
                std::vector<char> elem_storage(elem_type->size);
                elem_type->ops->construct(elem_storage.data(), elem_type);
                elem_type->ops->from_python(elem_storage.data(), py_value.ptr(), elem_type);

                // Push to window with current timestamp
                storage->push(elem_storage.data(), time);

                // Cleanup element
                if (elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem_storage.data(), elem_type);
                }

                view.mark_modified(time);
                return;
            }
        }
    }

    // view is already a TSView
    auto view = output->view();
    auto* schema = view.schema();

    if (schema && schema->ops && schema->ops->from_python) {
        // Get the underlying ValueView which has the data() method
        auto value_view = view.value_view();
        schema->ops->from_python(value_view.data(), py_value.ptr(), schema);

        // For TSB types, also mark individual fields as modified
        if (!meta) meta = output->meta();
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

        // For REF types, cache the value and notify reference observers so they can rebind immediately
        // The RefStorage loses path information during from_python conversion, so we cache
        // the original TimeSeriesReference for delta_value() to return later
        if (meta && meta->ts_kind == TSKind::REF) {
            cache_delta(output, py_value);
            output->register_delta_reset_callback();
            output->notify_reference_observers(time);
        }

        // For TSS and TSD types, register callback to clear delta at tick end
        // (reusing meta from the TSB check above)
        if (meta && (meta->ts_kind == TSKind::TSS || meta->ts_kind == TSKind::TSD)) {
            output->register_delta_reset_callback();
        }
    } else {
        // For collection types without value schema (TSL, TSD, TSS),
        // we can't store the value directly in C++ storage, but we should
        // still mark as modified so subscribers (like REF inputs) get notified.
        // Cache the Python value so delta_value() can return it later.
        cache_delta(output, py_value);
        view.mark_modified(time);

        // For TSS and TSD types, register callback to clear delta at tick end
        auto* meta = output->meta();
        if (meta && (meta->ts_kind == TSKind::TSS || meta->ts_kind == TSKind::TSD)) {
            output->register_delta_reset_callback();
        }
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
 * Uses the schema's to_python conversion with caching.
 * The cached value is valid as long as the output hasn't been modified.
 *
 * @param output The output to get from
 * @return The Python object, or None if not valid
 */
inline nb::object get_python_value(const TSOutput* output) {
    if (!output || !output->has_value()) return nb::none();

    // Check for cached value first
    auto cached = get_cached_value(output);
    if (!cached.is_none()) {
        return cached;
    }

    // view is already a TSView
    auto view = const_cast<TSOutput*>(output)->view();
    auto* schema = view.schema();

    if (!view.valid() || !schema) return nb::none();

    // Get the underlying ValueView which has the data() method
    auto value_view = view.value_view();
    auto result = value::value_to_python(value_view.data(), schema);

    // Cache the result using last_modified_time as the cache time
    cache_value(const_cast<TSOutput*>(output), result, output->last_modified_time());

    return result;
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

/**
 * Get the Python delta value from a TSOutput.
 *
 * For collection types (TSS, TSD, TSL), returns the cached delta.
 * For scalar types (TS) and bundles (TSB), uses DeltaView.
 *
 * @param output The output to get delta from
 * @param eval_time The current evaluation time
 * @param meta Optional type metadata (uses output->meta() if null)
 * @return The Python delta object, or None if not modified
 */
inline nb::object get_python_delta(const TSOutput* output, engine_time_t eval_time, const TSMeta* meta = nullptr) {
    if (!output) return nb::none();
    if (!meta) meta = output->meta();

    // Check if modified at current time
    auto view = const_cast<TSOutput*>(output)->view();
    if (!view.modified_at(eval_time)) {
        return nb::none();
    }

    // For collection types (TSD, TSL, TSS) and REF, check for cached delta
    // REF is included because RefStorage loses path information during conversion
    if (meta) {
        auto ts_kind = meta->ts_kind;
        if (ts_kind == TSKind::TSD ||
            ts_kind == TSKind::TSL ||
            ts_kind == TSKind::TSS ||
            ts_kind == TSKind::REF) {
            auto cached = get_cached_delta(output);
            if (!cached.is_none()) {
                return cached;
            }
        }
    }

    // For TS and TSB types, use DeltaView-based conversion
    // The view was already retrieved above
    auto delta = view.delta_view(eval_time);
    if (!delta.valid()) {
        return nb::none();
    }

    // For simple scalar types, just return the value
    if (!meta || meta->ts_kind == TSKind::TS) {
        auto value_view = delta.scalar_delta();
        if (!value_view.valid()) return nb::none();
        return value::value_to_python(value_view.data(), value_view.schema());
    }

    // For TSB, we'd need to recursively build the delta dict
    // But this function is mainly used for child outputs which are typically TS[T]
    // For complex types, just return the value for now
    auto value_view = view.value_view();
    if (!value_view.valid()) return nb::none();
    return value::value_to_python(value_view.data(), view.schema());
}

} // namespace hgraph::ts

// Include type-erased copy helpers (no nanobind dependency)
#include <hgraph/types/time_series/ts_copy_helpers.h>

#endif // HGRAPH_TS_PYTHON_HELPERS_H
