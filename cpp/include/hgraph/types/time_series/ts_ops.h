#pragma once

/**
 * @file ts_ops.h
 * @brief ts_ops - Operations vtable for time-series types.
 *
 * ts_ops provides polymorphic operations for time-series values,
 * similar to how value::TypeOps works for the Value layer.
 * This enables TSView to work uniformly across all TS kinds.
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {

// Forward declarations
struct ViewData;
class TSView;
class ShortPath;
class TSInput;

/**
 * @brief Operations vtable for time-series types.
 *
 * ts_ops enables polymorphic dispatch for TSView operations based on
 * the time-series kind (TS, TSB, TSL, TSD, TSS, TSW, REF, SIGNAL).
 *
 * Each TS kind has its own ts_ops instance with appropriate implementations.
 * The ops pointer is stored in ViewData and used by TSView for dispatch.
 */
struct ts_ops {
    // ========== Schema Access ==========

    /**
     * @brief Get the TSMeta for this time-series.
     */
    const TSMeta* (*ts_meta)(const ViewData& vd);

    // ========== Time-Series Semantics ==========

    /**
     * @brief Get the last modification time.
     *
     * For containers, returns the container's modification time.
     */
    engine_time_t (*last_modified_time)(const ViewData& vd);

    /**
     * @brief Check if modified at or after current_time.
     *
     * Uses >= comparison: modified if last_modified_time >= current_time.
     */
    bool (*modified)(const ViewData& vd, engine_time_t current_time);

    /**
     * @brief Check if the value has ever been set.
     *
     * Valid if last_modified_time != MIN_DT.
     */
    bool (*valid)(const ViewData& vd);

    /**
     * @brief Check if this AND all children are valid.
     *
     * For scalars, same as valid(). For containers, recursively checks children.
     */
    bool (*all_valid)(const ViewData& vd);

    /**
     * @brief Check if this view was obtained through a modified REF.
     *
     * When a REF changes target, views obtained through it are "sampled"
     * and report modified=true regardless of target modification.
     */
    bool (*sampled)(const ViewData& vd);

    // ========== Value Access ==========

    /**
     * @brief Get the value as a View.
     */
    value::View (*value)(const ViewData& vd);

    /**
     * @brief Get the delta value as a View.
     *
     * Returns an invalid View if no delta tracking for this kind.
     */
    value::View (*delta_value)(const ViewData& vd);

    /**
     * @brief Check if delta tracking is enabled for this kind.
     */
    bool (*has_delta)(const ViewData& vd);

    // ========== Mutation (for outputs) ==========

    /**
     * @brief Set the value from a View.
     *
     * Updates modification time and notifies observers.
     */
    void (*set_value)(ViewData& vd, const value::View& src, engine_time_t current_time);

    /**
     * @brief Apply a delta to the value.
     *
     * Updates modification time and notifies observers.
     */
    void (*apply_delta)(ViewData& vd, const value::View& delta, engine_time_t current_time);

    /**
     * @brief Invalidate the value (reset to never-set state).
     */
    void (*invalidate)(ViewData& vd);

    // ========== Python Interop ==========

    /**
     * @brief Convert the value to a Python object.
     */
    nb::object (*to_python)(const ViewData& vd);

    /**
     * @brief Convert the delta to a Python object.
     */
    nb::object (*delta_to_python)(const ViewData& vd);

    /**
     * @brief Set the value from a Python object.
     */
    void (*from_python)(ViewData& vd, const nb::object& src, engine_time_t current_time);

    // ========== Navigation ==========

    /**
     * @brief Get a child TSView by index.
     *
     * For TSB: index is field index
     * For TSL: index is element index
     * For TSD: index is slot index
     *
     * @return TSView for the child, or invalid TSView if not applicable
     */
    TSView (*child_at)(const ViewData& vd, size_t index, engine_time_t current_time);

    /**
     * @brief Get a child TSView by name.
     *
     * Only valid for TSB (bundle field access).
     *
     * @return TSView for the field, or invalid TSView if not found
     */
    TSView (*child_by_name)(const ViewData& vd, const std::string& name, engine_time_t current_time);

    /**
     * @brief Get a child TSView by key.
     *
     * Only valid for TSD (dict value access by key).
     *
     * @return TSView for the value, or invalid TSView if key not found
     */
    TSView (*child_by_key)(const ViewData& vd, const value::View& key, engine_time_t current_time);

    /**
     * @brief Get the number of children.
     *
     * For TSB: number of fields
     * For TSL: number of elements
     * For TSD: number of key-value pairs
     * For TSS: number of set elements
     * For scalars: 0
     */
    size_t (*child_count)(const ViewData& vd);

    // ========== Observer Management ==========

    /**
     * @brief Get the observer list for this time-series.
     */
    value::View (*observer)(const ViewData& vd);

    /**
     * @brief Notify observers of modification.
     */
    void (*notify_observers)(ViewData& vd, engine_time_t current_time);

    // ========== Link Management ==========

    /**
     * @brief Bind this position to a target.
     *
     * Creates a link from the current position to the target TSView.
     * After binding, navigation to this position will redirect to the target.
     *
     * @param vd The ViewData for the position to bind
     * @param target The target ViewData to bind to
     */
    void (*bind)(ViewData& vd, const ViewData& target);

    /**
     * @brief Remove the link at this position.
     *
     * After unbinding, the position reverts to local storage.
     *
     * @param vd The ViewData for the position to unbind
     */
    void (*unbind)(ViewData& vd);

    /**
     * @brief Check if this position is bound (linked).
     *
     * @param vd The ViewData to check
     * @return true if the position is linked to another target
     */
    bool (*is_bound)(const ViewData& vd);

    /**
     * @brief Check if this position has a peer (peered binding).
     *
     * For scalars: returns is_bound (scalars are always peered when bound).
     * For collections/bundles: returns true only if the binding happened
     * at the collection level (not individual element bindings from from_ts).
     *
     * @param vd The ViewData to check
     * @return true if peered binding
     */
    bool (*is_peered)(const ViewData& vd);

    // ========== Input Active State Management ==========

    /**
     * @brief Set the active state for an input at this position.
     *
     * Recursively sets the active state for this position and all children,
     * and manages subscriptions on linked outputs.
     *
     * For scalars: Sets the active bool and manages subscription
     * For bundles: Sets root bool, recursively sets on all fields
     * For collections: Sets root bool, recursively sets on all elements
     *
     * @param vd The ViewData for the current position
     * @param active_view The active schema view for this position
     * @param active The active state to set
     * @param input The TSInput for subscription management
     */
    void (*set_active)(ViewData& vd, value::View active_view, bool active, TSInput* input);

    // ========== Window-Specific Operations ==========
    // These are nullptr for non-window types (TSValue, TSB, TSL, TSD, TSS, REF, SIGNAL)

    /**
     * @brief Get timestamps for all values in the window.
     *
     * Only valid for TSW. Returns nullptr for other kinds.
     *
     * @return Pointer to timestamps array, or nullptr
     */
    const engine_time_t* (*window_value_times)(const ViewData& vd);

    /**
     * @brief Get the number of timestamps (same as window length).
     *
     * Only valid for TSW. Returns 0 for other kinds.
     */
    size_t (*window_value_times_count)(const ViewData& vd);

    /**
     * @brief Get the timestamp of the oldest entry in the window.
     *
     * Only valid for TSW. Returns MIN_DT for other kinds.
     */
    engine_time_t (*window_first_modified_time)(const ViewData& vd);

    /**
     * @brief Check if values were evicted from the window this tick.
     *
     * Only valid for TSW. Returns false for other kinds.
     */
    bool (*window_has_removed_value)(const ViewData& vd);

    /**
     * @brief Get the evicted value(s).
     *
     * Only valid for TSW. Returns invalid View for other kinds.
     * For fixed windows: single element
     * For time windows: may be multiple elements
     */
    value::View (*window_removed_value)(const ViewData& vd);

    /**
     * @brief Get the number of removed values.
     *
     * Only valid for TSW. Returns 0 for other kinds.
     * For fixed windows: 0 or 1
     * For time windows: 0 to N
     */
    size_t (*window_removed_value_count)(const ViewData& vd);

    /**
     * @brief Get the window capacity/duration parameter.
     *
     * Only valid for TSW.
     * For fixed windows: returns tick count (size_t)
     * For time windows: returns duration in nanoseconds (cast from engine_time_delta_t)
     */
    size_t (*window_size)(const ViewData& vd);

    /**
     * @brief Get the minimum window size parameter.
     *
     * Only valid for TSW.
     * For fixed windows: returns min tick count
     * For time windows: returns min duration in nanoseconds
     */
    size_t (*window_min_size)(const ViewData& vd);

    /**
     * @brief Get the current number of elements in the window.
     *
     * Only valid for TSW. Returns 0 for other kinds.
     */
    size_t (*window_length)(const ViewData& vd);

    // ========== Set-Specific Operations ==========
    // These are nullptr for non-set types (TSValue, TSB, TSL, TSD, TSW, REF, SIGNAL)

    /**
     * @brief Add an element to a set.
     *
     * Only valid for TSS. Updates timestamp and notifies observers on success.
     *
     * @param vd The ViewData for the set
     * @param elem The element to add
     * @param current_time The current engine time for timestamp update
     * @return true if element was added (not already present)
     */
    bool (*set_add)(ViewData& vd, const value::View& elem, engine_time_t current_time);

    /**
     * @brief Remove an element from a set.
     *
     * Only valid for TSS. Updates timestamp and notifies observers on success.
     *
     * @param vd The ViewData for the set
     * @param elem The element to remove
     * @param current_time The current engine time for timestamp update
     * @return true if element was removed (was present)
     */
    bool (*set_remove)(ViewData& vd, const value::View& elem, engine_time_t current_time);

    /**
     * @brief Clear all elements from a set.
     *
     * Only valid for TSS. Updates timestamp and notifies observers if set was non-empty.
     *
     * @param vd The ViewData for the set
     * @param current_time The current engine time for timestamp update
     */
    void (*set_clear)(ViewData& vd, engine_time_t current_time);

    // ========== Dict-Specific Operations ==========
    // These are nullptr for non-dict types (TSValue, TSB, TSL, TSS, TSW, REF, SIGNAL)

    /**
     * @brief Remove a key from a dict.
     *
     * Only valid for TSD. Updates timestamp and notifies observers on success.
     * The removed entry's value remains accessible during the current tick
     * (the slot is placed on a free list used in the next engine cycle).
     *
     * @param vd The ViewData for the dict
     * @param key The key to remove
     * @param current_time The current engine time for timestamp update
     * @return true if key was removed (was present)
     */
    bool (*dict_remove)(ViewData& vd, const value::View& key, engine_time_t current_time);

    /**
     * @brief Create a new entry in a dict.
     *
     * Only valid for TSD. Creates a new key-value entry with default-initialized
     * value storage. Updates timestamp and notifies observers on success.
     * If the key already exists, returns a view to the existing entry without
     * modification.
     *
     * @param vd The ViewData for the dict
     * @param key The key to create
     * @param current_time The current engine time for timestamp update
     * @return TSView for the created (or existing) value entry
     */
    TSView (*dict_create)(ViewData& vd, const value::View& key, engine_time_t current_time);

    /**
     * @brief Set a key-value pair in a dict.
     *
     * Only valid for TSD. Creates the entry if the key doesn't exist, then sets
     * the value. Updates both element and container timestamps, notifies observers.
     *
     * @param vd The ViewData for the dict
     * @param key The key to set
     * @param value The value to set
     * @param current_time The current engine time for timestamp update
     * @return TSView for the value entry
     */
    TSView (*dict_set)(ViewData& vd, const value::View& key, const value::View& value, engine_time_t current_time);
};

/**
 * @brief Clear thread-local caches that hold nb::object references.
 *
 * Must be called before Python interpreter shutdown to avoid
 * SIGSEGV from destroying nb::object after the GC has freed them.
 */
void clear_thread_local_caches();

/**
 * @brief Get the ts_ops for a specific TSKind.
 *
 * @param kind The time-series kind
 * @return Pointer to the ops table for that kind
 *
 * @note For TSW, this returns scalar_ops. Use get_ts_ops(const TSMeta*)
 * to get the correct window-specific ops based on is_duration_based.
 */
const ts_ops* get_ts_ops(TSKind kind);

/**
 * @brief Get the ts_ops for a TSMeta.
 *
 * For TSW types, this selects the appropriate window ops table based on
 * TSMeta::is_duration_based (fixed_window_ts_ops or time_window_ts_ops).
 *
 * @param meta The time-series metadata
 * @return Pointer to the ops table
 */
const ts_ops* get_ts_ops(const TSMeta* meta);

} // namespace hgraph
