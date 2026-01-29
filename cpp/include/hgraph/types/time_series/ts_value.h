#pragma once

/**
 * @file ts_value.h
 * @brief TSValue - Owning time-series value storage.
 *
 * TSValue is the owning counterpart to TSView, providing storage for a
 * time-series value with four parallel Value structures:
 *
 * 1. value_: User-visible data (derived from TSMeta's value schema)
 * 2. time_: Modification timestamps (recursive, mirrors data structure)
 * 3. observer_: Observer lists (recursive, mirrors data structure)
 * 4. delta_value_: Delta tracking data (only where TSS/TSD exist)
 *
 * Key design principles:
 * - Lazy delta clearing: when current_time > last_delta_clear_time_
 * - Time comparison: >= for modified(), > for delta clearing
 * - Slot-based delta tracking (indices, not copies)
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

namespace hgraph {

// Forward declarations
class TSView;
struct ViewData;

/**
 * @brief Owning time-series value storage with four parallel Values.
 *
 * TSValue owns the storage for a time-series value. It manages:
 * - The user-visible value (value_)
 * - Modification timestamps (time_)
 * - Observer lists (observer_)
 * - Delta tracking (delta_value_)
 *
 * Usage:
 * @code
 * // Create from TSMeta
 * TSValue ts(meta);
 *
 * // Access views
 * value::View value = ts.value_view();
 * value::View time = ts.time_view();
 *
 * // Check modification status
 * if (ts.modified(current_time)) {
 *     // Process the value
 * }
 *
 * // Get delta (with automatic lazy clearing)
 * value::View delta = ts.delta_value_view(current_time);
 * @endcode
 */
class TSValue {
public:
    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an invalid TSValue.
     */
    TSValue() noexcept = default;

    /**
     * @brief Construct from TSMeta.
     *
     * Allocates storage for all four parallel Values based on the
     * TSMeta's generated schemas.
     *
     * @param meta The time-series metadata
     */
    explicit TSValue(const TSMeta* meta);

    /**
     * @brief Destructor.
     */
    ~TSValue() = default;

    // ========== Move Semantics ==========

    TSValue(TSValue&& other) noexcept;
    TSValue& operator=(TSValue&& other) noexcept;

    // ========== Copy (Deleted) ==========

    TSValue(const TSValue&) = delete;
    TSValue& operator=(const TSValue&) = delete;

    // ========== Metadata Access ==========

    /**
     * @brief Get the time-series metadata.
     * @return The TSMeta, or nullptr if invalid
     */
    [[nodiscard]] const TSMeta* meta() const noexcept { return meta_; }

    // ========== View Access ==========

    /**
     * @brief Get a mutable view of the value data.
     * @return Mutable view of value_
     */
    [[nodiscard]] value::View value_view();

    /**
     * @brief Get a const view of the value data.
     * @return Const view of value_
     */
    [[nodiscard]] value::View value_view() const;

    /**
     * @brief Get a mutable view of the time data.
     * @return Mutable view of time_
     */
    [[nodiscard]] value::View time_view();

    /**
     * @brief Get a const view of the time data.
     * @return Const view of time_
     */
    [[nodiscard]] value::View time_view() const;

    /**
     * @brief Get a mutable view of the observer data.
     * @return Mutable view of observer_
     */
    [[nodiscard]] value::View observer_view();

    /**
     * @brief Get a const view of the observer data.
     * @return Const view of observer_
     */
    [[nodiscard]] value::View observer_view() const;

    /**
     * @brief Get a view of the delta value data with lazy clearing.
     *
     * If current_time > last_delta_clear_time_, the delta is cleared
     * before returning the view. This ensures delta reflects only
     * changes since the last tick.
     *
     * @param current_time The current engine time
     * @return Mutable view of delta_value_
     */
    [[nodiscard]] value::View delta_value_view(engine_time_t current_time);

    /**
     * @brief Get a const view of the delta value data.
     *
     * Does not perform lazy clearing. Use with caution as the delta
     * may contain stale data.
     *
     * @return Const view of delta_value_
     */
    [[nodiscard]] value::View delta_value_view() const;

    /**
     * @brief Get a mutable view of the link data.
     * @return Mutable view of link_
     */
    [[nodiscard]] value::View link_view();

    /**
     * @brief Get a const view of the link data.
     * @return Const view of link_
     */
    [[nodiscard]] value::View link_view() const;

    // ========== Time-Series Semantics ==========

    /**
     * @brief Get the last modification time.
     *
     * For atomic TS types, this is the direct timestamp.
     * For composite types (TSB/TSL/TSD), this is the container's timestamp.
     *
     * @return The last modification time, or MIN_ST if never modified
     */
    [[nodiscard]] engine_time_t last_modified_time() const;

    /**
     * @brief Check if modified at or after current_time.
     *
     * Uses >= comparison: something is modified at current_time if
     * last_modified_time >= current_time. This handles:
     * - Modification during this tick (equal)
     * - Modification in future tick (greater, for out-of-order processing)
     *
     * @param current_time The current engine time
     * @return true if last_modified_time >= current_time
     */
    [[nodiscard]] bool modified(engine_time_t current_time) const;

    /**
     * @brief Check if the value has ever been set.
     *
     * A value is valid if last_modified_time != MIN_ST.
     *
     * @return true if the value has been set at least once
     */
    [[nodiscard]] bool valid() const;

    /**
     * @brief Check if this time-series type has delta tracking.
     *
     * Delegates to hgraph::has_delta(meta_).
     *
     * @return true if delta tracking is enabled
     */
    [[nodiscard]] bool has_delta() const;

    // ========== TSView Access ==========

    /**
     * @brief Get a TSView for coordinated access.
     *
     * @param current_time The current engine time
     * @return TSView wrapping this TSValue
     */
    [[nodiscard]] TSView ts_view(engine_time_t current_time);

    /**
     * @brief Create ViewData for this TSValue.
     *
     * Creates the ViewData structure containing pointers to all four
     * parallel values and the ts_ops vtable. The ShortPath in the
     * returned ViewData is empty and should be set by the caller.
     *
     * @return ViewData with data pointers and ops
     */
    [[nodiscard]] ViewData make_view_data();

private:
    // ========== Internal Methods ==========

    /**
     * @brief Clear the delta value data.
     *
     * Called lazily when current_time > last_delta_clear_time_.
     */
    void clear_delta_value();

    /**
     * @brief Wire observers for collection types.
     *
     * Sets up SlotObserver connections for TSD/TSS delta tracking.
     */
    void wire_observers();

    /**
     * @brief Wire observers for TSB fields recursively.
     *
     * @param value_v View of the value structure
     * @param delta_v View of the delta structure
     */
    void wire_tsb_observers(value::View value_v, value::View delta_v);

    /**
     * @brief Wire observers for TSL elements recursively.
     *
     * @param value_v View of the value structure
     * @param delta_v View of the delta structure
     */
    void wire_tsl_observers(value::View value_v, value::View delta_v);

    // ========== Member Variables ==========

    /**
     * @brief The user-visible value.
     *
     * Schema from TSMeta->value_type for atomic, or TSMeta-based for composite.
     */
    value::Value<> value_;

    /**
     * @brief Modification timestamps (parallel to value structure).
     *
     * Schema from generate_time_schema(meta_).
     */
    value::Value<> time_;

    /**
     * @brief Observer lists (parallel to value structure).
     *
     * Schema from generate_observer_schema(meta_).
     */
    value::Value<> observer_;

    /**
     * @brief Delta tracking data.
     *
     * Schema from generate_delta_value_schema(meta_), may be null.
     */
    value::Value<> delta_value_;

    /**
     * @brief Link tracking data (parallel to value structure).
     *
     * Schema from generate_link_schema(meta_), may be null for scalar types.
     * For TSL/TSD: Single bool indicating collection-level link.
     * For TSB: fixed_list[bool] with one entry per field.
     */
    value::Value<> link_;

    /**
     * @brief The time-series metadata.
     */
    const TSMeta* meta_{nullptr};

    /**
     * @brief Last time delta was cleared.
     *
     * Used for lazy clearing: if current_time > last_delta_clear_time_,
     * the delta should be cleared before accessing.
     */
    engine_time_t last_delta_clear_time_{MIN_ST};
};

} // namespace hgraph
