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
     * Valid if last_modified_time != MIN_ST.
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
};

/**
 * @brief Get the ts_ops for a specific TSKind.
 *
 * @param kind The time-series kind
 * @return Pointer to the ops table for that kind
 */
const ts_ops* get_ts_ops(TSKind kind);

/**
 * @brief Get the ts_ops for a TSMeta.
 *
 * Convenience function that extracts the kind from the meta.
 *
 * @param meta The time-series metadata
 * @return Pointer to the ops table
 */
inline const ts_ops* get_ts_ops(const TSMeta* meta) {
    return meta ? get_ts_ops(meta->kind) : nullptr;
}

} // namespace hgraph
