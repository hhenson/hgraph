#pragma once

/**
 * @file ts_view.h
 * @brief TSView - Non-owning time-series view.
 *
 * TSView provides coordinated read/write access to a TSValue's four parallel
 * Value structures. It captures the current engine time at construction,
 * enabling proper modified() checks and delta clearing.
 *
 * TSView is lightweight (six pointers + time) and designed to be passed by value.
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_scalar_view.h>
#include <hgraph/types/time_series/ts_bundle_view.h>
#include <hgraph/types/time_series/ts_list_view.h>
#include <hgraph/types/time_series/ts_set_view.h>
#include <hgraph/types/time_series/ts_dict_view.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

namespace hgraph {

// Forward declaration
class TSValue;

/**
 * @brief Non-owning view of a time-series value.
 *
 * TSView provides access to the four parallel Value structures in a TSValue:
 * - value: The user-visible data
 * - time: Modification timestamps
 * - observer: Observer lists
 * - delta_value: Delta tracking data
 *
 * TSView captures the current_time at construction, which is used for:
 * - modified() checks (compares last_modified_time >= current_time)
 * - Lazy delta clearing (when current_time > last_delta_clear_time)
 *
 * Usage:
 * @code
 * TSView view = ts_value.ts_view(current_time);
 *
 * // Check modification status
 * if (view.modified()) {
 *     // Access value
 *     auto val = view.value().as<int>();
 *
 *     // Access delta if available
 *     if (view.has_delta()) {
 *         auto delta = view.delta_value();
 *     }
 * }
 * @endcode
 */
class TSView {
public:
    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an invalid TSView.
     */
    TSView() noexcept = default;

    /**
     * @brief Construct from a TSValue.
     *
     * Captures views of all four parallel Values and the current time.
     *
     * @param ts_value The owning TSValue
     * @param current_time The current engine time
     */
    TSView(TSValue& ts_value, engine_time_t current_time);

    /**
     * @brief Construct directly from views.
     *
     * Low-level constructor for advanced use cases.
     *
     * @param meta The time-series metadata
     * @param value_view View of the value data
     * @param time_view View of the time data
     * @param observer_view View of the observer data
     * @param delta_value_view View of the delta data
     * @param current_time The current engine time
     */
    TSView(const TSMeta* meta,
           value::View value_view,
           value::View time_view,
           value::View observer_view,
           value::View delta_value_view,
           engine_time_t current_time) noexcept;

    // ========== Metadata ==========

    /**
     * @brief Get the time-series metadata.
     * @return The TSMeta, or nullptr if invalid
     */
    [[nodiscard]] const TSMeta* meta() const noexcept { return meta_; }

    /**
     * @brief Get the current engine time.
     * @return The time captured at construction
     */
    [[nodiscard]] engine_time_t current_time() const noexcept { return current_time_; }

    // ========== View Access ==========

    /**
     * @brief Get a view of the value data.
     * @return View of value_
     */
    [[nodiscard]] value::View value() const { return value_view_; }

    /**
     * @brief Get a view of the time data.
     * @return View of time_
     */
    [[nodiscard]] value::View time() const { return time_view_; }

    /**
     * @brief Get a view of the observer data.
     * @return View of observer_
     */
    [[nodiscard]] value::View observer() const { return observer_view_; }

    /**
     * @brief Get a view of the delta value data.
     * @return View of delta_value_
     */
    [[nodiscard]] value::View delta_value() const { return delta_value_view_; }

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
     * last_modified_time >= current_time.
     *
     * @return true if last_modified_time >= current_time
     */
    [[nodiscard]] bool modified() const;

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
     * @return true if delta tracking is enabled
     */
    [[nodiscard]] bool has_delta() const;

    // ========== Kind-Specific Views ==========

    /**
     * @brief Get as a scalar view.
     *
     * Valid for: TSValue, TSW, SIGNAL
     *
     * @tparam T The scalar type
     * @return TSScalarView with modification tracking
     */
    template<typename T>
    [[nodiscard]] TSScalarView<T> as_scalar() {
        return TSScalarView<T>(value_view_, time_view_, observer_view_, current_time_);
    }

    /**
     * @brief Get as a bundle view.
     *
     * Valid for: TSB
     *
     * @return TSBView with field access and modification bubbling
     */
    [[nodiscard]] TSBView as_bundle() {
        return TSBView(meta_, value_view_, time_view_, observer_view_, current_time_);
    }

    /**
     * @brief Get as a list view.
     *
     * Valid for: TSL
     *
     * @return TSLView with element access and modification bubbling
     */
    [[nodiscard]] TSLView as_list() {
        return TSLView(meta_, value_view_, time_view_, observer_view_, current_time_);
    }

    /**
     * @brief Get as a set view.
     *
     * Valid for: TSS
     *
     * @return TSSView with delta tracking
     */
    [[nodiscard]] TSSView as_set() {
        return TSSView(meta_, value_view_, time_view_, observer_view_,
                       delta_value_view_, current_time_);
    }

    /**
     * @brief Get as a dict view.
     *
     * Valid for: TSD
     *
     * @return TSDView with delta tracking and modification bubbling
     */
    [[nodiscard]] TSDView as_dict() {
        return TSDView(meta_, value_view_, time_view_, observer_view_,
                       delta_value_view_, current_time_);
    }

private:
    value::View value_view_;
    value::View time_view_;
    value::View observer_view_;
    value::View delta_value_view_;
    const TSMeta* meta_{nullptr};
    engine_time_t current_time_{MIN_ST};
};

} // namespace hgraph
