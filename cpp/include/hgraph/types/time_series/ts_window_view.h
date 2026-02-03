#pragma once

/**
 * @file ts_window_view.h
 * @brief TSWView - View for time-series window (TSW) types.
 *
 * TSWView provides window operations with delta tracking.
 * It wraps ViewData and delegates all operations to the ts_ops vtable.
 *
 * Key design: The appropriate ops table (fixed_window_ts_ops or time_window_ts_ops)
 * is selected at construction time based on TSMeta::is_duration_based.
 * All methods simply delegate to the ops table - NO branching in this class.
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <vector>

namespace hgraph {

/**
 * @brief View for time-series window (TSW) types.
 *
 * TSWView provides read access to windowed time-series data.
 * All operations delegate to the ts_ops vtable - NO branching
 * occurs in this class.
 *
 * The appropriate ops table (fixed_window_ts_ops or time_window_ts_ops)
 * is selected at ViewData construction time based on TSMeta::is_duration_based.
 *
 * Usage:
 * @code
 * TSWView window = ts_view.as_window();
 *
 * // Access values
 * nb::object values = window.to_python();
 *
 * // Window parameters
 * size_t size = window.window_size();
 * size_t len = window.length();
 *
 * // Removed values
 * if (window.has_removed_value()) {
 *     value::View removed = window.removed_value();
 * }
 *
 * // Timestamps
 * engine_time_t first = window.first_modified_time();
 * @endcode
 */
class TSWView {
public:
    /**
     * @brief Construct a window view from ViewData.
     *
     * @param view_data ViewData containing all data pointers and metadata
     * @param current_time The current engine time
     */
    TSWView(ViewData view_data, engine_time_t current_time) noexcept
        : view_data_(std::move(view_data))
        , current_time_(current_time)
    {}

    /**
     * @brief Default constructor - creates invalid view.
     */
    TSWView() noexcept = default;

    // ========== Metadata ==========

    /**
     * @brief Get the TSMeta.
     */
    [[nodiscard]] const TSMeta* meta() const noexcept {
        return view_data_.meta;
    }

    /**
     * @brief Get the underlying ViewData.
     */
    [[nodiscard]] const ViewData& view_data() const noexcept {
        return view_data_;
    }

    /**
     * @brief Get the current engine time.
     */
    [[nodiscard]] engine_time_t current_time() const noexcept {
        return current_time_;
    }

    /**
     * @brief Check if this is a duration-based (time) window.
     * @return true if duration-based, false if tick-based
     */
    [[nodiscard]] bool is_duration_based() const noexcept {
        return meta() && meta()->is_duration_based;
    }

    // ========== Value Access (delegate to ops) ==========

    /**
     * @brief Get the window values as a View.
     */
    [[nodiscard]] value::View value() const {
        if (!view_data_.ops) return value::View{};
        return view_data_.ops->value(view_data_);
    }

    /**
     * @brief Get the delta value (newest element added this tick).
     */
    [[nodiscard]] value::View delta_value() const {
        if (!view_data_.ops) return value::View{};
        return view_data_.ops->delta_value(view_data_);
    }

    /**
     * @brief Convert the values to a Python object.
     */
    [[nodiscard]] nb::object to_python() const {
        if (!view_data_.ops) return nb::none();
        return view_data_.ops->to_python(view_data_);
    }

    /**
     * @brief Convert the delta to a Python object.
     */
    [[nodiscard]] nb::object delta_to_python() const {
        if (!view_data_.ops) return nb::none();
        return view_data_.ops->delta_to_python(view_data_);
    }

    // ========== Window-Specific Operations (delegate to ops) ==========

    /**
     * @brief Get timestamps for all values in the window.
     *
     * @return Pointer to timestamps array, or nullptr if invalid
     */
    [[nodiscard]] const engine_time_t* value_times() const {
        if (!view_data_.ops || !view_data_.ops->window_value_times) return nullptr;
        return view_data_.ops->window_value_times(view_data_);
    }

    /**
     * @brief Get the number of timestamps (same as window length).
     */
    [[nodiscard]] size_t value_times_count() const {
        if (!view_data_.ops || !view_data_.ops->window_value_times_count) return 0;
        return view_data_.ops->window_value_times_count(view_data_);
    }

    /**
     * @brief Get the timestamp of the oldest entry in the window.
     *
     * @return Timestamp or MIN_ST if empty
     */
    [[nodiscard]] engine_time_t first_modified_time() const {
        if (!view_data_.ops || !view_data_.ops->window_first_modified_time) return MIN_ST;
        return view_data_.ops->window_first_modified_time(view_data_);
    }

    /**
     * @brief Check if values were evicted from the window this tick.
     */
    [[nodiscard]] bool has_removed_value() const {
        if (!view_data_.ops || !view_data_.ops->window_has_removed_value) return false;
        return view_data_.ops->window_has_removed_value(view_data_);
    }

    /**
     * @brief Get the evicted value(s).
     *
     * For fixed windows: single element
     * For time windows: may be multiple elements (returned as array view)
     *
     * @return View of removed value, or invalid View if none
     */
    [[nodiscard]] value::View removed_value() const {
        if (!view_data_.ops || !view_data_.ops->window_removed_value) return value::View{};
        return view_data_.ops->window_removed_value(view_data_);
    }

    /**
     * @brief Get the number of removed values.
     *
     * For fixed windows: 0 or 1
     * For time windows: 0 to N
     */
    [[nodiscard]] size_t removed_value_count() const {
        if (!view_data_.ops || !view_data_.ops->window_removed_value_count) return 0;
        return view_data_.ops->window_removed_value_count(view_data_);
    }

    /**
     * @brief Get the window size parameter.
     *
     * For fixed windows: tick count
     * For time windows: duration in microseconds
     */
    [[nodiscard]] size_t window_size() const {
        if (!view_data_.ops || !view_data_.ops->window_size) return 0;
        return view_data_.ops->window_size(view_data_);
    }

    /**
     * @brief Get the minimum window size parameter.
     *
     * For fixed windows: minimum tick count
     * For time windows: minimum duration in microseconds
     */
    [[nodiscard]] size_t min_window_size() const {
        if (!view_data_.ops || !view_data_.ops->window_min_size) return 0;
        return view_data_.ops->window_min_size(view_data_);
    }

    /**
     * @brief Get the current number of elements in the window.
     */
    [[nodiscard]] size_t length() const {
        if (!view_data_.ops || !view_data_.ops->window_length) return 0;
        return view_data_.ops->window_length(view_data_);
    }

    /**
     * @brief Get direct access to removed value pointers (for time windows).
     *
     * For time windows, removed values are stored in a vector of void pointers.
     * This method provides direct access for Python interop.
     *
     * @return Pointer to vector of removed value pointers, or nullptr
     */
    [[nodiscard]] const std::vector<void*>* removed_value_ptrs() const {
        if (!is_duration_based()) return nullptr;
        // link_data holds std::vector<void*>* for time windows
        return static_cast<const std::vector<void*>*>(view_data_.link_data);
    }

    // ========== Time-Series Semantics (delegate to ops) ==========

    /**
     * @brief Get the last modification time.
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        if (!view_data_.ops) return MIN_ST;
        return view_data_.ops->last_modified_time(view_data_);
    }

    /**
     * @brief Check if modified this tick.
     */
    [[nodiscard]] bool modified() const {
        if (!view_data_.ops) return false;
        return view_data_.ops->modified(view_data_, current_time_);
    }

    /**
     * @brief Check if the window has ever been set.
     */
    [[nodiscard]] bool valid() const {
        if (!view_data_.ops) return false;
        return view_data_.ops->valid(view_data_);
    }

    /**
     * @brief Check if the window meets minimum size requirements.
     *
     * For fixed windows: length >= min_size
     * For time windows: span >= min_time_range
     */
    [[nodiscard]] bool all_valid() const {
        if (!view_data_.ops) return false;
        return view_data_.ops->all_valid(view_data_);
    }

private:
    ViewData view_data_;
    engine_time_t current_time_{MIN_ST};
};

} // namespace hgraph
