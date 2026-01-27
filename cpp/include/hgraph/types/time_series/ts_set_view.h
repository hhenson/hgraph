#pragma once

/**
 * @file ts_set_view.h
 * @brief TSSView - View for time-series set (TSS) types.
 *
 * TSSView provides set operations with delta tracking.
 * Access elements via values() to iterate, check membership via contains().
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/util/date_time.h>

namespace hgraph {

/**
 * @brief View for time-series set (TSS) types.
 *
 * TSSView provides set membership queries and delta tracking for
 * elements added/removed this tick.
 *
 * Usage:
 * @code
 * TSSView set = ts_view.as_set();
 *
 * // Check membership
 * value::View elem = value::make_scalar<int64_t>(42);
 * if (set.contains(elem)) { ... }
 *
 * // Iterate over current values
 * for (auto val : set.values()) {
 *     std::cout << val.as<int64_t>() << "\n";
 * }
 *
 * // Check delta - what was added/removed this tick
 * for (auto val : set.added()) { ... }
 * for (auto val : set.removed()) { ... }
 *
 * // Check specific element changes
 * if (set.was_added(elem)) { ... }
 * if (set.was_removed(elem)) { ... }
 * @endcode
 */
class TSSView {
public:
    /**
     * @brief Construct a set view from ViewData.
     *
     * @param view_data ViewData containing all data pointers and metadata
     * @param current_time The current engine time
     */
    TSSView(ViewData view_data, engine_time_t current_time) noexcept
        : view_data_(std::move(view_data))
        , current_time_(current_time)
    {}

    /**
     * @brief Default constructor - creates invalid view.
     */
    TSSView() noexcept = default;

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

    // ========== Set Operations ==========

    /**
     * @brief Check if the set contains an element.
     *
     * @param elem The element (as a value::View)
     * @return true if element is present
     */
    [[nodiscard]] bool contains(const value::View& elem) const {
        return value_view().as_set().contains(elem);
    }

    /**
     * @brief Get the set size.
     */
    [[nodiscard]] size_t size() const {
        return value_view().as_set().size();
    }

    // ========== Value Iteration ==========

    /**
     * @brief Get the underlying set view for iteration.
     *
     * Use begin()/end() on the returned SetView for range-based for loops:
     * @code
     * for (auto val : set.values()) {
     *     std::cout << val.as<int64_t>() << "\n";
     * }
     * @endcode
     *
     * @return SetView for iteration
     */
    [[nodiscard]] value::SetView values() const {
        return value_view().as_set();
    }

    // ========== Delta Access ==========

    /**
     * @brief Get the slot indices of elements added this tick.
     *
     * @return Vector of added slot indices
     */
    [[nodiscard]] const std::vector<size_t>& added_slots() const {
        return delta()->added();
    }

    /**
     * @brief Get the slot indices of elements removed this tick.
     *
     * @return Vector of removed slot indices
     */
    [[nodiscard]] const std::vector<size_t>& removed_slots() const {
        return delta()->removed();
    }

    /**
     * @brief Check if a specific element was added this tick.
     *
     * @param elem The element to check
     * @return true if element was added
     */
    [[nodiscard]] bool was_added(const value::View& elem) const {
        // Element must exist in the set
        auto set = value_view().as_set();
        if (!set.contains(elem)) {
            return false;
        }
        // Check if it's in the added slots
        // TODO: This requires mapping element to slot index
        // For now, return false until proper slot mapping is implemented
        return false;
    }

    /**
     * @brief Check if a specific element was removed this tick.
     *
     * @param elem The element to check
     * @return true if element was removed
     */
    [[nodiscard]] bool was_removed(const value::View& /*elem*/) const {
        // TODO: This requires tracking removed values in SetDelta
        // For now, return false until proper tracking is implemented
        return false;
    }

    // ========== Container-Level Access ==========

    /**
     * @brief Get the last modification time.
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return time_view().as<engine_time_t>();
    }

    /**
     * @brief Check if modified this tick.
     */
    [[nodiscard]] bool modified() const {
        return last_modified_time() >= current_time_;
    }

    /**
     * @brief Check if the set has ever been set.
     */
    [[nodiscard]] bool valid() const {
        return last_modified_time() != MIN_ST;
    }

private:
    /**
     * @brief Get the value view (internal).
     */
    [[nodiscard]] value::View value_view() const {
        return value::View(view_data_.value_data, meta()->value_type);
    }

    /**
     * @brief Get the time view (internal).
     */
    [[nodiscard]] value::View time_view() const {
        return value::View(view_data_.time_data,
            TSMetaSchemaCache::instance().get_time_schema(meta()));
    }

    /**
     * @brief Get the SetDelta (internal).
     */
    [[nodiscard]] const SetDelta* delta() const {
        return static_cast<const SetDelta*>(view_data_.delta_data);
    }

    ViewData view_data_;
    engine_time_t current_time_{MIN_ST};
};

} // namespace hgraph
