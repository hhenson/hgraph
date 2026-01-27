#pragma once

/**
 * @file ts_dict_view.h
 * @brief TSDView - View for time-series dict (TSD) types.
 *
 * TSDView provides key-based access to dict time-series.
 * Access values via at(key) to get TSView.
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/map_delta.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_view_range.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/util/date_time.h>

namespace hgraph {

// Forward declarations
class TSView;
class TSSView;

/**
 * @brief View for time-series dict (TSD) types.
 *
 * TSDView provides key-based access to dict entries as nested time-series views.
 * Use at(key) to navigate to child TSViews.
 *
 * Usage:
 * @code
 * TSDView dict = ts_view.as_dict();
 *
 * // Access value by key
 * value::View key = value::make_scalar<int64_t>(123);
 * TSView val = dict.at(key);
 * double price = val.value<double>();
 *
 * // Check key existence
 * if (dict.contains(key)) { ... }
 *
 * // Iterate over all keys
 * for (auto key : dict.keys()) { ... }
 *
 * // Iterate over all items
 * for (auto it = dict.items().begin(); it != dict.items().end(); ++it) {
 *     std::cout << it.key() << ": " << (*it).value<double>() << "\n";
 * }
 * @endcode
 */
class TSDView {
public:
    /**
     * @brief Construct a dict view from ViewData.
     *
     * @param view_data ViewData containing all data pointers and metadata
     * @param current_time The current engine time
     */
    TSDView(ViewData view_data, engine_time_t current_time) noexcept
        : view_data_(std::move(view_data))
        , current_time_(current_time)
    {}

    /**
     * @brief Default constructor - creates invalid view.
     */
    TSDView() noexcept = default;

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

    // ========== Value Navigation ==========

    /**
     * @brief Get a value as a TSView by key.
     *
     * @param key The key (as a value::View)
     * @return TSView for the value
     */
    [[nodiscard]] TSView at(const value::View& key) const {
        if (!view_data_.ops) {
            throw std::runtime_error("at() requires valid ops");
        }
        return view_data_.ops->child_by_key(view_data_, key, current_time_);
    }

    /**
     * @brief Check if the dict contains a key.
     *
     * @param key The key (as a value::View)
     * @return true if key is present
     */
    [[nodiscard]] bool contains(const value::View& key) const {
        return value_view().as_map().contains(key);
    }

    /**
     * @brief Get the number of key-value pairs.
     */
    [[nodiscard]] size_t size() const {
        return value_view().as_map().size();
    }

    // ========== Key Set Access ==========

    /**
     * @brief Get the key set as a SetView for iteration.
     *
     * Use begin()/end() on the returned SetView for range-based for loops:
     * @code
     * for (auto key : dict.keys()) {
     *     std::cout << key.as<int64_t>() << "\n";
     * }
     * @endcode
     *
     * @return SetView of the keys
     */
    [[nodiscard]] value::SetView keys() const {
        return value_view().as_map().keys();
    }

    // ========== Delta Access ==========

    /**
     * @brief Get the slot indices of keys added this tick.
     *
     * @return Vector of added slot indices
     */
    [[nodiscard]] const std::vector<size_t>& added_slots() const {
        return delta()->added();
    }

    /**
     * @brief Get the slot indices of keys removed this tick.
     *
     * @return Vector of removed slot indices
     */
    [[nodiscard]] const std::vector<size_t>& removed_slots() const {
        return delta()->removed();
    }

    /**
     * @brief Get the slot indices of keys with updated values this tick.
     *
     * @return Vector of updated slot indices
     */
    [[nodiscard]] const std::vector<size_t>& updated_slots() const {
        return delta()->updated();
    }

    // ========== Items Iteration ==========

    /**
     * @brief Iterate over all entries.
     *
     * Use it.key() to get key as value::View, *it to get TSView of value.
     *
     * @return TSDictRange for iteration
     */
    [[nodiscard]] TSDictRange items() const {
        if (!view_data_.valid()) {
            return TSDictRange{};
        }
        return TSDictRange(view_data_, meta(), 0, size(), current_time_);
    }

    /**
     * @brief Iterate over entries with valid values.
     *
     * @return TSDictRange that skips invalid values
     */
    [[nodiscard]] TSDictRange valid_items() const {
        // TODO: Implement filtered iteration
        return items();
    }

    /**
     * @brief Iterate over entries added this tick.
     *
     * @return TSDictRange for added items
     */
    [[nodiscard]] TSDictRange added_items() const {
        // TODO: Implement filtered iteration over added slots
        return TSDictRange{};
    }

    /**
     * @brief Iterate over entries with modified values this tick.
     *
     * @return TSDictRange for modified items
     */
    [[nodiscard]] TSDictRange modified_items() const {
        // TODO: Implement filtered iteration over modified slots
        return TSDictRange{};
    }

    // ========== Container-Level Access ==========

    /**
     * @brief Get the container's last modification time.
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return time_view().as_tuple().at(0).as<engine_time_t>();
    }

    /**
     * @brief Check if container is modified (any key/value changed).
     */
    [[nodiscard]] bool modified() const {
        return last_modified_time() >= current_time_;
    }

    /**
     * @brief Check if the dict has ever been set.
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
     * @brief Get the MapDelta (internal).
     */
    [[nodiscard]] const MapDelta* delta() const {
        return static_cast<const MapDelta*>(view_data_.delta_data);
    }

    ViewData view_data_;
    engine_time_t current_time_{MIN_ST};
};

} // namespace hgraph
