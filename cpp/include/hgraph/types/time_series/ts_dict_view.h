#pragma once

/**
 * @file ts_dict_view.h
 * @brief TSDView - View for time-series dict (TSD) types.
 *
 * TSDView provides key-based access to dict time-series with delta tracking
 * and proper modification notification that bubbles up.
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

// Forward declaration
class TSView;

/**
 * @brief View for time-series dict (TSD) types.
 *
 * TSDView provides dict operations with:
 * - Automatic delta tracking via MapDelta
 * - Modification time updates with bubbling
 * - Observer notification
 *
 * TSDView wraps a ViewData (containing all data pointers) plus current_time.
 *
 * Usage:
 * @code
 * TSDView dict_view(view_data, current_time);
 *
 * // Access value
 * auto val = dict_view.at<std::string>("key");
 *
 * // Mutate (automatically updates delta, time, and notifies)
 * dict_view.set("key", value);
 *
 * // Check delta
 * auto& added = dict_view.added_slots();
 * auto& removed = dict_view.removed_slots();
 * auto& updated = dict_view.updated_slots();
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

    // ========== View Access ==========

    /**
     * @brief Get the value view.
     */
    [[nodiscard]] value::View value_view() const {
        return value::View(view_data_.value_data, meta()->value_type);
    }

    /**
     * @brief Get the time view.
     */
    [[nodiscard]] value::View time_view() const {
        return value::View(view_data_.time_data,
            TSMetaSchemaCache::instance().get_time_schema(meta()));
    }

    /**
     * @brief Get the observer view.
     */
    [[nodiscard]] value::View observer_view() const {
        return value::View(view_data_.observer_data,
            TSMetaSchemaCache::instance().get_observer_schema(meta()));
    }

    /**
     * @brief Get the delta view.
     */
    [[nodiscard]] value::View delta_view() const {
        return value::View(view_data_.delta_data,
            TSMetaSchemaCache::instance().get_delta_value_schema(meta()));
    }

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

    // ========== TSView Navigation ==========

    /**
     * @brief Get a value as a TSView by slot index.
     *
     * Returns a proper TSView with full time-series semantics.
     *
     * @param slot The slot index (from delta)
     * @return TSView for the value
     */
    [[nodiscard]] TSView value_ts(size_t slot) const {
        if (!view_data_.ops) {
            throw std::runtime_error("value_ts requires valid ops");
        }
        return view_data_.ops->child_at(view_data_, slot, current_time_);
    }

    /**
     * @brief Iterate over all entries as TSViews by slot.
     *
     * Use it.index() to get slot index, *it to get TSView.
     * Note: Iterates over slots, not keys. Use items() to access keys.
     *
     * @return TSViewRange for iteration
     */
    [[nodiscard]] TSViewRange values() const {
        if (!view_data_.valid()) {
            return TSViewRange{};
        }
        return TSViewRange(view_data_, 0, size(), current_time_);
    }

    /**
     * @brief Iterate over all entries with key access.
     *
     * Use it.index() to get slot index, it.key() to get key as value::View,
     * *it to get TSView of the value.
     *
     * @return TSDictRange for iteration
     */
    [[nodiscard]] TSDictRange items() const {
        if (!view_data_.valid()) {
            return TSDictRange{};
        }
        return TSDictRange(view_data_, meta(), 0, size(), current_time_);
    }

    // ========== Container-Level Access ==========

    /**
     * @brief Get the container's last modification time.
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return time_view().as_tuple().at(0).as<engine_time_t>();
    }

    /**
     * @brief Check if container is modified.
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

    /**
     * @brief Get the dict size.
     */
    [[nodiscard]] size_t size() const {
        return value_view().as_map().size();
    }

    /**
     * @brief Check if empty.
     */
    [[nodiscard]] bool empty() const {
        return size() == 0;
    }

    // ========== Key Operations ==========

    /**
     * @brief Check if the dict contains a key.
     * @tparam K The key type
     * @param key The key to check
     */
    template<typename K>
    [[nodiscard]] bool contains(const K& key) const {
        return value_view().as_map().contains(key);
    }

    /**
     * @brief Get a value by key.
     * @tparam K The key type
     * @param key The key
     * @return View of the value
     */
    template<typename K>
    [[nodiscard]] value::View at(const K& key) const {
        return value_view().as_map().at(key);
    }

    // ========== Mutation ==========

    /**
     * @brief Set a value by key with modification tracking.
     *
     * This method:
     * 1. Sets/updates the value in the map
     * 2. MapDelta is updated automatically via SlotObserver
     * 3. Marks the value's time as current_time (using slot from delta)
     * 4. Notifies value observers
     * 5. Marks the container time as current_time (bubble up)
     * 6. Notifies container observers (bubble up)
     *
     * @tparam K The key type
     * @tparam V The value type
     * @param key The key
     * @param value The value
     */
    template<typename K, typename V>
    void set(const K& key, const V& value) {
        auto map = value_view().as_map();

        // Set the value (this triggers MapDelta's on_insert or on_update)
        map.set_item(key, value);

        // Get the affected slot from the delta
        // After set_item, it will be in either added() or updated()
        auto* d = delta();
        size_t slot = 0;
        if (!d->added().empty()) {
            slot = d->added().back();
        } else if (!d->updated().empty()) {
            slot = d->updated().back();
        }

        // Mark value modified
        mark_value_modified(slot);

        // Bubble up to container
        mark_container_modified();
    }

    /**
     * @brief Remove a key from the dict.
     *
     * @tparam K The key type
     * @param key The key to remove
     * @return true if key was removed
     */
    template<typename K>
    bool remove(const K& key) {
        auto map = value_view().as_map();
        bool removed = map.remove(key);

        if (removed) {
            // MapDelta's on_erase() is called automatically
            mark_container_modified();
        }

        return removed;
    }

    /**
     * @brief Clear all entries from the dict.
     */
    void clear() {
        auto map = value_view().as_map();
        if (!map.empty()) {
            map.clear();
            // MapDelta's on_clear() is called automatically
            mark_container_modified();
        }
    }

    // ========== Delta Access ==========

    /**
     * @brief Get the MapDelta for this dict.
     */
    [[nodiscard]] MapDelta* delta() {
        return static_cast<MapDelta*>(view_data_.delta_data);
    }

    /**
     * @brief Get the MapDelta (const).
     */
    [[nodiscard]] const MapDelta* delta() const {
        return static_cast<const MapDelta*>(view_data_.delta_data);
    }

    /**
     * @brief Get the added slot indices.
     */
    [[nodiscard]] const std::vector<size_t>& added_slots() const {
        return delta()->added();
    }

    /**
     * @brief Get the removed slot indices.
     */
    [[nodiscard]] const std::vector<size_t>& removed_slots() const {
        return delta()->removed();
    }

    /**
     * @brief Get the updated slot indices.
     */
    [[nodiscard]] const std::vector<size_t>& updated_slots() const {
        return delta()->updated();
    }

    /**
     * @brief Check if the dict was cleared this tick.
     */
    [[nodiscard]] bool was_cleared() const {
        return delta()->was_cleared();
    }

    /**
     * @brief Check if there are any delta changes.
     */
    [[nodiscard]] bool has_changes() const {
        return !delta()->empty();
    }

    // ========== Modification ==========

    /**
     * @brief Mark a value entry as modified by slot.
     */
    void mark_value_modified(size_t slot) {
        const TSMeta* value_meta = meta()->element_ts;

        // Get value's time and observer
        auto vt = time_view().as_tuple().at(1).as_list().at(slot);
        auto vo = observer_view().as_tuple().at(1).as_list().at(slot);

        // Update value time
        if (value_meta->is_scalar_ts()) {
            vt.as<engine_time_t>() = current_time_;
        } else {
            vt.as_tuple().at(0).as<engine_time_t>() = current_time_;
        }

        // Notify value observers
        if (value_meta->is_scalar_ts()) {
            auto* obs = static_cast<ObserverList*>(vo.data());
            obs->notify_modified(current_time_);
        } else {
            auto* obs = static_cast<ObserverList*>(vo.as_tuple().at(0).data());
            obs->notify_modified(current_time_);
        }
    }

    /**
     * @brief Mark the container as modified.
     */
    void mark_container_modified() {
        // Update container time
        time_view().as_tuple().at(0).as<engine_time_t>() = current_time_;

        // Notify container observers
        auto* container_obs = static_cast<ObserverList*>(
            observer_view().as_tuple().at(0).data());
        container_obs->notify_modified(current_time_);
    }

    // ========== Observer Access ==========

    /**
     * @brief Add an observer to the container.
     */
    void add_observer(Notifiable* obs) {
        auto* container_obs = static_cast<ObserverList*>(
            observer_view().as_tuple().at(0).data());
        container_obs->add_observer(obs);
    }

private:
    ViewData view_data_;
    engine_time_t current_time_{MIN_ST};
};

} // namespace hgraph
