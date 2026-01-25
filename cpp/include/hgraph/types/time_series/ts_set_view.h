#pragma once

/**
 * @file ts_set_view.h
 * @brief TSSView - View for time-series set (TSS) types.
 *
 * TSSView provides set operations with delta tracking and proper
 * modification notification.
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
 * TSSView provides set operations (add, remove, contains) with:
 * - Automatic delta tracking via SetDelta
 * - Modification time updates
 * - Observer notification
 *
 * TSSView wraps a ViewData (containing all data pointers) plus current_time.
 *
 * Usage:
 * @code
 * TSSView set_view(view_data, current_time);
 *
 * // Mutate (automatically updates delta, time, and notifies)
 * set_view.add(42);
 * set_view.remove(10);
 *
 * // Query
 * if (set_view.contains(42)) { ... }
 *
 * // Check delta
 * auto& added = set_view.added_slots();
 * auto& removed = set_view.removed_slots();
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

    // ========== Time-Series Semantics ==========

    /**
     * @brief Get the last modification time.
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return time_view().as<engine_time_t>();
    }

    /**
     * @brief Check if modified at or after current_time.
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

    // ========== Set Operations (Read) ==========

    /**
     * @brief Get the set size.
     */
    [[nodiscard]] size_t size() const {
        return value_view().as_set().size();
    }

    /**
     * @brief Check if empty.
     */
    [[nodiscard]] bool empty() const {
        return size() == 0;
    }

    /**
     * @brief Check if the set contains an element.
     * @tparam T The element type
     * @param elem The element to check
     */
    template<typename T>
    [[nodiscard]] bool contains(const T& elem) const {
        return value_view().as_set().contains(elem);
    }

    // ========== Set Operations (Write) ==========

    /**
     * @brief Add an element to the set.
     *
     * This method:
     * 1. Adds the element to the underlying set
     * 2. Updates the SetDelta (if element was actually added)
     * 3. Marks the modification time as current_time
     * 4. Notifies all observers
     *
     * @tparam T The element type
     * @param elem The element to add
     * @return true if element was added (not already present)
     */
    template<typename T>
    bool add(const T& elem) {
        auto set = value_view().as_set();
        bool added = set.add(elem);

        if (added) {
            // The SetDelta is wired as a SlotObserver to the KeySet,
            // so on_insert() is called automatically when the set adds.
            // We just need to mark modified and notify.
            mark_modified();
        }

        return added;
    }

    /**
     * @brief Remove an element from the set.
     *
     * @tparam T The element type
     * @param elem The element to remove
     * @return true if element was removed (was present)
     */
    template<typename T>
    bool remove(const T& elem) {
        auto set = value_view().as_set();
        bool removed = set.remove(elem);

        if (removed) {
            // SetDelta's on_erase() is called automatically.
            mark_modified();
        }

        return removed;
    }

    /**
     * @brief Clear all elements from the set.
     */
    void clear() {
        auto set = value_view().as_set();
        if (!set.empty()) {
            set.clear();
            // SetDelta's on_clear() is called automatically.
            mark_modified();
        }
    }

    // ========== Delta Access ==========

    /**
     * @brief Get the SetDelta for this set.
     */
    [[nodiscard]] SetDelta* delta() {
        return static_cast<SetDelta*>(view_data_.delta_data);
    }

    /**
     * @brief Get the SetDelta for this set (const).
     */
    [[nodiscard]] const SetDelta* delta() const {
        return static_cast<const SetDelta*>(view_data_.delta_data);
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
     * @brief Check if the set was cleared this tick.
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
     * @brief Mark as modified and notify observers.
     */
    void mark_modified() {
        // Update time
        time_view().as<engine_time_t>() = current_time_;

        // Notify observers
        auto* observers = static_cast<ObserverList*>(observer_view().data());
        observers->notify_modified(current_time_);
    }

    // ========== Observer Access ==========

    /**
     * @brief Add an observer.
     * @param obs The observer to add
     */
    void add_observer(Notifiable* obs) {
        auto* observers = static_cast<ObserverList*>(observer_view().data());
        observers->add_observer(obs);
    }

    /**
     * @brief Remove an observer.
     * @param obs The observer to remove
     */
    void remove_observer(Notifiable* obs) {
        auto* observers = static_cast<ObserverList*>(observer_view().data());
        observers->remove_observer(obs);
    }

private:
    ViewData view_data_;
    engine_time_t current_time_{MIN_ST};
};

} // namespace hgraph
