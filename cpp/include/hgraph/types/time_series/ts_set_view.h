#pragma once

/**
 * @file ts_set_view.h
 * @brief TSSView - View for time-series set (TSS) types.
 *
 * TSSView provides set operations with delta tracking.
 * Access elements via values() to iterate, check membership via contains().
 */

#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/types/time_series/slot_set.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/ts_view_range.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/value/value_view.h>
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
     * @return Set of added slot indices
     */
    [[nodiscard]] const SlotSet& added_slots() const {
        return delta()->added();
    }

    /**
     * @brief Get the slot indices of elements removed this tick.
     *
     * @return Set of removed slot indices
     */
    [[nodiscard]] const SlotSet& removed_slots() const {
        return delta()->removed();
    }

    /**
     * @brief Check if a specific slot was added this tick.
     *
     * @param slot The slot index to check
     * @return true if slot was added
     */
    [[nodiscard]] bool was_slot_added(size_t slot) const {
        return delta()->was_slot_added(slot);
    }

    /**
     * @brief Check if a specific slot was removed this tick.
     *
     * @param slot The slot index to check
     * @return true if slot was removed
     */
    [[nodiscard]] bool was_slot_removed(size_t slot) const {
        return delta()->was_slot_removed(slot);
    }

    /**
     * @brief Check if a specific element was added this tick.
     *
     * @param elem The element to check
     * @return true if element was added
     */
    [[nodiscard]] bool was_added(const value::View& elem) const {
        // Get the SetStorage
        auto* storage = static_cast<const value::SetStorage*>(view_data_.value_data);
        if (!storage) return false;

        // Find the slot for this element
        size_t slot = storage->key_set().find(elem.data());
        if (slot == static_cast<size_t>(-1)) {
            // Element not in set, so it wasn't added
            return false;
        }

        // O(1) lookup using set
        return delta()->was_slot_added(slot);
    }

    /**
     * @brief Check if a specific element was removed this tick.
     *
     * Uses O(1) hash-based lookup in the delta's removed key hashes.
     *
     * @param elem The element to check
     * @return true if element was removed
     */
    [[nodiscard]] bool was_removed(const value::View& elem) const {
        return delta()->was_key_removed(elem.data(), meta()->value_type);
    }

    // ========== Element Iteration ==========

    /**
     * @brief Iterate over elements added this tick.
     *
     * Returns a range yielding value::View for each element added.
     *
     * @code
     * for (auto elem : set_view.added()) {
     *     std::cout << elem.as<int64_t>() << " was added\n";
     * }
     * @endcode
     *
     * @return SlotElementRange yielding value::View for each added element
     */
    [[nodiscard]] SlotElementRange added() const {
        if (!view_data_.valid() || !delta()) {
            return SlotElementRange{};
        }
        auto* storage = static_cast<const value::SetStorage*>(view_data_.value_data);
        return SlotElementRange(storage, meta()->value_type, &delta()->added());
    }

    /**
     * @brief Iterate over elements removed this tick.
     *
     * Returns a range yielding value::View for each element removed.
     * The removed elements remain accessible in storage during the current tick
     * (they are placed on a free list that is only used in the next engine cycle).
     *
     * @code
     * for (auto elem : set_view.removed()) {
     *     std::cout << elem.as<int64_t>() << " was removed\n";
     * }
     * @endcode
     *
     * @return SlotElementRange yielding value::View for each removed element
     */
    [[nodiscard]] SlotElementRange removed() const {
        if (!view_data_.valid() || !delta()) {
            return SlotElementRange{};
        }
        auto* storage = static_cast<const value::SetStorage*>(view_data_.value_data);
        return SlotElementRange(storage, meta()->value_type, &delta()->removed());
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
