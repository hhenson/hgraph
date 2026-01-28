#pragma once

/**
 * @file ts_dict_view.h
 * @brief TSDView - View for time-series dict (TSD) types.
 *
 * TSDView provides key-based access to dict time-series.
 * Access values via at(key) to get TSView.
 */

#include <hgraph/types/time_series/map_delta.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/slot_set.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_set_view.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_view_range.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/value/map_storage.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

namespace hgraph {

// Forward declaration
class TSView;

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

    /**
     * @brief Get the key set as a TSSView with delta tracking.
     *
     * Returns a TSSView that provides TSS-like access to the dict's key set,
     * including delta tracking (added/removed keys).
     *
     * The returned TSSView shares the same delta tracking as this TSDView -
     * MapDelta composes SetDelta internally, so key additions/removals are
     * tracked through the embedded SetDelta.
     *
     * @code
     * TSSView key_set = dict.key_set();
     *
     * // Check if key was added this tick
     * if (key_set.was_added(some_key)) { ... }
     *
     * // Iterate over added keys
     * for (auto slot : key_set.added_slots()) {
     *     auto key = dict.keys().at_slot(slot);
     *     // ...
     * }
     * @endcode
     *
     * @return TSSView over the dict's key set
     */
    [[nodiscard]] TSSView key_set() const {
        if (!view_data_.valid()) {
            return TSSView{};
        }

        // Get the MapStorage and its embedded SetStorage
        auto* map_storage = static_cast<const value::MapStorage*>(view_data_.value_data);
        const value::SetStorage* set_storage = &map_storage->as_set();

        // Get the embedded SetDelta from MapDelta (composition)
        auto* map_delta = static_cast<MapDelta*>(view_data_.delta_data);
        SetDelta* set_delta = map_delta ? &map_delta->key_delta() : nullptr;

        // Get container time and observer from the TSD's time/observer tuples
        // TSD time structure: tuple[engine_time_t, var_list[...]]
        // We need the first element (container time)
        auto time_schema = TSMetaSchemaCache::instance().get_time_schema(meta());
        value::View time_tuple(view_data_.time_data, time_schema);
        void* container_time_ptr = const_cast<void*>(time_tuple.as_tuple().at(0).data());

        // TSD observer structure: tuple[ObserverList, var_list[...]]
        // We need the first element (container observer)
        auto observer_schema = TSMetaSchemaCache::instance().get_observer_schema(meta());
        value::View observer_tuple(view_data_.observer_data, observer_schema);
        void* container_observer_ptr = const_cast<void*>(observer_tuple.as_tuple().at(0).data());

        // Get or create TSMeta for TSS[KeyType]
        const TSMeta* tss_meta = TSTypeRegistry::instance().tss(meta()->key_type);

        // Build ViewData for the TSSView
        ViewData key_set_vd{
            view_data_.path,              // Same path (key set is part of TSD)
            const_cast<void*>(static_cast<const void*>(set_storage)),  // SetStorage
            container_time_ptr,           // Container time
            container_observer_ptr,       // Container observer
            set_delta,                    // Embedded SetDelta from MapDelta
            get_ts_ops(TSKind::TSS),      // TSS operations
            tss_meta                      // TSS[KeyType] metadata
        };

        return TSSView(std::move(key_set_vd), current_time_);
    }

    // ========== Delta Access ==========

    /**
     * @brief Get the slot indices of keys added this tick.
     *
     * @return Set of added slot indices
     */
    [[nodiscard]] const SlotSet& added_slots() const {
        return delta()->added();
    }

    /**
     * @brief Get the slot indices of keys removed this tick.
     *
     * @return Set of removed slot indices
     */
    [[nodiscard]] const SlotSet& removed_slots() const {
        return delta()->removed();
    }

    /**
     * @brief Get the slot indices of keys with updated values this tick.
     *
     * @return Set of updated slot indices
     */
    [[nodiscard]] const SlotSet& updated_slots() const {
        return delta()->updated();
    }

    /**
     * @brief Get the slot indices of keys modified (added or updated) this tick.
     *
     * @return Set of modified slot indices (union of added and updated)
     */
    [[nodiscard]] const SlotSet& modified_slots() const {
        return delta()->modified();
    }

    /**
     * @brief Check if a specific key was added this tick.
     *
     * @param key The key to check
     * @return true if key was added
     */
    [[nodiscard]] bool was_added(const value::View& key) const {
        // Get the MapStorage
        auto* storage = static_cast<const value::MapStorage*>(view_data_.value_data);
        if (!storage) return false;

        // Find the slot for this key
        size_t slot = storage->key_set().find(key.data());
        if (slot == static_cast<size_t>(-1)) {
            // Key not in map, so it wasn't added
            return false;
        }

        // O(1) lookup using set
        return delta()->was_slot_added(slot);
    }

    // ========== Key Iteration ==========

    /**
     * @brief Iterate over keys added this tick.
     *
     * @return SlotKeyRange yielding value::View for each added key
     */
    [[nodiscard]] SlotKeyRange added_keys() const {
        if (!view_data_.valid() || !delta()) {
            return SlotKeyRange{};
        }
        auto* storage = static_cast<const value::MapStorage*>(view_data_.value_data);
        return SlotKeyRange(storage, meta()->key_type, &delta()->added());
    }

    /**
     * @brief Iterate over keys with modified values this tick (added or updated).
     *
     * @return SlotKeyRange yielding value::View for each modified key
     */
    [[nodiscard]] SlotKeyRange modified_keys() const {
        if (!view_data_.valid() || !delta()) {
            return SlotKeyRange{};
        }
        auto* storage = static_cast<const value::MapStorage*>(view_data_.value_data);
        return SlotKeyRange(storage, meta()->key_type, &delta()->modified());
    }

    /**
     * @brief Iterate over keys with only value updates this tick (not new additions).
     *
     * @return SlotKeyRange yielding value::View for each updated key
     */
    [[nodiscard]] SlotKeyRange updated_keys() const {
        if (!view_data_.valid() || !delta()) {
            return SlotKeyRange{};
        }
        auto* storage = static_cast<const value::MapStorage*>(view_data_.value_data);
        return SlotKeyRange(storage, meta()->key_type, &delta()->updated());
    }

    /**
     * @brief Iterate over keys removed this tick.
     *
     * The removed keys remain accessible in storage during the current tick
     * (their slots are placed on a free list that is only used in the next engine cycle).
     *
     * @return SlotKeyRange yielding value::View for each removed key
     */
    [[nodiscard]] SlotKeyRange removed_keys() const {
        if (!view_data_.valid() || !delta()) {
            return SlotKeyRange{};
        }
        auto* storage = static_cast<const value::MapStorage*>(view_data_.value_data);
        return SlotKeyRange(storage, meta()->key_type, &delta()->removed());
    }

    // ========== Key Membership ==========

    /**
     * @brief Check if a specific key was removed this tick.
     *
     * Uses O(1) hash-based lookup in the delta's removed key hashes.
     *
     * @param key The key to check
     * @return true if key was removed
     */
    [[nodiscard]] bool was_removed(const value::View& key) const {
        return delta()->was_key_removed(key.data(), meta()->key_type);
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
     * @return FilteredTSDictRange that skips invalid values
     */
    [[nodiscard]] FilteredTSDictRange<TSFilter::VALID> valid_items() const {
        if (!view_data_.valid()) {
            return FilteredTSDictRange<TSFilter::VALID>{};
        }
        return FilteredTSDictRange<TSFilter::VALID>(view_data_, meta(), 0, size(), current_time_);
    }

    /**
     * @brief Iterate over entries added this tick.
     *
     * @return TSDictSlotRange for added items
     */
    [[nodiscard]] TSDictSlotRange added_items() const {
        if (!view_data_.valid() || !delta()) {
            return TSDictSlotRange{};
        }
        return TSDictSlotRange(view_data_, meta(), &delta()->added(), current_time_);
    }

    /**
     * @brief Iterate over entries with modified values this tick.
     *
     * This includes both additions and value updates.
     *
     * @return TSDictSlotRange for modified items
     */
    [[nodiscard]] TSDictSlotRange modified_items() const {
        if (!view_data_.valid() || !delta()) {
            return TSDictSlotRange{};
        }
        return TSDictSlotRange(view_data_, meta(), &delta()->modified(), current_time_);
    }

    /**
     * @brief Iterate over entries with only value updates this tick.
     *
     * This includes only pre-existing keys that had their values updated,
     * not new additions.
     *
     * @return TSDictSlotRange for updated items
     */
    [[nodiscard]] TSDictSlotRange updated_items() const {
        if (!view_data_.valid() || !delta()) {
            return TSDictSlotRange{};
        }
        return TSDictSlotRange(view_data_, meta(), &delta()->updated(), current_time_);
    }

    /**
     * @brief Iterate over entries removed this tick.
     *
     * The removed entries remain accessible in storage during the current tick
     * (their slots are placed on a free list that is only used in the next engine cycle).
     *
     * @return TSDictSlotRange for removed items
     */
    [[nodiscard]] TSDictSlotRange removed_items() const {
        if (!view_data_.valid() || !delta()) {
            return TSDictSlotRange{};
        }
        return TSDictSlotRange(view_data_, meta(), &delta()->removed(), current_time_);
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
