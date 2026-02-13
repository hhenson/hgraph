#pragma once

/**
 * @file ts_set_view.h
 * @brief TSSView - View for time-series set (TSS) types.
 *
 * TSSView provides set operations with delta tracking.
 * Access elements via values() to iterate, check membership via contains().
 *
 * TSS value layout is tuple(SetStorage, bool) where:
 *   - Element [0] = the set container (SetStorage)
 *   - Element [1] = nested is_empty TS[bool] value
 *
 * TSSView also supports a "raw" format (used by TSD key_set) where
 * value_data points to a raw SetStorage (not wrapped in a tuple).
 * The format is auto-detected from meta()->value_type->kind.
 */

#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/types/time_series/slot_set.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_view_range.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

namespace hgraph {

// Forward declaration - TSView is defined before this header is included
// (ts_view.h includes ts_set_view.h after defining TSView)
class TSView;

/**
 * @brief View for time-series set (TSS) types.
 *
 * TSSView provides set membership queries and delta tracking for
 * elements added/removed this tick.
 *
 * Supports two data formats:
 * - **Tuple format** (from TSTypeRegistry::tss()): value_data is tuple(SetStorage, bool),
 *   time/observer are also tuples with parallel structure for the is_empty child.
 * - **Raw format** (from TSTypeRegistry::tss_raw()): value_data is raw SetStorage,
 *   time/observer are raw scalars. Used by TSD key_set() embedding.
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
 * // Access nested is_empty TS[bool] child (tuple format only)
 * TSView is_empty = set.is_empty_ts();
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
        return set_value_view().as_set().contains(elem);
    }

    /**
     * @brief Get the set size.
     */
    [[nodiscard]] size_t size() const {
        return set_value_view().as_set().size();
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
        return set_value_view().as_set();
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
        auto* storage = set_storage_ptr();
        if (!storage) return false;

        // Find the slot for this element
        size_t slot = storage->key_set().find(elem.data());
        if (slot == static_cast<size_t>(-1)) {
            return false;
        }

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
        return delta()->was_key_removed(elem.data(), element_type());
    }

    // ========== Element Iteration ==========

    /**
     * @brief Iterate over elements added this tick.
     *
     * @return SlotElementRange yielding value::View for each added element
     */
    [[nodiscard]] SlotElementRange added() const {
        if (!view_data_.valid() || !delta()) {
            return SlotElementRange{};
        }
        auto* storage = set_storage_ptr();
        return SlotElementRange(storage, element_type(), &delta()->added());
    }

    /**
     * @brief Iterate over elements removed this tick.
     *
     * @return SlotElementRange yielding value::View for each removed element
     */
    [[nodiscard]] SlotElementRange removed() const {
        if (!view_data_.valid() || !delta()) {
            return SlotElementRange{};
        }
        auto* storage = set_storage_ptr();
        return SlotElementRange(storage, element_type(), &delta()->removed());
    }

    // ========== Container-Level Access ==========

    /**
     * @brief Get the last modification time.
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return container_time_view().as<engine_time_t>();
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
        return last_modified_time() != MIN_DT;
    }

    // ========== Mutation (for outputs) ==========

    /**
     * @brief Add an element to the set.
     *
     * @param elem The element to add
     * @return true if element was added (not already present)
     */
    bool add(const value::View& elem) {
        if (!view_data_.ops || !view_data_.ops->set_add) {
            throw std::runtime_error("add not available for this view");
        }
        return view_data_.ops->set_add(const_cast<ViewData&>(view_data_), elem, current_time_);
    }

    /**
     * @brief Remove an element from the set.
     *
     * @param elem The element to remove
     * @return true if element was removed (was present)
     */
    bool remove(const value::View& elem) {
        if (!view_data_.ops || !view_data_.ops->set_remove) {
            throw std::runtime_error("remove not available for this view");
        }
        return view_data_.ops->set_remove(const_cast<ViewData&>(view_data_), elem, current_time_);
    }

    /**
     * @brief Clear all elements from the set.
     */
    void clear() {
        if (!view_data_.ops || !view_data_.ops->set_clear) {
            throw std::runtime_error("clear not available for this view");
        }
        view_data_.ops->set_clear(const_cast<ViewData&>(view_data_), current_time_);
    }

    // ========== is_empty Child Access ==========

    /**
     * @brief Check if this TSSView has the nested is_empty TS[bool] child.
     *
     * Only available for tuple-format TSS (created via TSTypeRegistry::tss()).
     * Not available for raw-format TSS (used by TSD key_set embedding).
     */
    [[nodiscard]] bool has_is_empty_child() const noexcept {
        return meta() && meta()->value_type &&
               meta()->value_type->kind == value::TypeKind::Tuple;
    }

    /**
     * @brief Get a TSView for the nested is_empty TS[bool] child.
     *
     * The is_empty child is stored as element [1] in each parallel tuple.
     * Only available when has_is_empty_child() returns true.
     *
     * @return TSView for the is_empty child, or invalid TSView if not available
     */
    [[nodiscard]] TSView is_empty_ts() const {
        if (!has_is_empty_child() || !view_data_.valid()) {
            return TSView{};
        }

        auto& cache = TSMetaSchemaCache::instance();

        // Navigate to element [1] in each tuple structure
        // Value: tuple(SetStorage, bool) -> element [1] = is_empty bool
        auto value_tuple = value::View(view_data_.value_data, meta()->value_type);
        void* is_empty_value = const_cast<void*>(value_tuple.as_tuple().at(1).data());

        // Time: tuple(engine_time_t, engine_time_t) -> element [1] = is_empty time
        auto time_schema = cache.get_time_schema(meta());
        auto time_tuple = value::View(view_data_.time_data, time_schema);
        void* is_empty_time = const_cast<void*>(time_tuple.as_tuple().at(1).data());

        // Observer: tuple(ObserverList, ObserverList) -> element [1] = is_empty observer
        auto observer_schema = cache.get_observer_schema(meta());
        auto observer_tuple = value::View(view_data_.observer_data, observer_schema);
        void* is_empty_observer = const_cast<void*>(observer_tuple.as_tuple().at(1).data());

        // Get TSMeta for TS[bool]
        const TSMeta* ts_bool_meta = TSTypeRegistry::instance().ts(cache.bool_meta());

        ViewData child_vd{
            ShortPath{},                      // empty path — this is a synthetic child, not a node output
            is_empty_value,                   // bool*
            is_empty_time,                    // engine_time_t*
            is_empty_observer,                // ObserverList*
            nullptr,                          // no delta for scalar bool
            get_ts_ops(TSKind::TSValue),       // scalar TS ops
            ts_bool_meta                      // TS[bool] metadata
        };

        return TSView(std::move(child_vd), current_time_);
    }

private:
    /**
     * @brief Get the element TypeMeta.
     *
     * Handles both tuple format (field[0] is SetStorage schema)
     * and raw format (value_type is SetStorage schema directly).
     */
    [[nodiscard]] const value::TypeMeta* element_type() const {
        if (has_is_empty_child()) {
            // Tuple format: value_type->fields[0].type is SetStorage schema
            return meta()->value_type->fields[0].type->element_type;
        }
        // Raw format: value_type is SetStorage schema directly
        return meta()->value_type->element_type;
    }

    /**
     * @brief Get the set value view (element [0] of tuple, or raw set).
     */
    [[nodiscard]] value::View set_value_view() const {
        if (has_is_empty_child()) {
            // Tuple format: navigate to element [0]
            return value::View(view_data_.value_data, meta()->value_type)
                .as_tuple().at(0);
        }
        // Raw format: value_data IS the set
        return value::View(view_data_.value_data, meta()->value_type);
    }

    /**
     * @brief Get a const pointer to the SetStorage.
     */
    [[nodiscard]] const value::SetStorage* set_storage_ptr() const {
        return static_cast<const value::SetStorage*>(set_value_view().data());
    }

    /**
     * @brief Get the container time view (element [0] of time tuple, or raw).
     */
    [[nodiscard]] value::View container_time_view() const {
        if (has_is_empty_child()) {
            // Tuple format: navigate to element [0] of time tuple
            auto time_schema = TSMetaSchemaCache::instance().get_time_schema(meta());
            return value::View(view_data_.time_data, time_schema)
                .as_tuple().at(0);
        }
        // Raw format: time_data is directly engine_time_t
        return value::View(view_data_.time_data,
            TSMetaSchemaCache::instance().engine_time_meta());
    }

    /**
     * @brief Get the SetDelta (internal).
     */
    [[nodiscard]] const SetDelta* delta() const {
        return static_cast<const SetDelta*>(view_data_.delta_data);
    }

    ViewData view_data_;
    engine_time_t current_time_{MIN_DT};
};

} // namespace hgraph
