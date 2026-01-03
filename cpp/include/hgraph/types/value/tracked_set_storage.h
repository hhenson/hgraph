#pragma once

/**
 * @file tracked_set_storage.h
 * @brief Storage for sets with delta tracking (added/removed elements).
 *
 * TrackedSetStorage provides the underlying storage for TimeSeriesSet types,
 * tracking which elements were added or removed each evaluation cycle.
 */

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/value/type_registry.h>

namespace hgraph::value {

/**
 * @brief Storage structure for sets with delta tracking.
 *
 * Stores the current set value plus sets of added/removed elements for
 * delta propagation in TimeSeriesSet types. All three sets share the
 * same element type.
 */
struct TrackedSetStorage {
    PlainValue _value;            // Current set contents
    PlainValue _added;            // Elements added this cycle
    PlainValue _removed;          // Elements removed this cycle
    const TypeMeta* _element_type{nullptr};
    const TypeMeta* _set_schema{nullptr};  // Cached set schema

    // ========== Construction ==========

    TrackedSetStorage() = default;

    /**
     * @brief Construct with a specific element type.
     * @param element_type The TypeMeta for set elements
     */
    explicit TrackedSetStorage(const TypeMeta* element_type)
        : _element_type(element_type) {
        if (_element_type) {
            _set_schema = TypeRegistry::instance().set(_element_type).build();
            _value = PlainValue(_set_schema);
            _added = PlainValue(_set_schema);
            _removed = PlainValue(_set_schema);
        }
    }

    // Move-only semantics
    TrackedSetStorage(TrackedSetStorage&&) noexcept = default;
    TrackedSetStorage& operator=(TrackedSetStorage&&) noexcept = default;
    TrackedSetStorage(const TrackedSetStorage&) = delete;
    TrackedSetStorage& operator=(const TrackedSetStorage&) = delete;

    // ========== View Accessors ==========

    /**
     * @brief Get const view of current set value.
     * @return Empty invalid view if storage not initialized, otherwise set view.
     */
    [[nodiscard]] ConstSetView value() const {
        if (!_set_schema) return ConstSetView{};
        return _value.const_view().as_set();
    }

    /**
     * @brief Get mutable view of current set value.
     * @note Caller should ensure storage is initialized before calling.
     */
    [[nodiscard]] SetView value() {
        if (!_set_schema) {
            throw std::runtime_error("TrackedSetStorage::value(): storage not initialized");
        }
        return _value.view().as_set();
    }

    /**
     * @brief Get const view of added elements.
     * @return Empty invalid view if storage not initialized, otherwise set view.
     */
    [[nodiscard]] ConstSetView added() const {
        if (!_set_schema) return ConstSetView{};
        return _added.const_view().as_set();
    }

    /**
     * @brief Get const view of removed elements.
     * @return Empty invalid view if storage not initialized, otherwise set view.
     */
    [[nodiscard]] ConstSetView removed() const {
        if (!_set_schema) return ConstSetView{};
        return _removed.const_view().as_set();
    }

    // ========== Size and State ==========

    /**
     * @brief Get size of current set.
     */
    [[nodiscard]] size_t size() const {
        return value().size();
    }

    /**
     * @brief Check if set is empty.
     */
    [[nodiscard]] bool empty() const {
        return value().empty();
    }

    /**
     * @brief Check if there are any changes (added or removed elements).
     */
    [[nodiscard]] bool has_delta() const {
        return !added().empty() || !removed().empty();
    }

    // ========== Element Access ==========

    /**
     * @brief Check if element is in current set.
     */
    [[nodiscard]] bool contains(const ConstValueView& elem) const {
        return value().contains(elem);
    }

    /**
     * @brief Check if element was added this cycle.
     */
    [[nodiscard]] bool was_added(const ConstValueView& elem) const {
        return added().contains(elem);
    }

    /**
     * @brief Check if element was removed this cycle.
     */
    [[nodiscard]] bool was_removed(const ConstValueView& elem) const {
        return removed().contains(elem);
    }

    // ========== Mutation with Delta Tracking ==========

    /**
     * @brief Add an element to the set with delta tracking.
     *
     * If the element was in _removed, it's moved back (un-removed).
     * Otherwise it's added to both _value and _added.
     *
     * @param elem The element to add
     * @return true if element was newly added, false if already present
     */
    bool add(const ConstValueView& elem) {
        if (contains(elem)) {
            return false;  // Already in set
        }

        // Add to value
        value().insert(elem);

        // Track delta: if it was removed this cycle, just un-remove it
        auto removed_view = _removed.view().as_set();
        if (removed_view.contains(elem)) {
            removed_view.erase(elem);
        } else {
            // Otherwise track as newly added
            _added.view().as_set().insert(elem);
        }
        return true;
    }

    /**
     * @brief Remove an element from the set with delta tracking.
     *
     * If the element was in _added, it's just removed (un-added).
     * Otherwise it's removed from _value and added to _removed.
     *
     * @param elem The element to remove
     * @return true if element was removed, false if not present
     */
    bool remove(const ConstValueView& elem) {
        if (!contains(elem)) {
            return false;  // Not in set
        }

        // Remove from value
        value().erase(elem);

        // Track delta: if it was added this cycle, just un-add it
        auto added_view = _added.view().as_set();
        if (added_view.contains(elem)) {
            added_view.erase(elem);
        } else {
            // Otherwise track as newly removed
            _removed.view().as_set().insert(elem);
        }
        return true;
    }

    /**
     * @brief Clear all delta tracking (call at end of cycle).
     */
    void clear_deltas() {
        _added.view().as_set().clear();
        _removed.view().as_set().clear();
    }

    /**
     * @brief Clear the entire set.
     *
     * Items that were added in the same cycle are NOT marked as removed
     * (matches Python: self._removed = self._value - (self._added or set()))
     */
    void clear() {
        // Track elements as removed, but exclude items added this cycle
        auto val = value();
        auto added_view = _added.view().as_set();
        auto removed_view = _removed.view().as_set();
        for (auto elem : val) {
            // Only mark as removed if it wasn't added this cycle
            if (!added_view.contains(elem)) {
                removed_view.insert(elem);
            }
        }
        // Clear value and added
        value().clear();
        _added.view().as_set().clear();
    }

    // ========== Typed Convenience Methods ==========

    /**
     * @brief Check if typed element is in set.
     */
    template<typename T>
    [[nodiscard]] bool contains(const T& elem) const {
        Value<> temp(elem);
        return contains(temp.const_view());
    }

    /**
     * @brief Add typed element.
     */
    template<typename T>
    bool add(const T& elem) {
        Value<> temp(elem);
        return add(temp.const_view());
    }

    /**
     * @brief Remove typed element.
     */
    template<typename T>
    bool remove(const T& elem) {
        Value<> temp(elem);
        return remove(temp.const_view());
    }
};

} // namespace hgraph::value
