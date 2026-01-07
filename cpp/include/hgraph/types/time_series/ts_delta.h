#pragma once

/**
 * @file ts_delta.h
 * @brief Delta views and values for time-series collections.
 *
 * Delta types provide access to changes (additions/removals) that occurred
 * during a single tick. There are two forms:
 *
 * - **DeltaView**: Non-owning view into overlay data. Obtained by calling
 *   `delta_view(time)` on a collection view. Valid only while the overlay
 *   data is stable (until next modification or delta reset).
 *
 * - **DeltaValue**: Owning value that stores delta data. Can be created from
 *   a DeltaView (to capture a snapshot) or via fluent builder syntax.
 *
 * Usage patterns:
 * @code
 * // View-based access (non-owning)
 * TSSView set_view = ...;
 * if (auto delta = set_view.delta_view(current_time)) {
 *     for (auto& added : delta.added_values()) { ... }
 *     for (auto& removed : delta.removed_values()) { ... }
 * }
 *
 * // Capture delta as owned value
 * SetDeltaValue captured(delta);  // Copies data
 *
 * // Build delta programmatically
 * auto delta = SetDeltaValue::builder(element_schema)
 *     .add(value1)
 *     .add(value2)
 *     .remove(value3)
 *     .build();
 * @endcode
 */

#include <hgraph/types/value/value.h>
#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <vector>

namespace hgraph {

// Forward declarations
class SetDeltaValue;
class MapDeltaValue;

// ============================================================================
// SetDeltaView - Non-owning view of set delta
// ============================================================================

/**
 * @brief Non-owning view of set delta (additions and removals).
 *
 * Obtained by calling `TSSView::delta_view(time)`. The view is valid only
 * while the underlying overlay data is stable.
 */
class SetDeltaView {
public:
    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an invalid view.
     */
    SetDeltaView() noexcept = default;

    /**
     * @brief Construct from overlay and set view.
     *
     * @param overlay The SetTSOverlay containing delta information
     * @param set_view The underlying set data (for looking up added elements)
     * @param element_schema Schema for element values
     */
    SetDeltaView(SetTSOverlay* overlay,
                 value::ConstSetView set_view,
                 const value::TypeMeta* element_schema) noexcept;

    // ========== Validity ==========

    /**
     * @brief Check if the view is valid.
     */
    [[nodiscard]] bool valid() const noexcept { return _overlay != nullptr; }

    /**
     * @brief Boolean conversion.
     */
    explicit operator bool() const noexcept { return valid(); }

    // ========== Delta Access ==========

    /**
     * @brief Check if any elements were added.
     */
    [[nodiscard]] bool has_added() const noexcept;

    /**
     * @brief Check if any elements were removed.
     */
    [[nodiscard]] bool has_removed() const noexcept;

    /**
     * @brief Check if there are any changes.
     */
    [[nodiscard]] bool empty() const noexcept { return !has_added() && !has_removed(); }

    /**
     * @brief Get views of elements added this tick.
     *
     * Views point into the current set data and are valid only while
     * the set is not modified.
     */
    [[nodiscard]] std::vector<value::ConstValueView> added_values() const;

    /**
     * @brief Get views of elements removed this tick.
     *
     * Views point into the overlay's buffered data and are valid until
     * the next delta reset.
     */
    [[nodiscard]] std::vector<value::ConstValueView> removed_values() const;

    // ========== Conversion ==========

    /**
     * @brief Create an owning SetDeltaValue from this view.
     *
     * Copies all delta data into the returned value.
     */
    [[nodiscard]] SetDeltaValue to_value() const;

    // ========== Schema Access ==========

    /**
     * @brief Get the element schema.
     */
    [[nodiscard]] const value::TypeMeta* element_schema() const noexcept {
        return _element_schema;
    }

private:
    SetTSOverlay* _overlay{nullptr};
    value::ConstSetView _set_view;
    const value::TypeMeta* _element_schema{nullptr};
};

// ============================================================================
// SetDeltaValue - Owning set delta
// ============================================================================

/**
 * @brief Owning value containing set delta (additions and removals).
 *
 * Can be created from a SetDeltaView (captures snapshot) or via builder.
 */
class SetDeltaValue {
public:
    // ========== Builder ==========

    /**
     * @brief Fluent builder for constructing set deltas.
     */
    class Builder {
    public:
        /**
         * @brief Construct builder with element schema.
         */
        explicit Builder(const value::TypeMeta* element_schema) noexcept;

        /**
         * @brief Add an element to the "added" set.
         */
        Builder& add(value::PlainValue value);

        /**
         * @brief Add an element to the "added" set (typed).
         */
        template<typename T>
        Builder& add(const T& value);

        /**
         * @brief Add an element to the "removed" set.
         */
        Builder& remove(value::PlainValue value);

        /**
         * @brief Add an element to the "removed" set (typed).
         */
        template<typename T>
        Builder& remove(const T& value);

        /**
         * @brief Build the SetDeltaValue.
         */
        [[nodiscard]] SetDeltaValue build();

    private:
        const value::TypeMeta* _element_schema;
        std::vector<value::PlainValue> _added;
        std::vector<value::PlainValue> _removed;
    };

    /**
     * @brief Create a builder with the given element schema.
     */
    static Builder builder(const value::TypeMeta* element_schema) {
        return Builder(element_schema);
    }

    // ========== Construction ==========

    /**
     * @brief Default constructor - empty delta.
     */
    SetDeltaValue() noexcept = default;

    /**
     * @brief Construct from a SetDeltaView (copies data).
     */
    explicit SetDeltaValue(const SetDeltaView& view);

    /**
     * @brief Construct with explicit data.
     */
    SetDeltaValue(const value::TypeMeta* element_schema,
                  std::vector<value::PlainValue> added,
                  std::vector<value::PlainValue> removed) noexcept;

    // ========== Delta Access ==========

    /**
     * @brief Check if any elements were added.
     */
    [[nodiscard]] bool has_added() const noexcept { return !_added.empty(); }

    /**
     * @brief Check if any elements were removed.
     */
    [[nodiscard]] bool has_removed() const noexcept { return !_removed.empty(); }

    /**
     * @brief Check if the delta is empty.
     */
    [[nodiscard]] bool empty() const noexcept { return _added.empty() && _removed.empty(); }

    /**
     * @brief Get the added elements.
     */
    [[nodiscard]] const std::vector<value::PlainValue>& added() const noexcept { return _added; }

    /**
     * @brief Get the removed elements.
     */
    [[nodiscard]] const std::vector<value::PlainValue>& removed() const noexcept { return _removed; }

    /**
     * @brief Get views of added elements.
     */
    [[nodiscard]] std::vector<value::ConstValueView> added_values() const;

    /**
     * @brief Get views of removed elements.
     */
    [[nodiscard]] std::vector<value::ConstValueView> removed_values() const;

    // ========== Schema Access ==========

    /**
     * @brief Get the element schema.
     */
    [[nodiscard]] const value::TypeMeta* element_schema() const noexcept {
        return _element_schema;
    }

private:
    const value::TypeMeta* _element_schema{nullptr};
    std::vector<value::PlainValue> _added;
    std::vector<value::PlainValue> _removed;
};

// ============================================================================
// MapDeltaView - Non-owning view of map delta
// ============================================================================

/**
 * @brief Non-owning view of map delta (key additions and removals).
 *
 * Obtained by calling `TSDView::delta_view(time)`. The view is valid only
 * while the underlying overlay data is stable.
 */
class MapDeltaView {
public:
    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an invalid view.
     */
    MapDeltaView() noexcept = default;

    /**
     * @brief Construct from overlay and map view.
     *
     * @param overlay The MapTSOverlay containing delta information
     * @param map_view The underlying map data (for looking up entries)
     * @param key_schema Schema for key values
     * @param value_schema Schema for value values
     * @param time The current time (used for computing modified keys)
     */
    MapDeltaView(MapTSOverlay* overlay,
                 value::ConstMapView map_view,
                 const value::TypeMeta* key_schema,
                 const value::TypeMeta* value_schema,
                 engine_time_t time) noexcept;

    // ========== Validity ==========

    /**
     * @brief Check if the view is valid.
     */
    [[nodiscard]] bool valid() const noexcept { return _overlay != nullptr; }

    /**
     * @brief Boolean conversion.
     */
    explicit operator bool() const noexcept { return valid(); }

    // ========== Delta Access ==========

    /**
     * @brief Check if any keys were added.
     */
    [[nodiscard]] bool has_added() const noexcept;

    /**
     * @brief Check if any keys were removed.
     */
    [[nodiscard]] bool has_removed() const noexcept;

    /**
     * @brief Check if any existing keys had their values modified.
     */
    [[nodiscard]] bool has_modified() const noexcept;

    /**
     * @brief Check if there are any changes.
     */
    [[nodiscard]] bool empty() const noexcept { return !has_added() && !has_removed() && !has_modified(); }

    /**
     * @brief Get views of keys added this tick.
     */
    [[nodiscard]] std::vector<value::ConstValueView> added_keys() const;

    /**
     * @brief Get views of values added this tick.
     *
     * Corresponds 1:1 with added_keys().
     */
    [[nodiscard]] std::vector<value::ConstValueView> added_values() const;

    /**
     * @brief Get views of keys removed this tick.
     */
    [[nodiscard]] std::vector<value::ConstValueView> removed_keys() const;

    /**
     * @brief Get views of keys whose values were modified this tick.
     *
     * These are existing keys (not newly added) whose values changed.
     */
    [[nodiscard]] std::vector<value::ConstValueView> modified_keys() const;

    /**
     * @brief Get views of values that were modified this tick.
     *
     * Corresponds 1:1 with modified_keys().
     */
    [[nodiscard]] std::vector<value::ConstValueView> modified_values() const;

    /**
     * @brief Get the removed value overlays.
     *
     * TS overlays for removed entries, useful for accessing per-element
     * modification tracking on removed values.
     */
    [[nodiscard]] const std::vector<std::unique_ptr<TSOverlayStorage>>& removed_value_overlays() const;

    // ========== Key Set View ==========

    /**
     * @brief Get a SetDeltaView-like interface for just the keys.
     *
     * Mirrors how TSD exposes key_set() on the value side.
     */
    [[nodiscard]] SetDeltaView key_delta_view() const;

    // ========== Conversion ==========

    /**
     * @brief Create an owning MapDeltaValue from this view.
     */
    [[nodiscard]] MapDeltaValue to_value() const;

    // ========== Schema Access ==========

    [[nodiscard]] const value::TypeMeta* key_schema() const noexcept { return _key_schema; }
    [[nodiscard]] const value::TypeMeta* value_schema() const noexcept { return _value_schema; }

private:
    MapTSOverlay* _overlay{nullptr};
    value::ConstMapView _map_view;
    const value::TypeMeta* _key_schema{nullptr};
    const value::TypeMeta* _value_schema{nullptr};
    engine_time_t _time{MIN_DT};  ///< Time used for computing modified keys
};

// ============================================================================
// MapDeltaValue - Owning map delta
// ============================================================================

/**
 * @brief Owning value containing map delta (key/value additions and removals).
 */
class MapDeltaValue {
public:
    /**
     * @brief A key-value pair for map entries.
     */
    struct Entry {
        value::PlainValue key;
        value::PlainValue value;
    };

    // ========== Builder ==========

    /**
     * @brief Fluent builder for constructing map deltas.
     */
    class Builder {
    public:
        /**
         * @brief Construct builder with schemas.
         */
        Builder(const value::TypeMeta* key_schema,
                const value::TypeMeta* value_schema) noexcept;

        /**
         * @brief Add a key-value pair to the "added" entries.
         */
        Builder& add(value::PlainValue key, value::PlainValue value);

        /**
         * @brief Add a key-value pair (typed).
         */
        template<typename K, typename V>
        Builder& add(const K& key, const V& value);

        /**
         * @brief Add a key to the "removed" set.
         */
        Builder& remove(value::PlainValue key);

        /**
         * @brief Add a key to the "removed" set (typed).
         */
        template<typename K>
        Builder& remove(const K& key);

        /**
         * @brief Add a key-value pair to the "modified" entries.
         */
        Builder& modify(value::PlainValue key, value::PlainValue value);

        /**
         * @brief Add a key-value pair to modified (typed).
         */
        template<typename K, typename V>
        Builder& modify(const K& key, const V& value);

        /**
         * @brief Build the MapDeltaValue.
         */
        [[nodiscard]] MapDeltaValue build();

    private:
        const value::TypeMeta* _key_schema;
        const value::TypeMeta* _value_schema;
        std::vector<Entry> _added;
        std::vector<value::PlainValue> _removed_keys;
        std::vector<Entry> _modified;
    };

    /**
     * @brief Create a builder with the given schemas.
     */
    static Builder builder(const value::TypeMeta* key_schema,
                          const value::TypeMeta* value_schema) {
        return Builder(key_schema, value_schema);
    }

    // ========== Construction ==========

    /**
     * @brief Default constructor - empty delta.
     */
    MapDeltaValue() noexcept = default;

    /**
     * @brief Construct from a MapDeltaView (copies data).
     */
    explicit MapDeltaValue(const MapDeltaView& view);

    /**
     * @brief Construct with explicit data.
     */
    MapDeltaValue(const value::TypeMeta* key_schema,
                  const value::TypeMeta* value_schema,
                  std::vector<Entry> added,
                  std::vector<value::PlainValue> removed_keys,
                  std::vector<Entry> modified = {}) noexcept;

    // ========== Delta Access ==========

    [[nodiscard]] bool has_added() const noexcept { return !_added.empty(); }
    [[nodiscard]] bool has_removed() const noexcept { return !_removed_keys.empty(); }
    [[nodiscard]] bool has_modified() const noexcept { return !_modified.empty(); }
    [[nodiscard]] bool empty() const noexcept { return _added.empty() && _removed_keys.empty() && _modified.empty(); }

    [[nodiscard]] const std::vector<Entry>& added() const noexcept { return _added; }
    [[nodiscard]] const std::vector<value::PlainValue>& removed_keys() const noexcept { return _removed_keys; }
    [[nodiscard]] const std::vector<Entry>& modified() const noexcept { return _modified; }

    /**
     * @brief Get views of added keys.
     */
    [[nodiscard]] std::vector<value::ConstValueView> added_key_views() const;

    /**
     * @brief Get views of added values.
     */
    [[nodiscard]] std::vector<value::ConstValueView> added_value_views() const;

    /**
     * @brief Get views of removed keys.
     */
    [[nodiscard]] std::vector<value::ConstValueView> removed_key_views() const;

    /**
     * @brief Get views of modified keys.
     */
    [[nodiscard]] std::vector<value::ConstValueView> modified_key_views() const;

    /**
     * @brief Get views of modified values.
     */
    [[nodiscard]] std::vector<value::ConstValueView> modified_value_views() const;

    // ========== Schema Access ==========

    [[nodiscard]] const value::TypeMeta* key_schema() const noexcept { return _key_schema; }
    [[nodiscard]] const value::TypeMeta* value_schema() const noexcept { return _value_schema; }

private:
    const value::TypeMeta* _key_schema{nullptr};
    const value::TypeMeta* _value_schema{nullptr};
    std::vector<Entry> _added;
    std::vector<value::PlainValue> _removed_keys;
    std::vector<Entry> _modified;
};

}  // namespace hgraph
