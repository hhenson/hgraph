#pragma once

/**
 * @file ts_view.h
 * @brief Time-series views - TSView, TSMutableView, TSBView.
 *
 * Views provide type-safe access to TSValue data without owning the storage.
 * They carry the TSMeta schema for type information and support navigation
 * through nested structures.
 *
 * @note TRANSIENT: Views are transient and should be used immediately after
 * obtaining them. They are invalidated by structural changes to the underlying
 * container (insert, erase, swap operations on TSL/TSD/TSS). Do not hold views
 * across operations that may modify the container structure. For stable
 * references that survive structural changes, convert to StoredPath which uses
 * actual key values rather than slot indices.
 *
 * View Hierarchy:
 * - TSView: Read-only access with as_xxx() casting methods
 * - TSMutableView: Extends TSView with set() operations
 * - TSBView: Bundle-specific with field() navigation
 *
 * Casting Pattern:
 * @code
 * TSView view = ts_value.view();
 *
 * // For TS[T] - direct scalar access
 * int64_t x = view.as<int64_t>();
 *
 * // For TSB - cast to bundle view
 * TSBView bundle = view.as_bundle();
 * float price = bundle.field("price").as<float>();
 *
 * // For TSL - cast to list view
 * TSLView list = view.as_list();
 * TSView elem = list.element(0);
 * @endcode
 */

#include <optional>
#include <string>

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_path.h>

namespace hgraph {

// Forward declarations
struct TSValue;
struct TSMutableView;
struct TSBView;
struct TSLView;
struct TSDView;
struct TSSView;

/**
 * @brief Read-only view of a time-series value.
 *
 * TSView wraps a ConstValueView with TSMeta schema information.
 * It provides as_xxx() casting methods for type-specific access.
 * It also holds a pointer to the container (TSValue) for state access.
 */
struct TSView {
    // ========== Construction ==========

    /**
     * @brief Default constructor - invalid view.
     */
    TSView() noexcept = default;

    /**
     * @brief Construct from data pointer and schema (no container access).
     */
    TSView(const void* data, const TSMeta* ts_meta) noexcept;

    /**
     * @brief Construct from data pointer, schema, and container.
     */
    TSView(const void* data, const TSMeta* ts_meta, const TSValue* container) noexcept;


    /**
     * @brief Construct from data pointer, schema, and overlay (for overlay-based tracking).
     *
     * Used when creating views with overlay-backed modification tracking.
     * The overlay provides hierarchical timestamp tracking and delta information.
     */
    TSView(const void* data, const TSMeta* ts_meta, TSOverlayStorage* overlay) noexcept;

    /**
     * @brief Construct from data pointer, schema, overlay, and path (for child views).
     *
     * Used when creating child views during navigation (field, element access).
     * Propagates root and extends path for REF support.
     */
    TSView(const void* data, const TSMeta* ts_meta, TSOverlayStorage* overlay,
           const TSValue* root, LightweightPath path) noexcept;

    /**
     * @brief Construct from a TSValue (full access).
     */
    explicit TSView(const TSValue& ts_value);

    // ========== Validity ==========

    /**
     * @brief Check if the view is valid.
     */
    [[nodiscard]] bool valid() const noexcept;

    /**
     * @brief Boolean conversion.
     */
    explicit operator bool() const noexcept { return valid(); }

    // ========== Schema Access ==========

    /**
     * @brief Get the time-series schema.
     */
    [[nodiscard]] const TSMeta* ts_meta() const noexcept { return _ts_meta; }

    /**
     * @brief Get the underlying value view.
     */
    [[nodiscard]] value::ConstValueView value_view() const noexcept { return _view; }

    // ========== Scalar Access ==========

    /**
     * @brief Get the value as type T.
     *
     * Only valid for TS[T] types (scalar time-series).
     *
     * @tparam T The expected scalar type
     * @return Const reference to the value
     */
    template<typename T>
    [[nodiscard]] const T& as() const {
        return _view.as<T>();
    }

    /**
     * @brief Try to get the value as type T.
     *
     * @return Pointer to the value, or nullptr if type doesn't match
     */
    template<typename T>
    [[nodiscard]] const T* try_as() const {
        return _view.try_as<T>();
    }

    // ========== Type Casting ==========

    /**
     * @brief Cast to bundle view (for TSB types).
     *
     * @return TSBView for bundle navigation
     * @throws std::runtime_error if not a bundle type
     */
    [[nodiscard]] TSBView as_bundle() const;

    /**
     * @brief Cast to list view (for TSL types).
     *
     * @return TSLView for list navigation
     * @throws std::runtime_error if not a list type
     */
    [[nodiscard]] TSLView as_list() const;

    /**
     * @brief Cast to dict view (for TSD types).
     *
     * @return TSDView for dict navigation
     * @throws std::runtime_error if not a dict type
     */
    [[nodiscard]] TSDView as_dict() const;

    /**
     * @brief Cast to set view (for TSS types).
     *
     * @return TSSView for set navigation
     * @throws std::runtime_error if not a set type
     */
    [[nodiscard]] TSSView as_set() const;

    // ========== State Access ==========

    /**
     * @brief Check if the time-series has been set (is valid).
     */
    [[nodiscard]] bool ts_valid() const;

    /**
     * @brief Check if modified at the given time.
     * @param time The time to check against
     */
    [[nodiscard]] bool modified_at(engine_time_t time) const;

    /**
     * @brief Check if modified at the current evaluation time.
     *
     * This is a convenience method that gets the evaluation time from the owning
     * node's graph and calls modified_at(). Only valid when the view has a
     * container with an owning node that has a graph.
     *
     * @return true if modified at current evaluation time, false otherwise
     */
    [[nodiscard]] bool modified() const;

    /**
     * @brief Get the last modification time.
     */
    [[nodiscard]] engine_time_t last_modified_time() const;

    /**
     * @brief Get the owning node.
     * @return The Node that owns this time-series, or nullptr
     */
    [[nodiscard]] Node* owning_node() const;

    /**
     * @brief Check if the view has container access (for state queries).
     */
    [[nodiscard]] bool has_container() const noexcept { return _container != nullptr; }

    /**
     * @brief Check if the view has overlay access (for overlay-based state queries).
     */
    [[nodiscard]] bool has_overlay() const noexcept { return _overlay != nullptr; }

    /**
     * @brief Get the overlay storage for this view.
     *
     * The overlay provides hierarchical modification tracking and for
     * collection types, also provides delta information (added/removed elements).
     *
     * @return Pointer to the overlay, or nullptr if not available
     */
    [[nodiscard]] TSOverlayStorage* overlay() const noexcept { return _overlay; }

    // ========== Python Interop ==========

    /**
     * @brief Convert to Python object.
     */
    [[nodiscard]] nb::object to_python() const;

    // ========== Path Access ==========

    /**
     * @brief Get the root TSValue this view is derived from.
     * @return Pointer to root TSValue, or nullptr if unknown
     */
    [[nodiscard]] const TSValue* root() const noexcept { return _root; }

    /**
     * @brief Get the lightweight path from root to this view.
     * @return The path (empty for root views)
     */
    [[nodiscard]] const LightweightPath& path() const noexcept { return _path; }

    /**
     * @brief Get the path as a string for debugging.
     * @return String representation of the path
     */
    [[nodiscard]] std::string path_string() const { return _path.to_string(); }

    /**
     * @brief Check if this view has path tracking.
     * @return true if root is known and path is tracked
     */
    [[nodiscard]] bool has_path() const noexcept { return _root != nullptr; }

    // ========== Link Support ==========

    /**
     * @brief Check if this view has a link source for transparent link navigation.
     *
     * A link source is a TSValue with link support enabled (typically an input).
     * When navigating through such a view, child accesses may follow links
     * to external outputs transparently.
     */
    [[nodiscard]] bool has_link_source() const noexcept { return _link_source != nullptr; }

    /**
     * @brief Get the link source TSValue.
     * @return TSValue with link support, or nullptr
     */
    [[nodiscard]] const TSValue* link_source() const noexcept { return _link_source; }

    /**
     * @brief Set the link source for child navigation.
     *
     * This is used when creating input views to enable transparent link following.
     *
     * @param source TSValue with link support
     */
    void set_link_source(const TSValue* source) noexcept { _link_source = source; }

    /**
     * @brief Get the stored path for this view (for REF persistence).
     *
     * Converts the lightweight path to a fully serializable StoredPath
     * that can be used by REF types for persistence.
     *
     * @return StoredPath if path is tracked, nullopt otherwise
     * @throws std::runtime_error if path contains TSD ordinals (key value needed)
     */
    [[nodiscard]] std::optional<StoredPath> stored_path() const;

protected:
    value::ConstValueView _view;
    const TSMeta* _ts_meta{nullptr};
    const TSValue* _container{nullptr};       ///< Container for state access (can be null)
    TSOverlayStorage* _overlay{nullptr};      ///< Overlay for modification tracking (can be null)
    const TSValue* _root{nullptr};            ///< Root TSValue for path tracking (can be null)
    LightweightPath _path;                    ///< Path from root to this view
    const TSValue* _link_source{nullptr};     ///< TSValue with link support for transparent navigation (can be null)
};

/**
 * @brief Mutable view of a time-series value.
 *
 * Extends TSView with set() operations for modifying values.
 * Used for outputs where values need to be written.
 */
struct TSMutableView : TSView {
    // ========== Construction ==========

    /**
     * @brief Default constructor - invalid view.
     */
    TSMutableView() noexcept = default;

    /**
     * @brief Construct from data pointer and schema (no container access).
     */
    TSMutableView(void* data, const TSMeta* ts_meta) noexcept;

    /**
     * @brief Construct from data pointer, schema, and container.
     */
    TSMutableView(void* data, const TSMeta* ts_meta, TSValue* container) noexcept;

    /**
     * @brief Construct from data pointer, schema, and overlay (for overlay-based tracking).
     *
     * Used when creating mutable views with overlay-backed modification tracking.
     */
    TSMutableView(void* data, const TSMeta* ts_meta, TSOverlayStorage* overlay) noexcept;

    /**
     * @brief Construct from a TSValue (full access).
     */
    explicit TSMutableView(TSValue& ts_value);

    // ========== Mutable Value View ==========

    /**
     * @brief Get the mutable value view.
     */
    [[nodiscard]] value::ValueView mutable_value_view() noexcept { return _mutable_view; }

    // ========== Scalar Mutation ==========

    /**
     * @brief Set the value.
     *
     * Only valid for TS[T] types (scalar time-series).
     *
     * @tparam T The value type
     * @param val The value to set
     * @param time The engine time of this modification
     */
    template<typename T>
    void set(const T& val, engine_time_t time) {
        _mutable_view.as<T>() = val;
        notify_modified(time);
    }

    /**
     * @brief Get mutable reference to the value.
     *
     * @tparam T The expected scalar type
     * @return Mutable reference to the value
     */
    template<typename T>
    [[nodiscard]] T& as_mut() {
        return _mutable_view.as<T>();
    }

    // ========== Copy Operations ==========

    /**
     * @brief Copy value from another view.
     *
     * @param source The source view to copy from
     */
    void copy_from(const TSView& source);

    // ========== State Mutation ==========

    /**
     * @brief Notify that the value was modified at given time.
     * @param time The engine time of modification
     */
    void notify_modified(engine_time_t time);

    /**
     * @brief Mark the time-series as invalid (cleared).
     */
    void invalidate_ts();

    // ========== Python Interop ==========

    /**
     * @brief Set value from Python object.
     */
    void from_python(const nb::object& src);

private:
    value::ValueView _mutable_view;
    TSValue* _mutable_container{nullptr};     ///< Mutable container access
};

/**
 * @brief Bundle-specific view (for TSB types).
 *
 * Provides field-based navigation for bundle time-series types.
 */
struct TSBView : TSView {
    // ========== Construction ==========

    /**
     * @brief Default constructor - invalid view.
     */
    TSBView() noexcept = default;

    /**
     * @brief Construct from data pointer and schema.
     *
     * @param data Pointer to bundle data
     * @param ts_meta TSBTypeMeta schema (must be bundle type)
     */
    TSBView(const void* data, const TSBTypeMeta* ts_meta) noexcept;

    /**
     * @brief Construct from data pointer, schema, and overlay.
     *
     * @param data Pointer to bundle data
     * @param ts_meta TSBTypeMeta schema (must be bundle type)
     * @param overlay CompositeTSOverlay for hierarchical tracking
     */
    TSBView(const void* data, const TSBTypeMeta* ts_meta, CompositeTSOverlay* overlay) noexcept;

    // ========== Bundle Schema ==========

    /**
     * @brief Get the bundle schema.
     */
    [[nodiscard]] const TSBTypeMeta* bundle_meta() const noexcept {
        return static_cast<const TSBTypeMeta*>(_ts_meta);
    }

    // ========== Field Access ==========

    /**
     * @brief Get a field by name.
     *
     * @param name The field name
     * @return TSView for the field
     * @throws std::runtime_error if field not found
     */
    [[nodiscard]] TSView field(const std::string& name) const;

    /**
     * @brief Get a field by index.
     *
     * @param index The field index (0-based)
     * @return TSView for the field
     * @throws std::out_of_range if index out of bounds
     */
    [[nodiscard]] TSView field(size_t index) const;

    /**
     * @brief Get the number of fields.
     */
    [[nodiscard]] size_t field_count() const noexcept;

    /**
     * @brief Check if a field exists.
     */
    [[nodiscard]] bool has_field(const std::string& name) const noexcept;

    // ========== Delta Access ==========

    /**
     * @brief Get a delta view for this bundle at the given time.
     *
     * Returns a BundleDeltaView providing access to modified fields.
     * The time is used to determine which fields were modified at
     * the current tick (their overlay's last_modified_time == time).
     *
     * @param time The current engine time
     * @return BundleDeltaView (check valid() before use)
     *
     * @code
     * if (auto delta = bundle_view.delta_view(current_time)) {
     *     for (size_t idx : delta.modified_indices()) { ... }
     *     for (auto& val : delta.modified_values()) { ... }
     * }
     * @endcode
     */
    [[nodiscard]] BundleDeltaView delta_view(engine_time_t time);

    /**
     * @brief Get the composite overlay (typed access).
     * @return CompositeTSOverlay pointer if overlay exists and is correct type, nullptr otherwise
     */
    [[nodiscard]] CompositeTSOverlay* composite_overlay() const noexcept;
};

/**
 * @brief List-specific view (for TSL types).
 *
 * Provides element-based navigation for list time-series types.
 */
struct TSLView : TSView {
    // ========== Construction ==========

    TSLView() noexcept = default;
    TSLView(const void* data, const TSLTypeMeta* ts_meta) noexcept;

    /**
     * @brief Construct from data pointer, schema, and overlay.
     *
     * @param data Pointer to list data
     * @param ts_meta TSLTypeMeta schema (must be list type)
     * @param overlay ListTSOverlay for hierarchical tracking
     */
    TSLView(const void* data, const TSLTypeMeta* ts_meta, ListTSOverlay* overlay) noexcept;

    // ========== List Schema ==========

    [[nodiscard]] const TSLTypeMeta* list_meta() const noexcept {
        return static_cast<const TSLTypeMeta*>(_ts_meta);
    }

    // ========== Element Access ==========

    /**
     * @brief Get an element by index.
     */
    [[nodiscard]] TSView element(size_t index) const;

    /**
     * @brief Get the number of elements.
     */
    [[nodiscard]] size_t size() const noexcept;

    /**
     * @brief Check if the list has a fixed size.
     */
    [[nodiscard]] bool is_fixed_size() const noexcept;

    /**
     * @brief Get the fixed size (0 if dynamic).
     */
    [[nodiscard]] size_t fixed_size() const noexcept;

    // ========== Delta Access ==========

    /**
     * @brief Get a delta view for this list at the given time.
     *
     * Returns a ListDeltaView providing access to modified elements.
     * The time is used to determine which elements were modified at
     * the current tick (their overlay's last_modified_time == time).
     *
     * @param time The current engine time
     * @return ListDeltaView (check valid() before use)
     *
     * @code
     * if (auto delta = list_view.delta_view(current_time)) {
     *     for (size_t idx : delta.modified_indices()) { ... }
     *     for (auto& val : delta.modified_values()) { ... }
     * }
     * @endcode
     */
    [[nodiscard]] ListDeltaView delta_view(engine_time_t time);

    /**
     * @brief Get the list overlay (typed access).
     * @return ListTSOverlay pointer if overlay exists and is correct type, nullptr otherwise
     */
    [[nodiscard]] ListTSOverlay* list_overlay() const noexcept;
};

/**
 * @brief Dict-specific view (for TSD types).
 *
 * Provides key-value navigation for dict time-series types.
 */
struct TSDView : TSView {
    // ========== Construction ==========

    TSDView() noexcept = default;
    TSDView(const void* data, const TSDTypeMeta* ts_meta) noexcept;

    /**
     * @brief Construct from data pointer, schema, and overlay.
     *
     * @param data Pointer to dict data
     * @param ts_meta TSDTypeMeta schema (must be dict type)
     * @param overlay MapTSOverlay for hierarchical tracking
     */
    TSDView(const void* data, const TSDTypeMeta* ts_meta, MapTSOverlay* overlay) noexcept;

    // ========== Dict Schema ==========

    [[nodiscard]] const TSDTypeMeta* dict_meta() const noexcept {
        return static_cast<const TSDTypeMeta*>(_ts_meta);
    }

    // ========== Key/Value Access ==========

    /**
     * @brief Get a value by key.
     *
     * @tparam K The key type
     * @param key The key to look up
     * @return TSView for the value
     * @throws std::out_of_range if key not found
     */
    template<typename K>
    [[nodiscard]] TSView at(const K& key) const;

    /**
     * @brief Check if a key exists.
     */
    template<typename K>
    [[nodiscard]] bool contains(const K& key) const;

    /**
     * @brief Get the number of entries.
     */
    [[nodiscard]] size_t size() const noexcept;

    // ========== Delta Access ==========

    /**
     * @brief Get a delta view for this map at the given time.
     *
     * Returns a MapDeltaView providing access to key additions and removals.
     * The time is used for lazy cleanup - if it differs from the last
     * modification time, delta buffers are cleared first.
     *
     * @param time The current engine time
     * @return MapDeltaView (check valid() before use)
     *
     * @code
     * if (auto delta = dict_view.delta_view(current_time)) {
     *     for (auto& key : delta.added_keys()) { ... }
     *     for (auto& val : delta.added_values()) { ... }
     *     for (auto& key : delta.removed_keys()) { ... }
     * }
     * @endcode
     */
    [[nodiscard]] MapDeltaView delta_view(engine_time_t time);

    /**
     * @brief Get a key set view for SetTSOverlay-compatible key tracking.
     *
     * This mirrors how TSD exposes key_set() on the value side.
     *
     * @return KeySetOverlayView wrapping this map's key tracking, or invalid view if no overlay
     */
    [[nodiscard]] KeySetOverlayView key_set_view() const;

    /**
     * @brief Get the map overlay (typed access).
     * @return MapTSOverlay pointer if overlay exists and is correct type, nullptr otherwise
     */
    [[nodiscard]] MapTSOverlay* map_overlay() const noexcept;
};

/**
 * @brief Set-specific view (for TSS types).
 *
 * Provides set operations for set time-series types.
 */
struct TSSView : TSView {
    // ========== Construction ==========

    TSSView() noexcept = default;
    TSSView(const void* data, const TSSTypeMeta* ts_meta) noexcept;

    /**
     * @brief Construct from data pointer, schema, and overlay.
     *
     * @param data Pointer to set data
     * @param ts_meta TSSTypeMeta schema (must be set type)
     * @param overlay SetTSOverlay for hierarchical tracking
     */
    TSSView(const void* data, const TSSTypeMeta* ts_meta, SetTSOverlay* overlay) noexcept;

    // ========== Set Schema ==========

    [[nodiscard]] const TSSTypeMeta* set_meta() const noexcept {
        return static_cast<const TSSTypeMeta*>(_ts_meta);
    }

    // ========== Set Operations ==========

    /**
     * @brief Check if the set contains an element.
     */
    template<typename T>
    [[nodiscard]] bool contains(const T& element) const;

    /**
     * @brief Get the number of elements.
     */
    [[nodiscard]] size_t size() const noexcept;

    // ========== Delta Access ==========

    /**
     * @brief Get a delta view for this set at the given time.
     *
     * Returns a SetDeltaView providing access to additions and removals.
     * The time is used for lazy cleanup - if it differs from the last
     * modification time, delta buffers are cleared first.
     *
     * @param time The current engine time
     * @return SetDeltaView (check valid() before use)
     *
     * @code
     * if (auto delta = set_view.delta_view(current_time)) {
     *     for (auto& added : delta.added_values()) { ... }
     *     for (auto& removed : delta.removed_values()) { ... }
     * }
     * @endcode
     */
    [[nodiscard]] SetDeltaView delta_view(engine_time_t time);

    /**
     * @brief Get the set overlay (typed access).
     * @return SetTSOverlay pointer if overlay exists and is correct type, nullptr otherwise
     */
    [[nodiscard]] SetTSOverlay* set_overlay() const noexcept;
};

// ============================================================================
// Template Implementations
// ============================================================================

template<typename K>
TSView TSDView::at(const K& key) const {
    if (!valid()) {
        throw std::runtime_error("TSDView::at() called on invalid view");
    }

    // Get the map view from value layer
    value::ConstMapView map_view = _view.as_map();

    // Find the slot index for this key
    const value::TypeMeta* key_schema = dict_meta()->key_type();
    value::ConstValueView key_view(&key, key_schema);
    auto slot_idx = map_view.find_index(key_view);

    if (!slot_idx) {
        throw std::out_of_range("TSDView::at(): key not found");
    }

    // Get the value at this slot
    value::ConstValueView value_view = map_view.value_at(*slot_idx);

    // Get the element TS type
    const TSMeta* value_ts_type = dict_meta()->value_ts_type();

    // Extend path with the slot index (like list indices)
    LightweightPath child_path = _path.with(*slot_idx);

    // Get value overlay if available (MapTSOverlay uses value_overlay, not child)
    if (auto* map_ov = map_overlay()) {
        TSOverlayStorage* value_ov = map_ov->value_overlay(*slot_idx);
        return TSView(value_view.data(), value_ts_type, value_ov, _root, std::move(child_path));
    }

    // No overlay - return view with path but without tracking
    return TSView(value_view.data(), value_ts_type, nullptr, _root, std::move(child_path));
}

template<typename K>
bool TSDView::contains(const K& key) const {
    if (!valid()) {
        return false;
    }

    value::ConstMapView map_view = _view.as_map();
    const value::TypeMeta* key_schema = dict_meta()->key_type();
    value::ConstValueView key_view(&key, key_schema);
    return map_view.find_index(key_view).has_value();
}

template<typename T>
bool TSSView::contains(const T& element) const {
    if (!valid()) {
        return false;
    }

    value::ConstSetView set_view = _view.as_set();
    const value::TypeMeta* elem_schema = set_meta()->element_type();
    value::ConstValueView elem_view(&element, elem_schema);
    return set_view.find_index(elem_view).has_value();
}

}  // namespace hgraph
