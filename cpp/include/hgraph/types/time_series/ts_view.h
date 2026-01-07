#pragma once

/**
 * @file ts_view.h
 * @brief Time-series views - TSView, TSMutableView, TSBView.
 *
 * Views provide type-safe access to TSValue data without owning the storage.
 * They carry the TSMeta schema for type information and support navigation
 * through nested structures.
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

#include <hgraph/types/value/value.h>
#include <hgraph/types/time_series/ts_type_meta.h>

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
     * @brief Construct from data pointer, schema, and tracking view (for child views).
     *
     * Used when creating views for bundle fields, list elements, etc.
     * where we have hierarchical tracking but no direct container reference.
     */
    TSView(const void* data, const TSMeta* ts_meta, value::ConstValueView tracking_view) noexcept;

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
     * @brief Check if the view has tracking access (for hierarchical state queries).
     */
    [[nodiscard]] bool has_tracking() const noexcept { return _tracking_view.valid(); }

    /**
     * @brief Get the tracking view for this level.
     *
     * The tracking view contains engine_time_t timestamps following the
     * same structure as the data. For scalar types, this is a single timestamp.
     * For bundles/lists, navigate in parallel with the data.
     */
    [[nodiscard]] value::ConstValueView tracking_view() const noexcept { return _tracking_view; }

    // ========== Python Interop ==========

    /**
     * @brief Convert to Python object.
     */
    [[nodiscard]] nb::object to_python() const;

protected:
    value::ConstValueView _view;
    const TSMeta* _ts_meta{nullptr};
    const TSValue* _container{nullptr};      ///< Container for state access (can be null)
    value::ConstValueView _tracking_view;    ///< Tracking view for this level (can be invalid)
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
     * @brief Construct from data pointer, schema, and tracking view (for child views).
     *
     * Used when creating mutable views for bundle fields, list elements, etc.
     * where we have hierarchical tracking but no direct container reference.
     */
    TSMutableView(void* data, const TSMeta* ts_meta, value::ValueView tracking_view) noexcept;

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
        // TODO: Update modification tracking with time
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

    /**
     * @brief Get the mutable tracking view for this level.
     */
    [[nodiscard]] value::ValueView mutable_tracking_view() { return _mutable_tracking_view; }

private:
    value::ValueView _mutable_view;
    TSValue* _mutable_container{nullptr};     ///< Mutable container access
    value::ValueView _mutable_tracking_view;  ///< Mutable tracking view for this level
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
};

}  // namespace hgraph
