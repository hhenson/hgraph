#pragma once

/**
 * @file value_view.h
 * @brief Non-owning view classes for the Value type system.
 *
 * Views provide access to Value data without ownership. ConstValueView provides
 * read-only access, while ValueView provides mutable access. Both support:
 *
 * - Type kind queries (is_scalar, is_bundle, is_list, etc.)
 * - Type-safe value access (as<T>, try_as<T>, checked_as<T>)
 * - Conversion to specialized views (as_bundle, as_list, etc.)
 * - Python interop (to_python, from_python)
 *
 * Views are lightweight (two pointers) and are designed to be passed by value.
 */

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/type_registry.h>

#include <cassert>
#include <optional>
#include <stdexcept>
#include <string>

namespace hgraph::value {

// Forward declarations for specialized views
class ConstTupleView;
class TupleView;
class ConstBundleView;
class BundleView;
class ConstListView;
class ListView;
class ConstSetView;
class SetView;
class ConstMapView;
class MapView;
class ConstCyclicBufferView;
class CyclicBufferView;
class ConstQueueView;
class QueueView;

// Forward declaration for Value (used in clone)
template<typename Policy>
class Value;

// ============================================================================
// ConstValueView - Non-owning Const View
// ============================================================================

/**
 * @brief Non-owning const view into a Value.
 *
 * ConstValueView provides read-only access to value data. It stores a pointer
 * to the data and the type schema. Views are lightweight and should be passed
 * by value.
 *
 * A view is "valid" when both the data pointer and schema are non-null.
 * Operations on invalid views have undefined behavior in release builds;
 * debug builds assert validity.
 */
class ConstValueView {
public:
    // ========== Construction ==========

    /// Default constructor - creates an invalid view
    ConstValueView() noexcept = default;

    /**
     * @brief Construct a view from data and schema.
     *
     * @param data Pointer to the value data
     * @param schema The type schema
     */
    ConstValueView(const void* data, const TypeMeta* schema) noexcept
        : _data(data), _schema(schema) {}

    /// Copy constructor
    ConstValueView(const ConstValueView&) noexcept = default;

    /// Copy assignment
    ConstValueView& operator=(const ConstValueView&) noexcept = default;

    // ========== Validity ==========

    /**
     * @brief Check if the view is valid.
     *
     * A view is valid when both the data pointer and schema are non-null.
     *
     * @return true if the view is valid
     */
    [[nodiscard]] bool valid() const noexcept {
        return _data != nullptr && _schema != nullptr;
    }

    /**
     * @brief Boolean conversion - returns validity.
     */
    explicit operator bool() const noexcept {
        return valid();
    }

    /**
     * @brief Get the type schema.
     * @return The schema, or nullptr if invalid
     */
    [[nodiscard]] const TypeMeta* schema() const noexcept {
        return _schema;
    }

    // ========== Type Kind Queries ==========

    /**
     * @brief Check if this is a scalar type.
     * @return true if the type kind is Scalar
     */
    [[nodiscard]] bool is_scalar() const noexcept {
        return valid() && _schema->kind == TypeKind::Scalar;
    }

    /**
     * @brief Check if this is a tuple type.
     * @return true if the type kind is Tuple
     */
    [[nodiscard]] bool is_tuple() const noexcept {
        return valid() && _schema->kind == TypeKind::Tuple;
    }

    /**
     * @brief Check if this is a bundle type.
     * @return true if the type kind is Bundle
     */
    [[nodiscard]] bool is_bundle() const noexcept {
        return valid() && _schema->kind == TypeKind::Bundle;
    }

    /**
     * @brief Check if this is a list type.
     * @return true if the type kind is List
     */
    [[nodiscard]] bool is_list() const noexcept {
        return valid() && _schema->kind == TypeKind::List;
    }

    /**
     * @brief Check if this is a fixed-size list.
     * @return true if this is a list with fixed size
     */
    [[nodiscard]] bool is_fixed_list() const noexcept {
        return is_list() && _schema->is_fixed_size();
    }

    /**
     * @brief Check if this is a set type.
     * @return true if the type kind is Set
     */
    [[nodiscard]] bool is_set() const noexcept {
        return valid() && _schema->kind == TypeKind::Set;
    }

    /**
     * @brief Check if this is a map type.
     * @return true if the type kind is Map
     */
    [[nodiscard]] bool is_map() const noexcept {
        return valid() && _schema->kind == TypeKind::Map;
    }

    /**
     * @brief Check if this is a cyclic buffer type.
     * @return true if the type kind is CyclicBuffer
     */
    [[nodiscard]] bool is_cyclic_buffer() const noexcept {
        return valid() && _schema->kind == TypeKind::CyclicBuffer;
    }

    /**
     * @brief Check if this is a queue type.
     * @return true if the type kind is Queue
     */
    [[nodiscard]] bool is_queue() const noexcept {
        return valid() && _schema->kind == TypeKind::Queue;
    }

    // ========== Type Checking ==========

    /**
     * @brief Check if this view has a specific schema.
     *
     * Uses pointer equality (nominal typing).
     *
     * @param other The schema to compare against
     * @return true if the schemas are identical
     */
    [[nodiscard]] bool is_type(const TypeMeta* other) const noexcept {
        return _schema == other;
    }

    /**
     * @brief Check if this is a specific scalar type.
     *
     * @tparam T The scalar type to check
     * @return true if the schema matches the scalar type
     */
    template<typename T>
    [[nodiscard]] bool is_scalar_type() const noexcept {
        return valid() && _schema == scalar_type_meta<T>();
    }

    // ========== Scalar Type Access ==========

    /**
     * @brief Get the value as type T (debug assertion).
     *
     * Zero overhead in release builds. Asserts validity and type match
     * in debug builds.
     *
     * @tparam T The expected type
     * @return Const reference to the value
     */
    template<typename T>
    [[nodiscard]] const T& as() const {
        assert(valid() && "as<T>() on invalid view");
        assert(is_scalar_type<T>() && "as<T>() type mismatch");
        return *static_cast<const T*>(_data);
    }

    /**
     * @brief Try to get the value as type T.
     *
     * Safe access that returns nullptr on type mismatch.
     *
     * @tparam T The expected type
     * @return Pointer to the value, or nullptr if type doesn't match
     */
    template<typename T>
    [[nodiscard]] const T* try_as() const noexcept {
        return is_scalar_type<T>() ? static_cast<const T*>(_data) : nullptr;
    }

    /**
     * @brief Get the value as type T (throwing).
     *
     * Throws on invalid view or type mismatch. Use at API boundaries.
     *
     * @tparam T The expected type
     * @return Const reference to the value
     * @throws std::runtime_error if invalid or type mismatch
     */
    template<typename T>
    [[nodiscard]] const T& checked_as() const {
        if (!valid()) {
            throw std::runtime_error("checked_as<T>() on invalid view");
        }
        if (!is_scalar_type<T>()) {
            throw std::runtime_error("checked_as<T>() type mismatch");
        }
        return *static_cast<const T*>(_data);
    }

    // ========== Specialized View Conversions (Safe) ==========

    /**
     * @brief Try to convert to a tuple view.
     * @return The tuple view, or nullopt if not a tuple
     */
    [[nodiscard]] std::optional<ConstTupleView> try_as_tuple() const;

    /**
     * @brief Try to convert to a bundle view.
     * @return The bundle view, or nullopt if not a bundle
     */
    [[nodiscard]] std::optional<ConstBundleView> try_as_bundle() const;

    /**
     * @brief Try to convert to a list view.
     * @return The list view, or nullopt if not a list
     */
    [[nodiscard]] std::optional<ConstListView> try_as_list() const;

    /**
     * @brief Try to convert to a set view.
     * @return The set view, or nullopt if not a set
     */
    [[nodiscard]] std::optional<ConstSetView> try_as_set() const;

    /**
     * @brief Try to convert to a map view.
     * @return The map view, or nullopt if not a map
     */
    [[nodiscard]] std::optional<ConstMapView> try_as_map() const;

    /**
     * @brief Try to convert to a cyclic buffer view.
     * @return The cyclic buffer view, or nullopt if not a cyclic buffer
     */
    [[nodiscard]] std::optional<ConstCyclicBufferView> try_as_cyclic_buffer() const;

    /**
     * @brief Try to convert to a queue view.
     * @return The queue view, or nullopt if not a queue
     */
    [[nodiscard]] std::optional<ConstQueueView> try_as_queue() const;

    // ========== Specialized View Conversions (Throwing) ==========

    /**
     * @brief Convert to a tuple view.
     * @return The tuple view
     * @throws std::runtime_error if not a tuple
     */
    [[nodiscard]] ConstTupleView as_tuple() const;

    /**
     * @brief Convert to a bundle view.
     * @return The bundle view
     * @throws std::runtime_error if not a bundle
     */
    [[nodiscard]] ConstBundleView as_bundle() const;

    /**
     * @brief Convert to a list view.
     * @return The list view
     * @throws std::runtime_error if not a list
     */
    [[nodiscard]] ConstListView as_list() const;

    /**
     * @brief Convert to a set view.
     * @return The set view
     * @throws std::runtime_error if not a set
     */
    [[nodiscard]] ConstSetView as_set() const;

    /**
     * @brief Convert to a map view.
     * @return The map view
     * @throws std::runtime_error if not a map
     */
    [[nodiscard]] ConstMapView as_map() const;

    /**
     * @brief Convert to a cyclic buffer view.
     * @return The cyclic buffer view
     * @throws std::runtime_error if not a cyclic buffer
     */
    [[nodiscard]] ConstCyclicBufferView as_cyclic_buffer() const;

    /**
     * @brief Convert to a queue view.
     * @return The queue view
     * @throws std::runtime_error if not a queue
     */
    [[nodiscard]] ConstQueueView as_queue() const;

    // ========== Raw Access ==========

    /**
     * @brief Get the raw data pointer.
     * @return Const pointer to the data
     */
    [[nodiscard]] const void* data() const noexcept {
        return _data;
    }

    // ========== Operations ==========

    /**
     * @brief Check equality with another view.
     *
     * Uses the schema's equals operation.
     *
     * @param other The view to compare against
     * @return true if the values are equal
     */
    [[nodiscard]] bool equals(const ConstValueView& other) const {
        if (!valid() || !other.valid()) return false;
        if (_schema != other._schema) return false;
        return _schema->ops->equals(_data, other._data, _schema);
    }

    /**
     * @brief Compute the hash of the value.
     *
     * @return The hash value
     * @throws std::runtime_error if the type is not hashable
     */
    [[nodiscard]] size_t hash() const {
        assert(valid() && "hash() on invalid view");
        if (!_schema->ops->hash) {
            throw std::runtime_error("Type is not hashable");
        }
        return _schema->ops->hash(_data, _schema);
    }

    /**
     * @brief Convert the value to a string.
     * @return String representation of the value
     */
    [[nodiscard]] std::string to_string() const {
        assert(valid() && "to_string() on invalid view");
        return _schema->ops->to_string(_data, _schema);
    }

    // ========== Python Interop ==========

    /**
     * @brief Convert the value to a Python object.
     * @return The Python object representation
     */
    [[nodiscard]] nb::object to_python() const {
        assert(valid() && "to_python() on invalid view");
        return _schema->ops->to_python(_data, _schema);
    }

    // ========== Clone ==========

    /**
     * @brief Create an owning Value copy of this view's data.
     *
     * This is declared here but implemented in value.h to avoid
     * circular dependencies.
     *
     * @return A new Value containing a copy of this data
     */
    template<typename Policy = NoCache>
    [[nodiscard]] Value<Policy> clone() const;

protected:
    const void* _data{nullptr};
    const TypeMeta* _schema{nullptr};
};

// ============================================================================
// ValueView - Non-owning Mutable View
// ============================================================================

/**
 * @brief Non-owning mutable view into a Value.
 *
 * ValueView extends ConstValueView with mutable access. It stores both a
 * const and mutable pointer to the same data, allowing inheritance of const
 * operations while adding mutable ones.
 */
class ValueView : public ConstValueView {
public:
    // ========== Construction ==========

    /// Default constructor - creates an invalid view
    ValueView() noexcept = default;

    /**
     * @brief Construct a mutable view from data and schema.
     *
     * @param data Pointer to the value data
     * @param schema The type schema
     */
    ValueView(void* data, const TypeMeta* schema) noexcept
        : ConstValueView(data, schema), _mutable_data(data) {}

    /// Copy constructor
    ValueView(const ValueView&) noexcept = default;

    /// Copy assignment
    ValueView& operator=(const ValueView&) noexcept = default;

    // ========== Mutable Data Access ==========

    /**
     * @brief Get the mutable data pointer.
     * @return Mutable pointer to the data
     */
    [[nodiscard]] void* data() noexcept {
        return _mutable_data;
    }

    // Bring const version into scope
    using ConstValueView::data;

    // ========== Mutable Scalar Type Access ==========

    /**
     * @brief Get the value as type T (debug assertion, mutable).
     *
     * @tparam T The expected type
     * @return Mutable reference to the value
     */
    template<typename T>
    [[nodiscard]] T& as() {
        assert(valid() && "as<T>() on invalid view");
        assert(is_scalar_type<T>() && "as<T>() type mismatch");
        return *static_cast<T*>(_mutable_data);
    }

    // Bring const version into scope
    using ConstValueView::as;

    /**
     * @brief Try to get the value as type T (mutable).
     *
     * @tparam T The expected type
     * @return Mutable pointer to the value, or nullptr if type doesn't match
     */
    template<typename T>
    [[nodiscard]] T* try_as() noexcept {
        return is_scalar_type<T>() ? static_cast<T*>(_mutable_data) : nullptr;
    }

    // Bring const version into scope
    using ConstValueView::try_as;

    /**
     * @brief Get the value as type T (throwing, mutable).
     *
     * @tparam T The expected type
     * @return Mutable reference to the value
     * @throws std::runtime_error if invalid or type mismatch
     */
    template<typename T>
    [[nodiscard]] T& checked_as() {
        if (!valid()) {
            throw std::runtime_error("checked_as<T>() on invalid view");
        }
        if (!is_scalar_type<T>()) {
            throw std::runtime_error("checked_as<T>() type mismatch");
        }
        return *static_cast<T*>(_mutable_data);
    }

    // Bring const version into scope
    using ConstValueView::checked_as;

    // ========== Specialized Mutable View Conversions (Safe) ==========

    /**
     * @brief Try to convert to a mutable tuple view.
     * @return The tuple view, or nullopt if not a tuple
     */
    [[nodiscard]] std::optional<TupleView> try_as_tuple();

    /**
     * @brief Try to convert to a mutable bundle view.
     * @return The bundle view, or nullopt if not a bundle
     */
    [[nodiscard]] std::optional<BundleView> try_as_bundle();

    /**
     * @brief Try to convert to a mutable list view.
     * @return The list view, or nullopt if not a list
     */
    [[nodiscard]] std::optional<ListView> try_as_list();

    /**
     * @brief Try to convert to a mutable set view.
     * @return The set view, or nullopt if not a set
     */
    [[nodiscard]] std::optional<SetView> try_as_set();

    /**
     * @brief Try to convert to a mutable map view.
     * @return The map view, or nullopt if not a map
     */
    [[nodiscard]] std::optional<MapView> try_as_map();

    /**
     * @brief Try to convert to a mutable cyclic buffer view.
     * @return The cyclic buffer view, or nullopt if not a cyclic buffer
     */
    [[nodiscard]] std::optional<CyclicBufferView> try_as_cyclic_buffer();

    /**
     * @brief Try to convert to a mutable queue view.
     * @return The queue view, or nullopt if not a queue
     */
    [[nodiscard]] std::optional<QueueView> try_as_queue();

    // ========== Specialized Mutable View Conversions (Throwing) ==========

    /**
     * @brief Convert to a mutable tuple view.
     * @return The tuple view
     * @throws std::runtime_error if not a tuple
     */
    [[nodiscard]] TupleView as_tuple();

    /**
     * @brief Convert to a mutable bundle view.
     * @return The bundle view
     * @throws std::runtime_error if not a bundle
     */
    [[nodiscard]] BundleView as_bundle();

    /**
     * @brief Convert to a mutable list view.
     * @return The list view
     * @throws std::runtime_error if not a list
     */
    [[nodiscard]] ListView as_list();

    /**
     * @brief Convert to a mutable set view.
     * @return The set view
     * @throws std::runtime_error if not a set
     */
    [[nodiscard]] SetView as_set();

    /**
     * @brief Convert to a mutable map view.
     * @return The map view
     * @throws std::runtime_error if not a map
     */
    [[nodiscard]] MapView as_map();

    /**
     * @brief Convert to a mutable cyclic buffer view.
     * @return The cyclic buffer view
     * @throws std::runtime_error if not a cyclic buffer
     */
    [[nodiscard]] CyclicBufferView as_cyclic_buffer();

    /**
     * @brief Convert to a mutable queue view.
     * @return The queue view
     * @throws std::runtime_error if not a queue
     */
    [[nodiscard]] QueueView as_queue();

    // ========== Mutation ==========

    /**
     * @brief Copy data from another view.
     *
     * The source view must have the same schema.
     *
     * @param other The source view
     * @throws std::runtime_error if schemas don't match
     */
    void copy_from(const ConstValueView& other) {
        if (!valid() || !other.valid()) {
            throw std::runtime_error("copy_from with invalid view");
        }
        if (_schema != other.schema()) {
            throw std::runtime_error("copy_from schema mismatch");
        }
        _schema->ops->copy_assign(_mutable_data, other.data(), _schema);
    }

    // ========== Python Interop ==========

    /**
     * @brief Set the value from a Python object.
     *
     * @param src The Python object
     */
    void from_python(const nb::object& src) {
        assert(valid() && "from_python() on invalid view");
        _schema->ops->from_python(_mutable_data, src, _schema);
    }

    // ========== Root Tracking ==========

    // NOTE(type-safety): The set_root/root methods use void* for type erasure,
    // which loses the Policy template parameter type information. If set_root<PolicyA>()
    // is called and root<PolicyB>() is later called with a different policy,
    // undefined behavior results. Users must ensure Policy consistency.

    /**
     * @brief Set the root Value for notification chains.
     *
     * This is used for TSValue to track modifications to nested views.
     *
     * @warning The Policy type must match between set_root and root calls.
     *
     * @param root Pointer to the owning Value
     */
    template<typename Policy = NoCache>
    void set_root(Value<Policy>* root) {
        _root = static_cast<void*>(root);
    }

    /**
     * @brief Get the root Value.
     *
     * @warning The Policy type must match the Policy used in set_root.
     *
     * @return Pointer to the owning Value, or nullptr
     */
    template<typename Policy = NoCache>
    [[nodiscard]] Value<Policy>* root() const {
        return static_cast<Value<Policy>*>(_root);
    }

private:
    void* _mutable_data{nullptr};
    void* _root{nullptr};  // Optional, for notification chains
};

// ============================================================================
// Comparison Operators
// ============================================================================

/**
 * @brief Equality comparison for views.
 */
inline bool operator==(const ConstValueView& lhs, const ConstValueView& rhs) {
    return lhs.equals(rhs);
}

/**
 * @brief Inequality comparison for views.
 */
inline bool operator!=(const ConstValueView& lhs, const ConstValueView& rhs) {
    return !lhs.equals(rhs);
}

} // namespace hgraph::value
