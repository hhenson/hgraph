#pragma once

/**
 * @file value.h
 * @brief Main Value class - owning type-erased storage.
 *
 * The Value class provides owning storage for type-erased values with:
 * - Small Buffer Optimization (SBO) for common types
 * - Policy-based extensions (caching, tracking, etc.)
 * - Type-safe access via views
 * - Python interop with optional caching
 *
 * Usage:
 * @code
 * // Create scalar values
 * Value<> v1(42);                    // int64_t
 * Value<> v2(3.14);                  // double
 * Value<WithPythonCache> v3("hello"); // with caching
 *
 * // Access values
 * int64_t x = v1.as<int64_t>();
 * double* p = v2.try_as<double>();
 *
 * // Get views
 * View view = v1.view();
 * view.as<int64_t>() = 100;
 *
 * // Python interop
 * nb::object py = v1.to_python();
 * v1.from_python(py);
 * @endcode
 */

#include <hgraph/types/value/indexed_view.h>
#include <hgraph/types/value/policy.h>
#include <hgraph/types/value/value_storage.h>
#include <hgraph/types/value/path.h>
#include <hgraph/types/value/traversal.h>
#include <hgraph/types/value/visitor.h>
#include <hgraph/types/value/delta_view.h>

#include <type_traits>
#include <utility>

namespace hgraph::value {

// ============================================================================
// Value Class
// ============================================================================

/**
 * @brief Owning type-erased value storage with policy-based extensions.
 *
 * Value is the primary class for storing type-erased values. It manages
 * the lifetime of the stored data and provides type-safe access through
 * views and the as<T>() family of methods.
 *
 * The Policy template parameter controls optional extensions:
 * - NoCache (default): No extensions, zero overhead
 * - WithPythonCache: Caches Python object conversions
 *
 * @tparam Policy The extension policy (default: NoCache)
 */
template<typename Policy>
class Value : private PolicyStorage<Policy> {
public:
    // ========== Type Aliases ==========

    using policy_type = Policy;
    using storage_type = PolicyStorage<Policy>;

    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an empty/invalid Value.
     */
    Value() noexcept = default;

    /**
     * @brief Construct from a type schema.
     *
     * Allocates storage and default-constructs the value.
     *
     * @param schema The type schema
     */
    explicit Value(const TypeMeta* schema)
        : _tagged_schema(reinterpret_cast<uintptr_t>(schema))
        , _storage(ValueStorage::create(schema)) {}

    /**
     * @brief Construct from a scalar value.
     *
     * The type T must be a registered scalar type.
     *
     * @tparam T The value type
     * @param val The value to store
     */
    template<typename T, typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, Value>>>
    explicit Value(const T& val)
        : _tagged_schema(reinterpret_cast<uintptr_t>(scalar_type_meta<T>())) {
        _storage.construct<T>(val, schema());
    }

    /**
     * @brief Construct by copying from a view.
     *
     * Creates an owning copy of the data in the view.
     *
     * @param view The view to copy from
     */
    explicit Value(const View& view)
        : _tagged_schema(reinterpret_cast<uintptr_t>(view.schema())) {
        if (view.valid()) {
            _storage.construct(schema());
            schema()->ops->copy_assign(_storage.data(), view.data(), schema());
        }
    }

    // ========== Destructor ==========

    /**
     * @brief Destructor - destroys the contained value.
     */
    ~Value() = default;

    // ========== Move Semantics ==========

    /**
     * @brief Move constructor.
     */
    Value(Value&& other) noexcept
        : storage_type(std::move(static_cast<storage_type&>(other)))
        , _tagged_schema(other._tagged_schema)
        , _storage(std::move(other._storage)) {
        other._tagged_schema = 0;
    }

    /**
     * @brief Move assignment.
     */
    Value& operator=(Value&& other) noexcept {
        if (this != &other) {
            static_cast<storage_type&>(*this) = std::move(static_cast<storage_type&>(other));
            _tagged_schema = other._tagged_schema;
            _storage = std::move(other._storage);
            other._tagged_schema = 0;
        }
        return *this;
    }

    // ========== Copy (Explicit) ==========

    // Copying is disabled by default - use explicit copy methods
    Value(const Value&) = delete;
    Value& operator=(const Value&) = delete;

    /**
     * @brief Create a copy of a Value.
     *
     * @param other The Value to copy
     * @return A new Value containing a copy of the data
     */
    [[nodiscard]] static Value copy(const Value& other) {
        return Value(other.const_view());
    }

    /**
     * @brief Create a copy from a view.
     *
     * @param view The view to copy from
     * @return A new Value containing a copy of the data
     */
    [[nodiscard]] static Value copy(const View& view) {
        return Value(view);
    }

    // ========== Validity ==========

    /**
     * @brief Check if the Value contains data.
     *
     * A Value is valid if it has a schema, is not null, and storage has value.
     * This is equivalent to has_value().
     *
     * @return true if valid (contains data)
     */
    [[nodiscard]] bool valid() const noexcept {
        return has_value();
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
        return get_schema_ptr();
    }

    // ========== View Access ==========

    /**
     * @brief Get a mutable view of the data.
     *
     * If the policy has caching, this invalidates the cache.
     *
     * @return Mutable view
     */
    [[nodiscard]] View view() {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return View(_storage.data(), schema());
    }

    /**
     * @brief Get a const view of the data.
     * @return Const view
     */
    [[nodiscard]] View view() const {
        return View(_storage.data(), schema());
    }

    /**
     * @brief Get a const view of the data (explicit const version).
     * @return Const view
     */
    [[nodiscard]] View const_view() const {
        return View(_storage.data(), schema());
    }

    // ========== Specialized View Access ==========

    /**
     * @brief Get as a tuple view (mutable).
     */
    [[nodiscard]] TupleView as_tuple() {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return view().as_tuple();
    }

    /**
     * @brief Get as a tuple view (const).
     */
    [[nodiscard]] TupleView as_tuple() const {
        return const_view().as_tuple();
    }

    /**
     * @brief Get as a bundle view (mutable).
     */
    [[nodiscard]] BundleView as_bundle() {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return view().as_bundle();
    }

    /**
     * @brief Get as a bundle view (const).
     */
    [[nodiscard]] BundleView as_bundle() const {
        return const_view().as_bundle();
    }

    /**
     * @brief Get as a list view (mutable).
     */
    [[nodiscard]] ListView as_list() {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return view().as_list();
    }

    /**
     * @brief Get as a list view (const).
     */
    [[nodiscard]] ListView as_list() const {
        return const_view().as_list();
    }

    /**
     * @brief Get as a set view (mutable).
     */
    [[nodiscard]] SetView as_set() {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return view().as_set();
    }

    /**
     * @brief Get as a set view (const).
     */
    [[nodiscard]] const SetView as_set() const {
        return const_view().as_set();
    }

    /**
     * @brief Get as a map view (mutable).
     */
    [[nodiscard]] MapView as_map() {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return view().as_map();
    }

    /**
     * @brief Get as a map view (const).
     */
    [[nodiscard]] const MapView as_map() const {
        return const_view().as_map();
    }

    // ========== Type Access ==========

    /**
     * @brief Get the value as type T (debug assertion, mutable).
     *
     * Zero overhead in release builds.
     *
     * @tparam T The expected type
     * @return Mutable reference to the value
     */
    template<typename T>
    [[nodiscard]] T& as() {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        assert(valid() && "as<T>() on invalid Value");
        assert(is_scalar_type<T>() && "as<T>() type mismatch");
        return *static_cast<T*>(_storage.data());
    }

    /**
     * @brief Get the value as type T (debug assertion, const).
     */
    template<typename T>
    [[nodiscard]] const T& as() const {
        assert(valid() && "as<T>() on invalid Value");
        assert(is_scalar_type<T>() && "as<T>() type mismatch");
        return *static_cast<const T*>(_storage.data());
    }

    /**
     * @brief Try to get the value as type T (mutable).
     *
     * @return Pointer to the value, or nullptr if type doesn't match
     */
    template<typename T>
    [[nodiscard]] T* try_as() {
        if (!is_scalar_type<T>()) return nullptr;
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return static_cast<T*>(_storage.data());
    }

    /**
     * @brief Try to get the value as type T (const).
     */
    template<typename T>
    [[nodiscard]] const T* try_as() const {
        if (!is_scalar_type<T>()) return nullptr;
        return static_cast<const T*>(_storage.data());
    }

    /**
     * @brief Get the value as type T (throwing, mutable).
     *
     * @throws std::runtime_error if invalid or type mismatch
     */
    template<typename T>
    [[nodiscard]] T& checked_as() {
        if (!valid()) {
            throw std::runtime_error("checked_as<T>() on invalid Value");
        }
        if (!is_scalar_type<T>()) {
            throw std::runtime_error("checked_as<T>() type mismatch");
        }
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return *static_cast<T*>(_storage.data());
    }

    /**
     * @brief Get the value as type T (throwing, const).
     */
    template<typename T>
    [[nodiscard]] const T& checked_as() const {
        if (!valid()) {
            throw std::runtime_error("checked_as<T>() on invalid Value");
        }
        if (!is_scalar_type<T>()) {
            throw std::runtime_error("checked_as<T>() type mismatch");
        }
        return *static_cast<const T*>(_storage.data());
    }

    // ========== Type Checking ==========

    /**
     * @brief Check if this is a specific scalar type.
     */
    template<typename T>
    [[nodiscard]] bool is_scalar_type() const {
        return valid() && schema() == scalar_type_meta<T>();
    }

    // ========== Raw Access ==========

    /**
     * @brief Get the raw data pointer (mutable).
     */
    [[nodiscard]] void* data() {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return _storage.data();
    }

    /**
     * @brief Get the raw data pointer (const).
     */
    [[nodiscard]] const void* data() const {
        return _storage.data();
    }

    // ========== Operations ==========

    /**
     * @brief Check equality with another Value.
     */
    [[nodiscard]] bool equals(const Value& other) const {
        return const_view().equals(other.const_view());
    }

    /**
     * @brief Check equality with a view.
     */
    [[nodiscard]] bool equals(const View& other) const {
        return const_view().equals(other);
    }

    /**
     * @brief Compute the hash.
     */
    [[nodiscard]] size_t hash() const {
        return const_view().hash();
    }

    /**
     * @brief Convert to string.
     */
    [[nodiscard]] std::string to_string() const {
        return const_view().to_string();
    }

    // ========== Python Interop ==========

    /**
     * @brief Convert to a Python object.
     *
     * If the policy has caching, the result is cached and reused.
     *
     * @return The Python object representation
     */
    [[nodiscard]] nb::object to_python() const {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            if (this->has_cache()) {
                return this->get_cache();
            }
            auto result = schema()->ops->to_python(_storage.data(), schema());
            this->set_cache(result);
            return result;
        } else {
            return schema()->ops->to_python(_storage.data(), schema());
        }
    }

    /**
     * @brief Set the value from a Python object.
     *
     * If the policy has caching, this updates the cache.
     * If the policy has validation, this rejects None values.
     * If the policy has modification tracking, this notifies callbacks.
     *
     * @param src The Python object
     * @throws std::runtime_error if validation is enabled and src is None
     */
    void from_python(const nb::object& src) {
        // Validation check
        if constexpr (policy_traits<Policy>::has_validation) {
            if (src.is_none()) {
                throw std::runtime_error("Cannot set value to None");
            }
        }

        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }

        // Perform type conversion from Python object to native storage
        try {
            schema()->ops->from_python(_storage.data(), src, schema());
        } catch (const nb::python_error& e) {
            // Re-throw Python exceptions with preserved traceback
            throw;
        } catch (const std::exception& e) {
            // Wrap C++ exceptions with context about the conversion
            throw std::runtime_error(
                std::string("Value::from_python: type conversion failed: ") + e.what());
        }

        // Update cache if policy supports it
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->set_cache(src);
        }

        // Notify modification callbacks
        if constexpr (policy_traits<Policy>::has_modification_tracking) {
            this->notify_modified();
        }
    }

    // ========== Modification Tracking ==========

    /**
     * @brief Register a callback to be called when the value is modified.
     *
     * Only available when the policy has modification tracking.
     *
     * @param cb The callback function
     */
    template<typename Callback>
    void on_modified(Callback&& cb) {
        if constexpr (policy_traits<Policy>::has_modification_tracking) {
            storage_type::on_modified(std::forward<Callback>(cb));
        }
    }

    // ========== Delta Operations ==========

    /**
     * @brief Apply a set delta to this value.
     *
     * Adds elements from delta.added() and removes elements from delta.removed().
     * Only valid for Set values.
     *
     * @param delta The set delta to apply
     */
    void apply_delta(const SetDeltaView& delta) {
        if (!valid() || schema()->kind != TypeKind::Set) {
            throw std::runtime_error("apply_delta: Value must be a valid Set");
        }

        auto set_view = as_set();

        // Insert elements
        for (auto elem : delta.added()) {
            set_view.insert(elem);
        }

        // Remove elements
        for (auto elem : delta.removed()) {
            set_view.remove(elem);
        }
    }

    /**
     * @brief Apply a map delta to this value.
     *
     * Adds entries from delta.added_items(), updates from delta.updated_items(),
     * and removes keys from delta.removed_keys().
     * Only valid for Map values.
     *
     * @param delta The map delta to apply
     */
    void apply_delta(const MapDeltaView& delta) {
        if (!valid() || schema()->kind != TypeKind::Map) {
            throw std::runtime_error("apply_delta: Value must be a valid Map");
        }

        auto map_view = as_map();

        // Add new entries
        for (auto [key, value] : delta.added_items()) {
            map_view.add(key, value);
        }

        // Update existing entries
        for (auto [key, value] : delta.updated_items()) {
            map_view.set_item(key, value);
        }

        // Remove entries
        for (auto key : delta.removed_keys()) {
            map_view.remove(key);
        }
    }

    /**
     * @brief Apply a list delta to this value.
     *
     * Updates elements at indices specified in delta.updated_items().
     * Only valid for List values.
     *
     * @param delta The list delta to apply
     */
    void apply_delta(const ListDeltaView& delta) {
        if (!valid() || schema()->kind != TypeKind::List) {
            throw std::runtime_error("apply_delta: Value must be a valid List");
        }

        auto list_view = as_list();

        // Update elements at specific indices
        for (auto [idx_view, value] : delta.updated_items()) {
            size_t idx = idx_view.as<size_t>();
            list_view.set(idx, value);
        }
    }

    // ========== Null Value Semantics ==========

    /**
     * @brief Check if the value is in null state.
     *
     * Null state means the Value has a schema but no constructed value.
     * This is distinct from invalid (no schema) and valid (has value).
     *
     * @return true if in null state
     */
    [[nodiscard]] bool is_null() const noexcept {
        return (_tagged_schema & 1) != 0;
    }

    /**
     * @brief Check if the value is present (not null, not invalid).
     *
     * This is the primary check for whether the Value can be used.
     *
     * @return true if has a valid value
     */
    [[nodiscard]] bool has_value() const noexcept {
        return schema() != nullptr && !is_null() && _storage.has_value();
    }

    /**
     * @brief Reset to null state (keeps schema).
     *
     * After reset, has_value() returns false but schema() is still valid.
     * The storage is destroyed but can be reconstructed with emplace().
     */
    void reset() {
        if (schema() && !is_null()) {
            _storage.reset();
            set_null_flag(true);
        }
    }

    /**
     * @brief Construct value in place (from null to valid).
     *
     * Transitions from null state to valid state by constructing the value.
     * Has no effect if not in null state.
     */
    void emplace() {
        if (schema() && is_null()) {
            _storage.construct(schema());
            set_null_flag(false);
        }
    }

    /**
     * @brief Create a null Value with the given schema.
     *
     * The returned Value has a schema but no constructed value.
     * Call emplace() to construct the value.
     *
     * @param schema The type schema
     * @return A null Value with the given schema
     */
    [[nodiscard]] static Value make_null(const TypeMeta* schema) {
        Value v;
        v.set_schema(schema);
        v.set_null_flag(true);
        return v;
    }

private:
    // Pointer tagging: low bit of _tagged_schema indicates null state
    // Bit 0 = 1 means null (has schema but no value)
    // Bit 0 = 0 means not null (invalid or valid)
    uintptr_t _tagged_schema{0};
    ValueStorage _storage;

    /**
     * @brief Get the schema pointer (masking out the tag bit).
     */
    [[nodiscard]] const TypeMeta* get_schema_ptr() const noexcept {
        return reinterpret_cast<const TypeMeta*>(_tagged_schema & ~uintptr_t(1));
    }

    /**
     * @brief Set the schema pointer (preserving the tag bit).
     */
    void set_schema(const TypeMeta* schema) noexcept {
        bool was_null = is_null();
        _tagged_schema = reinterpret_cast<uintptr_t>(schema);
        if (was_null) set_null_flag(true);
    }

    /**
     * @brief Set the null flag.
     */
    void set_null_flag(bool null) noexcept {
        if (null) {
            _tagged_schema |= 1;
        } else {
            _tagged_schema &= ~uintptr_t(1);
        }
    }
};

// ============================================================================
// Type Aliases
// ============================================================================

/// Value with no extensions (default)
using PlainValue = Value<NoCache>;

/// Value with Python object caching
using CachedValue = Value<WithPythonCache>;

/// Value with caching and modification tracking (for time-series)
using TSValue = Value<CombinedPolicy<WithPythonCache, WithModificationTracking>>;

/// Value with validation (rejects None)
using ValidatedValue = Value<WithValidation>;

// ============================================================================
// View::clone Implementation
// ============================================================================

template<typename Policy>
Value<Policy> View::clone() const {
    return Value<Policy>(*this);
}

// ============================================================================
// Templated Method Implementations
// ============================================================================

// IndexedView::set<T>
template<typename T>
void IndexedView::set(size_t index, const T& value) {
    Value<> temp(value);
    set(index, temp.const_view());
}

// BundleView::set<T>
template<typename T>
void BundleView::set(std::string_view name, const T& value) {
    Value<> temp(value);
    set(name, temp.const_view());
}

// ListView::append<T>
template<typename T>
void ListView::append(const T& value) {
    Value<> temp(value);
    append(temp.const_view());
}

// ListView::reset<T>
template<typename T>
void ListView::reset(const T& sentinel) {
    Value<> temp(sentinel);
    reset(temp.const_view());
}

// SetView::contains<T>
template<typename T>
bool SetView::contains(const T& value) const {
    Value<> temp(value);
    return contains(temp.const_view());
}

// SetView::insert<T>
template<typename T>
bool SetView::insert(const T& value) {
    Value<> temp(value);
    return insert(temp.const_view());
}

// SetView::remove<T>
template<typename T>
bool SetView::remove(const T& value) {
    Value<> temp(value);
    return remove(temp.const_view());
}

// MapView::at<K> (const)
template<typename K>
View MapView::at(const K& key) const {
    Value<> temp(key);
    return at(temp.const_view());
}

// MapView::at<K> (mutable)
template<typename K>
View MapView::at(const K& key) {
    Value<> temp(key);
    return at(temp.const_view());
}

// MapView::contains<K>
template<typename K>
bool MapView::contains(const K& key) const {
    Value<> temp(key);
    return contains(temp.const_view());
}

// MapView::set_item<K, V>
template<typename K, typename V>
void MapView::set_item(const K& key, const V& value) {
    Value<> temp_key(key);
    Value<> temp_val(value);
    set_item(temp_key.const_view(), temp_val.const_view());
}

// MapView::add<K, V>
template<typename K, typename V>
bool MapView::add(const K& key, const V& value) {
    Value<> temp_key(key);
    Value<> temp_val(value);
    return add(temp_key.const_view(), temp_val.const_view());
}

// MapView::remove<K>
template<typename K>
bool MapView::remove(const K& key) {
    Value<> temp(key);
    return remove(temp.const_view());
}

// ============================================================================
// Comparison Operators
// ============================================================================

template<typename P1, typename P2>
bool operator==(const Value<P1>& lhs, const Value<P2>& rhs) {
    return lhs.equals(rhs.const_view());
}

template<typename P1, typename P2>
bool operator!=(const Value<P1>& lhs, const Value<P2>& rhs) {
    return !lhs.equals(rhs.const_view());
}

template<typename P>
bool operator==(const Value<P>& lhs, const View& rhs) {
    return lhs.equals(rhs);
}

template<typename P>
bool operator==(const View& lhs, const Value<P>& rhs) {
    return rhs.equals(lhs);
}

template<typename P>
bool operator!=(const Value<P>& lhs, const View& rhs) {
    return !lhs.equals(rhs);
}

template<typename P>
bool operator!=(const View& lhs, const Value<P>& rhs) {
    return !rhs.equals(lhs);
}

// ============================================================================
// Hash Support
// ============================================================================

} // namespace hgraph::value

namespace std {

/// Hash specialization for Value
template<typename Policy>
struct hash<hgraph::value::Value<Policy>> {
    size_t operator()(const hgraph::value::Value<Policy>& v) const {
        return v.hash();
    }
};

/// Hash specialization for View
template<>
struct hash<hgraph::value::View> {
    size_t operator()(const hgraph::value::View& v) const {
        return v.hash();
    }
};

} // namespace std
