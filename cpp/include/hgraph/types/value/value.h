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
 * ValueView view = v1.view();
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

#include <optional>
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
     * Creates a typed-null Value (schema is preserved, no payload yet).
     *
     * @param schema The type schema
     */
    explicit Value(const TypeMeta* schema)
        : _schema(schema) {}

    /**
     * @brief Construct from a scalar value.
     *
     * The type T must be a registered scalar type.
     *
     * @tparam T The value type
     * @param val The value to store
     */
    template<typename T, typename = std::enable_if_t<
        !std::is_same_v<std::decay_t<T>, Value> &&
        !std::is_base_of_v<ConstValueView, std::decay_t<T>>>>
    explicit Value(const T& val)
        : _schema(scalar_type_meta<T>()) {
        _storage.construct<T>(val, _schema);
    }

    /**
     * @brief Construct by copying from a view.
     *
     * Creates an owning copy of the data in the view.
     *
     * @param view The view to copy from
     */
    explicit Value(const ConstValueView& view)
        : _schema(view.schema()) {
        if (view.valid()) {
            _storage.construct(_schema);
            _schema->ops().copy(_storage.data(), view.data(), _schema);
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
        , _storage(std::move(other._storage))
        , _schema(other._schema) {
        other._schema = nullptr;
    }

    /**
     * @brief Move assignment.
     */
    Value& operator=(Value&& other) noexcept {
        if (this != &other) {
            static_cast<storage_type&>(*this) = std::move(static_cast<storage_type&>(other));
            _storage = std::move(other._storage);
            _schema = other._schema;
            other._schema = nullptr;
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
        Value result;
        result._schema = other._schema;
        if (other.has_value()) {
            result._storage.construct(other._schema);
            other._schema->ops().copy(result._storage.data(), other._storage.data(), other._schema);
        }
        return result;
    }

    /**
     * @brief Create a copy from a view.
     *
     * @param view The view to copy from
     * @return A new Value containing a copy of the data
     */
    [[nodiscard]] static Value copy(const ConstValueView& view) {
        return Value(view);
    }

    // ========== Validity ==========

    /**
     * @brief Check whether this Value currently contains data.
     *
     * A Value may keep its schema while being null (no data).
     *
     * @return true if data is present
     */
    [[nodiscard]] bool has_value() const noexcept {
        return _storage.has_value();
    }

    /**
     * @brief Check if the Value contains data.
     * @return true if valid (contains data)
     */
    [[nodiscard]] bool valid() const noexcept {
        return has_value();
    }

    /**
     * @brief Boolean conversion - returns validity.
     */
    explicit operator bool() const noexcept {
        return has_value();
    }

    /**
     * @brief Get the type schema.
     * @return The schema, or nullptr if invalid
     */
    [[nodiscard]] const TypeMeta* schema() const noexcept {
        return _schema;
    }

    // ========== View Access ==========

    /**
     * @brief Get a mutable view of the data.
     *
     * If the policy has caching, this invalidates the cache.
     *
     * @return Mutable view
     */
    [[nodiscard]] ValueView view() {
        if (!has_value()) {
            throw std::bad_optional_access();
        }
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return ValueView(_storage.data(), _schema);
    }

    /**
     * @brief Get a const view of the data.
     * @return Const view
     */
    [[nodiscard]] ConstValueView view() const {
        if (!has_value()) {
            throw std::bad_optional_access();
        }
        return ConstValueView(_storage.data(), _schema);
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
    [[nodiscard]] ConstTupleView as_tuple() const {
        return view().as_tuple();
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
    [[nodiscard]] ConstBundleView as_bundle() const {
        return view().as_bundle();
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
    [[nodiscard]] ConstListView as_list() const {
        return view().as_list();
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
    [[nodiscard]] ConstSetView as_set() const {
        return view().as_set();
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
    [[nodiscard]] ConstMapView as_map() const {
        return view().as_map();
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
        return valid() && _schema == scalar_type_meta<T>();
    }

    // ========== Raw Access ==========

    /**
     * @brief Get the raw data pointer (mutable).
     */
    [[nodiscard]] void* data() {
        if (!has_value()) {
            throw std::bad_optional_access();
        }
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return _storage.data();
    }

    /**
     * @brief Get the raw data pointer (const).
     */
    [[nodiscard]] const void* data() const {
        if (!has_value()) {
            throw std::bad_optional_access();
        }
        return _storage.data();
    }

    // ========== Operations ==========

    /**
     * @brief Check equality with another Value.
     */
    [[nodiscard]] bool equals(const Value& other) const {
        if (_schema != other._schema) return false;
        const bool lhs_has = has_value();
        const bool rhs_has = other.has_value();
        if (!lhs_has || !rhs_has) {
            return lhs_has == rhs_has;
        }
        return _schema->ops().equals(_storage.data(), other._storage.data(), _schema);
    }

    /**
     * @brief Check equality with a view.
     */
    [[nodiscard]] bool equals(const ConstValueView& other) const {
        if (!has_value() || !other.valid()) return false;
        if (_schema != other.schema()) return false;
        return _schema->ops().equals(_storage.data(), other.data(), _schema);
    }

    /**
     * @brief Compute the hash.
     */
    [[nodiscard]] size_t hash() const {
        return view().hash();
    }

    /**
     * @brief Convert to string.
     */
    [[nodiscard]] std::string to_string() const {
        return view().to_string();
    }

    // ========== Nullability ==========

    /**
     * @brief Reset to typed-null while preserving schema.
     */
    void reset() {
        const bool had_value = has_value();
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        _storage.reset();
        if constexpr (policy_traits<Policy>::has_modification_tracking) {
            if (had_value) {
                this->notify_modified();
            }
        }
    }

    /**
     * @brief Construct a default value for the current schema.
     *
     * If a value already exists, it is replaced.
     */
    void emplace() {
        if (!_schema) {
            throw std::runtime_error("emplace() on Value without schema");
        }
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        _storage.reset();
        _storage.construct(_schema);
        if constexpr (policy_traits<Policy>::has_modification_tracking) {
            this->notify_modified();
        }
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
        if (!has_value()) {
            return nb::none();
        }
        if constexpr (policy_traits<Policy>::has_python_cache) {
            if (this->has_cache()) {
                return this->get_cache();
            }
            auto result = _schema->ops().to_python(_storage.data(), _schema);
            this->set_cache(result);
            return result;
        } else {
            return _schema->ops().to_python(_storage.data(), _schema);
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

        // Non-validating policies treat Python None as typed-null.
        if (src.is_none()) {
            reset();
            return;
        }

        if (!_schema) {
            throw std::runtime_error("from_python() on Value without schema");
        }

        if (!has_value()) {
            _storage.construct(_schema);
        }

        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }

        // Perform type conversion from Python object to native storage
        try {
            _schema->ops().from_python(_storage.data(), src, _schema);
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

private:
    ValueStorage _storage;
    const TypeMeta* _schema{nullptr};
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
// ConstValueView::clone Implementation
// ============================================================================

template<typename Policy>
Value<Policy> ConstValueView::clone() const {
    return Value<Policy>(*this);
}

// ============================================================================
// Templated Method Implementations
// ============================================================================

// IndexedView::set<T>
template<typename T>
void IndexedView::set(size_t index, const T& value) {
    Value<> temp(value);
    set(index, ConstValueView(temp.view()));
}

// BundleView::set<T>
template<typename T>
void BundleView::set(std::string_view name, const T& value) {
    Value<> temp(value);
    set(name, ConstValueView(temp.view()));
}

// ListView::push_back<T>
template<typename T>
void ListView::push_back(const T& value) {
    Value<> temp(value);
    push_back(ConstValueView(temp.view()));
}

// ListView::reset<T>
template<typename T>
void ListView::reset(const T& sentinel) {
    Value<> temp(sentinel);
    reset(ConstValueView(temp.view()));
}

// ConstSetView::contains<T>
template<typename T>
bool ConstSetView::contains(const T& value) const {
    Value<> temp(value);
    return contains(ConstValueView(temp.view()));
}

// SetView::contains<T>
template<typename T>
bool SetView::contains(const T& value) const {
    Value<> temp(value);
    return contains(ConstValueView(temp.view()));
}

// SetView::add<T>
template<typename T>
bool SetView::add(const T& value) {
    Value<> temp(value);
    return add(ConstValueView(temp.view()));
}

// SetView::remove<T>
template<typename T>
bool SetView::remove(const T& value) {
    Value<> temp(value);
    return remove(ConstValueView(temp.view()));
}

// ConstMapView::at<K>
template<typename K>
ConstValueView ConstMapView::at(const K& key) const {
    Value<> temp(key);
    return at(ConstValueView(temp.view()));
}

// ConstMapView::contains<K>
template<typename K>
bool ConstMapView::contains(const K& key) const {
    Value<> temp(key);
    return contains(ConstValueView(temp.view()));
}

// MapView::at<K> (const)
template<typename K>
ConstValueView MapView::at(const K& key) const {
    Value<> temp(key);
    return at(ConstValueView(temp.view()));
}

// MapView::at<K> (mutable)
template<typename K>
ValueView MapView::at(const K& key) {
    Value<> temp(key);
    return at(ConstValueView(temp.view()));
}

// MapView::contains<K>
template<typename K>
bool MapView::contains(const K& key) const {
    Value<> temp(key);
    return contains(ConstValueView(temp.view()));
}

// MapView::set<K, V>
template<typename K, typename V>
void MapView::set(const K& key, const V& value) {
    Value<> temp_key(key);
    Value<> temp_val(value);
    set(ConstValueView(temp_key.view()), ConstValueView(temp_val.view()));
}

// MapView::add<K, V>
template<typename K, typename V>
bool MapView::add(const K& key, const V& value) {
    Value<> temp_key(key);
    Value<> temp_val(value);
    return add(ConstValueView(temp_key.view()), ConstValueView(temp_val.view()));
}

// MapView::remove<K>
template<typename K>
bool MapView::remove(const K& key) {
    Value<> temp(key);
    return remove(ConstValueView(temp.view()));
}

// ============================================================================
// Comparison Operators
// ============================================================================

template<typename P1, typename P2>
bool operator==(const Value<P1>& lhs, const Value<P2>& rhs) {
    return lhs.equals(rhs);
}

template<typename P1, typename P2>
bool operator!=(const Value<P1>& lhs, const Value<P2>& rhs) {
    return !lhs.equals(rhs);
}

template<typename P>
bool operator==(const Value<P>& lhs, const ConstValueView& rhs) {
    return lhs.equals(rhs);
}

template<typename P>
bool operator==(const ConstValueView& lhs, const Value<P>& rhs) {
    return rhs.equals(lhs);
}

template<typename P>
bool operator!=(const Value<P>& lhs, const ConstValueView& rhs) {
    return !lhs.equals(rhs);
}

template<typename P>
bool operator!=(const ConstValueView& lhs, const Value<P>& rhs) {
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

/// Hash specialization for ConstValueView
template<>
struct hash<hgraph::value::ConstValueView> {
    size_t operator()(const hgraph::value::ConstValueView& v) const {
        return v.hash();
    }
};

} // namespace std
