#pragma once

/**
 * @file value_storage.h
 * @brief Type-erased value storage with Small Buffer Optimization (SBO).
 *
 * ValueStorage provides efficient storage for type-erased values. Small values
 * (up to 24 bytes with alignment <= 8) are stored inline to avoid heap allocation.
 * Larger values are allocated on the heap.
 *
 * This implementation is inspired by EnTT's basic_any but simplified for our needs.
 * The key feature is the data() method that returns a raw pointer, enabling the
 * as<T>() access pattern.
 */

#include <hgraph/types/value/type_meta.h>

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <new>
#include <type_traits>
#include <utility>

// Debug flag for ValueStorage
#ifndef VALUE_STORAGE_DEBUG
#define VALUE_STORAGE_DEBUG 0
#endif

namespace hgraph::value {

// ============================================================================
// SBO Configuration
// ============================================================================

/// Size of the inline buffer for small buffer optimization
inline constexpr size_t SBO_BUFFER_SIZE = 24;

/// Alignment of the inline buffer
inline constexpr size_t SBO_ALIGNMENT = 8;

// ============================================================================
// ValueStorage
// ============================================================================

/**
 * @brief Type-erased storage with small buffer optimization.
 *
 * ValueStorage can hold any type, storing small types inline and larger types
 * on the heap. It provides raw pointer access via data() for type-safe casting.
 *
 * Invariants:
 * - If _schema is nullptr, the storage is empty
 * - If _schema is non-null, data() returns a valid pointer to a constructed object
 * - The object is always properly aligned
 *
 * @note This class does NOT provide automatic copy/move semantics.
 *       Use the explicit construct/destruct/copy_from methods.
 */
class ValueStorage {
public:
    // ========== Construction/Destruction ==========

    /// Default constructor - creates empty storage
    ValueStorage() noexcept = default;

    /// Destructor - destroys contained value if any
    ~ValueStorage() {
        reset();
    }

    // Non-copyable by default (use explicit copy methods)
    ValueStorage(const ValueStorage&) = delete;
    ValueStorage& operator=(const ValueStorage&) = delete;

    /// Move constructor
    ValueStorage(ValueStorage&& other) noexcept
        : _schema(other._schema)
        , _is_inline(other._is_inline) {
        if (_schema) {
            if (_is_inline) {
                // For trivially copyable types, memcpy is safe and efficient
                // For non-trivial types, use move_construct for proper placement new with move semantics
                if (_schema->is_trivially_copyable()) {
                    std::memcpy(&_storage.inline_buffer, &other._storage.inline_buffer, _schema->size);
                } else {
                    // Move-construct directly into uninitialized storage
                    _schema->ops->move_construct(data(), other.data(), _schema);
                }
            } else {
                // Transfer heap pointer
                _storage.heap_ptr = other._storage.heap_ptr;
            }
            // Clear other
            other._schema = nullptr;
            other._is_inline = true;
        }
    }

    /// Move assignment
    ValueStorage& operator=(ValueStorage&& other) noexcept {
        if (this != &other) {
            reset();
            _schema = other._schema;
            _is_inline = other._is_inline;
            if (_schema) {
                if (_is_inline) {
                    // For trivially copyable types, memcpy is safe and efficient
                    // For non-trivial types, use move_construct for proper placement new with move semantics
                    if (_schema->is_trivially_copyable()) {
                        std::memcpy(&_storage.inline_buffer, &other._storage.inline_buffer, _schema->size);
                    } else {
                        // Move-construct directly into uninitialized storage (reset() already cleared it)
                        _schema->ops->move_construct(data(), other.data(), _schema);
                    }
                } else {
                    _storage.heap_ptr = other._storage.heap_ptr;
                }
                other._schema = nullptr;
                other._is_inline = true;
            }
        }
        return *this;
    }

    // ========== Factory Methods ==========

    /**
     * @brief Create storage for a type described by schema.
     *
     * Allocates appropriate storage and default-constructs the value.
     *
     * @param schema The type schema
     * @return Initialized ValueStorage
     */
    static ValueStorage create(const TypeMeta* schema) {
        ValueStorage storage;
        storage.construct(schema);
        return storage;
    }

    /**
     * @brief Create storage initialized with a value.
     *
     * @tparam T The value type (must match a registered scalar type)
     * @param value The value to store
     * @param schema The type schema (must match T)
     * @return Initialized ValueStorage
     */
    template<typename T>
    static ValueStorage create(const T& value, const TypeMeta* schema) {
        ValueStorage storage;
        storage.construct<T>(value, schema);
        return storage;
    }

    // ========== State Management ==========

    /**
     * @brief Construct a value using the schema's default constructor.
     *
     * Storage must be empty before calling this.
     *
     * @param schema The type schema
     */
    void construct(const TypeMeta* schema) {
        assert(!_schema && "Storage must be empty before construct");
        assert(schema && schema->ops && "Schema and ops must be valid");

        _schema = schema;
        _is_inline = fits_inline(schema->size, schema->alignment);

        if (!_is_inline) {
            _storage.heap_ptr = ::operator new(schema->size, std::align_val_t{schema->alignment});
        }

        schema->ops->construct(data(), schema);
    }

    /**
     * @brief Construct a value by copying from a typed value.
     *
     * Storage must be empty before calling this.
     *
     * @tparam T The value type
     * @param value The value to copy
     * @param schema The type schema (must match T)
     */
    template<typename T>
    void construct(const T& value, const TypeMeta* schema) {
        assert(!_schema && "Storage must be empty before construct");
        assert(schema && "Schema must be valid");

        _schema = schema;
        _is_inline = fits_inline(sizeof(T), alignof(T));

        if (!_is_inline) {
            _storage.heap_ptr = ::operator new(sizeof(T), std::align_val_t{alignof(T)});
        }

        new (data()) T(value);
    }

    /**
     * @brief Destroy the contained value and reset to empty state.
     */
    void reset() {
        if (_schema) {
            _schema->ops->destruct(data(), _schema);

            if (!_is_inline) {
                ::operator delete(_storage.heap_ptr, std::align_val_t{_schema->alignment});
            }

            _schema = nullptr;
            _is_inline = true;
        }
    }

    /**
     * @brief Copy the value from another storage.
     *
     * Both storages must have the same schema.
     *
     * @param other The source storage
     */
    void copy_from(const ValueStorage& other) {
        if (!other._schema) {
            reset();
            return;
        }

        if (_schema == other._schema) {
            // Same schema - just copy the data
            _schema->ops->copy_assign(data(), other.data(), _schema);
        } else {
            // Different schema - reset and reconstruct
            reset();
            construct(other._schema);
            _schema->ops->copy_assign(data(), other.data(), _schema);
        }
    }

    // ========== Accessors ==========

    /**
     * @brief Check if the storage contains a value.
     * @return true if a value is stored
     */
    [[nodiscard]] bool has_value() const noexcept {
        return _schema != nullptr;
    }

    /**
     * @brief Check if the storage is empty.
     * @return true if no value is stored
     */
    [[nodiscard]] bool empty() const noexcept {
        return _schema == nullptr;
    }

    /**
     * @brief Get the type schema.
     * @return The schema, or nullptr if empty
     */
    [[nodiscard]] const TypeMeta* schema() const noexcept {
        return _schema;
    }

    /**
     * @brief Check if the value is stored inline.
     * @return true if using SBO, false if heap-allocated
     */
    [[nodiscard]] bool is_inline() const noexcept {
        return _is_inline;
    }

    /**
     * @brief Get a pointer to the stored data.
     *
     * @return Pointer to the value, or nullptr if empty
     */
    [[nodiscard]] void* data() noexcept {
        if (!_schema) return nullptr;
        return _is_inline ? static_cast<void*>(&_storage.inline_buffer) : _storage.heap_ptr;
    }

    /**
     * @brief Get a const pointer to the stored data.
     *
     * @return Const pointer to the value, or nullptr if empty
     */
    [[nodiscard]] const void* data() const noexcept {
        if (!_schema) return nullptr;
        return _is_inline ? static_cast<const void*>(&_storage.inline_buffer) : _storage.heap_ptr;
    }

    // ========== SBO Helpers ==========

    /**
     * @brief Check if a type fits in the inline buffer.
     *
     * @param size The type size in bytes
     * @param alignment The type alignment
     * @return true if the type can be stored inline
     */
    [[nodiscard]] static constexpr bool fits_inline(size_t size, size_t alignment) noexcept {
        return size <= SBO_BUFFER_SIZE && alignment <= SBO_ALIGNMENT;
    }

    /**
     * @brief Check if a type T fits in the inline buffer.
     *
     * @tparam T The type to check
     * @return true if T can be stored inline
     */
    template<typename T>
    [[nodiscard]] static constexpr bool fits_inline() noexcept {
        return fits_inline(sizeof(T), alignof(T));
    }

private:
    /// Union for SBO storage
    union Storage {
        alignas(SBO_ALIGNMENT) unsigned char inline_buffer[SBO_BUFFER_SIZE];
        void* heap_ptr;

        Storage() noexcept : heap_ptr(nullptr) {}
    };

    Storage _storage;
    const TypeMeta* _schema{nullptr};
    bool _is_inline{true};
};

// ============================================================================
// Static Assertions
// ============================================================================

// Verify common types fit in SBO
static_assert(ValueStorage::fits_inline<bool>(), "bool should fit inline");
static_assert(ValueStorage::fits_inline<int64_t>(), "int64_t should fit inline");
static_assert(ValueStorage::fits_inline<double>(), "double should fit inline");

} // namespace hgraph::value
