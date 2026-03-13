#pragma once

/**
 * @file type_meta.h
 * @brief Schema metadata for the value type system.
 *
 * `TypeMeta` describes what a value is, not how it is manipulated at runtime.
 * Runtime behavior now lives in the new value builder and view dispatch
 * infrastructure under `time_series/value`. The base schema layer therefore
 * carries only stable structural facts such as kind, size, alignment, fields,
 * and element types.
 */

#include <hgraph/types/value/value_fwd.h>
#include <hgraph/util/date_time.h>

#include <nanobind/nanobind.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>

namespace nb = nanobind;

namespace hgraph::value {

// ============================================================================
// Type Kind Enumeration
// ============================================================================

/**
 * @brief Categories of types in the Value system.
 *
 * Each type falls into one of these categories, which determines
 * what operations are available and how the data is laid out.
 */
enum class TypeKind : uint8_t {
    Atomic,        ///< Atomic values: int, double, bool, string, datetime, etc.
    Tuple,         ///< Indexed heterogeneous collection (unnamed, positional access only)
    Bundle,        ///< Named field collection (struct-like, index + name access)
    List,          ///< Indexed homogeneous collection (dynamic size)
    Set,           ///< Unordered unique elements
    Map,           ///< Key-value pairs
    CyclicBuffer,  ///< Fixed-size circular buffer (re-centers on read)
    Queue          ///< FIFO queue with optional max capacity
};

// ============================================================================
// Type Flags
// ============================================================================

/**
 * @brief Capability flags for types.
 *
 * These flags indicate what operations are supported and what
 * optimizations can be applied to a type.
 */
enum class TypeFlags : uint32_t {
    None                   = 0,
    TriviallyConstructible = 1 << 0,
    TriviallyDestructible  = 1 << 1,
    TriviallyCopyable      = 1 << 2,
    Hashable               = 1 << 3,
    Comparable             = 1 << 4,
    Equatable              = 1 << 5,
    BufferCompatible       = 1 << 6,  ///< numpy/Arrow compatible
    VariadicTuple          = 1 << 7,  ///< List represents tuple[T, ...], to_python returns tuple
};

/// Bitwise OR for TypeFlags
constexpr TypeFlags operator|(TypeFlags lhs, TypeFlags rhs) noexcept {
    return static_cast<TypeFlags>(
        static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs)
    );
}

/// Bitwise AND for TypeFlags
constexpr TypeFlags operator&(TypeFlags lhs, TypeFlags rhs) noexcept {
    return static_cast<TypeFlags>(
        static_cast<uint32_t>(lhs) & static_cast<uint32_t>(rhs)
    );
}

/// Bitwise NOT for TypeFlags
constexpr TypeFlags operator~(TypeFlags flags) noexcept {
    return static_cast<TypeFlags>(~static_cast<uint32_t>(flags));
}

/// Check if a flag is set
constexpr bool has_flag(TypeFlags flags, TypeFlags flag) noexcept {
    return (static_cast<uint32_t>(flags) & static_cast<uint32_t>(flag)) != 0;
}

// ============================================================================
// Bundle Field Information
// ============================================================================

/**
 * @brief Metadata for a single field in a Bundle type.
 *
 * Each field has a name, index (position), byte offset, and type.
 * Fields can be accessed by either name or index.
 */
struct BundleFieldInfo {
    const char* name;        ///< Field name for name-based access
    size_t index;            ///< Field position (0-based) for index-based access
    size_t offset;           ///< Byte offset from bundle start
    const TypeMeta* type;    ///< Field type schema
};

// ============================================================================
// Type Metadata
// ============================================================================

/**
 * @brief Complete metadata describing a type.
 *
 * TypeMeta is the schema for a type. It contains size/alignment information,
 * the type kind, capability flags, and composite element/field information.
 * Runtime behavior is resolved separately through the value builder layer.
 */
struct TypeMeta {
    size_t size;              ///< Size in bytes
    size_t alignment;         ///< Alignment requirement
    TypeKind kind;            ///< Type category
    TypeFlags flags;          ///< Capability flags

    // ========== Human-Readable Name ==========

    const char* name{nullptr};         ///< Human-readable type name (owned by TypeRegistry string pool)

    // ========== Composite Type Information ==========

    const TypeMeta* element_type;      ///< List/Set element type, Map value type
    const TypeMeta* key_type;          ///< Map key type (nullptr for non-maps)
    const BundleFieldInfo* fields;     ///< Bundle/Tuple field metadata
    size_t field_count;                ///< Number of fields (Bundle/Tuple)

    // ========== Fixed-Size Collection Information ==========

    size_t fixed_size;                 ///< 0 = dynamic, >0 = fixed capacity

    // ========== Static Lookup Methods ==========

    /**
     * @brief Look up a TypeMeta by name.
     *
     * @param type_name The human-readable type name (e.g., "int", "str", "bool")
     * @return Pointer to the TypeMeta, or nullptr if not found
     */
    static const TypeMeta* get(const std::string& type_name);

    /**
     * @brief Look up a TypeMeta by C++ type.
     *
     * @tparam T The C++ type
     * @return Pointer to the TypeMeta, or nullptr if not registered
     */
    template<typename T>
    static const TypeMeta* get();

    /**
     * @brief Look up a TypeMeta from a Python type object.
     *
     * Requires GIL to be held by caller.
     *
     * @param py_type The Python type object
     * @return Pointer to the TypeMeta, or nullptr if not found
     */
    static const TypeMeta* from_python_type(nb::handle py_type);

    // ========== Query Methods ==========

    /// Check if this is a fixed-size collection
    [[nodiscard]] constexpr bool is_fixed_size() const noexcept {
        return fixed_size > 0;
    }

    /// Check if a flag is set
    [[nodiscard]] constexpr bool has(TypeFlags flag) const noexcept {
        return has_flag(flags, flag);
    }

    /// Check if this type is trivially constructible
    [[nodiscard]] constexpr bool is_trivially_constructible() const noexcept {
        return has(TypeFlags::TriviallyConstructible);
    }

    /// Check if this type is trivially destructible
    [[nodiscard]] constexpr bool is_trivially_destructible() const noexcept {
        return has(TypeFlags::TriviallyDestructible);
    }

    /// Check if this type is trivially copyable
    [[nodiscard]] constexpr bool is_trivially_copyable() const noexcept {
        return has(TypeFlags::TriviallyCopyable);
    }

    /// Check if this type is hashable
    [[nodiscard]] constexpr bool is_hashable() const noexcept {
        return has(TypeFlags::Hashable);
    }

    /// Check if this type is comparable (ordered)
    [[nodiscard]] constexpr bool is_comparable() const noexcept {
        return has(TypeFlags::Comparable);
    }

    /// Check if this type is equatable
    [[nodiscard]] constexpr bool is_equatable() const noexcept {
        return has(TypeFlags::Equatable);
    }

    /// Check if this type is buffer compatible (numpy/Arrow)
    [[nodiscard]] constexpr bool is_buffer_compatible() const noexcept {
        return has(TypeFlags::BufferCompatible);
    }

    /// Check if this is a variadic tuple (tuple[T, ...])
    [[nodiscard]] constexpr bool is_variadic_tuple() const noexcept {
        return has(TypeFlags::VariadicTuple);
    }
};

// ============================================================================
// Scalar Type Flags Helper
// ============================================================================

/**
 * @brief Compute TypeFlags for a scalar type at compile time.
 *
 * @tparam T The scalar type
 * @return TypeFlags The flags for this type
 */
template<typename T>
constexpr TypeFlags compute_scalar_flags() {
    TypeFlags flags = TypeFlags::None;

    if constexpr (std::is_trivially_constructible_v<T>) {
        flags = flags | TypeFlags::TriviallyConstructible;
    }
    if constexpr (std::is_trivially_destructible_v<T>) {
        flags = flags | TypeFlags::TriviallyDestructible;
    }
    if constexpr (std::is_trivially_copyable_v<T>) {
        flags = flags | TypeFlags::TriviallyCopyable;
    }
    // Assume all scalar types are hashable, comparable, and equatable
    // unless specialized otherwise
    flags = flags | TypeFlags::Hashable | TypeFlags::Comparable | TypeFlags::Equatable;

    // BufferCompatible for numeric types that can be used in numpy arrays
    if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, double> ||
                  std::is_same_v<T, bool> || std::is_same_v<T, float> ||
                  std::is_same_v<T, int32_t> || std::is_same_v<T, int16_t> ||
                  std::is_same_v<T, int8_t> || std::is_same_v<T, uint64_t> ||
                  std::is_same_v<T, uint32_t> || std::is_same_v<T, uint16_t> ||
                  std::is_same_v<T, uint8_t>) {
        flags = flags | TypeFlags::BufferCompatible;
    }

    return flags;
}

} // namespace hgraph::value
