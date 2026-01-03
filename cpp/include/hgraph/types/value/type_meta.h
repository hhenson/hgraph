#pragma once

/**
 * @file type_meta.h
 * @brief Type metadata structures for the Value type system.
 *
 * TypeMeta describes the schema of a type: its size, alignment, kind,
 * capabilities, and type-erased operations. The TypeOps structure provides
 * function pointers for performing operations on type-erased data.
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
    Scalar,        ///< Atomic values: int, double, bool, string, datetime, etc.
    Tuple,         ///< Indexed heterogeneous collection (unnamed, positional access only)
    Bundle,        ///< Named field collection (struct-like, index + name access)
    List,          ///< Indexed homogeneous collection (dynamic size)
    Set,           ///< Unordered unique elements
    Map,           ///< Key-value pairs
    CyclicBuffer,  ///< Fixed-size circular buffer (re-centers on read)
    Queue,         ///< FIFO queue with optional max capacity
    Ref            ///< Reference to another time-series (future)
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
// Type Operations Virtual Table
// ============================================================================

/**
 * @brief Type-erased operations for a type.
 *
 * This structure contains function pointers for all operations that can
 * be performed on a type. Not all operations are supported by all types;
 * unsupported operations are set to nullptr.
 */
struct TypeOps {
    // ========== Core Operations (required for all types) ==========

    /// Default construct a value at dst
    void (*construct)(void* dst, const TypeMeta* schema);

    /// Destruct a value at obj
    void (*destruct)(void* obj, const TypeMeta* schema);

    /// Copy assign from src to dst (both must be valid)
    void (*copy_assign)(void* dst, const void* src, const TypeMeta* schema);

    /// Move assign from src to dst
    void (*move_assign)(void* dst, void* src, const TypeMeta* schema);

    /// Move construct a value at dst from src (placement new with move semantics)
    /// This is used when dst is uninitialized memory and src should be moved into it
    void (*move_construct)(void* dst, void* src, const TypeMeta* schema);

    /// Check equality of two values
    bool (*equals)(const void* a, const void* b, const TypeMeta* schema);

    /// Convert value to string representation
    std::string (*to_string)(const void* obj, const TypeMeta* schema);

    // ========== Python Interop ==========

    /// Convert C++ value to Python object
    nb::object (*to_python)(const void* obj, const TypeMeta* schema);

    /// Convert Python object to C++ value (dst must be constructed)
    void (*from_python)(void* dst, const nb::object& src, const TypeMeta* schema);

    // ========== Hashable Operations (optional) ==========

    /// Compute hash of value (nullptr if not hashable)
    size_t (*hash)(const void* obj, const TypeMeta* schema);

    // ========== Comparable Operations (optional) ==========

    /// Less-than comparison (nullptr if not comparable)
    bool (*less_than)(const void* a, const void* b, const TypeMeta* schema);

    // ========== Iterable Operations (optional) ==========

    /// Get number of elements (nullptr if not iterable)
    size_t (*size)(const void* obj, const TypeMeta* schema);

    // ========== Indexable Operations (optional) ==========

    /// Get element at index (nullptr if not indexable)
    const void* (*get_at)(const void* obj, size_t index, const TypeMeta* schema);

    /// Set element at index (nullptr if not indexable)
    void (*set_at)(void* obj, size_t index, const void* value, const TypeMeta* schema);

    // ========== Bundle Operations (optional) ==========

    /// Get field by name (nullptr if not a bundle)
    const void* (*get_field)(const void* obj, const char* name, const TypeMeta* schema);

    /// Set field by name (nullptr if not a bundle)
    void (*set_field)(void* obj, const char* name, const void* value, const TypeMeta* schema);

    // ========== Set Operations (optional) ==========

    /// Check if element is in set (nullptr if not a set)
    bool (*contains)(const void* obj, const void* element, const TypeMeta* schema);

    /// Insert element into set (nullptr if not a set)
    void (*insert)(void* obj, const void* element, const TypeMeta* schema);

    /// Remove element from set (nullptr if not a set)
    void (*erase)(void* obj, const void* element, const TypeMeta* schema);

    // ========== Map Operations (optional) ==========

    /// Get value by key (nullptr if not a map)
    const void* (*map_get)(const void* obj, const void* key, const TypeMeta* schema);

    /// Set value by key (nullptr if not a map)
    void (*map_set)(void* obj, const void* key, const void* value, const TypeMeta* schema);

    // ========== List Operations (optional) ==========

    /// Resize list (nullptr if not a resizable list)
    void (*resize)(void* obj, size_t new_size, const TypeMeta* schema);

    /// Clear all elements (nullptr if not clearable)
    void (*clear)(void* obj, const TypeMeta* schema);
};

// ============================================================================
// Type Metadata
// ============================================================================

/**
 * @brief Complete metadata describing a type.
 *
 * TypeMeta is the schema for a type. It contains size/alignment information,
 * the type kind, capability flags, and a pointer to the operations vtable.
 * For composite types, it also contains element/field information.
 */
struct TypeMeta {
    size_t size;              ///< Size in bytes
    size_t alignment;         ///< Alignment requirement
    TypeKind kind;            ///< Type category
    TypeFlags flags;          ///< Capability flags
    const TypeOps* ops;       ///< Type-erased operations vtable

    // ========== Composite Type Information ==========

    const TypeMeta* element_type;      ///< List/Set element type, Map value type
    const TypeMeta* key_type;          ///< Map key type (nullptr for non-maps)
    const BundleFieldInfo* fields;     ///< Bundle/Tuple field metadata
    size_t field_count;                ///< Number of fields (Bundle/Tuple)

    // ========== Fixed-Size Collection Information ==========

    size_t fixed_size;                 ///< 0 = dynamic, >0 = fixed capacity

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
// Scalar Type Operations Template
// ============================================================================

/**
 * @brief Default operations implementation for scalar types.
 *
 * This template provides type-specific implementations of all operations
 * for simple scalar types. It can be specialized for custom behavior.
 *
 * @tparam T The scalar type
 */
template<typename T>
struct ScalarOps {
    static void construct(void* dst, const TypeMeta*) {
        new (dst) T{};
    }

    static void destruct(void* obj, const TypeMeta*) {
        static_cast<T*>(obj)->~T();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta*) {
        *static_cast<T*>(dst) = *static_cast<const T*>(src);
    }

    static void move_assign(void* dst, void* src, const TypeMeta*) {
        *static_cast<T*>(dst) = std::move(*static_cast<T*>(src));
    }

    static void move_construct(void* dst, void* src, const TypeMeta*) {
        new (dst) T(std::move(*static_cast<T*>(src)));
    }

    static bool equals(const void* a, const void* b, const TypeMeta*) {
        return *static_cast<const T*>(a) == *static_cast<const T*>(b);
    }

    static size_t hash(const void* obj, const TypeMeta*) {
        return std::hash<T>{}(*static_cast<const T*>(obj));
    }

    static bool less_than(const void* a, const void* b, const TypeMeta*) {
        return *static_cast<const T*>(a) < *static_cast<const T*>(b);
    }

    static std::string to_string(const void* obj, const TypeMeta*) {
        if constexpr (std::is_same_v<T, std::string>) {
            return *static_cast<const T*>(obj);
        } else if constexpr (std::is_same_v<T, bool>) {
            return *static_cast<const T*>(obj) ? "true" : "false";
        } else if constexpr (std::is_arithmetic_v<T>) {
            return std::to_string(*static_cast<const T*>(obj));
        } else {
            // For complex types, return a placeholder
            return "<" + std::string(typeid(T).name()) + ">";
        }
    }

    // Python interop - to be specialized per type
    static nb::object to_python(const void* obj, const TypeMeta*) {
        return nb::cast(*static_cast<const T*>(obj));
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta*) {
        *static_cast<T*>(dst) = nb::cast<T>(src);
    }

    /// Get the operations vtable for this scalar type
    static constexpr TypeOps make_ops() {
        return TypeOps{
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &move_construct,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            &hash,
            &less_than,
            nullptr,  // size (not iterable)
            nullptr,  // get_at (not indexable)
            nullptr,  // set_at (not indexable)
            nullptr,  // get_field (not bundle)
            nullptr,  // set_field (not bundle)
            nullptr,  // contains (not set)
            nullptr,  // insert (not set)
            nullptr,  // erase (not set)
            nullptr,  // map_get (not map)
            nullptr,  // map_set (not map)
            nullptr,  // resize (not resizable)
            nullptr,  // clear (not clearable)
        };
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

// ============================================================================
// ScalarOps Specializations for DateTime Types
// ============================================================================

// Specialization for engine_date_t (year_month_day)
template<>
inline std::string ScalarOps<engine_date_t>::to_string(const void* obj, const TypeMeta*) {
    const auto& ymd = *static_cast<const engine_date_t*>(obj);
    return std::to_string(static_cast<int>(ymd.year())) + "-" +
           (static_cast<unsigned>(ymd.month()) < 10 ? "0" : "") +
           std::to_string(static_cast<unsigned>(ymd.month())) + "-" +
           (static_cast<unsigned>(ymd.day()) < 10 ? "0" : "") +
           std::to_string(static_cast<unsigned>(ymd.day()));
}

// Specialization for engine_time_t (time_point<system_clock, microseconds>)
template<>
inline std::string ScalarOps<engine_time_t>::to_string(const void* obj, const TypeMeta*) {
    const auto& tp = *static_cast<const engine_time_t*>(obj);
    auto current_day = std::chrono::floor<std::chrono::days>(tp);
    std::chrono::year_month_day ymd{current_day};
    auto time_of_day = tp - current_day;
    std::chrono::hh_mm_ss hms{time_of_day};

    std::string result = std::to_string(static_cast<int>(ymd.year())) + "-" +
        (static_cast<unsigned>(ymd.month()) < 10 ? "0" : "") +
        std::to_string(static_cast<unsigned>(ymd.month())) + "-" +
        (static_cast<unsigned>(ymd.day()) < 10 ? "0" : "") +
        std::to_string(static_cast<unsigned>(ymd.day())) + "T" +
        (hms.hours().count() < 10 ? "0" : "") +
        std::to_string(hms.hours().count()) + ":" +
        (hms.minutes().count() < 10 ? "0" : "") +
        std::to_string(hms.minutes().count()) + ":" +
        (hms.seconds().count() < 10 ? "0" : "") +
        std::to_string(hms.seconds().count());

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(hms.subseconds()).count();
    if (us > 0) {
        result += "." + std::to_string(us);
    }
    return result;
}

// Specialization for engine_time_delta_t (microseconds duration)
template<>
inline std::string ScalarOps<engine_time_delta_t>::to_string(const void* obj, const TypeMeta*) {
    const auto& d = *static_cast<const engine_time_delta_t*>(obj);
    auto total_us = d.count();
    auto total_secs = total_us / 1'000'000;
    auto remaining_us = total_us % 1'000'000;
    auto hours = total_secs / 3600;
    auto mins = (total_secs % 3600) / 60;
    auto secs = total_secs % 60;

    std::string result = std::to_string(hours) + ":" +
        (mins < 10 ? "0" : "") + std::to_string(mins) + ":" +
        (secs < 10 ? "0" : "") + std::to_string(secs);

    if (remaining_us > 0) {
        result += "." + std::to_string(remaining_us);
    }
    return result;
}

} // namespace hgraph::value
