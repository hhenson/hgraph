#pragma once

/**
 * @file type_meta.h
 * @brief Type metadata structures for the Value type system.
 *
 * TypeMeta describes the schema of a type: its size, alignment, kind,
 * capabilities, and type-erased operations. The type_ops structure provides
 * common function pointers plus a kind-tagged union of kind-specific ops.
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
// Kind-Specific Operation Sub-Structures
// ============================================================================

/// Operations specific to Atomic types
struct atomic_ops_t {
    bool (*less_than)(const void* a, const void* b, const TypeMeta* schema);
};

/// Operations specific to Bundle types (named fields, index + name access)
struct bundle_ops_t {
    size_t (*size)(const void* obj, const TypeMeta* schema);
    const void* (*get_at)(const void* obj, size_t index, const TypeMeta* schema);
    void (*set_at)(void* obj, size_t index, const void* value, const TypeMeta* schema);
    const void* (*get_field)(const void* obj, const char* name, const TypeMeta* schema);
    void (*set_field)(void* obj, const char* name, const void* value, const TypeMeta* schema);
};

/// Operations specific to Tuple types (positional access only)
struct tuple_ops_t {
    size_t (*size)(const void* obj, const TypeMeta* schema);
    const void* (*get_at)(const void* obj, size_t index, const TypeMeta* schema);
    void (*set_at)(void* obj, size_t index, const void* value, const TypeMeta* schema);
};

/// Operations specific to List types (dynamic homogeneous collection)
struct list_ops_t {
    size_t (*size)(const void* obj, const TypeMeta* schema);
    const void* (*get_at)(const void* obj, size_t index, const TypeMeta* schema);
    void (*set_at)(void* obj, size_t index, const void* value, const TypeMeta* schema);
    void (*resize)(void* obj, size_t new_size, const TypeMeta* schema);
    void (*clear)(void* obj, const TypeMeta* schema);
};

/// Operations specific to Set types (unique unordered elements)
struct set_ops_t {
    size_t (*size)(const void* obj, const TypeMeta* schema);
    const void* (*get_at)(const void* obj, size_t index, const TypeMeta* schema);
    bool (*contains)(const void* obj, const void* element, const TypeMeta* schema);
    void (*insert)(void* obj, const void* element, const TypeMeta* schema);
    void (*erase)(void* obj, const void* element, const TypeMeta* schema);
    void (*clear)(void* obj, const TypeMeta* schema);
};

/// Operations specific to Map types (key-value pairs)
struct map_ops_t {
    size_t (*size)(const void* obj, const TypeMeta* schema);
    bool (*contains)(const void* obj, const void* key, const TypeMeta* schema);
    const void* (*map_get)(const void* obj, const void* key, const TypeMeta* schema);
    void (*map_set)(void* obj, const void* key, const void* value, const TypeMeta* schema);
    void (*erase)(void* obj, const void* key, const TypeMeta* schema);
    void (*clear)(void* obj, const TypeMeta* schema);
};

/// Operations specific to CyclicBuffer types (fixed-size circular buffer)
/// Has all queue ops plus ordinal set_at for indexed writes.
struct cyclic_buffer_ops_t {
    size_t (*size)(const void* obj, const TypeMeta* schema);
    const void* (*get_at)(const void* obj, size_t index, const TypeMeta* schema);
    void (*set_at)(void* obj, size_t index, const void* value, const TypeMeta* schema);
    void (*push_back)(void* obj, const void* value, const TypeMeta* schema);
    void (*pop_front)(void* obj, const TypeMeta* schema);
    void (*clear)(void* obj, const TypeMeta* schema);
};

/// Operations specific to Queue types (FIFO with optional max capacity)
struct queue_ops_t {
    size_t (*size)(const void* obj, const TypeMeta* schema);
    const void* (*get_at)(const void* obj, size_t index, const TypeMeta* schema);
    void (*push_back)(void* obj, const void* value, const TypeMeta* schema);
    void (*pop_front)(void* obj, const TypeMeta* schema);
    void (*clear)(void* obj, const TypeMeta* schema);
};

// ============================================================================
// Type Operations — Tagged Union
// ============================================================================

/**
 * @brief Type-erased operations for a type.
 *
 * Contains 10 common function pointers (required for all types) plus a
 * TypeKind-tagged union of kind-specific operations. Stored by value in
 * TypeMeta to eliminate pointer indirection.
 *
 * Common ops are accessed directly as function pointers:
 *   schema->ops().construct(dst, schema)
 *
 * Kind-specific ops are accessed via dispatch methods:
 *   schema->ops().size(obj, schema)    // dispatches to the right sub-struct
 */
struct type_ops {
    // ========== Common Operations (required for all types) ==========

    void (*construct)(void* dst, const TypeMeta* schema);
    void (*destruct)(void* obj, const TypeMeta* schema);
    void (*copy_assign)(void* dst, const void* src, const TypeMeta* schema);
    void (*move_assign)(void* dst, void* src, const TypeMeta* schema);
    void (*move_construct)(void* dst, void* src, const TypeMeta* schema);
    bool (*equals)(const void* a, const void* b, const TypeMeta* schema);
    size_t (*hash)(const void* obj, const TypeMeta* schema);
    std::string (*to_string)(const void* obj, const TypeMeta* schema);
    nb::object (*to_python)(const void* obj, const TypeMeta* schema);
    void (*from_python)(void* dst, const nb::object& src, const TypeMeta* schema);

    // ========== Kind-Specific Tagged Union ==========

    TypeKind kind;

    union {
        atomic_ops_t atomic;
        bundle_ops_t bundle;
        tuple_ops_t tuple;
        list_ops_t list;
        set_ops_t set;
        map_ops_t map;
        cyclic_buffer_ops_t cyclic_buffer;
        queue_ops_t queue;
    } specific;

    // ========== Dispatch Methods for Kind-Specific Operations ==========

    /// Get number of elements. Returns 0 for Atomic.
    [[nodiscard]] size_t size(const void* obj, const TypeMeta* schema) const {
        switch (kind) {
            case TypeKind::Bundle:      return specific.bundle.size(obj, schema);
            case TypeKind::Tuple:       return specific.tuple.size(obj, schema);
            case TypeKind::List:        return specific.list.size(obj, schema);
            case TypeKind::Set:         return specific.set.size(obj, schema);
            case TypeKind::Map:         return specific.map.size(obj, schema);
            case TypeKind::CyclicBuffer:return specific.cyclic_buffer.size(obj, schema);
            case TypeKind::Queue:       return specific.queue.size(obj, schema);
            default:                    return 0;
        }
    }

    /// Whether this type supports size()
    [[nodiscard]] bool has_size() const { return kind != TypeKind::Atomic; }

    /// Get element at index. Returns nullptr for unsupported kinds.
    [[nodiscard]] const void* get_at(const void* obj, size_t index, const TypeMeta* schema) const {
        switch (kind) {
            case TypeKind::Bundle:      return specific.bundle.get_at(obj, index, schema);
            case TypeKind::Tuple:       return specific.tuple.get_at(obj, index, schema);
            case TypeKind::List:        return specific.list.get_at(obj, index, schema);
            case TypeKind::Set:         return specific.set.get_at(obj, index, schema);
            case TypeKind::CyclicBuffer:return specific.cyclic_buffer.get_at(obj, index, schema);
            case TypeKind::Queue:       return specific.queue.get_at(obj, index, schema);
            default:                    return nullptr;
        }
    }

    /// Set element at index. No-op for unsupported kinds.
    void set_at(void* obj, size_t index, const void* value, const TypeMeta* schema) const {
        switch (kind) {
            case TypeKind::Bundle:      specific.bundle.set_at(obj, index, value, schema); break;
            case TypeKind::Tuple:       specific.tuple.set_at(obj, index, value, schema); break;
            case TypeKind::List:        specific.list.set_at(obj, index, value, schema); break;
            case TypeKind::CyclicBuffer:specific.cyclic_buffer.set_at(obj, index, value, schema); break;
            default: break;
        }
    }

    /// Push a value to the back. Supported by CyclicBuffer and Queue.
    void push_back(void* obj, const void* value, const TypeMeta* schema) const {
        switch (kind) {
            case TypeKind::CyclicBuffer:specific.cyclic_buffer.push_back(obj, value, schema); break;
            case TypeKind::Queue:       specific.queue.push_back(obj, value, schema); break;
            default: break;
        }
    }

    /// Whether this type supports push_back()
    [[nodiscard]] bool has_push_back() const {
        return kind == TypeKind::CyclicBuffer || kind == TypeKind::Queue;
    }

    /// Remove the front element. Supported by CyclicBuffer and Queue.
    void pop_front(void* obj, const TypeMeta* schema) const {
        switch (kind) {
            case TypeKind::CyclicBuffer:specific.cyclic_buffer.pop_front(obj, schema); break;
            case TypeKind::Queue:       specific.queue.pop_front(obj, schema); break;
            default: break;
        }
    }

    /// Whether this type supports pop_front()
    [[nodiscard]] bool has_pop_front() const {
        return kind == TypeKind::CyclicBuffer || kind == TypeKind::Queue;
    }

    /// Get field by name (Bundle only). Returns nullptr for other kinds.
    [[nodiscard]] const void* get_field(const void* obj, const char* name, const TypeMeta* schema) const {
        if (kind == TypeKind::Bundle) return specific.bundle.get_field(obj, name, schema);
        return nullptr;
    }

    /// Set field by name (Bundle only). No-op for other kinds.
    void set_field(void* obj, const char* name, const void* value, const TypeMeta* schema) const {
        if (kind == TypeKind::Bundle) specific.bundle.set_field(obj, name, value, schema);
    }

    /// Check if element/key is contained. Returns false for unsupported kinds.
    [[nodiscard]] bool contains(const void* obj, const void* element, const TypeMeta* schema) const {
        switch (kind) {
            case TypeKind::Set: return specific.set.contains(obj, element, schema);
            case TypeKind::Map: return specific.map.contains(obj, element, schema);
            default:            return false;
        }
    }

    /// Insert element (Set only). No-op for other kinds.
    void insert(void* obj, const void* element, const TypeMeta* schema) const {
        if (kind == TypeKind::Set) specific.set.insert(obj, element, schema);
    }

    /// Erase element/key. No-op for unsupported kinds.
    void erase(void* obj, const void* element, const TypeMeta* schema) const {
        switch (kind) {
            case TypeKind::Set: specific.set.erase(obj, element, schema); break;
            case TypeKind::Map: specific.map.erase(obj, element, schema); break;
            default: break;
        }
    }

    /// Get map value by key (Map only). Returns nullptr for other kinds.
    [[nodiscard]] const void* map_get(const void* obj, const void* key, const TypeMeta* schema) const {
        if (kind == TypeKind::Map) return specific.map.map_get(obj, key, schema);
        return nullptr;
    }

    /// Set map value by key (Map only). No-op for other kinds.
    void map_set(void* obj, const void* key, const void* value, const TypeMeta* schema) const {
        if (kind == TypeKind::Map) specific.map.map_set(obj, key, value, schema);
    }

    /// Resize collection (List only). No-op for other kinds.
    void resize(void* obj, size_t new_size, const TypeMeta* schema) const {
        if (kind == TypeKind::List) specific.list.resize(obj, new_size, schema);
    }

    /// Whether this type supports resize()
    [[nodiscard]] bool has_resize() const { return kind == TypeKind::List; }

    /// Clear all elements. No-op for unsupported kinds.
    void clear(void* obj, const TypeMeta* schema) const {
        switch (kind) {
            case TypeKind::List:        specific.list.clear(obj, schema); break;
            case TypeKind::Set:         specific.set.clear(obj, schema); break;
            case TypeKind::Map:         specific.map.clear(obj, schema); break;
            case TypeKind::CyclicBuffer:specific.cyclic_buffer.clear(obj, schema); break;
            case TypeKind::Queue:       specific.queue.clear(obj, schema); break;
            default: break;
        }
    }

    /// Whether this type supports clear()
    [[nodiscard]] bool has_clear() const {
        return kind == TypeKind::List || kind == TypeKind::Set ||
               kind == TypeKind::Map || kind == TypeKind::CyclicBuffer ||
               kind == TypeKind::Queue;
    }

    /// Less-than comparison (Atomic only). Returns false for other kinds.
    [[nodiscard]] bool less_than(const void* a, const void* b, const TypeMeta* schema) const {
        if (kind == TypeKind::Atomic) return specific.atomic.less_than(a, b, schema);
        return false;
    }
};

// ============================================================================
// Type Metadata
// ============================================================================

/**
 * @brief Complete metadata describing a type.
 *
 * TypeMeta is the schema for a type. It contains size/alignment information,
 * the type kind, capability flags, and type-erased operations stored by value.
 * For composite types, it also contains element/field information.
 */
struct TypeMeta {
    size_t size;              ///< Size in bytes
    size_t alignment;         ///< Alignment requirement
    TypeKind kind;            ///< Type category
    TypeFlags flags;          ///< Capability flags

    // ========== Type-Erased Operations (stored by value) ==========

    type_ops ops_;            ///< Operations for this type

    /// Access the type operations
    [[nodiscard]] const type_ops& ops() const { return ops_; }

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

    /// Build the type_ops for this scalar type
    static type_ops make_ops() {
        type_ops ops{};
        // Common ops
        ops.construct = &construct;
        ops.destruct = &destruct;
        ops.copy_assign = &copy_assign;
        ops.move_assign = &move_assign;
        ops.move_construct = &move_construct;
        ops.equals = &equals;
        ops.hash = &hash;
        ops.to_string = &to_string;
        ops.to_python = &to_python;
        ops.from_python = &from_python;
        // Kind
        ops.kind = TypeKind::Atomic;
        // Atomic-specific ops
        ops.specific.atomic = {&less_than};
        return ops;
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
