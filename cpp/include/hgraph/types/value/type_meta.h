//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_TYPE_META_H
#define HGRAPH_VALUE_TYPE_META_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <typeinfo>

namespace hgraph::value {

    // Forward declarations
    struct TypeMeta;

    /**
     * TypeOps - Function pointers for type operations
     *
     * All operations take raw pointers and the TypeMeta for context.
     * This enables type-erased operations on any value.
     *
     * For arithmetic operations, dest must point to already-constructed storage
     * of the appropriate type. The operation writes the result to dest.
     * Operations return false if the operation is not supported for this type.
     */
    struct TypeOps {
        // Lifecycle
        void (*construct)(void* dest, const TypeMeta* meta);
        void (*destruct)(void* dest, const TypeMeta* meta);
        void (*copy_construct)(void* dest, const void* src, const TypeMeta* meta);
        void (*move_construct)(void* dest, void* src, const TypeMeta* meta);

        // Assignment
        void (*copy_assign)(void* dest, const void* src, const TypeMeta* meta);
        void (*move_assign)(void* dest, void* src, const TypeMeta* meta);

        // Comparison
        bool (*equals)(const void* a, const void* b, const TypeMeta* meta);
        bool (*less_than)(const void* a, const void* b, const TypeMeta* meta);

        // Hashing
        size_t (*hash)(const void* v, const TypeMeta* meta);

        // String representation (for logging/debugging)
        std::string (*to_string)(const void* v, const TypeMeta* meta);

        // Type name (Python-style type description, e.g., "int", "Dict[str, float]")
        std::string (*type_name)(const TypeMeta* meta);

        // Python interop (optional - can be nullptr)
        void* (*to_python)(const void* v, const TypeMeta* meta);
        void (*from_python)(void* dest, void* py_obj, const TypeMeta* meta);

        // Arithmetic binary operations (optional - nullptr if not supported)
        // These write result to dest, return true on success
        bool (*add)(void* dest, const void* a, const void* b, const TypeMeta* meta);
        bool (*subtract)(void* dest, const void* a, const void* b, const TypeMeta* meta);
        bool (*multiply)(void* dest, const void* a, const void* b, const TypeMeta* meta);
        bool (*divide)(void* dest, const void* a, const void* b, const TypeMeta* meta);
        bool (*floor_divide)(void* dest, const void* a, const void* b, const TypeMeta* meta);
        bool (*modulo)(void* dest, const void* a, const void* b, const TypeMeta* meta);
        bool (*power)(void* dest, const void* a, const void* b, const TypeMeta* meta);

        // Arithmetic unary operations (optional - nullptr if not supported)
        bool (*negate)(void* dest, const void* src, const TypeMeta* meta);
        bool (*absolute)(void* dest, const void* src, const TypeMeta* meta);
        bool (*invert)(void* dest, const void* src, const TypeMeta* meta);

        // Boolean conversion (optional - nullptr if not supported)
        bool (*to_bool)(const void* v, const TypeMeta* meta);

        // Container operations (optional - nullptr if not supported)
        size_t (*length)(const void* v, const TypeMeta* meta);
        bool (*contains)(const void* container, const void* element, const TypeMeta* meta);
    };

    /**
     * TypeFlags - Properties of a type
     */
    enum class TypeFlags : uint32_t {
        None = 0,
        TriviallyConstructible = 1 << 0,
        TriviallyDestructible = 1 << 1,
        TriviallyCopyable = 1 << 2,
        BufferCompatible = 1 << 3,  // Can expose via buffer protocol
        Hashable = 1 << 4,
        Comparable = 1 << 5,        // Supports < and ==
        Equatable = 1 << 6,         // Supports ==
        Arithmetic = 1 << 7,        // Supports +, -, *, /
        Integral = 1 << 8,          // Supports //, %, ~
        Container = 1 << 9,         // Supports len(), in
    };

    inline TypeFlags operator|(TypeFlags a, TypeFlags b) {
        return static_cast<TypeFlags>(static_cast<uint32_t>(a) | static_cast<uint32_t>(b));
    }

    inline TypeFlags operator&(TypeFlags a, TypeFlags b) {
        return static_cast<TypeFlags>(static_cast<uint32_t>(a) & static_cast<uint32_t>(b));
    }

    inline TypeFlags operator~(TypeFlags a) {
        return static_cast<TypeFlags>(~static_cast<uint32_t>(a));
    }

    inline bool has_flag(TypeFlags flags, TypeFlags test) {
        return (static_cast<uint32_t>(flags) & static_cast<uint32_t>(test)) != 0;
    }

    /**
     * BufferInfo - Information for buffer protocol exposure
     */
    struct BufferInfo {
        void* ptr{nullptr};
        size_t itemsize{0};
        size_t count{0};
        bool readonly{false};
    };

    /**
     * TypeKind - Classification of types
     */
    enum class TypeKind : uint8_t {
        Scalar,      // Single value (int, double, etc.)
        List,        // Fixed-size list (TSL with known size)
        DynamicList, // Variable-length list (tuple[T, ...])
        Set,         // Hash set
        Dict,        // Hash map
        Bundle,      // Struct-like composite
        Ref,         // Reference to another value
        Window,      // Time-series history (fixed or variable length)
    };

    /**
     * TypeMeta - Complete metadata for a type
     *
     * This is the core descriptor that enables type-erased operations.
     * For composite types (Bundle, List, etc.), additional metadata
     * is stored in a derived structure.
     */
    struct TypeMeta {
        size_t size;            // sizeof(T)
        size_t alignment;       // alignof(T)
        TypeFlags flags;
        TypeKind kind;
        const TypeOps* ops;
        const std::type_info* type_info;  // For debugging/RTTI (optional)
        const char* name;       // Human-readable name (optional)
        const char* numpy_format;  // Numpy dtype format char (e.g., "d" for double, "q" for int64), nullptr if not numpy-compatible

        // Convenience methods
        [[nodiscard]] bool is_buffer_compatible() const {
            return has_flag(flags, TypeFlags::BufferCompatible);
        }

        [[nodiscard]] bool is_trivially_copyable() const {
            return has_flag(flags, TypeFlags::TriviallyCopyable);
        }

        [[nodiscard]] bool is_trivially_destructible() const {
            return has_flag(flags, TypeFlags::TriviallyDestructible);
        }

        [[nodiscard]] bool is_hashable() const {
            return has_flag(flags, TypeFlags::Hashable);
        }

        [[nodiscard]] bool is_comparable() const {
            return has_flag(flags, TypeFlags::Comparable);
        }

        [[nodiscard]] bool is_numpy_compatible() const {
            return numpy_format != nullptr;
        }

        // Operation wrappers
        void construct_at(void* dest) const {
            if (ops->construct) ops->construct(dest, this);
        }

        void destruct_at(void* dest) const {
            if (ops->destruct) ops->destruct(dest, this);
        }

        void copy_construct_at(void* dest, const void* src) const {
            if (ops->copy_construct) ops->copy_construct(dest, src, this);
        }

        void move_construct_at(void* dest, void* src) const {
            if (ops->move_construct) ops->move_construct(dest, src, this);
        }

        void copy_assign_at(void* dest, const void* src) const {
            if (ops->copy_assign) ops->copy_assign(dest, src, this);
        }

        void move_assign_at(void* dest, void* src) const {
            if (ops->move_assign) ops->move_assign(dest, src, this);
        }

        [[nodiscard]] bool equals_at(const void* a, const void* b) const {
            return ops->equals ? ops->equals(a, b, this) : false;
        }

        [[nodiscard]] bool less_than_at(const void* a, const void* b) const {
            return ops->less_than ? ops->less_than(a, b, this) : false;
        }

        [[nodiscard]] size_t hash_at(const void* v) const {
            return ops->hash ? ops->hash(v, this) : 0;
        }

        [[nodiscard]] std::string to_string_at(const void* v) const {
            return ops->to_string ? ops->to_string(v, this) : "<no to_string>";
        }

        [[nodiscard]] std::string type_name_str() const {
            return ops->type_name ? ops->type_name(this) : (name ? name : "<unknown>");
        }

        // Arithmetic capability checks
        [[nodiscard]] bool is_arithmetic() const {
            return has_flag(flags, TypeFlags::Arithmetic);
        }

        [[nodiscard]] bool is_integral() const {
            return has_flag(flags, TypeFlags::Integral);
        }

        [[nodiscard]] bool is_container() const {
            return has_flag(flags, TypeFlags::Container);
        }

        [[nodiscard]] bool supports_add() const { return ops->add != nullptr; }
        [[nodiscard]] bool supports_subtract() const { return ops->subtract != nullptr; }
        [[nodiscard]] bool supports_multiply() const { return ops->multiply != nullptr; }
        [[nodiscard]] bool supports_divide() const { return ops->divide != nullptr; }
        [[nodiscard]] bool supports_floor_divide() const { return ops->floor_divide != nullptr; }
        [[nodiscard]] bool supports_modulo() const { return ops->modulo != nullptr; }
        [[nodiscard]] bool supports_power() const { return ops->power != nullptr; }
        [[nodiscard]] bool supports_negate() const { return ops->negate != nullptr; }
        [[nodiscard]] bool supports_absolute() const { return ops->absolute != nullptr; }
        [[nodiscard]] bool supports_invert() const { return ops->invert != nullptr; }
        [[nodiscard]] bool supports_to_bool() const { return ops->to_bool != nullptr; }
        [[nodiscard]] bool supports_length() const { return ops->length != nullptr; }
        [[nodiscard]] bool supports_contains() const { return ops->contains != nullptr; }

        // Arithmetic operation wrappers - return false if not supported
        [[nodiscard]] bool add_at(void* dest, const void* a, const void* b) const {
            return ops->add ? ops->add(dest, a, b, this) : false;
        }

        [[nodiscard]] bool subtract_at(void* dest, const void* a, const void* b) const {
            return ops->subtract ? ops->subtract(dest, a, b, this) : false;
        }

        [[nodiscard]] bool multiply_at(void* dest, const void* a, const void* b) const {
            return ops->multiply ? ops->multiply(dest, a, b, this) : false;
        }

        [[nodiscard]] bool divide_at(void* dest, const void* a, const void* b) const {
            return ops->divide ? ops->divide(dest, a, b, this) : false;
        }

        [[nodiscard]] bool floor_divide_at(void* dest, const void* a, const void* b) const {
            return ops->floor_divide ? ops->floor_divide(dest, a, b, this) : false;
        }

        [[nodiscard]] bool modulo_at(void* dest, const void* a, const void* b) const {
            return ops->modulo ? ops->modulo(dest, a, b, this) : false;
        }

        [[nodiscard]] bool power_at(void* dest, const void* a, const void* b) const {
            return ops->power ? ops->power(dest, a, b, this) : false;
        }

        [[nodiscard]] bool negate_at(void* dest, const void* src) const {
            return ops->negate ? ops->negate(dest, src, this) : false;
        }

        [[nodiscard]] bool absolute_at(void* dest, const void* src) const {
            return ops->absolute ? ops->absolute(dest, src, this) : false;
        }

        [[nodiscard]] bool invert_at(void* dest, const void* src) const {
            return ops->invert ? ops->invert(dest, src, this) : false;
        }

        [[nodiscard]] bool to_bool_at(const void* v) const {
            return ops->to_bool ? ops->to_bool(v, this) : false;
        }

        [[nodiscard]] size_t length_at(const void* v) const {
            return ops->length ? ops->length(v, this) : 0;
        }

        [[nodiscard]] bool contains_at(const void* container, const void* element) const {
            return ops->contains ? ops->contains(container, element, this) : false;
        }
    };

    /**
     * TypedPtr - A type-erased pointer with metadata
     *
     * Allows operating on any value through its TypeMeta.
     * This is a non-owning view.
     */
    struct TypedPtr {
        void* ptr{nullptr};
        const TypeMeta* meta{nullptr};

        TypedPtr() = default;
        TypedPtr(void* p, const TypeMeta* m) : ptr(p), meta(m) {}

        [[nodiscard]] bool valid() const { return ptr && meta; }

        // Value access (requires knowing the type)
        template<typename T>
        [[nodiscard]] T& as() { return *static_cast<T*>(ptr); }

        template<typename T>
        [[nodiscard]] const T& as() const { return *static_cast<const T*>(ptr); }

        // Type-erased operations
        void copy_from(const TypedPtr& src) const {
            if (valid() && src.valid() && meta == src.meta) {
                meta->copy_assign_at(ptr, src.ptr);
            }
        }

        [[nodiscard]] bool equals(const TypedPtr& other) const {
            if (!valid() || !other.valid() || meta != other.meta) return false;
            return meta->equals_at(ptr, other.ptr);
        }

        [[nodiscard]] size_t hash() const {
            return valid() ? meta->hash_at(ptr) : 0;
        }
    };

    /**
     * ConstTypedPtr - Const version of TypedPtr
     */
    struct ConstTypedPtr {
        const void* ptr{nullptr};
        const TypeMeta* meta{nullptr};

        ConstTypedPtr() = default;
        ConstTypedPtr(const void* p, const TypeMeta* m) : ptr(p), meta(m) {}
        ConstTypedPtr(const TypedPtr& tp) : ptr(tp.ptr), meta(tp.meta) {}

        [[nodiscard]] bool valid() const { return ptr && meta; }

        template<typename T>
        [[nodiscard]] const T& as() const { return *static_cast<const T*>(ptr); }

        [[nodiscard]] bool equals(const ConstTypedPtr& other) const {
            if (!valid() || !other.valid() || meta != other.meta) return false;
            return meta->equals_at(ptr, other.ptr);
        }

        [[nodiscard]] size_t hash() const {
            return valid() ? meta->hash_at(ptr) : 0;
        }
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_TYPE_META_H
