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

        // Python interop (optional - can be nullptr)
        void* (*to_python)(const void* v, const TypeMeta* meta);
        void (*from_python)(void* dest, void* py_obj, const TypeMeta* meta);
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
        Scalar,     // Single value (int, double, etc.)
        List,       // Fixed or dynamic list
        Set,        // Hash set
        Dict,       // Hash map
        Bundle,     // Struct-like composite
        Ref,        // Reference to another value
        Window,     // Time-series history (fixed or variable length)
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
