//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_SCALAR_TYPE_H
#define HGRAPH_VALUE_SCALAR_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <type_traits>
#include <cstring>

namespace hgraph::value {

    /**
     * ScalarTypeOps - Generate TypeOps for a scalar type T
     */
    template<typename T>
    struct ScalarTypeOps {
        static void construct(void* dest, const TypeMeta*) {
            new (dest) T{};
        }

        static void destruct(void* dest, const TypeMeta*) {
            static_cast<T*>(dest)->~T();
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta*) {
            new (dest) T(*static_cast<const T*>(src));
        }

        static void move_construct(void* dest, void* src, const TypeMeta*) {
            new (dest) T(std::move(*static_cast<T*>(src)));
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta*) {
            *static_cast<T*>(dest) = *static_cast<const T*>(src);
        }

        static void move_assign(void* dest, void* src, const TypeMeta*) {
            *static_cast<T*>(dest) = std::move(*static_cast<T*>(src));
        }

        static bool equals(const void* a, const void* b, const TypeMeta*) {
            if constexpr (requires(const T& x, const T& y) { x == y; }) {
                return *static_cast<const T*>(a) == *static_cast<const T*>(b);
            } else {
                return false;
            }
        }

        static bool less_than(const void* a, const void* b, const TypeMeta*) {
            if constexpr (requires(const T& x, const T& y) { x < y; }) {
                return *static_cast<const T*>(a) < *static_cast<const T*>(b);
            } else {
                return false;
            }
        }

        static size_t hash(const void* v, const TypeMeta*) {
            if constexpr (requires(const T& x) { std::hash<T>{}(x); }) {
                return std::hash<T>{}(*static_cast<const T*>(v));
            } else {
                return 0;
            }
        }

        static constexpr TypeOps ops = {
            .construct = construct,
            .destruct = destruct,
            .copy_construct = copy_construct,
            .move_construct = move_construct,
            .copy_assign = copy_assign,
            .move_assign = move_assign,
            .equals = equals,
            .less_than = less_than,
            .hash = hash,
            .to_python = nullptr,
            .from_python = nullptr,
        };
    };

    /**
     * Compute TypeFlags for a type T
     */
    template<typename T>
    constexpr TypeFlags compute_flags() {
        TypeFlags flags = TypeFlags::None;

        if constexpr (std::is_trivially_default_constructible_v<T>) {
            flags = flags | TypeFlags::TriviallyConstructible;
        }
        if constexpr (std::is_trivially_destructible_v<T>) {
            flags = flags | TypeFlags::TriviallyDestructible;
        }
        if constexpr (std::is_trivially_copyable_v<T>) {
            flags = flags | TypeFlags::TriviallyCopyable;
            flags = flags | TypeFlags::BufferCompatible;
        }
        if constexpr (requires(const T& x, const T& y) { x == y; }) {
            flags = flags | TypeFlags::Equatable;
        }
        if constexpr (requires(const T& x, const T& y) { x < y; x == y; }) {
            flags = flags | TypeFlags::Comparable;
        }
        if constexpr (requires(const T& x) { std::hash<T>{}(x); }) {
            flags = flags | TypeFlags::Hashable;
        }

        return flags;
    }

    /**
     * ScalarTypeMeta - TypeMeta for scalar types
     *
     * Usage:
     *   const TypeMeta* int_meta = ScalarTypeMeta<int>::get();
     */
    template<typename T>
    struct ScalarTypeMeta {
        static const TypeMeta instance;

        static const TypeMeta* get() { return &instance; }
    };

    template<typename T>
    const TypeMeta ScalarTypeMeta<T>::instance = {
        .size = sizeof(T),
        .alignment = alignof(T),
        .flags = compute_flags<T>(),
        .kind = TypeKind::Scalar,
        .ops = &ScalarTypeOps<T>::ops,
        .type_info = &typeid(T),
        .name = nullptr,
    };

    /**
     * Helper to get TypeMeta for any scalar type
     */
    template<typename T>
    const TypeMeta* scalar_type_meta() {
        return ScalarTypeMeta<T>::get();
    }

    /**
     * TypedValue - Owns storage for a value with its TypeMeta
     *
     * This provides isolated access to a single value within
     * potentially larger storage (e.g., a field in a bundle).
     */
    struct TypedValue {
        void* _storage{nullptr};
        const TypeMeta* _meta{nullptr};
        bool _owns_storage{false};

        TypedValue() = default;

        // Create with external storage (non-owning)
        TypedValue(void* storage, const TypeMeta* meta)
            : _storage(storage), _meta(meta), _owns_storage(false) {}

        // Create with owned storage
        static TypedValue create(const TypeMeta* meta) {
            TypedValue tv;
            tv._meta = meta;
            tv._storage = ::operator new(meta->size, std::align_val_t{meta->alignment});
            tv._owns_storage = true;
            meta->construct_at(tv._storage);
            return tv;
        }

        ~TypedValue() {
            if (_owns_storage && _storage && _meta) {
                _meta->destruct_at(_storage);
                ::operator delete(_storage, std::align_val_t{_meta->alignment});
            }
        }

        // Move only
        TypedValue(TypedValue&& other) noexcept
            : _storage(other._storage)
            , _meta(other._meta)
            , _owns_storage(other._owns_storage) {
            other._storage = nullptr;
            other._owns_storage = false;
        }

        TypedValue& operator=(TypedValue&& other) noexcept {
            if (this != &other) {
                if (_owns_storage && _storage && _meta) {
                    _meta->destruct_at(_storage);
                    ::operator delete(_storage, std::align_val_t{_meta->alignment});
                }
                _storage = other._storage;
                _meta = other._meta;
                _owns_storage = other._owns_storage;
                other._storage = nullptr;
                other._owns_storage = false;
            }
            return *this;
        }

        TypedValue(const TypedValue&) = delete;
        TypedValue& operator=(const TypedValue&) = delete;

        [[nodiscard]] bool valid() const { return _storage && _meta; }
        [[nodiscard]] const TypeMeta* meta() const { return _meta; }

        [[nodiscard]] TypedPtr ptr() { return {_storage, _meta}; }
        [[nodiscard]] ConstTypedPtr ptr() const { return {_storage, _meta}; }

        template<typename T>
        [[nodiscard]] T& as() { return *static_cast<T*>(_storage); }

        template<typename T>
        [[nodiscard]] const T& as() const { return *static_cast<const T*>(_storage); }

        void copy_from(const TypedValue& other) {
            if (valid() && other.valid() && _meta == other._meta) {
                _meta->copy_assign_at(_storage, other._storage);
            }
        }

        void copy_from(ConstTypedPtr src) {
            if (valid() && src.valid() && _meta == src.meta) {
                _meta->copy_assign_at(_storage, src.ptr);
            }
        }

        [[nodiscard]] bool equals(const TypedValue& other) const {
            if (!valid() || !other.valid() || _meta != other._meta) return false;
            return _meta->equals_at(_storage, other._storage);
        }

        [[nodiscard]] size_t hash() const {
            return valid() ? _meta->hash_at(_storage) : 0;
        }
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_SCALAR_TYPE_H
