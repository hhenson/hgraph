//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_LIST_TYPE_H
#define HGRAPH_VALUE_LIST_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <memory>
#include <cassert>

namespace hgraph::value {

    /**
     * ListTypeMeta - Extended TypeMeta for fixed-size list types
     *
     * Memory layout: contiguous array of elements
     *   [elem0, elem1, elem2, ..., elemN-1]
     */
    struct ListTypeMeta : TypeMeta {
        const TypeMeta* element_type;
        size_t count;

        // Get typed pointer to an element
        [[nodiscard]] TypedPtr element_ptr(void* list_storage, size_t index) const {
            if (index >= count) return {};
            char* base = static_cast<char*>(list_storage);
            return {base + index * element_type->size, element_type};
        }

        [[nodiscard]] ConstTypedPtr element_ptr(const void* list_storage, size_t index) const {
            if (index >= count) return {};
            const char* base = static_cast<const char*>(list_storage);
            return {base + index * element_type->size, element_type};
        }
    };

    /**
     * ListTypeOps - Operations for list types
     */
    struct ListTypeOps {
        static void construct(void* dest, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            char* ptr = static_cast<char*>(dest);
            for (size_t i = 0; i < list_meta->count; ++i) {
                list_meta->element_type->construct_at(ptr);
                ptr += list_meta->element_type->size;
            }
        }

        static void destruct(void* dest, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            char* ptr = static_cast<char*>(dest);
            // Destruct in reverse order
            ptr += (list_meta->count - 1) * list_meta->element_type->size;
            for (size_t i = list_meta->count; i > 0; --i) {
                list_meta->element_type->destruct_at(ptr);
                ptr -= list_meta->element_type->size;
            }
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            char* d = static_cast<char*>(dest);
            const char* s = static_cast<const char*>(src);
            for (size_t i = 0; i < list_meta->count; ++i) {
                list_meta->element_type->copy_construct_at(d, s);
                d += list_meta->element_type->size;
                s += list_meta->element_type->size;
            }
        }

        static void move_construct(void* dest, void* src, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            char* d = static_cast<char*>(dest);
            char* s = static_cast<char*>(src);
            for (size_t i = 0; i < list_meta->count; ++i) {
                list_meta->element_type->move_construct_at(d, s);
                d += list_meta->element_type->size;
                s += list_meta->element_type->size;
            }
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            char* d = static_cast<char*>(dest);
            const char* s = static_cast<const char*>(src);
            for (size_t i = 0; i < list_meta->count; ++i) {
                list_meta->element_type->copy_assign_at(d, s);
                d += list_meta->element_type->size;
                s += list_meta->element_type->size;
            }
        }

        static void move_assign(void* dest, void* src, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            char* d = static_cast<char*>(dest);
            char* s = static_cast<char*>(src);
            for (size_t i = 0; i < list_meta->count; ++i) {
                list_meta->element_type->move_assign_at(d, s);
                d += list_meta->element_type->size;
                s += list_meta->element_type->size;
            }
        }

        static bool equals(const void* a, const void* b, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            const char* pa = static_cast<const char*>(a);
            const char* pb = static_cast<const char*>(b);
            for (size_t i = 0; i < list_meta->count; ++i) {
                if (!list_meta->element_type->equals_at(pa, pb)) {
                    return false;
                }
                pa += list_meta->element_type->size;
                pb += list_meta->element_type->size;
            }
            return true;
        }

        static bool less_than(const void* a, const void* b, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            const char* pa = static_cast<const char*>(a);
            const char* pb = static_cast<const char*>(b);
            for (size_t i = 0; i < list_meta->count; ++i) {
                if (list_meta->element_type->less_than_at(pa, pb)) return true;
                if (list_meta->element_type->less_than_at(pb, pa)) return false;
                pa += list_meta->element_type->size;
                pb += list_meta->element_type->size;
            }
            return false;
        }

        static size_t hash(const void* v, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            const char* ptr = static_cast<const char*>(v);
            size_t result = 0;
            for (size_t i = 0; i < list_meta->count; ++i) {
                size_t elem_hash = list_meta->element_type->hash_at(ptr);
                result ^= elem_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
                ptr += list_meta->element_type->size;
            }
            return result;
        }

        static const TypeOps ops;
    };

    inline const TypeOps ListTypeOps::ops = {
        .construct = ListTypeOps::construct,
        .destruct = ListTypeOps::destruct,
        .copy_construct = ListTypeOps::copy_construct,
        .move_construct = ListTypeOps::move_construct,
        .copy_assign = ListTypeOps::copy_assign,
        .move_assign = ListTypeOps::move_assign,
        .equals = ListTypeOps::equals,
        .less_than = ListTypeOps::less_than,
        .hash = ListTypeOps::hash,
        .to_python = nullptr,
        .from_python = nullptr,
    };

    /**
     * ListTypeBuilder - Builds ListTypeMeta
     */
    class ListTypeBuilder {
    public:
        ListTypeBuilder& element_type(const TypeMeta* type) {
            _element_type = type;
            return *this;
        }

        template<typename T>
        ListTypeBuilder& element() {
            return element_type(scalar_type_meta<T>());
        }

        ListTypeBuilder& count(size_t n) {
            _count = n;
            return *this;
        }

        std::unique_ptr<ListTypeMeta> build(const char* type_name = nullptr) {
            assert(_element_type && _count > 0);

            auto meta = std::make_unique<ListTypeMeta>();

            // Inherit flags from element type
            TypeFlags flags = _element_type->flags;

            meta->size = _element_type->size * _count;
            meta->alignment = _element_type->alignment;
            meta->flags = flags;
            meta->kind = TypeKind::List;
            meta->ops = &ListTypeOps::ops;
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;  // Lists are not numpy-compatible
            meta->element_type = _element_type;
            meta->count = _count;

            return meta;
        }

    private:
        const TypeMeta* _element_type{nullptr};
        size_t _count{0};
    };

    /**
     * ListView - A value instance backed by a ListTypeMeta
     *
     * Provides isolated element access.
     */
    class ListView {
    public:
        ListView() = default;

        explicit ListView(const ListTypeMeta* meta)
            : _meta(meta) {
            if (_meta && _meta->size > 0) {
                _storage = ::operator new(_meta->size, std::align_val_t{_meta->alignment});
                _meta->construct_at(_storage);
                _owns_storage = true;
            }
        }

        // Create with external storage
        ListView(void* storage, const ListTypeMeta* meta)
            : _storage(storage), _meta(meta), _owns_storage(false) {}

        ~ListView() {
            if (_owns_storage && _storage && _meta) {
                _meta->destruct_at(_storage);
                ::operator delete(_storage, std::align_val_t{_meta->alignment});
            }
        }

        // Move only
        ListView(ListView&& other) noexcept
            : _storage(other._storage)
            , _meta(other._meta)
            , _owns_storage(other._owns_storage) {
            other._storage = nullptr;
            other._owns_storage = false;
        }

        ListView& operator=(ListView&& other) noexcept {
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

        ListView(const ListView&) = delete;
        ListView& operator=(const ListView&) = delete;

        [[nodiscard]] bool valid() const { return _storage && _meta; }
        [[nodiscard]] const ListTypeMeta* meta() const { return _meta; }
        [[nodiscard]] size_t size() const { return _meta ? _meta->count : 0; }

        [[nodiscard]] void* storage() { return _storage; }
        [[nodiscard]] const void* storage() const { return _storage; }

        // Element access
        [[nodiscard]] TypedPtr at(size_t index) {
            return _meta ? _meta->element_ptr(_storage, index) : TypedPtr{};
        }

        [[nodiscard]] ConstTypedPtr at(size_t index) const {
            return _meta ? _meta->element_ptr(_storage, index) : ConstTypedPtr{};
        }

        // Typed element access
        template<typename T>
        [[nodiscard]] T& get(size_t index) {
            return at(index).as<T>();
        }

        template<typename T>
        [[nodiscard]] const T& get(size_t index) const {
            return at(index).as<T>();
        }

        template<typename T>
        void set(size_t index, const T& value) {
            auto elem = at(index);
            if (elem.valid()) elem.as<T>() = value;
        }

        // Buffer access (only if buffer compatible)
        [[nodiscard]] BufferInfo buffer_info() const {
            if (!valid() || !_meta->is_buffer_compatible()) {
                return {};
            }
            return BufferInfo{
                .ptr = _storage,
                .itemsize = _meta->element_type->size,
                .count = _meta->count,
                .readonly = false,
            };
        }

    private:
        void* _storage{nullptr};
        const ListTypeMeta* _meta{nullptr};
        bool _owns_storage{false};
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_LIST_TYPE_H
