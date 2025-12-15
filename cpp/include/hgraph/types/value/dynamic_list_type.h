//
// Created by Claude on 15/12/2025.
//
// DynamicListTypeMeta - TypeMeta for variable-length lists (tuple[T, ...])
//

#ifndef HGRAPH_VALUE_DYNAMIC_LIST_TYPE_H
#define HGRAPH_VALUE_DYNAMIC_LIST_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <vector>
#include <memory>
#include <cassert>

namespace hgraph::value {

    /**
     * DynamicListStorage - Variable-length list storage using type-erased vector
     *
     * Stores elements contiguously with type-erased operations via TypeMeta.
     * Similar to SetStorage but maintains insertion order and allows duplicates.
     */
    class DynamicListStorage {
    public:
        explicit DynamicListStorage(const TypeMeta* element_type)
            : _element_type(element_type) {}

        ~DynamicListStorage() {
            clear();
        }

        // Copy constructor
        DynamicListStorage(const DynamicListStorage& other)
            : _element_type(other._element_type) {
            reserve(other._count);
            for (size_t i = 0; i < other._count; ++i) {
                push_back(other.get(i));
            }
        }

        // Move constructor
        DynamicListStorage(DynamicListStorage&& other) noexcept
            : _element_type(other._element_type)
            , _data(std::move(other._data))
            , _count(other._count) {
            other._count = 0;
        }

        // Copy assignment
        DynamicListStorage& operator=(const DynamicListStorage& other) {
            if (this != &other) {
                clear();
                _element_type = other._element_type;
                reserve(other._count);
                for (size_t i = 0; i < other._count; ++i) {
                    push_back(other.get(i));
                }
            }
            return *this;
        }

        // Move assignment
        DynamicListStorage& operator=(DynamicListStorage&& other) noexcept {
            if (this != &other) {
                clear();
                _element_type = other._element_type;
                _data = std::move(other._data);
                _count = other._count;
                other._count = 0;
            }
            return *this;
        }

        [[nodiscard]] size_t size() const { return _count; }
        [[nodiscard]] bool empty() const { return _count == 0; }
        [[nodiscard]] const TypeMeta* element_type() const { return _element_type; }

        void* get(size_t index) {
            assert(index < _count);
            return _data.data() + index * _element_type->size;
        }

        [[nodiscard]] const void* get(size_t index) const {
            assert(index < _count);
            return _data.data() + index * _element_type->size;
        }

        void push_back(const void* element) {
            size_t old_size = _data.size();
            _data.resize(old_size + _element_type->size);
            void* dest = _data.data() + old_size;
            _element_type->copy_construct_at(dest, element);
            ++_count;
        }

        void clear() {
            if (_element_type && !_element_type->is_trivially_destructible()) {
                for (size_t i = 0; i < _count; ++i) {
                    _element_type->destruct_at(get(i));
                }
            }
            _data.clear();
            _count = 0;
        }

        void reserve(size_t capacity) {
            _data.reserve(capacity * _element_type->size);
        }

        // Equality
        bool operator==(const DynamicListStorage& other) const {
            if (_count != other._count) return false;
            if (_element_type != other._element_type) return false;
            for (size_t i = 0; i < _count; ++i) {
                if (!_element_type->equals_at(get(i), other.get(i))) {
                    return false;
                }
            }
            return true;
        }

        bool operator!=(const DynamicListStorage& other) const {
            return !(*this == other);
        }

        // Hash
        [[nodiscard]] size_t hash() const {
            size_t h = 0;
            for (size_t i = 0; i < _count; ++i) {
                h = h * 31 + _element_type->hash_at(get(i));
            }
            return h;
        }

    private:
        const TypeMeta* _element_type;
        std::vector<char> _data;  // Type-erased storage
        size_t _count{0};
    };

    /**
     * DynamicListTypeMeta - TypeMeta for variable-length lists
     *
     * Used for Python's tuple[T, ...] which represents a variable-length
     * homogeneous sequence.
     */
    struct DynamicListTypeMeta : TypeMeta {
        const TypeMeta* element_type;
    };

    /**
     * DynamicListTypeOps - Operations for dynamic list types
     */
    struct DynamicListTypeOps {
        static void construct(void* dest, const TypeMeta* meta) {
            auto* list_meta = static_cast<const DynamicListTypeMeta*>(meta);
            new (dest) DynamicListStorage(list_meta->element_type);
        }

        static void destruct(void* dest, const TypeMeta*) {
            static_cast<DynamicListStorage*>(dest)->~DynamicListStorage();
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta*) {
            new (dest) DynamicListStorage(*static_cast<const DynamicListStorage*>(src));
        }

        static void move_construct(void* dest, void* src, const TypeMeta*) {
            new (dest) DynamicListStorage(std::move(*static_cast<DynamicListStorage*>(src)));
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta*) {
            *static_cast<DynamicListStorage*>(dest) = *static_cast<const DynamicListStorage*>(src);
        }

        static void move_assign(void* dest, void* src, const TypeMeta*) {
            *static_cast<DynamicListStorage*>(dest) = std::move(*static_cast<DynamicListStorage*>(src));
        }

        static bool equals(const void* a, const void* b, const TypeMeta*) {
            return *static_cast<const DynamicListStorage*>(a) == *static_cast<const DynamicListStorage*>(b);
        }

        static bool less_than(const void* a, const void* b, const TypeMeta*) {
            // Compare by hash for ordering
            return static_cast<const DynamicListStorage*>(a)->hash() <
                   static_cast<const DynamicListStorage*>(b)->hash();
        }

        static size_t hash(const void* v, const TypeMeta*) {
            return static_cast<const DynamicListStorage*>(v)->hash();
        }

        static std::string to_string(const void* v, const TypeMeta* meta) {
            auto* list_meta = static_cast<const DynamicListTypeMeta*>(meta);
            auto* storage = static_cast<const DynamicListStorage*>(v);

            std::string result = "[";
            for (size_t i = 0; i < storage->size(); ++i) {
                if (i > 0) result += ", ";
                result += list_meta->element_type->to_string_at(storage->get(i));
            }
            result += "]";
            return result;
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* list_meta = static_cast<const DynamicListTypeMeta*>(meta);
            return "DynamicList[" + list_meta->element_type->type_name_str() + "]";
        }

        static const TypeOps ops;
    };

    inline const TypeOps DynamicListTypeOps::ops = {
        .construct = DynamicListTypeOps::construct,
        .destruct = DynamicListTypeOps::destruct,
        .copy_construct = DynamicListTypeOps::copy_construct,
        .move_construct = DynamicListTypeOps::move_construct,
        .copy_assign = DynamicListTypeOps::copy_assign,
        .move_assign = DynamicListTypeOps::move_assign,
        .equals = DynamicListTypeOps::equals,
        .less_than = DynamicListTypeOps::less_than,
        .hash = DynamicListTypeOps::hash,
        .to_string = DynamicListTypeOps::to_string,
        .type_name = DynamicListTypeOps::type_name,
        .to_python = nullptr,  // Set by DynamicListTypeOpsWithPython
        .from_python = nullptr,
    };

    /**
     * DynamicListTypeBuilder - Builds DynamicListTypeMeta
     *
     * Usage:
     *   auto meta = DynamicListTypeBuilder()
     *       .element_type(int_type_meta)
     *       .build("IntList");
     */
    class DynamicListTypeBuilder {
    public:
        DynamicListTypeBuilder& element_type(const TypeMeta* type) {
            _element_type = type;
            return *this;
        }

        std::unique_ptr<DynamicListTypeMeta> build(const char* type_name = nullptr) {
            assert(_element_type);

            auto meta = std::make_unique<DynamicListTypeMeta>();

            meta->size = sizeof(DynamicListStorage);
            meta->alignment = alignof(DynamicListStorage);

            // Propagate flags from element type
            TypeFlags flags = TypeFlags::None;
            if (has_flag(_element_type->flags, TypeFlags::Hashable)) {
                flags = flags | TypeFlags::Hashable;
            }
            if (has_flag(_element_type->flags, TypeFlags::Equatable)) {
                flags = flags | TypeFlags::Equatable;
            }
            meta->flags = flags;

            meta->kind = TypeKind::List;  // Reuse List kind for dynamic lists
            meta->ops = &DynamicListTypeOps::ops;
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;  // Dynamic lists are not numpy-compatible
            meta->element_type = _element_type;

            return meta;
        }

    private:
        const TypeMeta* _element_type{nullptr};
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_DYNAMIC_LIST_TYPE_H
