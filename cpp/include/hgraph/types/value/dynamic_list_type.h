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
            size_t needed = old_size + _element_type->size;

            // For trivially copyable types, simple resize is safe
            if (_element_type->is_trivially_copyable() || _count == 0) {
                _data.resize(needed);
            } else if (_data.capacity() < needed) {
                // For non-trivially copyable types, we must properly move-construct
                // existing elements when reallocating to avoid dangling pointers
                std::vector<char> new_data;
                // Reserve with some growth factor to avoid repeated reallocations
                new_data.reserve(std::max(needed, _data.capacity() * 2));
                new_data.resize(needed);

                // Move existing elements to new storage
                for (size_t i = 0; i < _count; ++i) {
                    void* old_ptr = _data.data() + i * _element_type->size;
                    void* new_ptr = new_data.data() + i * _element_type->size;
                    _element_type->move_construct_at(new_ptr, old_ptr);
                    _element_type->destruct_at(old_ptr);
                }

                _data = std::move(new_data);
            } else {
                // Enough capacity, just resize
                _data.resize(needed);
            }

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
            size_t needed = capacity * _element_type->size;
            if (_data.capacity() >= needed) {
                return;  // Already have enough capacity
            }

            // For trivially copyable types or empty list, simple reserve is safe
            if (_element_type->is_trivially_copyable() || _count == 0) {
                _data.reserve(needed);
                return;
            }

            // For non-trivially copyable types, we must properly move-construct
            // existing elements when reallocating
            std::vector<char> new_data;
            new_data.reserve(needed);
            new_data.resize(_data.size());

            // Move existing elements to new storage
            for (size_t i = 0; i < _count; ++i) {
                void* old_ptr = _data.data() + i * _element_type->size;
                void* new_ptr = new_data.data() + i * _element_type->size;
                _element_type->move_construct_at(new_ptr, old_ptr);
                _element_type->destruct_at(old_ptr);
            }

            _data = std::move(new_data);
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

        // --- List Operation Helper Methods ---

        // Create a copy of this list
        [[nodiscard]] DynamicListStorage clone() const {
            DynamicListStorage result(_element_type);
            result.reserve(_count);
            for (size_t i = 0; i < _count; ++i) {
                result.push_back(get(i));
            }
            return result;
        }

        // Concatenation: returns a new list with elements from both
        [[nodiscard]] DynamicListStorage concat_with(const DynamicListStorage& other) const {
            assert(_element_type == other._element_type);
            DynamicListStorage result(_element_type);
            result.reserve(_count + other._count);
            for (size_t i = 0; i < _count; ++i) {
                result.push_back(get(i));
            }
            for (size_t i = 0; i < other._count; ++i) {
                result.push_back(other.get(i));
            }
            return result;
        }

        // Slice: returns a new list with elements from start (inclusive) to end (exclusive)
        [[nodiscard]] DynamicListStorage slice(size_t start, size_t end) const {
            if (start > _count) start = _count;
            if (end > _count) end = _count;
            if (start >= end) return DynamicListStorage(_element_type);

            DynamicListStorage result(_element_type);
            result.reserve(end - start);
            for (size_t i = start; i < end; ++i) {
                result.push_back(get(i));
            }
            return result;
        }

        // Find index of element (returns nullopt if not found)
        [[nodiscard]] std::optional<size_t> index_of(const void* elem) const {
            for (size_t i = 0; i < _count; ++i) {
                if (_element_type->equals_at(get(i), elem)) {
                    return i;
                }
            }
            return std::nullopt;
        }

        // Count occurrences of element
        [[nodiscard]] size_t count(const void* elem) const {
            size_t result = 0;
            for (size_t i = 0; i < _count; ++i) {
                if (_element_type->equals_at(get(i), elem)) {
                    ++result;
                }
            }
            return result;
        }

        // Pop element at index, shifting remaining elements
        void pop_at(size_t idx) {
            if (idx >= _count) return;

            // Destruct the element at idx
            _element_type->destruct_at(get(idx));

            // Move subsequent elements down
            for (size_t i = idx + 1; i < _count; ++i) {
                void* dest = _data.data() + (i - 1) * _element_type->size;
                void* src = _data.data() + i * _element_type->size;
                _element_type->move_construct_at(dest, src);
                _element_type->destruct_at(src);
            }

            --_count;
            _data.resize(_count * _element_type->size);
        }

        // Pop last element
        void pop_back() {
            if (_count > 0) {
                pop_at(_count - 1);
            }
        }

        // Extend: in-place concatenation
        void extend(const DynamicListStorage& other) {
            assert(_element_type == other._element_type);
            reserve(_count + other._count);
            for (size_t i = 0; i < other._count; ++i) {
                push_back(other.get(i));
            }
        }

        // Reverse in place
        void reverse() {
            if (_count <= 1) return;

            // Use a temporary buffer for swapping
            std::vector<char> temp(_element_type->size);

            for (size_t i = 0; i < _count / 2; ++i) {
                size_t j = _count - 1 - i;
                void* a = get(i);
                void* b = get(j);

                // temp = a
                _element_type->move_construct_at(temp.data(), a);
                _element_type->destruct_at(a);

                // a = b
                _element_type->move_construct_at(a, b);
                _element_type->destruct_at(b);

                // b = temp
                _element_type->move_construct_at(b, temp.data());
                _element_type->destruct_at(temp.data());
            }
        }

        // Insert at index (shifts existing elements)
        void insert_at(size_t idx, const void* elem) {
            if (idx > _count) idx = _count;

            // Grow the storage
            size_t old_size = _data.size();
            size_t needed = old_size + _element_type->size;

            if (_element_type->is_trivially_copyable() || _count == 0) {
                _data.resize(needed);
            } else if (_data.capacity() < needed) {
                std::vector<char> new_data;
                new_data.reserve(std::max(needed, _data.capacity() * 2));
                new_data.resize(needed);

                for (size_t i = 0; i < _count; ++i) {
                    void* old_ptr = _data.data() + i * _element_type->size;
                    void* new_ptr = new_data.data() + i * _element_type->size;
                    _element_type->move_construct_at(new_ptr, old_ptr);
                    _element_type->destruct_at(old_ptr);
                }
                _data = std::move(new_data);
            } else {
                _data.resize(needed);
            }

            // Shift elements after idx to make room
            for (size_t i = _count; i > idx; --i) {
                void* src = _data.data() + (i - 1) * _element_type->size;
                void* dest = _data.data() + i * _element_type->size;
                _element_type->move_construct_at(dest, src);
                _element_type->destruct_at(src);
            }

            // Insert the new element
            void* dest = _data.data() + idx * _element_type->size;
            _element_type->copy_construct_at(dest, elem);
            ++_count;
        }

        // Remove first occurrence of element
        bool remove_first(const void* elem) {
            auto idx = index_of(elem);
            if (idx) {
                pop_at(*idx);
                return true;
            }
            return false;
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

        // Container operations
        static size_t length(const void* v, const TypeMeta*) {
            return static_cast<const DynamicListStorage*>(v)->size();
        }

        static bool contains(const void* container, const void* element, const TypeMeta* meta) {
            auto* list_meta = static_cast<const DynamicListTypeMeta*>(meta);
            auto* storage = static_cast<const DynamicListStorage*>(container);
            for (size_t i = 0; i < storage->size(); ++i) {
                if (list_meta->element_type->equals_at(storage->get(i), element)) {
                    return true;
                }
            }
            return false;
        }

        // Boolean conversion - non-empty lists are truthy
        static bool to_bool(const void* v, const TypeMeta*) {
            return !static_cast<const DynamicListStorage*>(v)->empty();
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
        // Arithmetic operations - not supported for lists
        .add = nullptr,
        .subtract = nullptr,
        .multiply = nullptr,
        .divide = nullptr,
        .floor_divide = nullptr,
        .modulo = nullptr,
        .power = nullptr,
        .negate = nullptr,
        .absolute = nullptr,
        .invert = nullptr,
        // Boolean/Container operations
        .to_bool = DynamicListTypeOps::to_bool,
        .length = DynamicListTypeOps::length,
        .contains = DynamicListTypeOps::contains,
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

            // Propagate flags from element type and add Container
            TypeFlags flags = TypeFlags::Container;
            if (has_flag(_element_type->flags, TypeFlags::Hashable)) {
                flags = flags | TypeFlags::Hashable;
            }
            if (has_flag(_element_type->flags, TypeFlags::Equatable)) {
                flags = flags | TypeFlags::Equatable;
            }
            meta->flags = flags;

            meta->kind = TypeKind::DynamicList;  // Variable-length list
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
