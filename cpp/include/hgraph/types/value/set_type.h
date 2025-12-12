//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_SET_TYPE_H
#define HGRAPH_VALUE_SET_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <unordered_set>
#include <memory>
#include <cassert>
#include <vector>

namespace hgraph::value {

    /**
     * SetTypeMeta - Extended TypeMeta for set types
     *
     * Sets are dynamic collections that use type-erased storage.
     * The element type must be hashable and equatable.
     */
    struct SetTypeMeta : TypeMeta {
        const TypeMeta* element_type;
    };

    /**
     * SetStorage - Internal storage for a type-erased set
     *
     * Uses a vector of element storage + hash set of indices.
     * This allows type-erased element storage while maintaining
     * set semantics.
     */
    class SetStorage {
    public:
        SetStorage() = default;

        explicit SetStorage(const TypeMeta* elem_type)
            : _element_type(elem_type) {}

        ~SetStorage() {
            clear();
        }

        SetStorage(SetStorage&& other) noexcept
            : _element_type(other._element_type)
            , _elements(std::move(other._elements))
            , _index_set(std::move(other._index_set)) {
            other._element_type = nullptr;
        }

        SetStorage& operator=(SetStorage&& other) noexcept {
            if (this != &other) {
                clear();
                _element_type = other._element_type;
                _elements = std::move(other._elements);
                _index_set = std::move(other._index_set);
                other._element_type = nullptr;
            }
            return *this;
        }

        SetStorage(const SetStorage&) = delete;
        SetStorage& operator=(const SetStorage&) = delete;

        [[nodiscard]] const TypeMeta* element_type() const { return _element_type; }
        [[nodiscard]] size_t size() const { return _index_set.size(); }
        [[nodiscard]] bool empty() const { return _index_set.empty(); }

        // Add element (returns true if added, false if already present)
        bool add(const void* value) {
            if (!_element_type) return false;

            // Check if already present
            for (size_t idx : _index_set) {
                if (_element_type->equals_at(element_ptr(idx), value)) {
                    return false;  // Already exists
                }
            }

            // Add new element - index is element count, not byte offset
            size_t new_idx = _elements.size() / _element_type->size;
            _elements.resize(_elements.size() + _element_type->size);
            void* dest = element_ptr(new_idx);
            _element_type->copy_construct_at(dest, value);
            _index_set.insert(new_idx);
            return true;
        }

        // Remove element
        bool remove(const void* value) {
            if (!_element_type) return false;

            for (auto it = _index_set.begin(); it != _index_set.end(); ++it) {
                if (_element_type->equals_at(element_ptr(*it), value)) {
                    _element_type->destruct_at(element_ptr(*it));
                    _index_set.erase(it);
                    return true;
                }
            }
            return false;
        }

        // Check if element exists
        [[nodiscard]] bool contains(const void* value) const {
            if (!_element_type) return false;

            for (size_t idx : _index_set) {
                if (_element_type->equals_at(element_ptr(idx), value)) {
                    return true;
                }
            }
            return false;
        }

        // Clear all elements
        void clear() {
            if (_element_type) {
                for (size_t idx : _index_set) {
                    _element_type->destruct_at(element_ptr(idx));
                }
            }
            _elements.clear();
            _index_set.clear();
        }

        // Iteration support
        class Iterator {
        public:
            using SetIter = std::unordered_set<size_t>::const_iterator;

            Iterator(const SetStorage* storage, SetIter it)
                : _storage(storage), _it(it) {}

            ConstTypedPtr operator*() const {
                return {_storage->element_ptr(*_it), _storage->_element_type};
            }

            Iterator& operator++() {
                ++_it;
                return *this;
            }

            bool operator!=(const Iterator& other) const {
                return _it != other._it;
            }

        private:
            const SetStorage* _storage;
            SetIter _it;
        };

        [[nodiscard]] Iterator begin() const {
            return Iterator(this, _index_set.begin());
        }

        [[nodiscard]] Iterator end() const {
            return Iterator(this, _index_set.end());
        }

    private:
        [[nodiscard]] void* element_ptr(size_t idx) {
            return _elements.data() + idx * _element_type->size;
        }

        [[nodiscard]] const void* element_ptr(size_t idx) const {
            return _elements.data() + idx * _element_type->size;
        }

        const TypeMeta* _element_type{nullptr};
        std::vector<char> _elements;  // Raw storage for elements
        std::unordered_set<size_t> _index_set;  // Indices of active elements
    };

    /**
     * SetTypeOps - Operations for set types
     */
    struct SetTypeOps {
        static void construct(void* dest, const TypeMeta* meta) {
            auto* set_meta = static_cast<const SetTypeMeta*>(meta);
            new (dest) SetStorage(set_meta->element_type);
        }

        static void destruct(void* dest, const TypeMeta*) {
            static_cast<SetStorage*>(dest)->~SetStorage();
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta* meta) {
            auto* set_meta = static_cast<const SetTypeMeta*>(meta);
            auto* src_set = static_cast<const SetStorage*>(src);
            new (dest) SetStorage(set_meta->element_type);
            auto* dest_set = static_cast<SetStorage*>(dest);
            for (auto elem : *src_set) {
                dest_set->add(elem.ptr);
            }
        }

        static void move_construct(void* dest, void* src, const TypeMeta*) {
            new (dest) SetStorage(std::move(*static_cast<SetStorage*>(src)));
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta* meta) {
            auto* dest_set = static_cast<SetStorage*>(dest);
            auto* src_set = static_cast<const SetStorage*>(src);
            dest_set->clear();
            for (auto elem : *src_set) {
                dest_set->add(elem.ptr);
            }
        }

        static void move_assign(void* dest, void* src, const TypeMeta*) {
            *static_cast<SetStorage*>(dest) = std::move(*static_cast<SetStorage*>(src));
        }

        static bool equals(const void* a, const void* b, const TypeMeta*) {
            auto* set_a = static_cast<const SetStorage*>(a);
            auto* set_b = static_cast<const SetStorage*>(b);
            if (set_a->size() != set_b->size()) return false;
            for (auto elem : *set_a) {
                if (!set_b->contains(elem.ptr)) return false;
            }
            return true;
        }

        static bool less_than(const void* a, const void* b, const TypeMeta*) {
            // Sets don't have natural ordering - compare by size
            auto* set_a = static_cast<const SetStorage*>(a);
            auto* set_b = static_cast<const SetStorage*>(b);
            return set_a->size() < set_b->size();
        }

        static size_t hash(const void* v, const TypeMeta*) {
            auto* set = static_cast<const SetStorage*>(v);
            size_t result = 0;
            for (auto elem : *set) {
                // XOR hashes (order-independent)
                result ^= elem.hash();
            }
            return result;
        }

        static const TypeOps ops;
    };

    inline const TypeOps SetTypeOps::ops = {
        .construct = SetTypeOps::construct,
        .destruct = SetTypeOps::destruct,
        .copy_construct = SetTypeOps::copy_construct,
        .move_construct = SetTypeOps::move_construct,
        .copy_assign = SetTypeOps::copy_assign,
        .move_assign = SetTypeOps::move_assign,
        .equals = SetTypeOps::equals,
        .less_than = SetTypeOps::less_than,
        .hash = SetTypeOps::hash,
        .to_python = nullptr,
        .from_python = nullptr,
    };

    /**
     * SetTypeBuilder - Builds SetTypeMeta
     */
    class SetTypeBuilder {
    public:
        SetTypeBuilder& element_type(const TypeMeta* type) {
            _element_type = type;
            return *this;
        }

        template<typename T>
        SetTypeBuilder& element() {
            return element_type(scalar_type_meta<T>());
        }

        std::unique_ptr<SetTypeMeta> build(const char* type_name = nullptr) {
            assert(_element_type);
            assert(has_flag(_element_type->flags, TypeFlags::Hashable));
            assert(has_flag(_element_type->flags, TypeFlags::Equatable));

            auto meta = std::make_unique<SetTypeMeta>();

            meta->size = sizeof(SetStorage);
            meta->alignment = alignof(SetStorage);
            meta->flags = TypeFlags::Hashable | TypeFlags::Equatable;
            meta->kind = TypeKind::Set;
            meta->ops = &SetTypeOps::ops;
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->element_type = _element_type;

            return meta;
        }

    private:
        const TypeMeta* _element_type{nullptr};
    };

    /**
     * SetView - A value instance backed by a SetTypeMeta
     */
    class SetView {
    public:
        SetView() = default;

        explicit SetView(const SetTypeMeta* meta)
            : _meta(meta) {
            if (_meta) {
                _storage = new SetStorage(_meta->element_type);
                _owns_storage = true;
            }
        }

        // Create with external storage
        SetView(SetStorage* storage, const SetTypeMeta* meta)
            : _storage(storage), _meta(meta), _owns_storage(false) {}

        ~SetView() {
            if (_owns_storage && _storage) {
                delete _storage;
            }
        }

        // Move only
        SetView(SetView&& other) noexcept
            : _storage(other._storage)
            , _meta(other._meta)
            , _owns_storage(other._owns_storage) {
            other._storage = nullptr;
            other._owns_storage = false;
        }

        SetView& operator=(SetView&& other) noexcept {
            if (this != &other) {
                if (_owns_storage && _storage) {
                    delete _storage;
                }
                _storage = other._storage;
                _meta = other._meta;
                _owns_storage = other._owns_storage;
                other._storage = nullptr;
                other._owns_storage = false;
            }
            return *this;
        }

        SetView(const SetView&) = delete;
        SetView& operator=(const SetView&) = delete;

        [[nodiscard]] bool valid() const { return _storage && _meta; }
        [[nodiscard]] const SetTypeMeta* meta() const { return _meta; }
        [[nodiscard]] size_t size() const { return _storage ? _storage->size() : 0; }
        [[nodiscard]] bool empty() const { return !_storage || _storage->empty(); }

        [[nodiscard]] SetStorage* storage() { return _storage; }
        [[nodiscard]] const SetStorage* storage() const { return _storage; }

        // Typed operations
        template<typename T>
        bool add(const T& value) {
            return _storage ? _storage->add(&value) : false;
        }

        template<typename T>
        bool remove(const T& value) {
            return _storage ? _storage->remove(&value) : false;
        }

        template<typename T>
        [[nodiscard]] bool contains(const T& value) const {
            return _storage ? _storage->contains(&value) : false;
        }

        void clear() {
            if (_storage) _storage->clear();
        }

        // Iteration
        [[nodiscard]] SetStorage::Iterator begin() const {
            return _storage ? _storage->begin() : SetStorage::Iterator(nullptr, {});
        }

        [[nodiscard]] SetStorage::Iterator end() const {
            return _storage ? _storage->end() : SetStorage::Iterator(nullptr, {});
        }

    private:
        SetStorage* _storage{nullptr};
        const SetTypeMeta* _meta{nullptr};
        bool _owns_storage{false};
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_SET_TYPE_H
