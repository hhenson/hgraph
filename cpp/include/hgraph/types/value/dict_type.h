//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_DICT_TYPE_H
#define HGRAPH_VALUE_DICT_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <memory>
#include <cassert>
#include <vector>

namespace hgraph::value {

    /**
     * DictTypeMeta - Extended TypeMeta for dictionary types
     *
     * Dictionaries are dynamic key-value collections with type-erased storage.
     * Key type must be hashable and equatable.
     */
    struct DictTypeMeta : TypeMeta {
        const TypeMeta* key_type;
        const TypeMeta* value_type;
    };

    /**
     * DictStorage - Internal storage for a type-erased dictionary
     *
     * Uses parallel vectors for keys and values with an index set for lookup.
     */
    class DictStorage {
    public:
        DictStorage() = default;

        DictStorage(const TypeMeta* key_type, const TypeMeta* value_type)
            : _key_type(key_type), _value_type(value_type) {}

        ~DictStorage() {
            clear();
        }

        DictStorage(DictStorage&& other) noexcept
            : _key_type(other._key_type)
            , _value_type(other._value_type)
            , _keys(std::move(other._keys))
            , _values(std::move(other._values))
            , _active_indices(std::move(other._active_indices)) {
            other._key_type = nullptr;
            other._value_type = nullptr;
        }

        DictStorage& operator=(DictStorage&& other) noexcept {
            if (this != &other) {
                clear();
                _key_type = other._key_type;
                _value_type = other._value_type;
                _keys = std::move(other._keys);
                _values = std::move(other._values);
                _active_indices = std::move(other._active_indices);
                other._key_type = nullptr;
                other._value_type = nullptr;
            }
            return *this;
        }

        DictStorage(const DictStorage&) = delete;
        DictStorage& operator=(const DictStorage&) = delete;

        [[nodiscard]] const TypeMeta* key_type() const { return _key_type; }
        [[nodiscard]] const TypeMeta* value_type() const { return _value_type; }
        [[nodiscard]] size_t size() const { return _active_indices.size(); }
        [[nodiscard]] bool empty() const { return _active_indices.empty(); }

        // Insert or update a key-value pair
        void insert(const void* key, const void* value) {
            if (!_key_type || !_value_type) return;

            // Check if key exists
            for (size_t idx : _active_indices) {
                if (_key_type->equals_at(key_ptr(idx), key)) {
                    // Update existing value
                    _value_type->copy_assign_at(value_ptr(idx), value);
                    return;
                }
            }

            // Add new entry
            size_t new_idx = _keys.size() / _key_type->size;
            _keys.resize(_keys.size() + _key_type->size);
            _values.resize(_values.size() + _value_type->size);

            _key_type->copy_construct_at(key_ptr(new_idx), key);
            _value_type->copy_construct_at(value_ptr(new_idx), value);
            _active_indices.push_back(new_idx);
        }

        // Remove by key
        bool remove(const void* key) {
            if (!_key_type) return false;

            for (auto it = _active_indices.begin(); it != _active_indices.end(); ++it) {
                if (_key_type->equals_at(key_ptr(*it), key)) {
                    _key_type->destruct_at(key_ptr(*it));
                    _value_type->destruct_at(value_ptr(*it));
                    _active_indices.erase(it);
                    return true;
                }
            }
            return false;
        }

        // Check if key exists
        [[nodiscard]] bool contains(const void* key) const {
            if (!_key_type) return false;

            for (size_t idx : _active_indices) {
                if (_key_type->equals_at(key_ptr(idx), key)) {
                    return true;
                }
            }
            return false;
        }

        // Get value by key (returns nullptr if not found)
        [[nodiscard]] void* get(const void* key) {
            if (!_key_type) return nullptr;

            for (size_t idx : _active_indices) {
                if (_key_type->equals_at(key_ptr(idx), key)) {
                    return value_ptr(idx);
                }
            }
            return nullptr;
        }

        [[nodiscard]] const void* get(const void* key) const {
            if (!_key_type) return nullptr;

            for (size_t idx : _active_indices) {
                if (_key_type->equals_at(key_ptr(idx), key)) {
                    return value_ptr(idx);
                }
            }
            return nullptr;
        }

        // Get typed pointer to value
        [[nodiscard]] TypedPtr get_typed(const void* key) {
            void* val = get(key);
            return val ? TypedPtr{val, _value_type} : TypedPtr{};
        }

        [[nodiscard]] ConstTypedPtr get_typed(const void* key) const {
            const void* val = get(key);
            return val ? ConstTypedPtr{val, _value_type} : ConstTypedPtr{};
        }

        // Clear all entries
        void clear() {
            if (_key_type && _value_type) {
                for (size_t idx : _active_indices) {
                    _key_type->destruct_at(key_ptr(idx));
                    _value_type->destruct_at(value_ptr(idx));
                }
            }
            _keys.clear();
            _values.clear();
            _active_indices.clear();
        }

        // Iteration support
        struct KeyValuePair {
            ConstTypedPtr key;
            TypedPtr value;
        };

        struct ConstKeyValuePair {
            ConstTypedPtr key;
            ConstTypedPtr value;
        };

        class Iterator {
        public:
            using IndexIter = std::vector<size_t>::const_iterator;

            Iterator(DictStorage* storage, IndexIter it)
                : _storage(storage), _it(it) {}

            KeyValuePair operator*() const {
                return {
                    {_storage->key_ptr(*_it), _storage->_key_type},
                    {_storage->value_ptr(*_it), _storage->_value_type}
                };
            }

            Iterator& operator++() {
                ++_it;
                return *this;
            }

            bool operator!=(const Iterator& other) const {
                return _it != other._it;
            }

        private:
            DictStorage* _storage;
            IndexIter _it;
        };

        class ConstIterator {
        public:
            using IndexIter = std::vector<size_t>::const_iterator;

            ConstIterator(const DictStorage* storage, IndexIter it)
                : _storage(storage), _it(it) {}

            ConstKeyValuePair operator*() const {
                return {
                    {_storage->key_ptr(*_it), _storage->_key_type},
                    {_storage->value_ptr(*_it), _storage->_value_type}
                };
            }

            ConstIterator& operator++() {
                ++_it;
                return *this;
            }

            bool operator!=(const ConstIterator& other) const {
                return _it != other._it;
            }

        private:
            const DictStorage* _storage;
            IndexIter _it;
        };

        [[nodiscard]] Iterator begin() {
            return Iterator(this, _active_indices.begin());
        }

        [[nodiscard]] Iterator end() {
            return Iterator(this, _active_indices.end());
        }

        [[nodiscard]] ConstIterator begin() const {
            return ConstIterator(this, _active_indices.begin());
        }

        [[nodiscard]] ConstIterator end() const {
            return ConstIterator(this, _active_indices.end());
        }

    private:
        [[nodiscard]] void* key_ptr(size_t idx) {
            return _keys.data() + idx * _key_type->size;
        }

        [[nodiscard]] const void* key_ptr(size_t idx) const {
            return _keys.data() + idx * _key_type->size;
        }

        [[nodiscard]] void* value_ptr(size_t idx) {
            return _values.data() + idx * _value_type->size;
        }

        [[nodiscard]] const void* value_ptr(size_t idx) const {
            return _values.data() + idx * _value_type->size;
        }

        const TypeMeta* _key_type{nullptr};
        const TypeMeta* _value_type{nullptr};
        std::vector<char> _keys;
        std::vector<char> _values;
        std::vector<size_t> _active_indices;
    };

    /**
     * DictTypeOps - Operations for dict types
     */
    struct DictTypeOps {
        static void construct(void* dest, const TypeMeta* meta) {
            auto* dict_meta = static_cast<const DictTypeMeta*>(meta);
            new (dest) DictStorage(dict_meta->key_type, dict_meta->value_type);
        }

        static void destruct(void* dest, const TypeMeta*) {
            static_cast<DictStorage*>(dest)->~DictStorage();
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta* meta) {
            auto* dict_meta = static_cast<const DictTypeMeta*>(meta);
            auto* src_dict = static_cast<const DictStorage*>(src);
            new (dest) DictStorage(dict_meta->key_type, dict_meta->value_type);
            auto* dest_dict = static_cast<DictStorage*>(dest);
            for (auto kv : *src_dict) {
                dest_dict->insert(kv.key.ptr, kv.value.ptr);
            }
        }

        static void move_construct(void* dest, void* src, const TypeMeta*) {
            new (dest) DictStorage(std::move(*static_cast<DictStorage*>(src)));
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta*) {
            auto* dest_dict = static_cast<DictStorage*>(dest);
            auto* src_dict = static_cast<const DictStorage*>(src);
            dest_dict->clear();
            for (auto kv : *src_dict) {
                dest_dict->insert(kv.key.ptr, kv.value.ptr);
            }
        }

        static void move_assign(void* dest, void* src, const TypeMeta*) {
            *static_cast<DictStorage*>(dest) = std::move(*static_cast<DictStorage*>(src));
        }

        static bool equals(const void* a, const void* b, const TypeMeta*) {
            auto* dict_a = static_cast<const DictStorage*>(a);
            auto* dict_b = static_cast<const DictStorage*>(b);
            if (dict_a->size() != dict_b->size()) return false;

            for (auto kv_a : *dict_a) {
                auto val_b = dict_b->get_typed(kv_a.key.ptr);
                if (!val_b.valid()) return false;
                if (!kv_a.value.equals(val_b)) return false;
            }
            return true;
        }

        static bool less_than(const void* a, const void* b, const TypeMeta*) {
            // Dicts don't have natural ordering - compare by size
            auto* dict_a = static_cast<const DictStorage*>(a);
            auto* dict_b = static_cast<const DictStorage*>(b);
            return dict_a->size() < dict_b->size();
        }

        static size_t hash(const void* v, const TypeMeta*) {
            auto* dict = static_cast<const DictStorage*>(v);
            size_t result = 0;
            for (auto kv : *dict) {
                // Combine key and value hashes (order-independent via XOR)
                size_t pair_hash = kv.key.hash() ^ (kv.value.hash() * 31);
                result ^= pair_hash;
            }
            return result;
        }

        static const TypeOps ops;
    };

    inline const TypeOps DictTypeOps::ops = {
        .construct = DictTypeOps::construct,
        .destruct = DictTypeOps::destruct,
        .copy_construct = DictTypeOps::copy_construct,
        .move_construct = DictTypeOps::move_construct,
        .copy_assign = DictTypeOps::copy_assign,
        .move_assign = DictTypeOps::move_assign,
        .equals = DictTypeOps::equals,
        .less_than = DictTypeOps::less_than,
        .hash = DictTypeOps::hash,
        .to_python = nullptr,
        .from_python = nullptr,
    };

    /**
     * DictTypeBuilder - Builds DictTypeMeta
     */
    class DictTypeBuilder {
    public:
        DictTypeBuilder& key_type(const TypeMeta* type) {
            _key_type = type;
            return *this;
        }

        DictTypeBuilder& value_type(const TypeMeta* type) {
            _value_type = type;
            return *this;
        }

        template<typename K>
        DictTypeBuilder& key() {
            return key_type(scalar_type_meta<K>());
        }

        template<typename V>
        DictTypeBuilder& value() {
            return value_type(scalar_type_meta<V>());
        }

        std::unique_ptr<DictTypeMeta> build(const char* type_name = nullptr) {
            assert(_key_type && _value_type);
            assert(has_flag(_key_type->flags, TypeFlags::Hashable));
            assert(has_flag(_key_type->flags, TypeFlags::Equatable));

            auto meta = std::make_unique<DictTypeMeta>();

            // Value hashability determines dict hashability
            TypeFlags flags = TypeFlags::Equatable;
            if (has_flag(_value_type->flags, TypeFlags::Hashable)) {
                flags = flags | TypeFlags::Hashable;
            }

            meta->size = sizeof(DictStorage);
            meta->alignment = alignof(DictStorage);
            meta->flags = flags;
            meta->kind = TypeKind::Dict;
            meta->ops = &DictTypeOps::ops;
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->key_type = _key_type;
            meta->value_type = _value_type;

            return meta;
        }

    private:
        const TypeMeta* _key_type{nullptr};
        const TypeMeta* _value_type{nullptr};
    };

    /**
     * DictView - A value instance backed by a DictTypeMeta
     */
    class DictView {
    public:
        DictView() = default;

        explicit DictView(const DictTypeMeta* meta)
            : _meta(meta) {
            if (_meta) {
                _storage = new DictStorage(_meta->key_type, _meta->value_type);
                _owns_storage = true;
            }
        }

        // Create with external storage
        DictView(DictStorage* storage, const DictTypeMeta* meta)
            : _storage(storage), _meta(meta), _owns_storage(false) {}

        ~DictView() {
            if (_owns_storage && _storage) {
                delete _storage;
            }
        }

        // Move only
        DictView(DictView&& other) noexcept
            : _storage(other._storage)
            , _meta(other._meta)
            , _owns_storage(other._owns_storage) {
            other._storage = nullptr;
            other._owns_storage = false;
        }

        DictView& operator=(DictView&& other) noexcept {
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

        DictView(const DictView&) = delete;
        DictView& operator=(const DictView&) = delete;

        [[nodiscard]] bool valid() const { return _storage && _meta; }
        [[nodiscard]] const DictTypeMeta* meta() const { return _meta; }
        [[nodiscard]] size_t size() const { return _storage ? _storage->size() : 0; }
        [[nodiscard]] bool empty() const { return !_storage || _storage->empty(); }

        [[nodiscard]] DictStorage* storage() { return _storage; }
        [[nodiscard]] const DictStorage* storage() const { return _storage; }

        // Typed operations
        template<typename K, typename V>
        void insert(const K& key, const V& value) {
            if (_storage) _storage->insert(&key, &value);
        }

        template<typename K>
        bool remove(const K& key) {
            return _storage ? _storage->remove(&key) : false;
        }

        template<typename K>
        [[nodiscard]] bool contains(const K& key) const {
            return _storage ? _storage->contains(&key) : false;
        }

        template<typename K, typename V>
        [[nodiscard]] V* get(const K& key) {
            if (!_storage) return nullptr;
            return static_cast<V*>(_storage->get(&key));
        }

        template<typename K, typename V>
        [[nodiscard]] const V* get(const K& key) const {
            if (!_storage) return nullptr;
            return static_cast<const V*>(_storage->get(&key));
        }

        template<typename K>
        [[nodiscard]] TypedPtr get_typed(const K& key) {
            return _storage ? _storage->get_typed(&key) : TypedPtr{};
        }

        template<typename K>
        [[nodiscard]] ConstTypedPtr get_typed(const K& key) const {
            return _storage ? _storage->get_typed(&key) : ConstTypedPtr{};
        }

        void clear() {
            if (_storage) _storage->clear();
        }

        // Iteration
        [[nodiscard]] DictStorage::Iterator begin() {
            return _storage ? _storage->begin() : DictStorage::Iterator(nullptr, {});
        }

        [[nodiscard]] DictStorage::Iterator end() {
            return _storage ? _storage->end() : DictStorage::Iterator(nullptr, {});
        }

        [[nodiscard]] DictStorage::ConstIterator cbegin() const {
            return _storage ? static_cast<const DictStorage*>(_storage)->begin()
                            : DictStorage::ConstIterator(nullptr, {});
        }

        [[nodiscard]] DictStorage::ConstIterator cend() const {
            return _storage ? static_cast<const DictStorage*>(_storage)->end()
                            : DictStorage::ConstIterator(nullptr, {});
        }

    private:
        DictStorage* _storage{nullptr};
        const DictTypeMeta* _meta{nullptr};
        bool _owns_storage{false};
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_DICT_TYPE_H
