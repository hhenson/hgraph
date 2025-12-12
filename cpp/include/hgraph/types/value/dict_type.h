//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_DICT_TYPE_H
#define HGRAPH_VALUE_DICT_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <ankerl/unordered_dense.h>
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

    // Forward declaration
    class DictStorage;

    /**
     * DictIndexHash - Transparent hash functor for DictStorage
     *
     * Supports heterogeneous lookup: can hash both indices (existing keys)
     * and raw pointers (lookup keys).
     */
    struct DictIndexHash {
        using is_transparent = void;
        using is_avalanching = void;

        const DictStorage* storage{nullptr};

        DictIndexHash() = default;
        DictIndexHash(const DictStorage* s) : storage(s) {}

        [[nodiscard]] auto operator()(size_t idx) const -> uint64_t;
        [[nodiscard]] auto operator()(const void* ptr) const -> uint64_t;
    };

    /**
     * DictIndexEqual - Transparent equality functor for DictStorage
     *
     * Supports heterogeneous lookup: can compare indices with indices,
     * indices with pointers, and pointers with indices.
     */
    struct DictIndexEqual {
        using is_transparent = void;

        const DictStorage* storage{nullptr};

        DictIndexEqual() = default;
        DictIndexEqual(const DictStorage* s) : storage(s) {}

        [[nodiscard]] bool operator()(size_t a, size_t b) const;
        [[nodiscard]] bool operator()(size_t idx, const void* ptr) const;
        [[nodiscard]] bool operator()(const void* ptr, size_t idx) const;
    };

    /**
     * DictStorage - Internal storage for a type-erased dictionary
     *
     * Uses ankerl::unordered_dense for O(1) operations with robin-hood hashing.
     * Keys and values are stored in parallel contiguous vectors.
     * The set stores indices into the key/value vectors.
     */
    class DictStorage {
    public:
        friend struct DictIndexHash;
        friend struct DictIndexEqual;

        using IndexSet = ankerl::unordered_dense::set<size_t, DictIndexHash, DictIndexEqual>;

        DictStorage() = default;

        DictStorage(const TypeMeta* key_type, const TypeMeta* value_type)
            : _key_type(key_type)
            , _value_type(value_type)
            , _index_set(0, DictIndexHash(this), DictIndexEqual(this)) {
        }

        ~DictStorage() {
            clear();
        }

        DictStorage(DictStorage&& other) noexcept
            : _key_type(other._key_type)
            , _value_type(other._value_type)
            , _keys(std::move(other._keys))
            , _values(std::move(other._values))
            , _entry_count(other._entry_count)
            , _index_set(0, DictIndexHash(this), DictIndexEqual(this)) {
            // Rebuild index set with our own functors pointing to this
            for (size_t idx : other._index_set) {
                _index_set.insert(idx);
            }
            other._key_type = nullptr;
            other._value_type = nullptr;
            other._entry_count = 0;
            other._index_set.clear();
        }

        DictStorage& operator=(DictStorage&& other) noexcept {
            if (this != &other) {
                clear();
                _key_type = other._key_type;
                _value_type = other._value_type;
                _keys = std::move(other._keys);
                _values = std::move(other._values);
                _entry_count = other._entry_count;
                // Rebuild index set with our own functors
                _index_set = IndexSet(0, DictIndexHash(this), DictIndexEqual(this));
                for (size_t idx : other._index_set) {
                    _index_set.insert(idx);
                }
                other._key_type = nullptr;
                other._value_type = nullptr;
                other._entry_count = 0;
                other._index_set.clear();
            }
            return *this;
        }

        DictStorage(const DictStorage&) = delete;
        DictStorage& operator=(const DictStorage&) = delete;

        [[nodiscard]] const TypeMeta* key_type() const { return _key_type; }
        [[nodiscard]] const TypeMeta* value_type() const { return _value_type; }
        [[nodiscard]] size_t size() const { return _index_set.size(); }
        [[nodiscard]] bool empty() const { return _index_set.empty(); }

        // Insert or update a key-value pair - O(1) average
        void insert(const void* key, const void* value) {
            if (!_key_type || !_value_type) return;

            // Check if key already exists using heterogeneous lookup
            auto it = _index_set.find(key);
            if (it != _index_set.end()) {
                // Update existing value
                _value_type->copy_assign_at(value_ptr(*it), value);
                return;
            }

            // Add new entry
            size_t new_idx = _entry_count;
            _keys.resize(_keys.size() + _key_type->size);
            _values.resize(_values.size() + _value_type->size);
            _key_type->copy_construct_at(key_ptr(new_idx), key);
            _value_type->copy_construct_at(value_ptr(new_idx), value);
            ++_entry_count;

            // Insert index into set
            _index_set.insert(new_idx);
        }

        // Remove by key - O(1) average
        bool remove(const void* key) {
            if (!_key_type || _index_set.empty()) return false;

            auto it = _index_set.find(key);
            if (it == _index_set.end()) {
                return false;
            }

            size_t idx = *it;
            _key_type->destruct_at(key_ptr(idx));
            _value_type->destruct_at(value_ptr(idx));
            _index_set.erase(it);
            return true;
        }

        // Check if key exists - O(1) average
        [[nodiscard]] bool contains(const void* key) const {
            if (!_key_type || _index_set.empty()) return false;
            return _index_set.find(key) != _index_set.end();
        }

        // Get value by key - O(1) average (returns nullptr if not found)
        [[nodiscard]] void* get(const void* key) {
            if (!_key_type || _index_set.empty()) return nullptr;

            auto it = _index_set.find(key);
            if (it == _index_set.end()) {
                return nullptr;
            }
            return value_ptr(*it);
        }

        [[nodiscard]] const void* get(const void* key) const {
            if (!_key_type || _index_set.empty()) return nullptr;

            auto it = _index_set.find(key);
            if (it == _index_set.end()) {
                return nullptr;
            }
            return value_ptr(*it);
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
                for (size_t idx : _index_set) {
                    _key_type->destruct_at(key_ptr(idx));
                    _value_type->destruct_at(value_ptr(idx));
                }
            }
            _keys.clear();
            _values.clear();
            _index_set.clear();
            _entry_count = 0;
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
            Iterator(DictStorage* storage, typename IndexSet::iterator it)
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

            bool operator==(const Iterator& other) const {
                return _it == other._it;
            }

        private:
            DictStorage* _storage;
            typename IndexSet::iterator _it;
        };

        class ConstIterator {
        public:
            ConstIterator(const DictStorage* storage, typename IndexSet::const_iterator it)
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

            bool operator==(const ConstIterator& other) const {
                return _it == other._it;
            }

        private:
            const DictStorage* _storage;
            typename IndexSet::const_iterator _it;
        };

        [[nodiscard]] Iterator begin() {
            return Iterator(this, _index_set.begin());
        }

        [[nodiscard]] Iterator end() {
            return Iterator(this, _index_set.end());
        }

        [[nodiscard]] ConstIterator begin() const {
            return ConstIterator(this, _index_set.begin());
        }

        [[nodiscard]] ConstIterator end() const {
            return ConstIterator(this, _index_set.end());
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
        std::vector<char> _keys;           // Raw storage for keys
        std::vector<char> _values;         // Raw storage for values
        size_t _entry_count{0};            // Total entries allocated
        IndexSet _index_set;               // Robin-hood hash set of indices
    };

    // Implementation of DictIndexHash
    inline auto DictIndexHash::operator()(size_t idx) const -> uint64_t {
        return storage->_key_type->hash_at(storage->key_ptr(idx));
    }

    inline auto DictIndexHash::operator()(const void* ptr) const -> uint64_t {
        return storage->_key_type->hash_at(ptr);
    }

    // Implementation of DictIndexEqual
    inline bool DictIndexEqual::operator()(size_t a, size_t b) const {
        return storage->_key_type->equals_at(
            storage->key_ptr(a),
            storage->key_ptr(b)
        );
    }

    inline bool DictIndexEqual::operator()(size_t idx, const void* ptr) const {
        return storage->_key_type->equals_at(storage->key_ptr(idx), ptr);
    }

    inline bool DictIndexEqual::operator()(const void* ptr, size_t idx) const {
        return storage->_key_type->equals_at(ptr, storage->key_ptr(idx));
    }

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
