//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_DICT_TYPE_H
#define HGRAPH_VALUE_DICT_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <hgraph/types/value/set_type.h>
#include <ankerl/unordered_dense.h>
#include <memory>
#include <cassert>
#include <optional>
#include <vector>

namespace hgraph::value {

    /**
     * DictTypeMeta - Extended TypeMeta for dictionary types
     *
     * Dictionaries are dynamic key-value collections with type-erased storage.
     * Key type must be hashable and equatable.
     *
     * Dict is conceptually a Set<K> + Values<V>, so we embed the key set meta
     * rather than the raw key type. This enables:
     * - Direct keys() access returning a SetView
     * - Shared modification tracking between Set and Dict
     * - Clean ownership (the SetTypeMeta is embedded, not a separate allocation)
     */
    struct DictTypeMeta : TypeMeta {
        SetTypeMeta key_set_meta;  // Embedded set meta for keys
        const TypeMeta* value_type;

        // Accessor for API that expects a pointer to SetTypeMeta
        [[nodiscard]] const SetTypeMeta* key_set_type() const {
            return &key_set_meta;
        }

        // Convenience accessor for key element type
        [[nodiscard]] const TypeMeta* key_type() const {
            return key_set_meta.element_type;
        }
    };

    /**
     * DictStorage - Internal storage for a type-erased dictionary
     *
     * Composes SetStorage for keys, with parallel value storage.
     * This enables:
     * - Direct keys() access to the underlying SetStorage
     * - Shared index scheme between keys and values
     * - Consistent API with SetStorage
     */
    class DictStorage {
    public:
        DictStorage() = default;

        DictStorage(const TypeMeta* key_type, const TypeMeta* value_type)
            : _key_set(key_type)
            , _value_type(value_type) {
        }

        ~DictStorage() {
            clear_values();
        }

        DictStorage(DictStorage&& other) noexcept
            : _key_set(std::move(other._key_set))
            , _values(std::move(other._values))
            , _value_type(other._value_type) {
            other._value_type = nullptr;
        }

        DictStorage& operator=(DictStorage&& other) noexcept {
            if (this != &other) {
                clear_values();
                _key_set = std::move(other._key_set);
                _values = std::move(other._values);
                _value_type = other._value_type;
                other._value_type = nullptr;
            }
            return *this;
        }

        DictStorage(const DictStorage&) = delete;
        DictStorage& operator=(const DictStorage&) = delete;

        // Expose key set for direct access
        [[nodiscard]] const SetStorage& keys() const { return _key_set; }
        [[nodiscard]] SetStorage& keys() { return _key_set; }

        [[nodiscard]] const TypeMeta* key_type() const { return _key_set.element_type(); }
        [[nodiscard]] const TypeMeta* value_type() const { return _value_type; }
        [[nodiscard]] size_t size() const { return _key_set.size(); }
        [[nodiscard]] bool empty() const { return _key_set.empty(); }

        // Insert or update a key-value pair - O(1) average
        // Returns (was_new_key, index)
        std::pair<bool, size_t> insert(const void* key, const void* value) {
            if (!key_type() || !_value_type) return {false, 0};

            // Try to add key to set
            auto [added, idx] = _key_set.add_with_index(key);

            if (added) {
                // New key - allocate value storage
                ensure_value_capacity(idx + 1);
                _value_type->copy_construct_at(value_ptr(idx), value);
            } else {
                // Existing key - update value
                _value_type->copy_assign_at(value_ptr(idx), value);
            }

            return {added, idx};
        }

        // Remove by key - O(1) average
        // Returns (was_removed, index)
        std::pair<bool, size_t> remove(const void* key) {
            if (!key_type() || _key_set.empty()) return {false, 0};

            auto [removed, idx] = _key_set.remove_with_index(key);

            if (removed) {
                // Destruct the value (key already destructed by SetStorage)
                _value_type->destruct_at(value_ptr(idx));
            }

            return {removed, idx};
        }

        // Check if key exists - O(1) average
        [[nodiscard]] bool contains(const void* key) const {
            return _key_set.contains(key);
        }

        // Find the entry index for a key - O(1) average
        [[nodiscard]] std::optional<size_t> find_index(const void* key) const {
            return _key_set.find_index(key);
        }

        // Get value by key - O(1) average (returns nullptr if not found)
        [[nodiscard]] void* get(const void* key) {
            auto idx = _key_set.find_index(key);
            return idx ? value_ptr(*idx) : nullptr;
        }

        [[nodiscard]] const void* get(const void* key) const {
            auto idx = _key_set.find_index(key);
            return idx ? value_ptr(*idx) : nullptr;
        }

        // Get value by index directly
        [[nodiscard]] void* value_at(size_t idx) {
            return value_ptr(idx);
        }

        [[nodiscard]] const void* value_at(size_t idx) const {
            return value_ptr(idx);
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
            // Clear values first (keys will be cleared by SetStorage)
            for (auto it = _key_set.begin(); it != _key_set.end(); ++it) {
                auto elem = *it;
                // The iterator gives us the element ptr, but we need the index
                // Use find_index to get it
                auto idx = _key_set.find_index(elem.ptr);
                if (idx) {
                    _value_type->destruct_at(value_ptr(*idx));
                }
            }
            _values.clear();
            _key_set.clear();
        }

        // Fragmentation ratio (delegates to key set)
        [[nodiscard]] double fragmentation_ratio() const {
            return _key_set.fragmentation_ratio();
        }

        // Compact storage to eliminate holes from removed entries
        // Returns a mapping from old indices to new indices
        std::vector<std::pair<size_t, size_t>> compact() {
            if (!_value_type || _key_set.empty()) {
                _values.clear();
                return _key_set.compact();
            }

            // Get current live count before compaction
            size_t live_count = _key_set.size();

            // If nothing removed, already compact
            if (_key_set.fragmentation_ratio() == 0.0) {
                return {};
            }

            // We need to rearrange values based on index mapping.
            // Since SetStorage::compact() sorts indices, we can predict the mapping:
            // Collect and sort live indices, then remap to 0, 1, 2, ...

            // First, collect live indices from key_set (before compaction changes them)
            std::vector<size_t> live_indices;
            for (auto it = _key_set.begin(); it != _key_set.end(); ++it) {
                auto idx = _key_set.find_index((*it).ptr);
                if (idx) live_indices.push_back(*idx);
            }
            std::sort(live_indices.begin(), live_indices.end());

            // Build new value storage before compacting keys
            std::vector<char> new_values;
            new_values.resize(live_count * _value_type->size);

            std::vector<std::pair<size_t, size_t>> index_mapping;
            index_mapping.reserve(live_indices.size());

            for (size_t new_idx = 0; new_idx < live_indices.size(); ++new_idx) {
                size_t old_idx = live_indices[new_idx];
                index_mapping.emplace_back(old_idx, new_idx);

                void* new_ptr = new_values.data() + new_idx * _value_type->size;
                void* old_ptr = value_ptr(old_idx);
                _value_type->move_construct_at(new_ptr, old_ptr);
                _value_type->destruct_at(old_ptr);
            }

            // Now compact the key set (which will produce the same mapping)
            _key_set.compact();

            _values = std::move(new_values);
            return index_mapping;
        }

        // Iteration support - iterates over key-value pairs
        struct KeyValuePair {
            ConstTypedPtr key;
            TypedPtr value;
            size_t index;
        };

        struct ConstKeyValuePair {
            ConstTypedPtr key;
            ConstTypedPtr value;
            size_t index;
        };

        class Iterator {
        public:
            Iterator() : _storage(nullptr) {}
            Iterator(DictStorage* storage, SetStorage::Iterator it)
                : _storage(storage), _it(it) {}

            KeyValuePair operator*() const {
                auto key_elem = *_it;
                auto idx = _storage->_key_set.find_index(key_elem.ptr);
                size_t index = idx.value_or(0);
                return {
                    key_elem,
                    {_storage->value_ptr(index), _storage->_value_type},
                    index
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
            SetStorage::Iterator _it;
        };

        class ConstIterator {
        public:
            ConstIterator() : _storage(nullptr) {}
            ConstIterator(const DictStorage* storage, SetStorage::Iterator it)
                : _storage(storage), _it(it) {}

            ConstKeyValuePair operator*() const {
                auto key_elem = *_it;
                auto idx = _storage->_key_set.find_index(key_elem.ptr);
                size_t index = idx.value_or(0);
                return {
                    key_elem,
                    {_storage->value_ptr(index), _storage->_value_type},
                    index
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
            SetStorage::Iterator _it;
        };

        [[nodiscard]] Iterator begin() {
            return Iterator(this, _key_set.begin());
        }

        [[nodiscard]] Iterator end() {
            return Iterator(this, _key_set.end());
        }

        [[nodiscard]] ConstIterator begin() const {
            return ConstIterator(this, _key_set.begin());
        }

        [[nodiscard]] ConstIterator end() const {
            return ConstIterator(this, _key_set.end());
        }

    private:
        void ensure_value_capacity(size_t count) {
            size_t needed = count * _value_type->size;
            if (_values.size() >= needed) {
                return; // Already have enough capacity
            }

            // For trivially copyable types, simple resize is safe
            if (_value_type->is_trivially_copyable()) {
                _values.resize(needed);
                return;
            }

            // For non-trivially copyable types (like nested DictStorage), we must
            // properly move-construct existing values to avoid dangling pointers
            // in internal data structures (like SetStorage's hash functors).
            std::vector<char> new_values(needed);

            // Move existing values to new storage - only move slots that have
            // been constructed (tracked by _constructed_count)
            // Note: We track indices that are live in the key set
            size_t old_capacity = _values.size() / _value_type->size;
            for (auto it = _key_set.begin(); it != _key_set.end(); ++it) {
                auto key_elem = *it;
                auto idx = _key_set.find_index(key_elem.ptr);
                // Only move if the index is within old capacity and was previously constructed
                if (idx && *idx < old_capacity) {
                    void* old_ptr = value_ptr(*idx);
                    void* new_ptr = new_values.data() + (*idx) * _value_type->size;
                    _value_type->move_construct_at(new_ptr, old_ptr);
                    _value_type->destruct_at(old_ptr);
                }
            }

            _values = std::move(new_values);
        }

        void clear_values() {
            if (_value_type) {
                for (auto it = _key_set.begin(); it != _key_set.end(); ++it) {
                    auto key_elem = *it;
                    auto idx = _key_set.find_index(key_elem.ptr);
                    if (idx) {
                        _value_type->destruct_at(value_ptr(*idx));
                    }
                }
            }
            _values.clear();
        }

        [[nodiscard]] void* value_ptr(size_t idx) {
            return _values.data() + idx * _value_type->size;
        }

        [[nodiscard]] const void* value_ptr(size_t idx) const {
            return _values.data() + idx * _value_type->size;
        }

        SetStorage _key_set;
        std::vector<char> _values;
        const TypeMeta* _value_type{nullptr};
    };

    /**
     * DictTypeOps - Operations for dict types
     */
    struct DictTypeOps {
        static void construct(void* dest, const TypeMeta* meta) {
            auto* dict_meta = static_cast<const DictTypeMeta*>(meta);
            new (dest) DictStorage(dict_meta->key_type(), dict_meta->value_type);
        }

        static void destruct(void* dest, const TypeMeta*) {
            static_cast<DictStorage*>(dest)->~DictStorage();
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta* meta) {
            auto* dict_meta = static_cast<const DictTypeMeta*>(meta);
            auto* src_dict = static_cast<const DictStorage*>(src);
            new (dest) DictStorage(dict_meta->key_type(), dict_meta->value_type);
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

        static std::string to_string(const void* v, const TypeMeta*) {
            auto* dict = static_cast<const DictStorage*>(v);
            std::string result = "{";
            bool first = true;
            for (auto kv : *dict) {
                if (!first) result += ", ";
                first = false;
                result += kv.key.meta->to_string_at(kv.key.ptr);
                result += ": ";
                result += kv.value.meta->to_string_at(kv.value.ptr);
            }
            result += "}";
            return result;
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* dict_meta = static_cast<const DictTypeMeta*>(meta);
            // Format: Dict[key_type, value_type]
            return "Dict[" + dict_meta->key_set_meta.element_type->type_name_str() +
                   ", " + dict_meta->value_type->type_name_str() + "]";
        }

        // Container operations
        static size_t length(const void* v, const TypeMeta*) {
            return static_cast<const DictStorage*>(v)->size();
        }

        static bool contains(const void* container, const void* key, const TypeMeta*) {
            return static_cast<const DictStorage*>(container)->contains(key);
        }

        // Boolean conversion - non-empty dicts are truthy
        static bool to_bool(const void* v, const TypeMeta*) {
            return !static_cast<const DictStorage*>(v)->empty();
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
        .to_string = DictTypeOps::to_string,
        .type_name = DictTypeOps::type_name,
        .to_python = nullptr,
        .from_python = nullptr,
        // Arithmetic operations - not supported for dicts
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
        .to_bool = DictTypeOps::to_bool,
        .length = DictTypeOps::length,
        .contains = DictTypeOps::contains,
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
            TypeFlags flags = TypeFlags::Equatable | TypeFlags::Container;
            if (has_flag(_value_type->flags, TypeFlags::Hashable)) {
                flags = flags | TypeFlags::Hashable;
            }

            // Initialize the DictTypeMeta
            meta->size = sizeof(DictStorage);
            meta->alignment = alignof(DictStorage);
            meta->flags = flags;
            meta->kind = TypeKind::Dict;
            meta->ops = &DictTypeOps::ops;
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;  // Dicts are not numpy-compatible
            meta->value_type = _value_type;

            // Initialize the embedded SetTypeMeta for keys
            meta->key_set_meta.size = sizeof(SetStorage);
            meta->key_set_meta.alignment = alignof(SetStorage);
            meta->key_set_meta.flags = TypeFlags::Hashable | TypeFlags::Equatable;
            meta->key_set_meta.kind = TypeKind::Set;
            meta->key_set_meta.ops = &SetTypeOps::ops;
            meta->key_set_meta.type_info = nullptr;
            meta->key_set_meta.name = nullptr;  // Anonymous set type for keys
            meta->key_set_meta.numpy_format = nullptr;
            meta->key_set_meta.element_type = _key_type;

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
                _storage = new DictStorage(_meta->key_type(), _meta->value_type);
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

        template<typename K>
        [[nodiscard]] std::optional<size_t> find_index(const K& key) const {
            return _storage ? _storage->find_index(&key) : std::nullopt;
        }

        void clear() {
            if (_storage) _storage->clear();
        }

        // Fragmentation ratio: 0.0 = no waste, 1.0 = all waste
        [[nodiscard]] double fragmentation_ratio() const {
            return _storage ? _storage->fragmentation_ratio() : 0.0;
        }

        // Compact storage to eliminate holes
        std::vector<std::pair<size_t, size_t>> compact() {
            return _storage ? _storage->compact() : std::vector<std::pair<size_t, size_t>>{};
        }

        // Iteration
        [[nodiscard]] DictStorage::Iterator begin() {
            return _storage ? _storage->begin() : DictStorage::Iterator{};
        }

        [[nodiscard]] DictStorage::Iterator end() {
            return _storage ? _storage->end() : DictStorage::Iterator{};
        }

        [[nodiscard]] DictStorage::ConstIterator cbegin() const {
            return _storage ? static_cast<const DictStorage*>(_storage)->begin()
                            : DictStorage::ConstIterator{};
        }

        [[nodiscard]] DictStorage::ConstIterator cend() const {
            return _storage ? static_cast<const DictStorage*>(_storage)->end()
                            : DictStorage::ConstIterator{};
        }

    private:
        DictStorage* _storage{nullptr};
        const DictTypeMeta* _meta{nullptr};
        bool _owns_storage{false};
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_DICT_TYPE_H
