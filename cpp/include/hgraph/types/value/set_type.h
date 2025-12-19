//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_SET_TYPE_H
#define HGRAPH_VALUE_SET_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <ankerl/unordered_dense.h>
#include <algorithm>
#include <memory>
#include <cassert>
#include <optional>
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

    // Forward declaration
    class SetStorage;

    /**
     * IndexHash - Transparent hash functor for SetStorage
     *
     * Supports heterogeneous lookup: can hash both indices (existing elements)
     * and raw pointers (lookup keys).
     */
    struct SetIndexHash {
        using is_transparent = void;
        using is_avalanching = void;

        const SetStorage* storage{nullptr};

        SetIndexHash() = default;
        SetIndexHash(const SetStorage* s) : storage(s) {}

        [[nodiscard]] auto operator()(size_t idx) const -> uint64_t;
        [[nodiscard]] auto operator()(const void* ptr) const -> uint64_t;
    };

    /**
     * IndexEqual - Transparent equality functor for SetStorage
     *
     * Supports heterogeneous lookup: can compare indices with indices,
     * indices with pointers, and pointers with indices.
     */
    struct SetIndexEqual {
        using is_transparent = void;

        const SetStorage* storage{nullptr};

        SetIndexEqual() = default;
        SetIndexEqual(const SetStorage* s) : storage(s) {}

        [[nodiscard]] bool operator()(size_t a, size_t b) const;
        [[nodiscard]] bool operator()(size_t idx, const void* ptr) const;
        [[nodiscard]] bool operator()(const void* ptr, size_t idx) const;
    };

    /**
     * SetStorage - Internal storage for a type-erased set
     *
     * Uses ankerl::unordered_dense for O(1) operations with robin-hood hashing.
     * Elements are stored contiguously in a vector for cache efficiency.
     * The set stores indices into the element vector.
     */
    class SetStorage {
    public:
        friend struct SetIndexHash;
        friend struct SetIndexEqual;

        using IndexSet = ankerl::unordered_dense::set<size_t, SetIndexHash, SetIndexEqual>;

        SetStorage() = default;

        explicit SetStorage(const TypeMeta* elem_type)
            : _element_type(elem_type)
            , _index_set(0, SetIndexHash(this), SetIndexEqual(this)) {
        }

        ~SetStorage() {
            clear();
        }

        SetStorage(SetStorage&& other) noexcept
            : _element_type(other._element_type)
            , _elements(std::move(other._elements))
            , _element_count(other._element_count)
            , _index_set(0, SetIndexHash(this), SetIndexEqual(this)) {
            // Rebuild index set with our own functors pointing to this
            for (size_t idx : other._index_set) {
                _index_set.insert(idx);
            }
            other._element_type = nullptr;
            other._element_count = 0;
            other._index_set.clear();
        }

        SetStorage& operator=(SetStorage&& other) noexcept {
            if (this != &other) {
                clear();
                _element_type = other._element_type;
                _elements = std::move(other._elements);
                _element_count = other._element_count;
                // Rebuild index set with our own functors
                _index_set = IndexSet(0, SetIndexHash(this), SetIndexEqual(this));
                for (size_t idx : other._index_set) {
                    _index_set.insert(idx);
                }
                other._element_type = nullptr;
                other._element_count = 0;
                other._index_set.clear();
            }
            return *this;
        }

        SetStorage(const SetStorage&) = delete;
        SetStorage& operator=(const SetStorage&) = delete;

        [[nodiscard]] const TypeMeta* element_type() const { return _element_type; }
        [[nodiscard]] size_t size() const { return _index_set.size(); }
        [[nodiscard]] bool empty() const { return _index_set.empty(); }

        // Find the stable index for a value (for tracking/observer lookup)
        [[nodiscard]] std::optional<size_t> find_index(const void* value) const {
            if (!_element_type || _index_set.empty()) return std::nullopt;
            auto it = _index_set.find(value);
            if (it == _index_set.end()) return std::nullopt;
            return *it;
        }

        // Add element (returns true if added, false if already present)
        bool add(const void* value) {
            auto [added, index] = add_with_index(value);
            return added;
        }

        // Add element with index return (for tracking)
        // Returns (was_added, index) - index is valid even if element already existed
        std::pair<bool, size_t> add_with_index(const void* value) {
            if (!_element_type) return {false, 0};

            // Check if already present using heterogeneous lookup
            auto it = _index_set.find(value);
            if (it != _index_set.end()) {
                return {false, *it};  // Already present, return existing index
            }

            // Add new element
            size_t new_idx = _element_count;
            _elements.resize(_elements.size() + _element_type->size);
            void* dest = element_ptr(new_idx);
            _element_type->copy_construct_at(dest, value);
            ++_element_count;

            // Insert index into set
            _index_set.insert(new_idx);
            return {true, new_idx};
        }

        // Remove element
        bool remove(const void* value) {
            auto [removed, index] = remove_with_index(value);
            return removed;
        }

        // Remove element with index return (for tracking)
        // Returns (was_removed, index) - index is the removed element's index
        std::pair<bool, size_t> remove_with_index(const void* value) {
            if (!_element_type || _index_set.empty()) return {false, 0};

            auto it = _index_set.find(value);
            if (it == _index_set.end()) {
                return {false, 0};
            }

            size_t idx = *it;
            _element_type->destruct_at(element_ptr(idx));
            _index_set.erase(it);
            return {true, idx};
        }

        // Check if element exists - O(1) average
        [[nodiscard]] bool contains(const void* value) const {
            if (!_element_type || _index_set.empty()) return false;
            return _index_set.find(value) != _index_set.end();
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
            _element_count = 0;
        }

        // Fragmentation ratio: 0.0 = no waste, 1.0 = all waste
        // Returns the ratio of unused slots to total allocated slots
        [[nodiscard]] double fragmentation_ratio() const {
            if (_element_count == 0) return 0.0;
            size_t live_count = _index_set.size();
            return 1.0 - static_cast<double>(live_count) / static_cast<double>(_element_count);
        }

        // --- Set Algebra Helper Methods ---

        // Create a copy of this set
        [[nodiscard]] SetStorage clone() const {
            SetStorage result(_element_type);
            for (auto elem : *this) {
                result.add(elem.ptr);
            }
            return result;
        }

        // Union: returns a new set containing all elements from both sets
        [[nodiscard]] SetStorage union_with(const SetStorage& other) const {
            assert(_element_type == other._element_type);
            SetStorage result = clone();
            for (auto elem : other) {
                result.add(elem.ptr);
            }
            return result;
        }

        // Intersection: returns a new set containing elements in both sets
        [[nodiscard]] SetStorage intersection_with(const SetStorage& other) const {
            assert(_element_type == other._element_type);
            SetStorage result(_element_type);
            for (auto elem : *this) {
                if (other.contains(elem.ptr)) {
                    result.add(elem.ptr);
                }
            }
            return result;
        }

        // Difference: returns a new set containing elements in this but not other
        [[nodiscard]] SetStorage difference_with(const SetStorage& other) const {
            assert(_element_type == other._element_type);
            SetStorage result(_element_type);
            for (auto elem : *this) {
                if (!other.contains(elem.ptr)) {
                    result.add(elem.ptr);
                }
            }
            return result;
        }

        // Symmetric difference: returns elements in either but not both
        [[nodiscard]] SetStorage symmetric_difference_with(const SetStorage& other) const {
            assert(_element_type == other._element_type);
            SetStorage result(_element_type);
            for (auto elem : *this) {
                if (!other.contains(elem.ptr)) {
                    result.add(elem.ptr);
                }
            }
            for (auto elem : other) {
                if (!contains(elem.ptr)) {
                    result.add(elem.ptr);
                }
            }
            return result;
        }

        // Subset check: returns true if all elements of this are in other
        [[nodiscard]] bool is_subset_of(const SetStorage& other) const {
            assert(_element_type == other._element_type);
            for (auto elem : *this) {
                if (!other.contains(elem.ptr)) {
                    return false;
                }
            }
            return true;
        }

        // Proper subset: subset but not equal
        [[nodiscard]] bool is_proper_subset_of(const SetStorage& other) const {
            return size() < other.size() && is_subset_of(other);
        }

        // Superset check: returns true if all elements of other are in this
        [[nodiscard]] bool is_superset_of(const SetStorage& other) const {
            return other.is_subset_of(*this);
        }

        // Proper superset: superset but not equal
        [[nodiscard]] bool is_proper_superset_of(const SetStorage& other) const {
            return size() > other.size() && is_superset_of(other);
        }

        // Disjoint check: returns true if sets have no common elements
        [[nodiscard]] bool is_disjoint_with(const SetStorage& other) const {
            assert(_element_type == other._element_type);
            // Iterate over smaller set for efficiency
            if (size() <= other.size()) {
                for (auto elem : *this) {
                    if (other.contains(elem.ptr)) {
                        return false;
                    }
                }
            } else {
                for (auto elem : other) {
                    if (contains(elem.ptr)) {
                        return false;
                    }
                }
            }
            return true;
        }

        // Discard: remove if present (silent if missing), returns true if removed
        bool discard(const void* value) {
            return remove(value);  // remove already returns false if not found
        }

        // Update: in-place union with another set
        void update(const SetStorage& other) {
            assert(_element_type == other._element_type);
            for (auto elem : other) {
                add(elem.ptr);
            }
        }

        // Intersection update: in-place intersection
        void intersection_update(const SetStorage& other) {
            assert(_element_type == other._element_type);
            // Collect elements to remove (can't modify during iteration)
            std::vector<size_t> to_remove;
            for (size_t idx : _index_set) {
                if (!other.contains(element_ptr(idx))) {
                    to_remove.push_back(idx);
                }
            }
            for (size_t idx : to_remove) {
                _element_type->destruct_at(element_ptr(idx));
                _index_set.erase(idx);
            }
        }

        // Difference update: in-place difference
        void difference_update(const SetStorage& other) {
            assert(_element_type == other._element_type);
            for (auto elem : other) {
                remove(elem.ptr);
            }
        }

        // Symmetric difference update: in-place symmetric difference
        void symmetric_difference_update(const SetStorage& other) {
            assert(_element_type == other._element_type);
            // Elements in other but not this: add them
            // Elements in both: remove them from this
            std::vector<const void*> to_add;
            std::vector<const void*> to_remove;

            for (auto elem : other) {
                if (contains(elem.ptr)) {
                    to_remove.push_back(elem.ptr);
                } else {
                    to_add.push_back(elem.ptr);
                }
            }

            for (const void* ptr : to_remove) {
                remove(ptr);
            }
            for (const void* ptr : to_add) {
                add(ptr);
            }
        }

        // Compact storage to eliminate holes from removed elements
        // Returns a mapping from old indices to new indices
        // Indices not in the map are no longer valid (removed elements)
        std::vector<std::pair<size_t, size_t>> compact() {
            std::vector<std::pair<size_t, size_t>> index_mapping;
            if (!_element_type || _index_set.empty()) {
                _elements.clear();
                _element_count = 0;
                return index_mapping;
            }

            // If already compact, nothing to do
            if (_index_set.size() == _element_count) {
                return index_mapping;
            }

            // Build new storage
            std::vector<char> new_elements;
            new_elements.reserve(_index_set.size() * _element_type->size);

            // Collect and sort live indices for deterministic order
            std::vector<size_t> live_indices(_index_set.begin(), _index_set.end());
            std::sort(live_indices.begin(), live_indices.end());

            index_mapping.reserve(live_indices.size());
            size_t new_idx = 0;

            for (size_t old_idx : live_indices) {
                // Record mapping
                index_mapping.emplace_back(old_idx, new_idx);

                // Move element to new position
                new_elements.resize(new_elements.size() + _element_type->size);
                void* new_ptr = new_elements.data() + new_idx * _element_type->size;
                void* old_ptr = element_ptr(old_idx);
                _element_type->move_construct_at(new_ptr, old_ptr);
                _element_type->destruct_at(old_ptr);

                ++new_idx;
            }

            // Rebuild index set with new indices
            _index_set.clear();
            for (size_t i = 0; i < new_idx; ++i) {
                _index_set.insert(i);
            }

            // Swap in new storage
            _elements = std::move(new_elements);
            _element_count = new_idx;

            return index_mapping;
        }

        // Iteration support - iterates over active elements via the index set
        class Iterator {
        public:
            Iterator() : _storage(nullptr) {}
            Iterator(const SetStorage* storage, typename IndexSet::const_iterator it)
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

            bool operator==(const Iterator& other) const {
                return _it == other._it;
            }

        private:
            const SetStorage* _storage;
            typename IndexSet::const_iterator _it;
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
        std::vector<char> _elements;       // Raw storage for elements
        size_t _element_count{0};          // Total elements allocated
        IndexSet _index_set;               // Robin-hood hash set of indices
    };

    // Implementation of SetIndexHash
    inline auto SetIndexHash::operator()(size_t idx) const -> uint64_t {
        return storage->_element_type->hash_at(storage->element_ptr(idx));
    }

    inline auto SetIndexHash::operator()(const void* ptr) const -> uint64_t {
        return storage->_element_type->hash_at(ptr);
    }

    // Implementation of SetIndexEqual
    inline bool SetIndexEqual::operator()(size_t a, size_t b) const {
        return storage->_element_type->equals_at(
            storage->element_ptr(a),
            storage->element_ptr(b)
        );
    }

    inline bool SetIndexEqual::operator()(size_t idx, const void* ptr) const {
        return storage->_element_type->equals_at(storage->element_ptr(idx), ptr);
    }

    inline bool SetIndexEqual::operator()(const void* ptr, size_t idx) const {
        return storage->_element_type->equals_at(ptr, storage->element_ptr(idx));
    }

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

        static std::string to_string(const void* v, const TypeMeta*) {
            auto* set = static_cast<const SetStorage*>(v);
            std::string result = "{";
            bool first = true;
            for (auto elem : *set) {
                if (!first) result += ", ";
                first = false;
                result += elem.meta->to_string_at(elem.ptr);
            }
            result += "}";
            return result;
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* set_meta = static_cast<const SetTypeMeta*>(meta);
            // Format: Set[element_type]
            return "Set[" + set_meta->element_type->type_name_str() + "]";
        }

        // Container operations
        static size_t length(const void* v, const TypeMeta*) {
            return static_cast<const SetStorage*>(v)->size();
        }

        static bool contains(const void* container, const void* element, const TypeMeta*) {
            return static_cast<const SetStorage*>(container)->contains(element);
        }

        // Boolean conversion - non-empty sets are truthy
        static bool to_bool(const void* v, const TypeMeta*) {
            return !static_cast<const SetStorage*>(v)->empty();
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
        .to_string = SetTypeOps::to_string,
        .type_name = SetTypeOps::type_name,
        .to_python = nullptr,
        .from_python = nullptr,
        // Arithmetic operations - not supported for sets
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
        .to_bool = SetTypeOps::to_bool,
        .length = SetTypeOps::length,
        .contains = SetTypeOps::contains,
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
            meta->flags = TypeFlags::Hashable | TypeFlags::Equatable | TypeFlags::Container;
            meta->kind = TypeKind::Set;
            meta->ops = &SetTypeOps::ops;
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;  // Sets are not numpy-compatible
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

        // Fragmentation ratio: 0.0 = no waste, 1.0 = all waste
        [[nodiscard]] double fragmentation_ratio() const {
            return _storage ? _storage->fragmentation_ratio() : 0.0;
        }

        // Compact storage to eliminate holes
        std::vector<std::pair<size_t, size_t>> compact() {
            return _storage ? _storage->compact() : std::vector<std::pair<size_t, size_t>>{};
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
