//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_TIME_SERIES_VALUE_H
#define HGRAPH_VALUE_TIME_SERIES_VALUE_H

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/modification_tracker.h>
#include <hgraph/types/value/observer_storage.h>
#include <hgraph/types/value/bundle_type.h>
#include <hgraph/types/value/dict_type.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/string_utils.h>
#include <algorithm>
#include <cstring>

// Forward declarations
namespace hgraph::ts {
    class TSOutput;
    class DeltaView;
}

// Forward declarations in hgraph namespace (where they're actually defined)
namespace hgraph {
    struct TSMeta;
    enum class TSKind : uint8_t;
}

namespace hgraph::value {

    // Forward declarations
    class TSValue;
    class TSView;
    class ObserverStorage;

    // ============================================================================
    // ValuePath - Lightweight path from root TSOutput to current view position
    // ============================================================================

    /**
     * ValuePath - Tracks navigation path from a root TSOutput
     *
     * Used for:
     * - REF creation (TS→REF) - Need to know root and path to create reference
     * - Owning node access - REF needs the node that owns the output
     * - Debug output - Path can be reconstructed for debugging
     *
     * Uses small vector optimization:
     * - Paths with depth ≤ 4 use inline storage (no heap allocation)
     * - Deeper paths fall back to heap allocation
     *
     * Memory: ~48 bytes fixed (fits in cache line with TSView)
     */
    struct ValuePath {
        const ts::TSOutput* root{nullptr};

        // Small vector optimization - most paths are short (depth ≤ 4)
        static constexpr size_t INLINE_CAPACITY = 4;

    private:
        union Storage {
            size_t inline_indices[INLINE_CAPACITY];
            struct {
                size_t* heap_indices;
                size_t heap_capacity;
            };
            Storage() : inline_indices{} {}
        } _storage;
        uint8_t _depth{0};
        bool _uses_heap{false};

    public:
        ValuePath() = default;

        explicit ValuePath(const ts::TSOutput* root_output)
            : root(root_output), _depth(0), _uses_heap(false) {}

        // Copy constructor
        ValuePath(const ValuePath& other)
            : root(other.root), _depth(other._depth), _uses_heap(other._uses_heap) {
            if (_uses_heap && other._storage.heap_indices) {
                _storage.heap_capacity = other._storage.heap_capacity;
                _storage.heap_indices = new size_t[_storage.heap_capacity];
                std::memcpy(_storage.heap_indices, other._storage.heap_indices,
                           _depth * sizeof(size_t));
            } else {
                std::memcpy(_storage.inline_indices, other._storage.inline_indices,
                           sizeof(_storage.inline_indices));
            }
        }

        // Move constructor
        ValuePath(ValuePath&& other) noexcept
            : root(other.root), _depth(other._depth), _uses_heap(other._uses_heap) {
            if (_uses_heap) {
                _storage.heap_indices = other._storage.heap_indices;
                _storage.heap_capacity = other._storage.heap_capacity;
                other._storage.heap_indices = nullptr;
                other._storage.heap_capacity = 0;
            } else {
                std::memcpy(_storage.inline_indices, other._storage.inline_indices,
                           sizeof(_storage.inline_indices));
            }
            other.root = nullptr;
            other._depth = 0;
            other._uses_heap = false;
        }

        // Copy assignment
        ValuePath& operator=(const ValuePath& other) {
            if (this != &other) {
                // Clean up existing heap storage
                if (_uses_heap && _storage.heap_indices) {
                    delete[] _storage.heap_indices;
                }
                root = other.root;
                _depth = other._depth;
                _uses_heap = other._uses_heap;
                if (_uses_heap && other._storage.heap_indices) {
                    _storage.heap_capacity = other._storage.heap_capacity;
                    _storage.heap_indices = new size_t[_storage.heap_capacity];
                    std::memcpy(_storage.heap_indices, other._storage.heap_indices,
                               _depth * sizeof(size_t));
                } else {
                    std::memcpy(_storage.inline_indices, other._storage.inline_indices,
                               sizeof(_storage.inline_indices));
                }
            }
            return *this;
        }

        // Move assignment
        ValuePath& operator=(ValuePath&& other) noexcept {
            if (this != &other) {
                // Clean up existing heap storage
                if (_uses_heap && _storage.heap_indices) {
                    delete[] _storage.heap_indices;
                }
                root = other.root;
                _depth = other._depth;
                _uses_heap = other._uses_heap;
                if (_uses_heap) {
                    _storage.heap_indices = other._storage.heap_indices;
                    _storage.heap_capacity = other._storage.heap_capacity;
                    other._storage.heap_indices = nullptr;
                    other._storage.heap_capacity = 0;
                } else {
                    std::memcpy(_storage.inline_indices, other._storage.inline_indices,
                               sizeof(_storage.inline_indices));
                }
                other.root = nullptr;
                other._depth = 0;
                other._uses_heap = false;
            }
            return *this;
        }

        ~ValuePath() {
            if (_uses_heap && _storage.heap_indices) {
                delete[] _storage.heap_indices;
            }
        }

        // Returns a new path with the given index appended
        [[nodiscard]] ValuePath with(size_t index) const {
            ValuePath result(*this);
            result.push(index);
            return result;
        }

        // Appends an index to this path (mutating)
        void push(size_t index) {
            size_t new_depth = static_cast<size_t>(_depth) + 1;

            if (!_uses_heap && new_depth <= INLINE_CAPACITY) {
                // Can still use inline storage
                _storage.inline_indices[_depth] = index;
                _depth = static_cast<uint8_t>(new_depth);
            } else if (!_uses_heap) {
                // Need to transition to heap storage
                size_t new_capacity = std::max(static_cast<size_t>(8), new_depth * 2);
                size_t* new_indices = new size_t[new_capacity];
                std::memcpy(new_indices, _storage.inline_indices,
                           _depth * sizeof(size_t));
                new_indices[_depth] = index;
                _storage.heap_indices = new_indices;
                _storage.heap_capacity = new_capacity;
                _uses_heap = true;
                _depth = static_cast<uint8_t>(new_depth);
            } else {
                // Already using heap
                if (new_depth > _storage.heap_capacity) {
                    size_t new_capacity = _storage.heap_capacity * 2;
                    size_t* new_indices = new size_t[new_capacity];
                    std::memcpy(new_indices, _storage.heap_indices,
                               _depth * sizeof(size_t));
                    delete[] _storage.heap_indices;
                    _storage.heap_indices = new_indices;
                    _storage.heap_capacity = new_capacity;
                }
                _storage.heap_indices[_depth] = index;
                _depth = static_cast<uint8_t>(new_depth);
            }
        }

        // Access
        [[nodiscard]] const ts::TSOutput* root_output() const { return root; }
        [[nodiscard]] size_t depth() const { return _depth; }
        [[nodiscard]] bool empty() const { return _depth == 0; }

        [[nodiscard]] size_t operator[](size_t i) const {
            if (i >= _depth) return 0;
            return _uses_heap ? _storage.heap_indices[i] : _storage.inline_indices[i];
        }

        // Get owning node from root TSOutput (defined in ts_output.h)
        [[nodiscard]] node_ptr owning_node() const;

        // Debug string representation
        [[nodiscard]] std::string to_string() const {
            std::string result = "root";
            for (size_t i = 0; i < _depth; ++i) {
                result += "[";
                result += std::to_string((*this)[i]);
                result += "]";
            }
            return result;
        }
    };

    /**
     * TSView - Mutable view with explicit time parameters
     *
     * Time is passed to mutation methods, not stored in the view.
     * This avoids stale time issues and enables explicit control over
     * when modifications are marked.
     *
     * Navigation (field/element) returns sub-views that propagate
     * modifications to the parent.
     *
     * Observer Support:
     * - Optionally holds a pointer to an ObserverStorage for notifications
     * - Modifications trigger notifications that propagate upward
     * - Subscribe/unsubscribe available for hierarchical subscriptions
     *
     * Time-Series Metadata Support:
     * - Optionally holds TSMeta for ts_kind() queries
     * - Optionally holds ValuePath for root/path tracking (REF creation)
     * - delta_view() method when meta is provided
     *
     * This class replaces the previous TSOutputView, consolidating all
     * time-series output view functionality into a single type.
     */
    class TSView {
    public:
        TSView() = default;

        // Basic construction (no TS metadata) - for internal/lower-level use
        TSView(ValueView value_view, ModificationTracker tracker,
                            ObserverStorage* observer = nullptr)
            : _value_view(value_view), _tracker(tracker), _observer(observer) {}

        // Full construction (with TS metadata) - for TSOutput::view()
        TSView(ValueView value_view, ModificationTracker tracker,
                            ObserverStorage* observer, const TSMeta* ts_meta,
                            ValuePath path = {})
            : _value_view(value_view), _tracker(tracker), _observer(observer),
              _ts_meta(ts_meta), _path(std::move(path)) {}

        [[nodiscard]] bool valid() const { return _value_view.valid() && _tracker.valid(); }
        [[nodiscard]] const TypeMeta* schema() const { return _value_view.schema(); }
        [[nodiscard]] const TypeMeta* value_schema() const { return _value_view.schema(); }
        [[nodiscard]] TypeKind kind() const { return _value_view.kind(); }

        // === Time-Series type queries ===
        [[nodiscard]] const TSMeta* ts_meta() const { return _ts_meta; }
        [[nodiscard]] TSKind ts_kind() const;  // Defined after TSKind is complete

        // === Path tracking (for REF creation) ===
        [[nodiscard]] const ValuePath& path() const { return _path; }
        [[nodiscard]] std::string path_string() const { return _path.to_string(); }
        [[nodiscard]] const ts::TSOutput* root_output() const { return _path.root_output(); }
        [[nodiscard]] node_ptr owning_node() const { return _path.owning_node(); }

        // === Delta view access ===
        [[nodiscard]] ts::DeltaView delta_view(engine_time_t time) const;  // Defined in ts_output.h

        // Raw access (without auto-tracking - use with caution)
        [[nodiscard]] ValueView value_view() { return _value_view; }
        [[nodiscard]] ConstValueView value_view() const {
            return static_cast<const ConstValueView&>(_value_view);
        }
        [[nodiscard]] ModificationTracker tracker() { return _tracker; }
        [[nodiscard]] ModificationTracker tracker() const { return _tracker; }
        [[nodiscard]] ObserverStorage* observer() { return _observer; }
        [[nodiscard]] const ObserverStorage* observer() const { return _observer; }

        // === Scalar access ===
        template<typename T>
        [[nodiscard]] T& as() {
            return _value_view.as<T>();
        }

        template<typename T>
        [[nodiscard]] const T& as() const {
            return static_cast<const ConstValueView&>(_value_view).as<T>();
        }

        // Time passed at mutation point
        template<typename T>
        void set(const T& val, engine_time_t time) {
            _value_view.as<T>() = val;
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        void mark_modified(engine_time_t time) {
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        void mark_invalid() {
            _tracker.mark_invalid();
        }

        // === Query methods (time as parameter) ===
        [[nodiscard]] bool modified_at(engine_time_t time) const {
            return _tracker.modified_at(time);
        }

        [[nodiscard]] bool has_value() const {
            return _tracker.valid_value();
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            return _tracker.last_modified_time();
        }

        // === Bundle field navigation ===
        // Returns sub-view. If no specific child observer exists, we pass the parent
        // observer so that notifications still propagate up through the hierarchy.
        // Also propagates ts_meta (field metadata) and extends path.
        [[nodiscard]] TSView field(size_t index) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            ObserverStorage* child_observer = _observer ? _observer->child(index) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            const TSMeta* field_meta = _ts_meta ? field_meta_at(index) : nullptr;
            return {_value_view.field(index), _tracker.field(index), effective_observer,
                    field_meta, _path.with(index)};
        }

        [[nodiscard]] TSView field(const std::string& name) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            auto field_view = _value_view.field(name);
            auto field_tracker = _tracker.field(name);
            ObserverStorage* child_observer = nullptr;
            size_t field_index = 0;
            if (field_view.valid()) {
                auto* bundle_meta = static_cast<const BundleTypeMeta*>(schema());
                auto it = bundle_meta->name_to_index.find(name);
                if (it != bundle_meta->name_to_index.end()) {
                    field_index = it->second;
                    if (_observer) {
                        child_observer = _observer->child(field_index);
                    }
                }
            }
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            const TSMeta* field_meta = _ts_meta ? field_meta_at(field_index) : nullptr;
            return {field_view, field_tracker, effective_observer, field_meta, _path.with(field_index)};
        }

        [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const {
            return _tracker.field_modified_at(index, time);
        }

        [[nodiscard]] size_t field_count() const {
            return _value_view.field_count();
        }

        // === List element navigation ===
        // Also propagates ts_meta (element metadata) and extends path.
        [[nodiscard]] TSView element(size_t index) {
            if (!valid() || kind() != TypeKind::List) {
                return {};
            }
            ObserverStorage* child_observer = _observer ? _observer->child(index) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            const TSMeta* elem_meta = _ts_meta ? element_meta_at() : nullptr;
            return {_value_view.element(index), _tracker.element(index), effective_observer,
                    elem_meta, _path.with(index)};
        }

        [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const {
            return _tracker.element_modified_at(index, time);
        }

        [[nodiscard]] size_t list_size() const {
            return _value_view.list_size();
        }

        // === Set operations (time as parameter) ===
        template<typename T>
        bool add(const T& element, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Set) return false;
            bool added = _value_view.set_add(element);
            if (added) {
                _tracker.mark_modified(time);
                if (_observer) {
                    _observer->notify(time);
                }
            }
            return added;
        }

        template<typename T>
        bool remove(const T& element, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Set) return false;
            bool removed = _value_view.set_remove(element);
            if (removed) {
                _tracker.mark_modified(time);
                if (_observer) {
                    _observer->notify(time);
                }
            }
            return removed;
        }

        template<typename T>
        [[nodiscard]] bool contains(const T& element) const {
            if (!valid() || kind() != TypeKind::Set) return false;
            return _value_view.set_contains(element);
        }

        [[nodiscard]] size_t set_size() const {
            return _value_view.set_size();
        }

        // === Dict operations (time as parameter) ===
        template<typename K, typename V>
        void insert(const K& key, const V& value, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Dict) return;

            bool is_new_key = !_value_view.dict_contains(key);
            _value_view.dict_insert(key, value);

            if (is_new_key) {
                _tracker.mark_modified(time);
            }
            if (_observer) {
                _observer->notify(time);
            }
        }

        template<typename K>
        [[nodiscard]] bool dict_contains(const K& key) const {
            if (!valid() || kind() != TypeKind::Dict) return false;
            return _value_view.dict_contains(key);
        }

        template<typename K>
        [[nodiscard]] ConstValueView dict_get(const K& key) const {
            if (!valid() || kind() != TypeKind::Dict) return {};
            return _value_view.dict_get(key);
        }

        // Dict entry navigation - returns sub-view for a specific entry
        // Also propagates ts_meta (value metadata) and extends path.
        template<typename K>
        [[nodiscard]] TSView entry(const K& key) {
            if (!valid() || kind() != TypeKind::Dict) {
                return {};
            }
            auto entry_view = _value_view.dict_get(key);
            if (!entry_view.valid()) {
                return {};
            }
            auto* storage = static_cast<DictStorage*>(_value_view.data());
            auto idx = storage->find_index(&key);
            if (!idx) {
                return {};
            }
            ObserverStorage* child_observer = _observer ? _observer->child(*idx) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            const TSMeta* value_meta = _ts_meta ? value_meta_at() : nullptr;
            return {entry_view, _tracker, effective_observer, value_meta, _path.with(*idx)};
        }

        template<typename K>
        bool dict_remove(const K& key, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Dict) return false;
            bool removed = _value_view.dict_remove(key);
            if (removed) {
                _tracker.mark_modified(time);
                if (_observer) {
                    _observer->notify(time);
                }
            }
            return removed;
        }

        [[nodiscard]] size_t dict_size() const {
            return _value_view.dict_size();
        }

        // Dict entry navigation using ConstValueView as key
        // Also propagates ts_meta (value metadata) and extends path.
        [[nodiscard]] TSView entry(ConstValueView key) {
            if (!valid() || kind() != TypeKind::Dict || !key.valid()) {
                return {};
            }
            auto entry_view = _value_view.dict_get(key);
            if (!entry_view.valid()) {
                return {};
            }
            auto idx = _value_view.dict_find_index(key);
            if (!idx) {
                return {};
            }
            ObserverStorage* child_observer = _observer ? _observer->child(*idx) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            const TSMeta* valu_meta = _ts_meta ? value_meta_at() : nullptr;
            return {entry_view, _tracker, effective_observer, valu_meta, _path.with(*idx)};
        }

        // === Child observer management ===

        /**
         * Ensure a child observer exists at the given index.
         *
         * This is used when you want to subscribe at a child level before navigation.
         * Returns the child observer, creating it if necessary.
         *
         * @param index The child index (field index for bundles, element index for lists, etc.)
         * @param child_meta Optional type metadata for the child (for proper initialization)
         * @return Pointer to the child observer, or nullptr if no observer exists at all
         */
        [[nodiscard]] ObserverStorage* ensure_child_observer(size_t index, const TypeMeta* child_meta = nullptr) {
            if (!_observer) return nullptr;
            return _observer->ensure_child(index, child_meta);
        }

        /**
         * Navigate to a field with child observer ensured.
         *
         * Unlike field(), this ensures a child observer exists for the field,
         * enabling subscriptions at this level.
         * Also propagates ts_meta (field metadata) and extends path.
         */
        [[nodiscard]] TSView field_with_observer(size_t index) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            auto field_value = _value_view.field(index);
            if (!field_value.valid()) {
                return {};
            }
            ObserverStorage* child_observer = ensure_child_observer(index, field_value.schema());
            const TSMeta* field_meta = _ts_meta ? field_meta_at(index) : nullptr;
            return {field_value, _tracker.field(index), child_observer, field_meta, _path.with(index)};
        }

        [[nodiscard]] TSView field_with_observer(const std::string& name) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(schema());
            auto it = bundle_meta->name_to_index.find(name);
            if (it == bundle_meta->name_to_index.end()) {
                return {};
            }
            return field_with_observer(it->second);
        }

        /**
         * Navigate to an element with child observer ensured.
         * Also propagates ts_meta (element metadata) and extends path.
         */
        [[nodiscard]] TSView element_with_observer(size_t index) {
            if (!valid() || kind() != TypeKind::List) {
                return {};
            }
            auto elem_value = _value_view.element(index);
            if (!elem_value.valid()) {
                return {};
            }
            ObserverStorage* child_observer = ensure_child_observer(index, elem_value.schema());
            const TSMeta* elem_meta = _ts_meta ? element_meta_at() : nullptr;
            return {elem_value, _tracker.element(index), child_observer, elem_meta, _path.with(index)};
        }

        /**
         * Navigate to a dict entry with child observer ensured.
         *
         * Uses ConstValueView for type-safe key passing.
         * Also propagates ts_meta (value metadata) and extends path.
         */
        [[nodiscard]] TSView entry_with_observer(ConstValueView key) {
            if (!valid() || kind() != TypeKind::Dict || !key.valid()) {
                return {};
            }
            auto entry_view = _value_view.dict_get(key);
            if (!entry_view.valid()) {
                return {};
            }
            auto idx = _value_view.dict_find_index(key);
            if (!idx) {
                return {};
            }
            auto* dict_meta = static_cast<const DictTypeMeta*>(schema());
            ObserverStorage* child_observer = ensure_child_observer(*idx, dict_meta->value_type);
            const TSMeta* valu_meta = _ts_meta ? value_meta_at() : nullptr;
            return {entry_view, _tracker, child_observer, valu_meta, _path.with(*idx)};
        }

        // === Window operations (time as parameter) ===
        // Note: timestamp is the value's timestamp, eval_time is for modification tracking
        template<typename T>
        void window_push(const T& value, engine_time_t timestamp, engine_time_t eval_time) {
            if (!valid() || kind() != TypeKind::Window) return;
            _value_view.window_push(value, timestamp);
            _tracker.mark_modified(eval_time);
            if (_observer) {
                _observer->notify(eval_time);
            }
        }

        // Type-safe window push using ConstValueView
        void window_push(ConstValueView value, engine_time_t timestamp, engine_time_t eval_time) {
            if (!valid() || kind() != TypeKind::Window || !value.valid()) return;
            _value_view.window_push(value, timestamp);
            _tracker.mark_modified(eval_time);
            if (_observer) {
                _observer->notify(eval_time);
            }
        }

        [[nodiscard]] ConstValueView window_get(size_t index) const {
            if (!valid() || kind() != TypeKind::Window) return {};
            return _value_view.window_get(index);
        }

        [[nodiscard]] size_t window_size() const {
            return _value_view.window_size();
        }

        [[nodiscard]] bool window_empty() const {
            return _value_view.window_empty();
        }

        [[nodiscard]] bool window_full() const {
            return _value_view.window_full();
        }

        [[nodiscard]] engine_time_t window_timestamp(size_t index) const {
            return _value_view.window_timestamp(index);
        }

        [[nodiscard]] engine_time_t window_oldest_timestamp() const {
            return _value_view.window_oldest_timestamp();
        }

        [[nodiscard]] engine_time_t window_newest_timestamp() const {
            return _value_view.window_newest_timestamp();
        }

        [[nodiscard]] const TypeMeta* window_element_type() const {
            return _value_view.window_element_type();
        }

        [[nodiscard]] bool window_is_fixed_length() const {
            return _value_view.window_is_fixed_length();
        }

        [[nodiscard]] bool window_is_variable_length() const {
            return _value_view.window_is_variable_length();
        }

        void window_compact(engine_time_t eval_time) {
            if (!valid() || kind() != TypeKind::Window) return;
            _value_view.window_compact(eval_time);
        }

        void window_evict_expired(engine_time_t eval_time) {
            if (!valid() || kind() != TypeKind::Window) return;
            _value_view.window_evict_expired(eval_time);
        }

        void window_clear(engine_time_t time) {
            if (!valid() || kind() != TypeKind::Window) return;
            _value_view.window_clear();
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        // === Ref operations (time as parameter) ===

        [[nodiscard]] bool ref_is_empty() const {
            if (!valid() || kind() != TypeKind::Ref) return true;
            return _value_view.ref_is_empty();
        }

        [[nodiscard]] bool ref_is_bound() const {
            if (!valid() || kind() != TypeKind::Ref) return false;
            return _value_view.ref_is_bound();
        }

        [[nodiscard]] bool ref_is_unbound() const {
            if (!valid() || kind() != TypeKind::Ref) return false;
            return _value_view.ref_is_unbound();
        }

        [[nodiscard]] bool ref_is_valid() const {
            if (!valid() || kind() != TypeKind::Ref) return false;
            return _value_view.ref_is_valid();
        }

        [[nodiscard]] const ValueRef* ref_target() const {
            if (!valid() || kind() != TypeKind::Ref) return nullptr;
            return _value_view.ref_target();
        }

        [[nodiscard]] ValueRef* ref_target() {
            if (!valid() || kind() != TypeKind::Ref) return nullptr;
            return _value_view.ref_target();
        }

        [[nodiscard]] size_t ref_item_count() const {
            if (!valid() || kind() != TypeKind::Ref) return 0;
            return _value_view.ref_item_count();
        }

        [[nodiscard]] const TypeMeta* ref_value_type() const {
            if (!valid() || kind() != TypeKind::Ref) return nullptr;
            return _value_view.ref_value_type();
        }

        [[nodiscard]] bool ref_is_atomic() const {
            if (!valid() || kind() != TypeKind::Ref) return false;
            return _value_view.ref_is_atomic();
        }

        [[nodiscard]] bool ref_can_be_unbound() const {
            if (!valid() || kind() != TypeKind::Ref) return false;
            return _value_view.ref_can_be_unbound();
        }

        // Ref navigation for unbound refs
        [[nodiscard]] TSView ref_item(size_t index) {
            if (!valid() || kind() != TypeKind::Ref || !ref_is_unbound()) {
                return {};
            }
            ObserverStorage* child_observer = _observer ? _observer->child(index) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {_value_view.ref_item(index), _tracker.ref_item(index), effective_observer};
        }

        [[nodiscard]] bool ref_item_modified_at(size_t index, engine_time_t time) const {
            return _tracker.ref_item_modified_at(index, time);
        }

        // Mutable ref operations with tracking
        void ref_bind(ValueRef target, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Ref) return;
            _value_view.ref_bind(target);
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        void ref_clear(engine_time_t time) {
            if (!valid() || kind() != TypeKind::Ref) return;
            _value_view.ref_clear();
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        void ref_make_unbound(size_t count, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Ref) return;
            _value_view.ref_make_unbound(count);
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        void ref_set_item(size_t index, ValueRef target, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Ref || !ref_is_unbound()) return;
            _value_view.ref_set_item(index, target);
            auto item_tracker = _tracker.ref_item(index);
            if (item_tracker.valid()) {
                item_tracker.mark_modified(time);
            }
            if (_observer) {
                _observer->notify(time);
            }
        }

        // === String representation ===
        [[nodiscard]] std::string to_string() const {
            return _value_view.to_string();
        }

        // Debug string with modification status (requires time parameter)
        [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
            std::string result = "TS[";
            result += _value_view.schema() ? (_value_view.schema()->name ? _value_view.schema()->name : "?") : "null";
            result += "]@";
            // Use const accessor for data pointer
            result += std::to_string(reinterpret_cast<uintptr_t>(
                static_cast<const ConstValueView&>(_value_view).data()));
            result += "(value=\"";
            result += to_string();
            result += "\", modified=";
            result += _tracker.modified_at(time) ? "true" : "false";
            result += ")";
            return result;
        }

    private:
        ValueView _value_view;
        ModificationTracker _tracker;
        ObserverStorage* _observer{nullptr};
        const TSMeta* _ts_meta{nullptr};  // Optional TS type metadata
        ValuePath _path;  // Path from root TSOutput (for REF creation)

        // Helper methods for navigation - defined in ts_output.h where TSMeta is complete
        [[nodiscard]] const TSMeta* field_meta_at(size_t index) const;
        [[nodiscard]] const TSMeta* element_meta_at() const;
        [[nodiscard]] const TSMeta* value_meta_at() const;
    };

    /**
     * TSValue - Owning container for time-series value
     *
     * Combines Value (data storage) with ModificationTrackerStorage
     * (modification tracking) into a unified time-series value.
     *
     * Provides:
     * - Value storage and access
     * - Modification tracking at appropriate granularity per type
     * - Hierarchical propagation (field change → bundle modified)
     * - Observer/notification support (lazy allocation)
     */
    class TSValue {
    public:
        TSValue() = default;

        explicit TSValue(const TypeMeta* schema)
            : _value(schema), _tracker(schema) {}

        // Move only
        TSValue(TSValue&&) noexcept = default;
        TSValue& operator=(TSValue&&) noexcept = default;
        TSValue(const TSValue&) = delete;
        TSValue& operator=(const TSValue&) = delete;

        // Schema access
        [[nodiscard]] const TypeMeta* schema() const { return _value.schema(); }
        [[nodiscard]] TypeKind kind() const { return _value.kind(); }
        [[nodiscard]] bool valid() const { return _value.valid() && _tracker.valid(); }

        // Value access (read-only)
        [[nodiscard]] ConstValueView value() const { return _value.const_view(); }

        // Modification state queries (time as parameter)
        [[nodiscard]] bool modified_at(engine_time_t time) const {
            return _tracker.tracker().modified_at(time);
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            return _tracker.tracker().last_modified_time();
        }

        [[nodiscard]] bool has_value() const {
            return _tracker.tracker().valid_value();
        }

        void mark_invalid() {
            _tracker.tracker().mark_invalid();
        }

        // Mutable access - returns view without stored time
        [[nodiscard]] TSView view() {
            return {_value.view(), _tracker.tracker(), _observers.get()};
        }

        // Direct scalar access (convenience for simple TS values)
        template<typename T>
        void set_value(const T& val, engine_time_t time) {
            _value.view().as<T>() = val;
            _tracker.tracker().mark_modified(time);
            if (_observers) {
                _observers->notify(time);
            }
        }

        template<typename T>
        [[nodiscard]] const T& as() const {
            return _value.const_view().as<T>();
        }

        // Observer/subscription API (lazy allocation)
        void subscribe(hgraph::Notifiable* notifiable) {
            ensure_observers();
            _observers->subscribe(notifiable);
        }

        void unsubscribe(hgraph::Notifiable* notifiable) {
            if (_observers) {
                _observers->unsubscribe(notifiable);
            }
        }

        [[nodiscard]] bool has_observers() const {
            return _observers && _observers->has_subscribers();
        }

        // Access underlying storage (for advanced use)
        [[nodiscard]] Value& underlying_value() { return _value; }
        [[nodiscard]] const Value& underlying_value() const { return _value; }
        [[nodiscard]] ModificationTrackerStorage& underlying_tracker() { return _tracker; }
        [[nodiscard]] const ModificationTrackerStorage& underlying_tracker() const { return _tracker; }
        [[nodiscard]] ObserverStorage* underlying_observers() { return _observers.get(); }
        [[nodiscard]] const ObserverStorage* underlying_observers() const { return _observers.get(); }

        // String representation - value only
        [[nodiscard]] std::string to_string() const {
            return _value.to_string();
        }

        // Debug string with modification status (requires time parameter)
        [[nodiscard]] std::string to_debug_string(engine_time_t current_time) const {
            std::string result = "TS[";
            result += schema() ? (schema()->name ? schema()->name : "?") : "null";
            result += "]@";
            result += std::to_string(reinterpret_cast<uintptr_t>(_value.data()));
            result += "(value=\"";
            result += to_string();
            result += "\", modified=";
            result += modified_at(current_time) ? "true" : "false";
            result += ", last_modified=";
            result += hgraph::to_string(last_modified_time());
            result += ")";
            return result;
        }

        // Ensure observer storage exists (for subscription at nested levels)
        void ensure_observers() {
            if (!_observers) {
                _observers = std::make_unique<ObserverStorage>(_value.schema());
            }
        }

        // Access observer storage directly (may be null if never subscribed)
        [[nodiscard]] ObserverStorage* observers() { return _observers.get(); }
        [[nodiscard]] const ObserverStorage* observers() const { return _observers.get(); }

    private:
        Value _value;
        ModificationTrackerStorage _tracker;
        std::unique_ptr<ObserverStorage> _observers;  // Lazy: nullptr until first subscribe
    };

    // Alias for backward compatibility
    using TimeSeriesValueView = TSView;
    using TimeSeriesValue = TSValue;

} // namespace hgraph::value

#endif // HGRAPH_VALUE_TIME_SERIES_VALUE_H