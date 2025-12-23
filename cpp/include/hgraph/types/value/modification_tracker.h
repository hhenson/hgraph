//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_MODIFICATION_TRACKER_H
#define HGRAPH_VALUE_MODIFICATION_TRACKER_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/bundle_type.h>
#include <hgraph/types/value/list_type.h>
#include <hgraph/types/value/set_type.h>
#include <hgraph/types/value/dict_type.h>
#include <hgraph/types/value/ref_type.h>
#include <hgraph/util/date_time.h>
#include <ankerl/unordered_dense.h>
#include <memory>
#include <cassert>
#include <vector>
#include <string>

namespace hgraph::value {

    /**
     * SetModificationStorage - Dynamic storage for set modification timestamps
     *
     * Tracks per-element modification times using a unified approach:
     * - Normal time = when element was added/last modified
     * - MAX_DT = element marked for removal (pending deletion at end of cycle)
     *
     * This enables TSS (Time-Series Set) to track:
     * - added() = elements where time == current_time
     * - removed() = elements where time == MAX_DT
     * - Deferred deletion allows accessing removed elements during the tick
     *
     * At end of evaluation cycle, finalize_removals() must be called to
     * actually delete MAX_DT-marked elements from SetStorage.
     */
    struct SetModificationStorage {
        engine_time_t structural_modified{MIN_DT};
        ankerl::unordered_dense::map<size_t, engine_time_t> element_modified_at;
        const TypeMeta* element_type{nullptr};

        SetModificationStorage() = default;

        explicit SetModificationStorage(const TypeMeta* elem_type)
            : element_type(elem_type) {}

        void mark_structural_modified(engine_time_t time) {
            if (time > structural_modified) {
                structural_modified = time;
            }
        }

        // Mark element as added at given time
        void mark_element_added(size_t index, engine_time_t time) {
            element_modified_at[index] = time;
            mark_structural_modified(time);
        }

        // Mark element for removal (deferred deletion)
        // Element stays in SetStorage until finalize_removals() is called
        void mark_for_removal(size_t index, engine_time_t time) {
            element_modified_at[index] = MAX_DT;
            mark_structural_modified(time);
        }

        // Check if element is pending removal
        [[nodiscard]] bool is_pending_removal(size_t index) const {
            auto it = element_modified_at.find(index);
            return it != element_modified_at.end() && it->second == MAX_DT;
        }

        // Remove tracking for an element (after actual deletion)
        void remove_element_tracking(size_t index) {
            element_modified_at.erase(index);
        }

        [[nodiscard]] bool structurally_modified_at(engine_time_t time) const {
            return structural_modified == time;
        }

        // Check if element was added at specific time (excludes pending removals)
        [[nodiscard]] bool element_added_at_time(size_t index, engine_time_t time) const {
            auto it = element_modified_at.find(index);
            return it != element_modified_at.end() && it->second == time && it->second != MAX_DT;
        }

        [[nodiscard]] engine_time_t element_last_modified_time(size_t index) const {
            auto it = element_modified_at.find(index);
            if (it == element_modified_at.end()) return MIN_DT;
            // MAX_DT means pending removal - treat as not having a valid modification time
            return it->second != MAX_DT ? it->second : MIN_DT;
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            return structural_modified;
        }

        // Delta access - count of elements added at given time
        [[nodiscard]] size_t added_count(engine_time_t time) const {
            size_t count = 0;
            for (const auto& [idx, t] : element_modified_at) {
                if (t == time) ++count;
            }
            return count;
        }

        // Delta access - count of elements pending removal
        [[nodiscard]] size_t removed_count() const {
            size_t count = 0;
            for (const auto& [idx, t] : element_modified_at) {
                if (t == MAX_DT) ++count;
            }
            return count;
        }

        // Get indices of elements added at given time
        [[nodiscard]] std::vector<size_t> added_indices(engine_time_t time) const {
            std::vector<size_t> result;
            for (const auto& [idx, t] : element_modified_at) {
                if (t == time) result.push_back(idx);
            }
            return result;
        }

        // Get indices of elements pending removal
        [[nodiscard]] std::vector<size_t> removed_indices() const {
            std::vector<size_t> result;
            for (const auto& [idx, t] : element_modified_at) {
                if (t == MAX_DT) result.push_back(idx);
            }
            return result;
        }

        // Finalize removals - actually delete elements from SetStorage
        // Must be called at end of evaluation cycle
        // Returns number of elements removed
        size_t finalize_removals(SetStorage* storage) {
            auto indices = removed_indices();
            for (size_t idx : indices) {
                storage->remove_by_index(idx);
                element_modified_at.erase(idx);
            }
            return indices.size();
        }

        // Clear delta state for next tick (keeps element tracking, clears structural flag)
        // Note: Does NOT finalize removals - that must be done separately
        void clear_delta() {
            // Nothing to clear with MAX_DT approach - removals stay marked until finalized
        }

        void clear() {
            structural_modified = MIN_DT;
            element_modified_at.clear();
        }
    };

    /**
     * DictModificationStorage - Dynamic storage for dict modification timestamps
     *
     * Tracks:
     * - Structural modifications (add/remove keys) via SetModificationStorage
     * - Per-entry value modifications (value changes on existing keys)
     * - Old values for delta_value support
     *
     * This enables TSD (Time-Series Dict) to track both structural changes
     * (added/removed keys) and value modifications on existing keys.
     */
    struct DictModificationStorage {
        // Key tracking - reuses Set's logic entirely
        SetModificationStorage key_tracking;

        // Value modification times (for updates to existing keys)
        ankerl::unordered_dense::map<size_t, engine_time_t> value_modified_at;

        // Old value tracking for updates (delta_value support)
        std::vector<char> old_values;
        std::vector<size_t> old_value_indices;
        size_t old_value_count{0};
        const TypeMeta* value_type{nullptr};

        DictModificationStorage() = default;

        DictModificationStorage(const TypeMeta* key_type, const TypeMeta* val_type)
            : key_tracking(key_type), value_type(val_type) {}

        // Structural operations delegate to key_tracking
        void mark_structural_modified(engine_time_t time) {
            key_tracking.mark_structural_modified(time);
        }

        void mark_key_added(size_t index, engine_time_t time) {
            key_tracking.mark_element_added(index, time);
        }

        // Mark key for removal (deferred deletion using MAX_DT)
        void mark_key_for_removal(size_t index, engine_time_t time) {
            key_tracking.mark_for_removal(index, time);
        }

        // Check if key is pending removal
        [[nodiscard]] bool is_key_pending_removal(size_t index) const {
            return key_tracking.is_pending_removal(index);
        }

        void remove_key_tracking(size_t index) {
            key_tracking.remove_element_tracking(index);
            value_modified_at.erase(index);
        }

        // Get indices of keys pending removal
        [[nodiscard]] std::vector<size_t> removed_key_indices() const {
            return key_tracking.removed_indices();
        }

        // Finalize key removals - actually delete keys from DictStorage
        // Must be called at end of evaluation cycle
        size_t finalize_key_removals(SetStorage* key_set_storage) {
            auto indices = key_tracking.removed_indices();
            for (size_t idx : indices) {
                key_set_storage->remove_by_index(idx);
                key_tracking.remove_element_tracking(idx);
                value_modified_at.erase(idx);
            }
            return indices.size();
        }

        // Value modification tracking
        void mark_value_modified(size_t index, engine_time_t time) {
            value_modified_at[index] = time;
        }

        // Record old value before updating (for delta_value)
        void record_old_value(size_t index, const void* old_val) {
            if (!value_type) return;
            size_t val_size = value_type->size;
            size_t offset = old_value_count * val_size;
            old_values.resize(offset + val_size);
            value_type->copy_construct_at(old_values.data() + offset, old_val);
            old_value_indices.push_back(index);
            ++old_value_count;
        }

        [[nodiscard]] bool structurally_modified_at(engine_time_t time) const {
            return key_tracking.structurally_modified_at(time);
        }

        [[nodiscard]] bool key_added_at(size_t index, engine_time_t time) const {
            return key_tracking.element_added_at_time(index, time);
        }

        [[nodiscard]] bool value_modified_at_time(size_t index, engine_time_t time) const {
            auto it = value_modified_at.find(index);
            return it != value_modified_at.end() && it->second == time;
        }

        // Combined: key added OR value modified
        [[nodiscard]] bool entry_modified_at(size_t index, engine_time_t time) const {
            return key_added_at(index, time) || value_modified_at_time(index, time);
        }

        [[nodiscard]] engine_time_t entry_last_modified_time(size_t index) const {
            engine_time_t key_time = key_tracking.element_last_modified_time(index);
            auto it = value_modified_at.find(index);
            engine_time_t val_time = (it != value_modified_at.end()) ? it->second : MIN_DT;
            return std::max(key_time, val_time);
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            engine_time_t max_time = key_tracking.last_modified_time();
            for (const auto& [idx, time] : value_modified_at) {
                if (time > max_time) {
                    max_time = time;
                }
            }
            return max_time;
        }

        // Delta access for old values
        [[nodiscard]] size_t updated_value_count() const {
            return old_value_count;
        }

        [[nodiscard]] const void* old_value(size_t i) const {
            if (!value_type || i >= old_value_count) return nullptr;
            return old_values.data() + i * value_type->size;
        }

        [[nodiscard]] size_t old_value_entry_index(size_t i) const {
            return i < old_value_indices.size() ? old_value_indices[i] : 0;
        }

        // Cleanup - destructs non-trivial types
        void clear_delta() {
            key_tracking.clear_delta();
            // Destruct old values
            if (value_type && old_value_count > 0) {
                for (size_t i = 0; i < old_value_count; ++i) {
                    void* ptr = old_values.data() + i * value_type->size;
                    value_type->destruct_at(ptr);
                }
            }
            old_values.clear();
            old_value_indices.clear();
            old_value_count = 0;
        }

        void clear() {
            key_tracking.clear();
            value_modified_at.clear();
            clear_delta();
        }
    };

    // Forward declaration
    class ModificationTracker;

    /**
     * ModificationTrackerStorage - Hierarchical storage for modification tracking
     *
     * Mirrors the type structure to track modifications at any level:
     * - Root level: tracks overall modification time
     * - Field/element level: tracks modifications for nested structures
     *
     * Modifications propagate upward: a change at a leaf updates all ancestors.
     *
     * Storage layout by type:
     * - Scalar: single engine_time_t (own timestamp only)
     * - Bundle: own timestamp + recursive child storage for each field
     * - List: own timestamp + recursive child storage for each element
     * - Set: SetModificationStorage (structural + per-element tracking)
     * - Dict: DictModificationStorage (structural + per-entry timestamps)
     * - Window: single engine_time_t (atomic)
     * - Ref: own timestamp + optional child storage for composite refs
     */
    class ModificationTrackerStorage {
    public:
        ModificationTrackerStorage() = default;

        explicit ModificationTrackerStorage(const TypeMeta* value_meta)
            : _value_meta(value_meta) {
            if (!_value_meta) return;
            allocate_storage();
        }

        ~ModificationTrackerStorage() {
            deallocate_storage();
        }

        // Move only
        ModificationTrackerStorage(ModificationTrackerStorage&& other) noexcept
            : _value_meta(other._value_meta)
            , _storage(other._storage)
            , _parent(other._parent)
            , _children(std::move(other._children)) {
            other._value_meta = nullptr;
            other._storage = nullptr;
            other._parent = nullptr;
            // Update children's parent pointers
            for (auto& child : _children) {
                if (child) {
                    child->_parent = this;
                }
            }
        }

        ModificationTrackerStorage& operator=(ModificationTrackerStorage&& other) noexcept {
            if (this != &other) {
                deallocate_storage();
                _value_meta = other._value_meta;
                _storage = other._storage;
                _parent = other._parent;
                _children = std::move(other._children);
                other._value_meta = nullptr;
                other._storage = nullptr;
                other._parent = nullptr;
                // Update children's parent pointers
                for (auto& child : _children) {
                    if (child) {
                        child->_parent = this;
                    }
                }
            }
            return *this;
        }

        ModificationTrackerStorage(const ModificationTrackerStorage&) = delete;
        ModificationTrackerStorage& operator=(const ModificationTrackerStorage&) = delete;

        [[nodiscard]] const TypeMeta* value_meta() const { return _value_meta; }
        [[nodiscard]] void* storage() { return _storage; }
        [[nodiscard]] const void* storage() const { return _storage; }
        [[nodiscard]] bool valid() const { return _value_meta && _storage; }

        // Parent linkage for upward modification propagation
        void set_parent(ModificationTrackerStorage* parent) { _parent = parent; }
        [[nodiscard]] ModificationTrackerStorage* parent() const { return _parent; }

        // Get own timestamp (for propagation)
        [[nodiscard]] engine_time_t* timestamp_ptr() {
            if (!_storage) return nullptr;
            switch (_value_meta->kind) {
                case TypeKind::Scalar:
                case TypeKind::Bundle:
                case TypeKind::List:
                case TypeKind::DynamicList:
                case TypeKind::Window:
                case TypeKind::Ref:
                    return static_cast<engine_time_t*>(_storage);
                case TypeKind::Set:
                    return &static_cast<SetModificationStorage*>(_storage)->structural_modified;
                case TypeKind::Dict:
                    return &static_cast<DictModificationStorage*>(_storage)->key_tracking.structural_modified;
                default:
                    return nullptr;
            }
        }

        // Child storage access (for hierarchical tracking)
        [[nodiscard]] ModificationTrackerStorage* child(size_t index) {
            if (index >= _children.size()) {
                return nullptr;
            }
            return _children[index].get();
        }

        [[nodiscard]] const ModificationTrackerStorage* child(size_t index) const {
            if (index >= _children.size()) {
                return nullptr;
            }
            return _children[index].get();
        }

        // Propagate modification time to parent
        void propagate_to_parent(engine_time_t time) {
            if (_parent) {
                auto* parent_ts = _parent->timestamp_ptr();
                if (parent_ts && time > *parent_ts) {
                    *parent_ts = time;
                }
                _parent->propagate_to_parent(time);
            }
        }

        ModificationTracker tracker();
        [[nodiscard]] ModificationTracker tracker() const;

    private:
        void allocate_storage();
        void deallocate_storage();

        const TypeMeta* _value_meta{nullptr};
        void* _storage{nullptr};  // Own timestamp storage
        ModificationTrackerStorage* _parent{nullptr};  // For upward propagation
        std::vector<std::unique_ptr<ModificationTrackerStorage>> _children;  // Child storage for nested types
    };

    /**
     * ModificationTracker - View into modification tracking storage
     *
     * Non-owning view that provides access to modification timestamps.
     * Can represent the whole value or a sub-element (field, element).
     *
     * For nested types, sub-trackers reference child ModificationTrackerStorage
     * to enable proper hierarchical tracking and propagation.
     */
    class ModificationTracker {
    public:
        ModificationTracker() = default;

        // Tracker backed by storage (has access to children and parent propagation)
        explicit ModificationTracker(ModificationTrackerStorage* storage)
            : _owner(storage)
            , _storage(storage ? storage->storage() : nullptr)
            , _value_meta(storage ? storage->value_meta() : nullptr) {}

        [[nodiscard]] bool valid() const { return _storage && _value_meta; }
        [[nodiscard]] const TypeMeta* value_schema() const { return _value_meta; }
        [[nodiscard]] void* storage() { return _storage; }
        [[nodiscard]] const void* storage() const { return _storage; }
        [[nodiscard]] ModificationTrackerStorage* owner() const { return _owner; }

        // Query modification state
        [[nodiscard]] bool modified_at(engine_time_t time) const {
            return last_modified_time() == time;
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            if (!valid()) return MIN_DT;

            switch (_value_meta->kind) {
                case TypeKind::Scalar:
                case TypeKind::Bundle:
                case TypeKind::List:
                case TypeKind::DynamicList:
                case TypeKind::Window:
                case TypeKind::Ref:
                    return *static_cast<engine_time_t*>(_storage);

                case TypeKind::Set:
                    return static_cast<SetModificationStorage*>(_storage)->last_modified_time();

                case TypeKind::Dict:
                    return static_cast<DictModificationStorage*>(_storage)->last_modified_time();

                default:
                    return MIN_DT;
            }
        }

        [[nodiscard]] bool valid_value() const {
            return last_modified_time() > MIN_DT;
        }

        // Mark as modified (propagates to parent via storage hierarchy)
        void mark_modified(engine_time_t time) {
            if (!valid()) return;

            switch (_value_meta->kind) {
                case TypeKind::Scalar:
                case TypeKind::Bundle:
                case TypeKind::List:
                case TypeKind::DynamicList:
                case TypeKind::Window:
                case TypeKind::Ref: {
                    auto* ts = static_cast<engine_time_t*>(_storage);
                    if (time > *ts) {
                        *ts = time;
                    }
                    break;
                }
                case TypeKind::Set:
                    static_cast<SetModificationStorage*>(_storage)->mark_structural_modified(time);
                    break;

                case TypeKind::Dict:
                    static_cast<DictModificationStorage*>(_storage)->mark_structural_modified(time);
                    break;

                default:
                    break;
            }

            // Propagate to parent via storage hierarchy
            if (_owner) {
                _owner->propagate_to_parent(time);
            }
        }

        void mark_invalid() {
            if (!valid()) return;

            switch (_value_meta->kind) {
                case TypeKind::Scalar:
                case TypeKind::Bundle:
                case TypeKind::List:
                case TypeKind::DynamicList:
                case TypeKind::Window:
                case TypeKind::Ref:
                    *static_cast<engine_time_t*>(_storage) = MIN_DT;
                    break;

                case TypeKind::Set:
                    static_cast<SetModificationStorage*>(_storage)->clear();
                    break;

                case TypeKind::Dict:
                    static_cast<DictModificationStorage*>(_storage)->clear();
                    break;

                default:
                    break;
            }
        }

        // For bundles - field-level tracking using child storage
        [[nodiscard]] ModificationTracker field(size_t index) {
            if (!valid() || _value_meta->kind != TypeKind::Bundle) {
                return {};
            }

            auto* bundle_meta = static_cast<const BundleTypeMeta*>(_value_meta);
            if (index >= bundle_meta->fields.size()) {
                return {};
            }

            // Use child storage if available (hierarchical mode)
            if (_owner) {
                auto* child_storage = _owner->child(index);
                if (child_storage) {
                    return ModificationTracker(child_storage);
                }
            }

            // Fallback for non-hierarchical trackers (shouldn't happen in normal use)
            return {};
        }

        [[nodiscard]] ModificationTracker field(const std::string& name) {
            if (!valid() || _value_meta->kind != TypeKind::Bundle) {
                return {};
            }

            auto* bundle_meta = static_cast<const BundleTypeMeta*>(_value_meta);
            auto it = bundle_meta->name_to_index.find(name);
            if (it == bundle_meta->name_to_index.end()) {
                return {};
            }

            return field(it->second);
        }

        [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Bundle) {
                return false;
            }

            auto* bundle_meta = static_cast<const BundleTypeMeta*>(_value_meta);
            if (index >= bundle_meta->fields.size()) {
                return false;
            }

            // Use child storage to check modification
            if (_owner) {
                auto* child_storage = _owner->child(index);
                if (child_storage) {
                    auto* child_ts = child_storage->timestamp_ptr();
                    return child_ts && *child_ts == time;
                }
            }

            return false;
        }

        // For lists - element-level tracking using child storage
        [[nodiscard]] ModificationTracker element(size_t index) {
            if (!valid() || _value_meta->kind != TypeKind::List) {
                return {};
            }

            auto* list_meta = static_cast<const ListTypeMeta*>(_value_meta);
            if (index >= list_meta->count) {
                return {};
            }

            // Use child storage if available (hierarchical mode)
            if (_owner) {
                auto* child_storage = _owner->child(index);
                if (child_storage) {
                    return ModificationTracker(child_storage);
                }
            }

            // Fallback for non-hierarchical trackers
            return {};
        }

        [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::List) {
                return false;
            }

            auto* list_meta = static_cast<const ListTypeMeta*>(_value_meta);
            if (index >= list_meta->count) {
                return false;
            }

            // Use child storage to check modification
            if (_owner) {
                auto* child_storage = _owner->child(index);
                if (child_storage) {
                    auto* child_ts = child_storage->timestamp_ptr();
                    return child_ts && *child_ts == time;
                }
            }

            return false;
        }

        // For sets - structural and element tracking
        [[nodiscard]] bool set_structurally_modified_at(engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Set) return false;
            return static_cast<SetModificationStorage*>(_storage)->structurally_modified_at(time);
        }

        void mark_set_element_added(size_t index, engine_time_t time) {
            if (!valid() || _value_meta->kind != TypeKind::Set) return;
            auto* storage = static_cast<SetModificationStorage*>(_storage);
            storage->mark_element_added(index, time);
            if (_owner) _owner->propagate_to_parent(time);
        }

        // Mark element for removal (deferred deletion using MAX_DT)
        void mark_set_for_removal(size_t index, engine_time_t time) {
            if (!valid() || _value_meta->kind != TypeKind::Set) return;
            auto* storage = static_cast<SetModificationStorage*>(_storage);
            storage->mark_for_removal(index, time);
            if (_owner) _owner->propagate_to_parent(time);
        }

        // Check if element is pending removal
        // Works for both Set tracker and Dict tracker (checks dict's key_tracking)
        [[nodiscard]] bool is_set_pending_removal(size_t index) const {
            if (!valid()) return false;
            if (_value_meta->kind == TypeKind::Set) {
                return static_cast<SetModificationStorage*>(_storage)->is_pending_removal(index);
            }
            if (_value_meta->kind == TypeKind::Dict) {
                // For dict's key_set view, check the key_tracking in DictModificationStorage
                return static_cast<DictModificationStorage*>(_storage)->is_key_pending_removal(index);
            }
            return false;
        }

        [[nodiscard]] bool set_element_added_at(size_t index, engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Set) return false;
            return static_cast<SetModificationStorage*>(_storage)->element_added_at_time(index, time);
        }

        void remove_set_element_tracking(size_t index) {
            if (!valid() || _value_meta->kind != TypeKind::Set) return;
            static_cast<SetModificationStorage*>(_storage)->remove_element_tracking(index);
        }

        // Set delta access
        [[nodiscard]] size_t set_added_count(engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Set) return 0;
            return static_cast<SetModificationStorage*>(_storage)->added_count(time);
        }

        [[nodiscard]] size_t set_removed_count() const {
            if (!valid() || _value_meta->kind != TypeKind::Set) return 0;
            return static_cast<SetModificationStorage*>(_storage)->removed_count();
        }

        // Get indices of elements added at given time
        [[nodiscard]] std::vector<size_t> set_added_indices(engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Set) return {};
            return static_cast<SetModificationStorage*>(_storage)->added_indices(time);
        }

        // Get indices of elements pending removal
        [[nodiscard]] std::vector<size_t> set_removed_indices() const {
            if (!valid() || _value_meta->kind != TypeKind::Set) return {};
            return static_cast<SetModificationStorage*>(_storage)->removed_indices();
        }

        // Finalize set removals - actually delete elements from SetStorage
        // Must be called at end of evaluation cycle
        size_t finalize_set_removals(SetStorage* set_storage) {
            if (!valid() || _value_meta->kind != TypeKind::Set) return 0;
            return static_cast<SetModificationStorage*>(_storage)->finalize_removals(set_storage);
        }

        void clear_set_delta() {
            if (!valid() || _value_meta->kind != TypeKind::Set) return;
            static_cast<SetModificationStorage*>(_storage)->clear_delta();
        }

        // For dicts - structural and entry tracking
        [[nodiscard]] bool dict_structurally_modified_at(engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return false;
            return static_cast<DictModificationStorage*>(_storage)->structurally_modified_at(time);
        }

        void mark_dict_key_added(size_t entry_index, engine_time_t time) {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return;
            auto* storage = static_cast<DictModificationStorage*>(_storage);
            storage->mark_key_added(entry_index, time);
            if (_owner) _owner->propagate_to_parent(time);
        }

        // Mark dict key for removal (deferred deletion using MAX_DT)
        void mark_dict_key_for_removal(size_t entry_index, engine_time_t time) {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return;
            auto* storage = static_cast<DictModificationStorage*>(_storage);
            storage->mark_key_for_removal(entry_index, time);
            if (_owner) _owner->propagate_to_parent(time);
        }

        // Check if dict key is pending removal
        [[nodiscard]] bool is_dict_key_pending_removal(size_t entry_index) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return false;
            return static_cast<DictModificationStorage*>(_storage)->is_key_pending_removal(entry_index);
        }

        void mark_dict_value_modified(size_t entry_index, engine_time_t time) {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return;
            auto* storage = static_cast<DictModificationStorage*>(_storage);
            storage->mark_value_modified(entry_index, time);
            if (_owner) _owner->propagate_to_parent(time);
        }

        void record_dict_old_value(size_t entry_index, const void* old_val) {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return;
            static_cast<DictModificationStorage*>(_storage)->record_old_value(entry_index, old_val);
        }

        [[nodiscard]] bool dict_key_added_at(size_t entry_index, engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return false;
            return static_cast<DictModificationStorage*>(_storage)->key_added_at(entry_index, time);
        }

        [[nodiscard]] bool dict_value_modified_at(size_t entry_index, engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return false;
            return static_cast<DictModificationStorage*>(_storage)->value_modified_at_time(entry_index, time);
        }

        [[nodiscard]] bool dict_entry_modified_at(size_t entry_index, engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return false;
            return static_cast<DictModificationStorage*>(_storage)->entry_modified_at(entry_index, time);
        }

        [[nodiscard]] engine_time_t dict_entry_last_modified(size_t entry_index) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return MIN_DT;
            return static_cast<DictModificationStorage*>(_storage)->entry_last_modified_time(entry_index);
        }

        void remove_dict_entry_tracking(size_t entry_index) {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return;
            static_cast<DictModificationStorage*>(_storage)->remove_key_tracking(entry_index);
        }

        // Dict delta access
        [[nodiscard]] size_t dict_added_count(engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return 0;
            return static_cast<DictModificationStorage*>(_storage)->key_tracking.added_count(time);
        }

        [[nodiscard]] size_t dict_removed_count() const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return 0;
            return static_cast<DictModificationStorage*>(_storage)->key_tracking.removed_count();
        }

        // Get indices of dict keys pending removal
        [[nodiscard]] std::vector<size_t> dict_removed_key_indices() const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return {};
            return static_cast<DictModificationStorage*>(_storage)->removed_key_indices();
        }

        // Finalize dict key removals - actually delete keys from DictStorage's key set
        // Must be called at end of evaluation cycle
        size_t finalize_dict_key_removals(SetStorage* key_set_storage) {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return 0;
            return static_cast<DictModificationStorage*>(_storage)->finalize_key_removals(key_set_storage);
        }

        [[nodiscard]] size_t dict_updated_count() const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return 0;
            return static_cast<DictModificationStorage*>(_storage)->updated_value_count();
        }

        [[nodiscard]] ConstTypedPtr dict_old_value(size_t i) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return {};
            auto* storage = static_cast<DictModificationStorage*>(_storage);
            const void* val = storage->old_value(i);
            return val ? ConstTypedPtr{val, storage->value_type} : ConstTypedPtr{};
        }

        [[nodiscard]] size_t dict_old_value_entry_index(size_t i) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return 0;
            return static_cast<DictModificationStorage*>(_storage)->old_value_entry_index(i);
        }

        void clear_dict_delta() {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return;
            static_cast<DictModificationStorage*>(_storage)->clear_delta();
        }

        // For refs - item-level tracking (composite refs only)
        [[nodiscard]] ModificationTracker ref_item(size_t index) {
            if (!valid() || _value_meta->kind != TypeKind::Ref) {
                return {};
            }

            auto* ref_meta = static_cast<const RefTypeMeta*>(_value_meta);
            if (ref_meta->item_count == 0 || index >= ref_meta->item_count) {
                return {};  // Atomic ref or out of bounds
            }

            // Use child storage if available (hierarchical mode)
            if (_owner) {
                auto* child_storage = _owner->child(index);
                if (child_storage) {
                    return ModificationTracker(child_storage);
                }
            }

            return {};
        }

        [[nodiscard]] bool ref_item_modified_at(size_t index, engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Ref) {
                return false;
            }

            auto* ref_meta = static_cast<const RefTypeMeta*>(_value_meta);
            if (ref_meta->item_count == 0 || index >= ref_meta->item_count) {
                return false;
            }

            // Use child storage to check modification
            if (_owner) {
                auto* child_storage = _owner->child(index);
                if (child_storage) {
                    auto* child_ts = child_storage->timestamp_ptr();
                    return child_ts && *child_ts == time;
                }
            }

            return false;
        }

    private:
        ModificationTrackerStorage* _owner{nullptr};  // Owning storage (for hierarchy access)
        void* _storage{nullptr};                      // Direct storage pointer
        const TypeMeta* _value_meta{nullptr};
    };

    // Implementation of ModificationTrackerStorage methods

    inline void ModificationTrackerStorage::allocate_storage() {
        if (!_value_meta) return;

        switch (_value_meta->kind) {
            case TypeKind::Scalar:
            case TypeKind::Window:  // Window is atomic
                // Single timestamp (no children)
                _storage = new engine_time_t{MIN_DT};
                break;

            case TypeKind::Set: {
                auto* set_meta = static_cast<const SetTypeMeta*>(_value_meta);
                _storage = new SetModificationStorage(set_meta->element_type);
                break;
            }

            case TypeKind::Bundle: {
                // Own timestamp + recursive children for each field
                _storage = new engine_time_t{MIN_DT};
                auto* bundle_meta = static_cast<const BundleTypeMeta*>(_value_meta);
                _children.reserve(bundle_meta->fields.size());
                for (const auto& field : bundle_meta->fields) {
                    auto child = std::make_unique<ModificationTrackerStorage>(field.type);
                    child->set_parent(this);
                    _children.push_back(std::move(child));
                }
                break;
            }

            case TypeKind::List: {
                // Own timestamp + recursive children for each element
                _storage = new engine_time_t{MIN_DT};
                auto* list_meta = static_cast<const ListTypeMeta*>(_value_meta);
                _children.reserve(list_meta->count);
                for (size_t i = 0; i < list_meta->count; ++i) {
                    auto child = std::make_unique<ModificationTrackerStorage>(list_meta->element_type);
                    child->set_parent(this);
                    _children.push_back(std::move(child));
                }
                break;
            }

            case TypeKind::DynamicList:
                // Variable-length list (tuple[T, ...]) - just own timestamp, no children
                // The list as a whole is marked modified when any element changes
                _storage = new engine_time_t{MIN_DT};
                break;

            case TypeKind::Dict: {
                auto* dict_meta = static_cast<const DictTypeMeta*>(_value_meta);
                _storage = new DictModificationStorage(dict_meta->key_type(), dict_meta->value_type);
                break;
            }

            case TypeKind::Ref: {
                // Own timestamp + recursive children for composite refs
                _storage = new engine_time_t{MIN_DT};
                auto* ref_meta = static_cast<const RefTypeMeta*>(_value_meta);
                if (ref_meta->item_count > 0) {
                    _children.reserve(ref_meta->item_count);
                    for (size_t i = 0; i < ref_meta->item_count; ++i) {
                        // Each item is a ref to the same value type
                        auto child = std::make_unique<ModificationTrackerStorage>(ref_meta);
                        child->set_parent(this);
                        _children.push_back(std::move(child));
                    }
                }
                break;
            }

            default:
                break;
        }
    }

    inline void ModificationTrackerStorage::deallocate_storage() {
        // Children are automatically cleaned up via unique_ptr
        _children.clear();

        if (!_storage || !_value_meta) return;

        switch (_value_meta->kind) {
            case TypeKind::Scalar:
            case TypeKind::Window:
            case TypeKind::Bundle:
            case TypeKind::List:
            case TypeKind::DynamicList:
            case TypeKind::Ref:
                // All use single timestamp now (children are separate)
                delete static_cast<engine_time_t*>(_storage);
                break;

            case TypeKind::Set:
                delete static_cast<SetModificationStorage*>(_storage);
                break;

            case TypeKind::Dict:
                delete static_cast<DictModificationStorage*>(_storage);
                break;

            default:
                break;
        }

        _storage = nullptr;
    }

    inline ModificationTracker ModificationTrackerStorage::tracker() {
        return ModificationTracker(this);
    }

    inline ModificationTracker ModificationTrackerStorage::tracker() const {
        // const_cast is safe here because the returned ModificationTracker
        // will only be used for const operations (modified_at, last_modified_time, etc.)
        return ModificationTracker(const_cast<ModificationTrackerStorage*>(this));
    }

} // namespace hgraph::value

#endif // HGRAPH_VALUE_MODIFICATION_TRACKER_H
