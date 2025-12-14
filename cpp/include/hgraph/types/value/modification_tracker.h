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
     * Tracks:
     * - Structural modifications (add/remove elements)
     * - Per-element tracking (when each element was added)
     * - Delta tracking (removed elements copied before destruction)
     *
     * This enables TSS (Time-Series Set) to track both structural changes
     * and provide delta_value access to added/removed elements.
     */
    struct SetModificationStorage {
        engine_time_t structural_modified{MIN_DT};
        ankerl::unordered_dense::map<size_t, engine_time_t> element_added_at;

        // Delta tracking for removed elements (cleared each tick by engine)
        std::vector<char> removed_elements;
        size_t removed_element_count{0};
        const TypeMeta* element_type{nullptr};

        SetModificationStorage() = default;

        explicit SetModificationStorage(const TypeMeta* elem_type)
            : element_type(elem_type) {}

        void mark_structural_modified(engine_time_t time) {
            if (time > structural_modified) {
                structural_modified = time;
            }
        }

        void mark_element_added(size_t index, engine_time_t time) {
            element_added_at[index] = time;
            mark_structural_modified(time);
        }

        void record_removal(const void* elem) {
            if (!element_type) return;
            size_t elem_size = element_type->size;
            size_t offset = removed_element_count * elem_size;
            removed_elements.resize(offset + elem_size);
            element_type->copy_construct_at(removed_elements.data() + offset, elem);
            ++removed_element_count;
        }

        void remove_element_tracking(size_t index) {
            element_added_at.erase(index);
        }

        [[nodiscard]] bool structurally_modified_at(engine_time_t time) const {
            return structural_modified == time;
        }

        [[nodiscard]] bool element_added_at_time(size_t index, engine_time_t time) const {
            auto it = element_added_at.find(index);
            return it != element_added_at.end() && it->second == time;
        }

        [[nodiscard]] engine_time_t element_last_modified_time(size_t index) const {
            auto it = element_added_at.find(index);
            return it != element_added_at.end() ? it->second : MIN_DT;
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            return structural_modified;
        }

        // Delta access
        [[nodiscard]] size_t added_count(engine_time_t time) const {
            size_t count = 0;
            for (const auto& [idx, t] : element_added_at) {
                if (t == time) ++count;
            }
            return count;
        }

        [[nodiscard]] size_t removed_count() const {
            return removed_element_count;
        }

        [[nodiscard]] const void* removed_element(size_t i) const {
            if (!element_type || i >= removed_element_count) return nullptr;
            return removed_elements.data() + i * element_type->size;
        }

        // Cleanup - destructs non-trivial removed elements
        void clear_delta() {
            if (element_type && removed_element_count > 0) {
                for (size_t i = 0; i < removed_element_count; ++i) {
                    void* ptr = removed_elements.data() + i * element_type->size;
                    element_type->destruct_at(ptr);
                }
            }
            removed_elements.clear();
            removed_element_count = 0;
        }

        void clear() {
            structural_modified = MIN_DT;
            element_added_at.clear();
            clear_delta();
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

        void record_key_removal(const void* key) {
            key_tracking.record_removal(key);
        }

        void remove_key_tracking(size_t index) {
            key_tracking.remove_element_tracking(index);
            value_modified_at.erase(index);
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
     * ModificationTrackerStorage - Owning storage for modification tracking
     *
     * Allocates and manages the appropriate storage based on TypeKind.
     *
     * Storage layout by type:
     * - Scalar: single engine_time_t
     * - Bundle: array of engine_time_t [bundle_time][field0_time][field1_time]...
     * - List: array of engine_time_t [list_time][elem0_time][elem1_time]...
     * - Set: single engine_time_t (set is atomic - tracks structural changes only)
     * - Dict: DictModificationStorage (structural + per-entry timestamps)
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
            , _storage(other._storage) {
            other._value_meta = nullptr;
            other._storage = nullptr;
        }

        ModificationTrackerStorage& operator=(ModificationTrackerStorage&& other) noexcept {
            if (this != &other) {
                deallocate_storage();
                _value_meta = other._value_meta;
                _storage = other._storage;
                other._value_meta = nullptr;
                other._storage = nullptr;
            }
            return *this;
        }

        ModificationTrackerStorage(const ModificationTrackerStorage&) = delete;
        ModificationTrackerStorage& operator=(const ModificationTrackerStorage&) = delete;

        [[nodiscard]] const TypeMeta* value_meta() const { return _value_meta; }
        [[nodiscard]] void* storage() { return _storage; }
        [[nodiscard]] const void* storage() const { return _storage; }
        [[nodiscard]] bool valid() const { return _value_meta && _storage; }

        ModificationTracker tracker();
        [[nodiscard]] ModificationTracker tracker() const;

    private:
        void allocate_storage();
        void deallocate_storage();

        const TypeMeta* _value_meta{nullptr};
        void* _storage{nullptr};
    };

    /**
     * ModificationTracker - View into modification tracking storage
     *
     * Non-owning view that provides access to modification timestamps.
     * Can represent the whole value or a sub-element (field, element).
     *
     * For nested types, sub-trackers maintain a _parent_time pointer
     * to enable hierarchical propagation.
     */
    class ModificationTracker {
    public:
        ModificationTracker() = default;

        // Root tracker (no parent propagation)
        ModificationTracker(void* storage, const TypeMeta* value_meta)
            : _storage(storage), _value_meta(value_meta), _parent_time(nullptr) {}

        // Sub-tracker with parent propagation
        ModificationTracker(void* storage, const TypeMeta* value_meta, engine_time_t* parent_time)
            : _storage(storage), _value_meta(value_meta), _parent_time(parent_time) {}

        [[nodiscard]] bool valid() const { return _storage && _value_meta; }
        [[nodiscard]] const TypeMeta* value_schema() const { return _value_meta; }

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
                case TypeKind::Window:  // Window is atomic
                case TypeKind::Ref:     // Ref uses same pattern (single or array based on item_count)
                    // First element is always the container's own timestamp
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

        // Mark as modified (propagates to parent if hierarchical)
        void mark_modified(engine_time_t time) {
            if (!valid()) return;

            switch (_value_meta->kind) {
                case TypeKind::Scalar:
                case TypeKind::Bundle:
                case TypeKind::List:
                case TypeKind::Window:  // Window is atomic
                case TypeKind::Ref: {   // Ref uses same pattern
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

            // Propagate to parent
            propagate_to_parent(time);
        }

        void mark_invalid() {
            if (!valid()) return;

            switch (_value_meta->kind) {
                case TypeKind::Scalar:
                case TypeKind::Bundle:
                case TypeKind::List:
                case TypeKind::Window:  // Window is atomic
                case TypeKind::Ref:     // Ref uses same pattern
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

        // For bundles - field-level tracking
        [[nodiscard]] ModificationTracker field(size_t index) {
            if (!valid() || _value_meta->kind != TypeKind::Bundle) {
                return {};
            }

            auto* bundle_meta = static_cast<const BundleTypeMeta*>(_value_meta);
            if (index >= bundle_meta->fields.size()) {
                return {};
            }

            // Storage layout: [bundle_time][field0_time][field1_time]...
            auto* times = static_cast<engine_time_t*>(_storage);
            engine_time_t* field_time = &times[1 + index];
            engine_time_t* parent_time = &times[0];  // Bundle's own timestamp

            const auto& field_meta = bundle_meta->fields[index];
            return {field_time, field_meta.type, parent_time};
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

            auto* times = static_cast<engine_time_t*>(_storage);
            return times[1 + index] == time;
        }

        // For lists - element-level tracking
        [[nodiscard]] ModificationTracker element(size_t index) {
            if (!valid() || _value_meta->kind != TypeKind::List) {
                return {};
            }

            auto* list_meta = static_cast<const ListTypeMeta*>(_value_meta);
            if (index >= list_meta->count) {
                return {};
            }

            // Storage layout: [list_time][elem0_time][elem1_time]...
            auto* times = static_cast<engine_time_t*>(_storage);
            engine_time_t* elem_time = &times[1 + index];
            engine_time_t* parent_time = &times[0];  // List's own timestamp

            return {elem_time, list_meta->element_type, parent_time};
        }

        [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::List) {
                return false;
            }

            auto* list_meta = static_cast<const ListTypeMeta*>(_value_meta);
            if (index >= list_meta->count) {
                return false;
            }

            auto* times = static_cast<engine_time_t*>(_storage);
            return times[1 + index] == time;
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
            propagate_to_parent(time);
        }

        void record_set_removal(const void* elem) {
            if (!valid() || _value_meta->kind != TypeKind::Set) return;
            auto* storage = static_cast<SetModificationStorage*>(_storage);
            storage->record_removal(elem);
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

        [[nodiscard]] ConstTypedPtr set_removed_element(size_t i) const {
            if (!valid() || _value_meta->kind != TypeKind::Set) return {};
            auto* storage = static_cast<SetModificationStorage*>(_storage);
            const void* elem = storage->removed_element(i);
            return elem ? ConstTypedPtr{elem, storage->element_type} : ConstTypedPtr{};
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
            propagate_to_parent(time);
        }

        void record_dict_key_removal(const void* key) {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return;
            static_cast<DictModificationStorage*>(_storage)->record_key_removal(key);
        }

        void mark_dict_value_modified(size_t entry_index, engine_time_t time) {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return;
            auto* storage = static_cast<DictModificationStorage*>(_storage);
            storage->mark_value_modified(entry_index, time);
            propagate_to_parent(time);
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

        [[nodiscard]] ConstTypedPtr dict_removed_key(size_t i) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return {};
            auto* storage = static_cast<DictModificationStorage*>(_storage);
            const void* key = storage->key_tracking.removed_element(i);
            return key ? ConstTypedPtr{key, storage->key_tracking.element_type} : ConstTypedPtr{};
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

            // Storage layout: [ref_time][item0_time][item1_time]...
            auto* times = static_cast<engine_time_t*>(_storage);
            engine_time_t* item_time = &times[1 + index];
            engine_time_t* parent_time = &times[0];  // Ref's own timestamp

            // Each item is itself a ref, so use the same ref_meta for items
            // (This is a simplification - in practice, items might have different types)
            return {item_time, ref_meta, parent_time};
        }

        [[nodiscard]] bool ref_item_modified_at(size_t index, engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Ref) {
                return false;
            }

            auto* ref_meta = static_cast<const RefTypeMeta*>(_value_meta);
            if (ref_meta->item_count == 0 || index >= ref_meta->item_count) {
                return false;
            }

            auto* times = static_cast<engine_time_t*>(_storage);
            return times[1 + index] == time;
        }

    private:
        void propagate_to_parent(engine_time_t time) {
            if (_parent_time && time > *_parent_time) {
                *_parent_time = time;
            }
        }

        void* _storage{nullptr};
        const TypeMeta* _value_meta{nullptr};
        engine_time_t* _parent_time{nullptr};  // For hierarchical propagation
    };

    // Implementation of ModificationTrackerStorage methods

    inline void ModificationTrackerStorage::allocate_storage() {
        if (!_value_meta) return;

        switch (_value_meta->kind) {
            case TypeKind::Scalar:
            case TypeKind::Window:  // Window is atomic
                // Single timestamp
                _storage = new engine_time_t{MIN_DT};
                break;

            case TypeKind::Set: {
                auto* set_meta = static_cast<const SetTypeMeta*>(_value_meta);
                _storage = new SetModificationStorage(set_meta->element_type);
                break;
            }

            case TypeKind::Bundle: {
                auto* bundle_meta = static_cast<const BundleTypeMeta*>(_value_meta);
                size_t count = 1 + bundle_meta->fields.size();  // bundle + fields
                auto* times = new engine_time_t[count];
                for (size_t i = 0; i < count; ++i) {
                    times[i] = MIN_DT;
                }
                _storage = times;
                break;
            }

            case TypeKind::List: {
                auto* list_meta = static_cast<const ListTypeMeta*>(_value_meta);
                size_t count = 1 + list_meta->count;  // list + elements
                auto* times = new engine_time_t[count];
                for (size_t i = 0; i < count; ++i) {
                    times[i] = MIN_DT;
                }
                _storage = times;
                break;
            }

            case TypeKind::Dict:
                _storage = new DictModificationStorage();
                break;

            case TypeKind::Ref: {
                auto* ref_meta = static_cast<const RefTypeMeta*>(_value_meta);
                if (ref_meta->item_count == 0) {
                    // Atomic ref - single timestamp
                    _storage = new engine_time_t{MIN_DT};
                } else {
                    // Composite ref - ref + items
                    size_t count = 1 + ref_meta->item_count;
                    auto* times = new engine_time_t[count];
                    for (size_t i = 0; i < count; ++i) {
                        times[i] = MIN_DT;
                    }
                    _storage = times;
                }
                break;
            }

            default:
                break;
        }
    }

    inline void ModificationTrackerStorage::deallocate_storage() {
        if (!_storage || !_value_meta) return;

        switch (_value_meta->kind) {
            case TypeKind::Scalar:
            case TypeKind::Window:  // Window is atomic
                delete static_cast<engine_time_t*>(_storage);
                break;

            case TypeKind::Set:
                delete static_cast<SetModificationStorage*>(_storage);
                break;

            case TypeKind::Bundle:
            case TypeKind::List:
                delete[] static_cast<engine_time_t*>(_storage);
                break;

            case TypeKind::Dict:
                delete static_cast<DictModificationStorage*>(_storage);
                break;

            case TypeKind::Ref: {
                auto* ref_meta = static_cast<const RefTypeMeta*>(_value_meta);
                if (ref_meta->item_count == 0) {
                    // Atomic ref - single timestamp
                    delete static_cast<engine_time_t*>(_storage);
                } else {
                    // Composite ref - array
                    delete[] static_cast<engine_time_t*>(_storage);
                }
                break;
            }

            default:
                break;
        }

        _storage = nullptr;
    }

    inline ModificationTracker ModificationTrackerStorage::tracker() {
        return {_storage, _value_meta};
    }

    inline ModificationTracker ModificationTrackerStorage::tracker() const {
        // const_cast is safe here because the returned ModificationTracker
        // will only be used for const operations (modified_at, last_modified_time, etc.)
        return {const_cast<void*>(_storage), _value_meta};
    }

} // namespace hgraph::value

#endif // HGRAPH_VALUE_MODIFICATION_TRACKER_H
