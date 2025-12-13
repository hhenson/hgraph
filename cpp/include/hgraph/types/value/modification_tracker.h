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
     * DictModificationStorage - Dynamic storage for dict modification timestamps
     *
     * Tracks:
     * - Structural modifications (add/remove keys)
     * - Per-entry modifications (value changes on existing keys)
     *
     * This is needed because TSD (Time-Series Dict) tracks both structural changes
     * (added/removed keys) and value modifications on existing keys.
     */
    struct DictModificationStorage {
        engine_time_t structural_modified{MIN_DT};
        ankerl::unordered_dense::map<size_t, engine_time_t> entry_times;

        DictModificationStorage() = default;

        void mark_structural_modified(engine_time_t time) {
            if (time > structural_modified) {
                structural_modified = time;
            }
        }

        void mark_entry_modified(size_t index, engine_time_t time) {
            auto& current = entry_times[index];
            if (time > current) {
                current = time;
            }
        }

        void remove_entry(size_t index) {
            entry_times.erase(index);
        }

        [[nodiscard]] bool structurally_modified_at(engine_time_t time) const {
            return structural_modified == time;
        }

        [[nodiscard]] bool entry_modified_at(size_t index, engine_time_t time) const {
            auto it = entry_times.find(index);
            return it != entry_times.end() && it->second == time;
        }

        [[nodiscard]] engine_time_t entry_last_modified_time(size_t index) const {
            auto it = entry_times.find(index);
            return it != entry_times.end() ? it->second : MIN_DT;
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            engine_time_t max_time = structural_modified;
            for (const auto& [idx, time] : entry_times) {
                if (time > max_time) {
                    max_time = time;
                }
            }
            return max_time;
        }

        void clear() {
            structural_modified = MIN_DT;
            entry_times.clear();
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
                case TypeKind::Set:
                case TypeKind::Window:  // Window is atomic like Set
                case TypeKind::Ref:     // Ref uses same pattern (single or array based on item_count)
                    // First element is always the container's own timestamp
                    return *static_cast<engine_time_t*>(_storage);

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
                case TypeKind::Set:
                case TypeKind::Window:  // Window is atomic like Set
                case TypeKind::Ref: {   // Ref uses same pattern
                    auto* ts = static_cast<engine_time_t*>(_storage);
                    if (time > *ts) {
                        *ts = time;
                    }
                    break;
                }
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
                case TypeKind::Set:
                case TypeKind::Window:  // Window is atomic like Set
                case TypeKind::Ref:     // Ref uses same pattern
                    *static_cast<engine_time_t*>(_storage) = MIN_DT;
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

        // For dicts - structural and entry tracking
        [[nodiscard]] bool structurally_modified_at(engine_time_t time) const {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return false;
            return static_cast<DictModificationStorage*>(_storage)->structurally_modified_at(time);
        }

        void mark_dict_entry_modified(size_t entry_index, engine_time_t time) {
            if (!valid() || _value_meta->kind != TypeKind::Dict) return;

            auto* storage = static_cast<DictModificationStorage*>(_storage);
            storage->mark_entry_modified(entry_index, time);

            // Propagate to parent
            propagate_to_parent(time);
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
            static_cast<DictModificationStorage*>(_storage)->remove_entry(entry_index);
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
            case TypeKind::Set:
            case TypeKind::Window:  // Window is atomic like Set
                // Single timestamp (set/window is atomic)
                _storage = new engine_time_t{MIN_DT};
                break;

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
            case TypeKind::Set:
            case TypeKind::Window:  // Window is atomic like Set
                delete static_cast<engine_time_t*>(_storage);
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
