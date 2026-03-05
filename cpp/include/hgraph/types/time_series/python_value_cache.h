#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/feature_extension.h>
#include <hgraph/types/time_series/view_data.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

namespace hgraph {

struct HGRAPH_EXPORT PythonDeltaCacheEntry {
    engine_time_t time{MIN_DT};
    DeltaSemantics semantics{DeltaSemantics::Strict};
    nb::object value{};

    [[nodiscard]] bool is_valid() const noexcept {
        return value.is_valid();
    }

    [[nodiscard]] bool is_valid_for(engine_time_t current_time, DeltaSemantics current_semantics) const noexcept {
        return value.is_valid() && time == current_time && semantics == current_semantics;
    }

    void clear() {
        value = nb::object();
        time = MIN_DT;
        semantics = DeltaSemantics::Strict;
    }

    void abandon() {
        if (value.is_valid()) {
            (void)value.release();
        }
        time = MIN_DT;
        semantics = DeltaSemantics::Strict;
    }
};

struct HGRAPH_EXPORT KeyedDeltaLookupCacheEntry {
    const void* value_data{nullptr};
    const void* delta_data{nullptr};
    const void* observer_data{nullptr};
    const void* link_data{nullptr};
    std::vector<size_t> path{};
    const value::TypeMeta* key_type_meta{nullptr};
    engine_time_t evaluation_time{MIN_DT};
    std::unordered_map<value::Value, value::Value, ValueHash, ValueEqual> values{};

    void clear() {
        value_data = nullptr;
        delta_data = nullptr;
        observer_data = nullptr;
        link_data = nullptr;
        path.clear();
        key_type_meta = nullptr;
        evaluation_time = MIN_DT;
        values.clear();
    }

    void abandon() {
        clear();
    }
};

struct HGRAPH_EXPORT TsdKeySetDeltaCacheEntry {
    const void* value_data{nullptr};
    const void* delta_data{nullptr};
    const void* observer_data{nullptr};
    const void* link_data{nullptr};
    std::vector<size_t> path{};
    const value::TypeMeta* key_type_meta{nullptr};
    engine_time_t evaluation_time{MIN_DT};
    std::vector<value::Value> added{};
    std::vector<value::Value> removed{};
    std::unordered_set<value::View> added_lookup{};
    std::unordered_set<value::View> removed_lookup{};

    void clear() {
        value_data = nullptr;
        delta_data = nullptr;
        observer_data = nullptr;
        link_data = nullptr;
        path.clear();
        key_type_meta = nullptr;
        evaluation_time = MIN_DT;
        added.clear();
        removed.clear();
        added_lookup.clear();
        removed_lookup.clear();
    }

    void abandon() {
        clear();
    }
};

/**
 * Schema-shaped cache for Python conversions.
 *
 * Storage policy by TS kind:
 * - TSValue/TSS/TSW/REF/SIGNAL: single nb::object cache.
 * - TSL/TSB/TSD: vector<nb::object> cache indexed by slot.
 *
 * Delta cache is parallel and time-keyed:
 * - Root delta slot for every node.
 * - Per-slot delta values for slot-addressable nodes.
 */
class HGRAPH_EXPORT PythonValueCacheNode {
public:
    PythonValueCacheNode() = default;

    explicit PythonValueCacheNode(const TSMeta* meta)
        : meta_(meta) {
        initialize_storage();
    }

    PythonValueCacheNode(const PythonValueCacheNode&) = delete;
    PythonValueCacheNode& operator=(const PythonValueCacheNode&) = delete;
    PythonValueCacheNode(PythonValueCacheNode&&) noexcept = default;
    PythonValueCacheNode& operator=(PythonValueCacheNode&&) noexcept = default;
    ~PythonValueCacheNode() = default;

    [[nodiscard]] const TSMeta* meta() const noexcept { return meta_; }
    [[nodiscard]] bool is_scalar_cache() const noexcept { return std::holds_alternative<nb::object>(storage_); }
    [[nodiscard]] bool is_slot_cache() const noexcept { return std::holds_alternative<SlotStorage>(storage_); }

    [[nodiscard]] nb::object* scalar_value() noexcept {
        return std::get_if<nb::object>(&storage_);
    }

    [[nodiscard]] const nb::object* scalar_value() const noexcept {
        return std::get_if<nb::object>(&storage_);
    }

    [[nodiscard]] PythonDeltaCacheEntry* delta_root_value() noexcept {
        return &delta_root_value_;
    }

    [[nodiscard]] const PythonDeltaCacheEntry* delta_root_value() const noexcept {
        return &delta_root_value_;
    }

    [[nodiscard]] KeyedDeltaLookupCacheEntry* keyed_delta_lookup_cache() noexcept {
        return &keyed_delta_lookup_cache_;
    }

    [[nodiscard]] const KeyedDeltaLookupCacheEntry* keyed_delta_lookup_cache() const noexcept {
        return &keyed_delta_lookup_cache_;
    }

    [[nodiscard]] TsdKeySetDeltaCacheEntry* tsd_key_set_delta_cache() noexcept {
        return &tsd_key_set_delta_cache_;
    }

    [[nodiscard]] const TsdKeySetDeltaCacheEntry* tsd_key_set_delta_cache() const noexcept {
        return &tsd_key_set_delta_cache_;
    }

    [[nodiscard]] nb::object* slot_value(size_t slot, bool create) noexcept {
        SlotStorage* slots = std::get_if<SlotStorage>(&storage_);
        if (slots == nullptr) {
            return nullptr;
        }
        if (!ensure_slot_capacity(*slots, slot, create)) {
            return nullptr;
        }
        return &slots->slot_values[slot];
    }

    [[nodiscard]] const nb::object* slot_value(size_t slot) const noexcept {
        const SlotStorage* slots = std::get_if<SlotStorage>(&storage_);
        if (slots == nullptr || slot >= slots->slot_values.size()) {
            return nullptr;
        }
        return &slots->slot_values[slot];
    }

    [[nodiscard]] PythonDeltaCacheEntry* delta_slot_value(size_t slot, bool create) noexcept {
        DeltaSlotStorage* slots = std::get_if<DeltaSlotStorage>(&delta_storage_);
        if (slots == nullptr) {
            return nullptr;
        }
        if (!ensure_slot_capacity(*slots, slot, create)) {
            return nullptr;
        }
        return &slots->slot_values[slot];
    }

    [[nodiscard]] const PythonDeltaCacheEntry* delta_slot_value(size_t slot) const noexcept {
        const DeltaSlotStorage* slots = std::get_if<DeltaSlotStorage>(&delta_storage_);
        if (slots == nullptr || slot >= slots->slot_values.size()) {
            return nullptr;
        }
        return &slots->slot_values[slot];
    }

    [[nodiscard]] PythonValueCacheNode* child_node(size_t slot, bool create) {
        SlotStorage* slots = std::get_if<SlotStorage>(&storage_);
        if (slots == nullptr) {
            return nullptr;
        }

        const TSMeta* child = child_meta_for_slot(slot);
        if (child == nullptr) {
            return nullptr;
        }

        if (!ensure_slot_capacity(*slots, slot, create)) {
            return nullptr;
        }

        if (!slots->slot_children[slot]) {
            if (!create) {
                return nullptr;
            }
            slots->slot_children[slot] = std::make_unique<PythonValueCacheNode>(child);
        }
        return slots->slot_children[slot].get();
    }

    [[nodiscard]] const PythonValueCacheNode* child_node(size_t slot) const {
        const SlotStorage* slots = std::get_if<SlotStorage>(&storage_);
        if (slots == nullptr || slot >= slots->slot_children.size()) {
            return nullptr;
        }
        const auto& child = slots->slot_children[slot];
        return child ? child.get() : nullptr;
    }

    [[nodiscard]] bool empty() const noexcept {
        bool value_empty = true;
        if (const nb::object* scalar = std::get_if<nb::object>(&storage_); scalar != nullptr) {
            value_empty = !scalar->is_valid();
        }
        if (const auto* slots = std::get_if<SlotStorage>(&storage_); slots != nullptr) {
            for (const nb::object& value : slots->slot_values) {
                if (value.is_valid()) {
                    value_empty = false;
                    break;
                }
            }
            for (const auto& child : slots->slot_children) {
                if (child != nullptr && !child->empty()) {
                    return false;
                }
            }
        }

        bool delta_empty = !delta_root_value_.is_valid();
        if (const auto* slots = std::get_if<DeltaSlotStorage>(&delta_storage_); slots != nullptr) {
            for (const PythonDeltaCacheEntry& value : slots->slot_values) {
                if (value.is_valid()) {
                    delta_empty = false;
                    break;
                }
            }
        }

        return value_empty &&
               delta_empty &&
               keyed_delta_lookup_cache_.values.empty() &&
               tsd_key_set_delta_cache_.added.empty() &&
               tsd_key_set_delta_cache_.removed.empty();
    }

    void clear_subtree() {
        if (nb::object* scalar = std::get_if<nb::object>(&storage_); scalar != nullptr) {
            *scalar = nb::object();
        }

        if (auto* slots = std::get_if<SlotStorage>(&storage_); slots != nullptr) {
            for (nb::object& value : slots->slot_values) {
                value = nb::object();
            }
            for (auto& child : slots->slot_children) {
                if (child != nullptr) {
                    child->clear_subtree();
                }
            }
        }

        delta_root_value_.clear();
        if (auto* slots = std::get_if<DeltaSlotStorage>(&delta_storage_); slots != nullptr) {
            for (PythonDeltaCacheEntry& value : slots->slot_values) {
                value.clear();
            }
        }
        keyed_delta_lookup_cache_.clear();
        tsd_key_set_delta_cache_.clear();
    }

    void clear_delta_subtree() {
        delta_root_value_.clear();
        if (auto* slots = std::get_if<DeltaSlotStorage>(&delta_storage_); slots != nullptr) {
            for (PythonDeltaCacheEntry& value : slots->slot_values) {
                value.clear();
            }
        }
        keyed_delta_lookup_cache_.clear();
        tsd_key_set_delta_cache_.clear();

        if (auto* slots = std::get_if<SlotStorage>(&storage_); slots != nullptr) {
            for (auto& child : slots->slot_children) {
                if (child != nullptr) {
                    child->clear_delta_subtree();
                }
            }
        }
    }

    // Detach Python refs without decref; used only when interpreter is unavailable.
    void abandon_subtree() {
        if (nb::object* scalar = std::get_if<nb::object>(&storage_); scalar != nullptr) {
            if (scalar->is_valid()) {
                (void)scalar->release();
            }
        }

        if (auto* slots = std::get_if<SlotStorage>(&storage_); slots != nullptr) {
            for (nb::object& value : slots->slot_values) {
                if (value.is_valid()) {
                    (void)value.release();
                }
            }
            for (auto& child : slots->slot_children) {
                if (child != nullptr) {
                    child->abandon_subtree();
                }
            }
        }

        delta_root_value_.abandon();
        if (auto* slots = std::get_if<DeltaSlotStorage>(&delta_storage_); slots != nullptr) {
            for (PythonDeltaCacheEntry& value : slots->slot_values) {
                value.abandon();
            }
        }
        keyed_delta_lookup_cache_.abandon();
        tsd_key_set_delta_cache_.abandon();
    }

    // Detach delta Python refs without decref; used only when interpreter is unavailable.
    void abandon_delta_subtree() {
        delta_root_value_.abandon();
        if (auto* slots = std::get_if<DeltaSlotStorage>(&delta_storage_); slots != nullptr) {
            for (PythonDeltaCacheEntry& value : slots->slot_values) {
                value.abandon();
            }
        }
        keyed_delta_lookup_cache_.abandon();
        tsd_key_set_delta_cache_.abandon();

        if (auto* slots = std::get_if<SlotStorage>(&storage_); slots != nullptr) {
            for (auto& child : slots->slot_children) {
                if (child != nullptr) {
                    child->abandon_delta_subtree();
                }
            }
        }
    }

private:
    struct SlotStorage {
        std::vector<nb::object> slot_values{};
        std::vector<std::unique_ptr<PythonValueCacheNode>> slot_children{};
    };

    struct DeltaSlotStorage {
        std::vector<PythonDeltaCacheEntry> slot_values{};
    };

    static bool is_slot_kind(const TSMeta* meta) noexcept {
        if (meta == nullptr) {
            return false;
        }
        return meta->kind == TSKind::TSL || meta->kind == TSKind::TSB || meta->kind == TSKind::TSD;
    }

    static size_t fixed_slot_count(const TSMeta* meta) noexcept {
        if (meta == nullptr) {
            return 0;
        }
        switch (meta->kind) {
            case TSKind::TSL:
                return meta->fixed_size();
            case TSKind::TSB:
                return meta->field_count();
            default:
                return 0;
        }
    }

    [[nodiscard]] bool dynamic_slots() const noexcept {
        return meta_ != nullptr && meta_->kind == TSKind::TSD;
    }

    [[nodiscard]] const TSMeta* child_meta_for_slot(size_t slot) const noexcept {
        if (meta_ == nullptr) {
            return nullptr;
        }
        switch (meta_->kind) {
            case TSKind::TSL:
                return slot < meta_->fixed_size() ? meta_->element_ts() : nullptr;
            case TSKind::TSB:
                if (meta_->fields() == nullptr || slot >= meta_->field_count()) {
                    return nullptr;
                }
                return meta_->fields()[slot].ts_type;
            case TSKind::TSD:
                return meta_->element_ts();
            default:
                return nullptr;
        }
    }

    [[nodiscard]] bool ensure_slot_capacity(SlotStorage& slots, size_t slot, bool create) const noexcept {
        if (slot < slots.slot_values.size()) {
            return true;
        }
        if (!create) {
            return false;
        }
        if (!dynamic_slots()) {
            return false;
        }
        const size_t new_size = slot + 1;
        slots.slot_values.resize(new_size);
        slots.slot_children.resize(new_size);
        return true;
    }

    [[nodiscard]] bool ensure_slot_capacity(DeltaSlotStorage& slots, size_t slot, bool create) const noexcept {
        if (slot < slots.slot_values.size()) {
            return true;
        }
        if (!create) {
            return false;
        }
        if (!dynamic_slots()) {
            return false;
        }
        const size_t new_size = slot + 1;
        slots.slot_values.resize(new_size);
        return true;
    }

    void initialize_storage() {
        if (!is_slot_kind(meta_)) {
            storage_ = nb::object{};
            delta_storage_ = DeltaSlotStorage{};
            return;
        }

        SlotStorage storage{};
        DeltaSlotStorage delta_storage{};
        const size_t size = fixed_slot_count(meta_);
        storage.slot_values.resize(size);
        storage.slot_children.resize(size);
        delta_storage.slot_values.resize(size);
        storage_ = std::move(storage);
        delta_storage_ = std::move(delta_storage);
    }

    const TSMeta* meta_{nullptr};
    std::variant<nb::object, SlotStorage> storage_{nb::object{}};
    PythonDeltaCacheEntry delta_root_value_{};
    std::variant<DeltaSlotStorage> delta_storage_{DeltaSlotStorage{}};
    KeyedDeltaLookupCacheEntry keyed_delta_lookup_cache_{};
    TsdKeySetDeltaCacheEntry tsd_key_set_delta_cache_{};
};

}  // namespace hgraph
