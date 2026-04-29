#ifndef HGRAPH_CPP_ROOT_TS_STATE_H
#define HGRAPH_CPP_ROOT_TS_STATE_H

#include <hgraph/util/date_time.h>
#include <hgraph/v2/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/v2/types/metadata/type_binding.h>
#include <hgraph/v2/types/utils/memory_utils.h>
#include <hgraph/v2/types/utils/slot_observer.h>
#include <hgraph/v2/types/utils/value_slot_store.h>
#include <hgraph/v2/types/value/container_storage.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <stdexcept>

namespace hgraph::v2
{
    struct TsValueOps;
    using TsValueTypeBinding = TypeBinding<TSValueTypeMetaData, TsValueOps>;

    struct TsNodeState
    {
        engine_time_t last_modified_time{MIN_DT};
        bool          linked{false};
    };

    struct TsInputRootState
    {
        size_t active_path_count{0};
    };

    struct TsOutputRootState
    {
        size_t alternative_count{0};
        size_t ref_subscription_count{0};
    };

    struct TsScalarState
    {};

    struct TsDynamicListState
    {
        size_t logical_size{0};

        struct SlotStateObserver final : SlotObserver
        {
            explicit SlotStateObserver(TsDynamicListState *owner_) noexcept : owner(owner_) {}

            void on_capacity(size_t, size_t new_capacity) override {
                if (owner == nullptr || owner->child_states == nullptr) { return; }
                owner->child_states->reserve_to(new_capacity);
            }

            void on_insert(size_t slot) override {
                if (owner == nullptr || owner->child_states == nullptr) { return; }
                if (!owner->child_states->has_slot(slot)) { owner->child_states->construct_at(slot); }
                owner->logical_size = std::max(owner->logical_size, slot + 1);
            }

            void on_remove(size_t) override {}

            void on_erase(size_t slot) override {
                if (owner == nullptr || owner->child_states == nullptr) { return; }
                owner->child_states->destroy_at(slot);
                owner->logical_size = std::min(owner->logical_size, slot);
            }

            void on_clear() override {
                if (owner == nullptr || owner->child_states == nullptr) { return; }
                owner->child_states->destroy_all();
                owner->logical_size = 0;
            }

            TsDynamicListState *owner{nullptr};
        };

        TsDynamicListState() = default;

        TsDynamicListState(TsDynamicListState &&other) noexcept
            : logical_size(other.logical_size), child_states(std::move(other.child_states)),
              slot_observer(std::move(other.slot_observer)), bound_storage(other.bound_storage),
              observer_registered(other.observer_registered) {
            if (slot_observer != nullptr) { slot_observer->owner = this; }
            if (observer_registered) { rebind_observer_after_move(); }
            other.bound_storage       = nullptr;
            other.logical_size        = 0;
            other.observer_registered = false;
        }

        TsDynamicListState &operator=(TsDynamicListState &&other) noexcept {
            if (this != &other) {
                detach_observer();
                logical_size        = other.logical_size;
                child_states        = std::move(other.child_states);
                slot_observer       = std::move(other.slot_observer);
                bound_storage       = other.bound_storage;
                observer_registered = other.observer_registered;
                if (slot_observer != nullptr) { slot_observer->owner = this; }
                if (observer_registered) { rebind_observer_after_move(); }
                other.bound_storage       = nullptr;
                other.logical_size        = 0;
                other.observer_registered = false;
            }
            return *this;
        }

        TsDynamicListState(const TsDynamicListState &)            = delete;
        TsDynamicListState &operator=(const TsDynamicListState &) = delete;

        ~TsDynamicListState() { detach_observer(); }

        void ensure_bound(detail::DynamicListStorage &storage, const TsValueTypeBinding &element_binding,
                          const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator()) {
            if (child_states == nullptr) {
                child_states = std::make_unique<ValueSlotStore>(element_binding.checked_plan(), allocator);
            }
            if (bound_storage != &storage || !observer_registered) {
                detach_observer();
                bound_storage = &storage;
                attach_observer();
                child_states->reserve_to(storage.values.slot_capacity());
                reconcile_slots(storage);
            }
            logical_size = storage.size;
        }

        [[nodiscard]] void *child_state_memory(size_t index) noexcept {
            return child_states != nullptr && index < logical_size && child_states->has_slot(index)
                       ? child_states->value_memory(index)
                       : nullptr;
        }

        [[nodiscard]] const void *child_state_memory(size_t index) const noexcept {
            return child_states != nullptr && index < logical_size && child_states->has_slot(index)
                       ? child_states->value_memory(index)
                       : nullptr;
        }

      private:
        std::unique_ptr<ValueSlotStore>    child_states{};
        std::unique_ptr<SlotStateObserver> slot_observer{};
        detail::DynamicListStorage        *bound_storage{nullptr};
        bool                               observer_registered{false};

        void reconcile_slots(const detail::DynamicListStorage &storage) {
            if (child_states == nullptr) { return; }
            child_states->reserve_to(storage.values.slot_capacity());
            for (size_t index = 0; index < storage.values.slot_capacity(); ++index) {
                if (index < storage.size && !child_states->has_slot(index)) {
                    child_states->construct_at(index);
                } else if (index >= storage.size && child_states->has_slot(index)) {
                    child_states->destroy_at(index);
                }
            }
        }

        void attach_observer() {
            if (bound_storage == nullptr || observer_registered) { return; }
            if (slot_observer == nullptr) { slot_observer = std::make_unique<SlotStateObserver>(this); }
            bound_storage->add_slot_observer(slot_observer.get());
            observer_registered = true;
        }

        void detach_observer() noexcept {
            if (bound_storage == nullptr || !observer_registered || slot_observer == nullptr) { return; }
            auto &entries = bound_storage->observers.entries();
            if (std::find(entries.begin(), entries.end(), slot_observer.get()) != entries.end()) {
                bound_storage->remove_slot_observer(slot_observer.get());
            }
            observer_registered = false;
        }

        void rebind_observer_after_move() {
            if (bound_storage == nullptr || slot_observer == nullptr) { return; }
            auto &entries = bound_storage->observers.entries();
            if (std::find(entries.begin(), entries.end(), slot_observer.get()) == entries.end()) {
                bound_storage->add_slot_observer(slot_observer.get());
            }
        }
    };

    struct TsSetState
    {
        size_t pending_erase_count{0};
    };

    struct TsDictState
    {
        size_t pending_erase_count{0};

        struct SlotStateObserver final : SlotObserver
        {
            explicit SlotStateObserver(TsDictState *owner_) noexcept : owner(owner_) {}

            void on_capacity(size_t, size_t new_capacity) override {
                if (owner == nullptr || owner->child_states == nullptr) { return; }
                owner->child_states->reserve_to(new_capacity);
            }

            void on_insert(size_t slot) override {
                if (owner == nullptr || owner->child_states == nullptr) { return; }
                if (!owner->child_states->has_slot(slot)) { owner->child_states->construct_at(slot); }
                owner->refresh_pending_erase_count();
            }

            void on_remove(size_t) override {
                if (owner != nullptr) { ++owner->pending_erase_count; }
            }

            void on_erase(size_t slot) override {
                if (owner == nullptr || owner->child_states == nullptr) { return; }
                owner->child_states->destroy_at(slot);
                if (owner->pending_erase_count > 0) { --owner->pending_erase_count; }
            }

            void on_clear() override {
                if (owner == nullptr || owner->child_states == nullptr) { return; }
                owner->child_states->destroy_all();
                owner->pending_erase_count = 0;
            }

            TsDictState *owner{nullptr};
        };

        TsDictState() = default;

        TsDictState(TsDictState &&other) noexcept
            : pending_erase_count(other.pending_erase_count), child_states(std::move(other.child_states)),
              slot_observer(std::move(other.slot_observer)), bound_storage(other.bound_storage),
              observer_registered(other.observer_registered) {
            if (slot_observer != nullptr) { slot_observer->owner = this; }
            if (observer_registered) { rebind_observer_after_move(); }
            other.bound_storage       = nullptr;
            other.pending_erase_count = 0;
            other.observer_registered = false;
        }

        TsDictState &operator=(TsDictState &&other) noexcept {
            if (this != &other) {
                detach_observer();
                pending_erase_count = other.pending_erase_count;
                child_states        = std::move(other.child_states);
                slot_observer       = std::move(other.slot_observer);
                bound_storage       = other.bound_storage;
                observer_registered = other.observer_registered;
                if (slot_observer != nullptr) { slot_observer->owner = this; }
                if (observer_registered) { rebind_observer_after_move(); }
                other.bound_storage       = nullptr;
                other.pending_erase_count = 0;
                other.observer_registered = false;
            }
            return *this;
        }

        TsDictState(const TsDictState &)            = delete;
        TsDictState &operator=(const TsDictState &) = delete;

        ~TsDictState() { detach_observer(); }

        void ensure_bound(detail::MapStorage &storage, const TsValueTypeBinding &value_binding,
                          const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator()) {
            if (child_states == nullptr) {
                child_states = std::make_unique<ValueSlotStore>(value_binding.checked_plan(), allocator);
            }
            if (bound_storage != &storage || !observer_registered) {
                detach_observer();
                bound_storage = &storage;
                attach_observer();
                child_states->reserve_to(bound_storage->keys.slot_capacity());
                reconcile_slots(storage);
            }
            refresh_pending_erase_count();
        }

        [[nodiscard]] void *child_state_memory(size_t slot) noexcept {
            return child_states != nullptr && child_states->has_slot(slot) ? child_states->value_memory(slot) : nullptr;
        }

        [[nodiscard]] const void *child_state_memory(size_t slot) const noexcept {
            return child_states != nullptr && child_states->has_slot(slot) ? child_states->value_memory(slot) : nullptr;
        }

      private:
        std::unique_ptr<ValueSlotStore>    child_states{};
        std::unique_ptr<SlotStateObserver> slot_observer{};
        detail::MapStorage                *bound_storage{nullptr};
        bool                               observer_registered{false};

        void reconcile_slots(const detail::MapStorage &storage) {
            if (child_states == nullptr) { return; }
            child_states->reserve_to(storage.keys.slot_capacity());
            for (size_t slot = 0; slot < storage.keys.slot_capacity(); ++slot) {
                if (storage.keys.slot_constructed(slot) && !child_states->has_slot(slot)) {
                    child_states->construct_at(slot);
                } else if (!storage.keys.slot_constructed(slot) && child_states->has_slot(slot)) {
                    child_states->destroy_at(slot);
                }
            }
        }

        void attach_observer() {
            if (bound_storage == nullptr || observer_registered) { return; }
            if (slot_observer == nullptr) { slot_observer = std::make_unique<SlotStateObserver>(this); }
            bound_storage->keys.add_slot_observer(slot_observer.get());
            observer_registered = true;
        }

        void detach_observer() noexcept {
            if (bound_storage == nullptr || !observer_registered || slot_observer == nullptr) { return; }
            auto &entries = bound_storage->keys.observers.entries();
            if (std::find(entries.begin(), entries.end(), slot_observer.get()) != entries.end()) {
                bound_storage->keys.remove_slot_observer(slot_observer.get());
            }
            observer_registered = false;
        }

        void rebind_observer_after_move() {
            if (bound_storage == nullptr || slot_observer == nullptr) { return; }
            auto &entries = bound_storage->keys.observers.entries();
            if (std::find(entries.begin(), entries.end(), slot_observer.get()) == entries.end()) {
                bound_storage->keys.add_slot_observer(slot_observer.get());
            }
        }

        void refresh_pending_erase_count() noexcept {
            pending_erase_count = bound_storage != nullptr ? bound_storage->keys.pending_erase_count() : 0;
        }
    };

    struct TsWindowState
    {
        engine_time_t first_observed_time{MIN_DT};
        bool          ready{false};
    };

    struct TsReferenceState
    {
        bool bound{false};
    };

    struct TsSignalState
    {};

    struct TsStatePlanLayout
    {
        static constexpr size_t npos = std::numeric_limits<size_t>::max();

        size_t generic_offset{npos};
        size_t endpoint_offset{npos};
        size_t kind_offset{npos};

        [[nodiscard]] bool has_endpoint() const noexcept { return endpoint_offset != npos; }
    };

    namespace detail
    {
        [[nodiscard]] inline const MemoryUtils::StoragePlan &ts_value_state_plan_impl(const TSValueTypeMetaData &type);
        [[nodiscard]] inline const MemoryUtils::StoragePlan &ts_kind_state_plan(const TSValueTypeMetaData &type);

        [[nodiscard]] inline const MemoryUtils::StoragePlan &ts_bundle_kind_state_plan(const TSValueTypeMetaData &type) {
            auto                   builder = MemoryUtils::named_tuple().reserve(type.field_count());
            const TSFieldMetaData *fields  = type.fields();
            for (size_t index = 0; index < type.field_count(); ++index) {
                builder.add_field(fields[index].name, ts_value_state_plan_impl(*fields[index].type));
            }
            return builder.build();
        }

        [[nodiscard]] inline const MemoryUtils::StoragePlan &ts_list_kind_state_plan(const TSValueTypeMetaData &type) {
            if (type.fixed_size() != 0) {
                return MemoryUtils::array_plan(ts_value_state_plan_impl(*type.element_ts()), type.fixed_size());
            }
            return MemoryUtils::plan_for<TsDynamicListState>();
        }

        [[nodiscard]] inline const MemoryUtils::StoragePlan &ts_kind_state_plan(const TSValueTypeMetaData &type) {
            switch (type.kind) {
                case TSValueTypeKind::Bundle: return ts_bundle_kind_state_plan(type);
                case TSValueTypeKind::List: return ts_list_kind_state_plan(type);
                case TSValueTypeKind::Set: return MemoryUtils::plan_for<TsSetState>();
                case TSValueTypeKind::Dict: return MemoryUtils::plan_for<TsDictState>();
                case TSValueTypeKind::Window: return MemoryUtils::plan_for<TsWindowState>();
                case TSValueTypeKind::Reference: return MemoryUtils::plan_for<TsReferenceState>();
                case TSValueTypeKind::Signal: return MemoryUtils::plan_for<TsSignalState>();
                case TSValueTypeKind::Value: return MemoryUtils::plan_for<TsScalarState>();
            }
            throw std::logic_error("Unsupported TSValueTypeKind");
        }

        [[nodiscard]] inline const MemoryUtils::StoragePlan &ts_value_state_plan_impl(const TSValueTypeMetaData &type) {
            return MemoryUtils::named_tuple()
                .add_field("generic", MemoryUtils::plan_for<TsNodeState>())
                .add_field("kind", ts_kind_state_plan(type))
                .build();
        }

        [[nodiscard]] inline TsStatePlanLayout ts_state_layout(const MemoryUtils::StoragePlan &plan) {
            if (!plan.is_named_tuple()) { throw std::logic_error("TS state plans must be named tuples"); }

            TsStatePlanLayout layout{};
            const size_t      component_count = plan.component_count();
            if (component_count == 2) {
                layout.generic_offset = plan.component(0).offset;
                layout.kind_offset    = plan.component(1).offset;
                return layout;
            }
            if (component_count == 3) {
                layout.generic_offset  = plan.component(0).offset;
                layout.endpoint_offset = plan.component(1).offset;
                layout.kind_offset     = plan.component(2).offset;
                return layout;
            }

            throw std::logic_error("TS state plans must have either 2 or 3 top-level components");
        }
    }  // namespace detail

    [[nodiscard]] inline const MemoryUtils::StoragePlan &ts_value_state_plan(const TSValueTypeMetaData &type) {
        return detail::ts_value_state_plan_impl(type);
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &ts_input_state_plan(const TSValueTypeMetaData &type) {
        return MemoryUtils::named_tuple()
            .add_field("generic", MemoryUtils::plan_for<TsNodeState>())
            .add_field("input", MemoryUtils::plan_for<TsInputRootState>())
            .add_field("kind", detail::ts_kind_state_plan(type))
            .build();
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &ts_output_state_plan(const TSValueTypeMetaData &type) {
        return MemoryUtils::named_tuple()
            .add_field("generic", MemoryUtils::plan_for<TsNodeState>())
            .add_field("output", MemoryUtils::plan_for<TsOutputRootState>())
            .add_field("kind", detail::ts_kind_state_plan(type))
            .build();
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_STATE_H
