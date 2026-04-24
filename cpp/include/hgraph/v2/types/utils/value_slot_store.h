#ifndef HGRAPH_CPP_ROOT_V2_VALUE_SLOT_STORE_H
#define HGRAPH_CPP_ROOT_V2_VALUE_SLOT_STORE_H

#include <hgraph/v2/types/utils/slot_observer.h>
#include <hgraph/v2/types/utils/stable_slot_storage.h>

#include <sul/dynamic_bitset.hpp>

#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>

namespace hgraph::v2
{
    /**
     * Value-side stable-slot storage for keyed runtimes.
     *
     * The keyed owner decides which slot ids are live. This store owns the
     * parallel value memory and the small amount of bookkeeping defined over
     * those same stable slot ids:
     *
     * - non-moving slot-backed value memory
     * - one stable bound storage plan
     * - per-slot updated flags for the current mutation epoch
     * - per-slot constructed flags for live payload ownership
     * - structural observers mirroring capacity / insert / remove / erase /
     *   clear events
     */
    struct ValueSlotStore
    {
        ValueSlotStore(const MemoryUtils::StoragePlan &plan,
                       const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : value_storage(allocator)
        {
            bind_plan(plan);
        }

        ValueSlotStore(const ValueSlotStore &) = delete;
        ValueSlotStore &operator=(const ValueSlotStore &) = delete;

        ValueSlotStore(ValueSlotStore &&other) noexcept
            : value_storage(std::move(other.value_storage))
            , updated(std::move(other.updated))
            , constructed(std::move(other.constructed))
            , observers(std::move(other.observers))
            , m_value_plan(std::exchange(other.m_value_plan, nullptr))
        {
            other.updated.clear();
            other.constructed.clear();
        }

        ValueSlotStore &operator=(ValueSlotStore &&other) noexcept
        {
            if (this != &other) {
                destroy_all();
                value_storage = std::move(other.value_storage);
                updated = std::move(other.updated);
                constructed = std::move(other.constructed);
                observers = std::move(other.observers);
                m_value_plan = std::exchange(other.m_value_plan, nullptr);
                other.updated.clear();
                other.constructed.clear();
            }
            return *this;
        }

        ~ValueSlotStore()
        {
            destroy_all();
        }

        StableSlotStorage value_storage{};
        sul::dynamic_bitset<> updated{};
        sul::dynamic_bitset<> constructed{};
        SlotObserverList observers{};

        [[nodiscard]] size_t slot_capacity() const noexcept { return value_storage.slot_capacity(); }
        [[nodiscard]] size_t stride() const noexcept { return value_storage.stride(); }
        [[nodiscard]] const MemoryUtils::StoragePlan *plan() const noexcept { return m_value_plan; }

        [[nodiscard]] void *value_memory(size_t slot) noexcept { return value_storage.slot_data(slot); }

        [[nodiscard]] const void *value_memory(size_t slot) const noexcept { return value_storage.slot_data(slot); }

        void reserve_to(size_t capacity)
        {
            const auto &plan = require_bound_plan();
            value_storage.reserve_to(capacity, plan.layout.size, plan.layout.alignment);
            updated.resize(capacity);
            constructed.resize(capacity);
        }

        template <typename T>
        void reserve_to(size_t capacity)
        {
            require_type<T>();
            reserve_to(capacity);
        }

        [[nodiscard]] bool slot_updated(size_t slot) const noexcept
        {
            return slot < updated.size() && updated.test(slot);
        }

        [[nodiscard]] bool has_slot(size_t slot) const noexcept
        {
            return slot < constructed.size() && constructed.test(slot);
        }

        void mark_updated(size_t slot) noexcept
        {
            if (slot < updated.size()) {
                updated.set(slot);
            }
        }

        void clear_updated(size_t slot) noexcept
        {
            if (slot < updated.size()) {
                updated.reset(slot);
            }
        }

        void clear_all_updated() noexcept { updated.reset(); }

        template <typename T>
        [[nodiscard]] T *try_value(size_t slot)
        {
            if (!has_slot(slot)) {
                return nullptr;
            }
            require_type<T>();
            return MemoryUtils::cast<T>(value_memory(slot));
        }

        template <typename T>
        [[nodiscard]] const T *try_value(size_t slot) const
        {
            if (!has_slot(slot)) {
                return nullptr;
            }
            require_type<T>();
            return MemoryUtils::cast<T>(value_memory(slot));
        }

        void construct_at(size_t slot)
        {
            const auto &plan = require_bound_plan();
            require_unconstructed_slot(slot);
            plan.default_construct(value_memory(slot));
            constructed.set(slot);
        }

        void construct_at(size_t slot, const void *src)
        {
            const auto &plan = require_bound_plan();
            require_unconstructed_slot(slot);
            plan.copy_construct(value_memory(slot), src);
            constructed.set(slot);
        }

        template <typename T, typename... Args>
        T &construct_at(size_t slot, Args &&...args)
        {
            require_type<T>();
            require_unconstructed_slot(slot);

            T *value = MemoryUtils::cast<T>(value_memory(slot));
            std::construct_at(value, std::forward<Args>(args)...);
            constructed.set(slot);
            return *value;
        }

        void destroy_at(size_t slot) noexcept
        {
            if (!has_slot(slot) || m_value_plan == nullptr) {
                return;
            }

            m_value_plan->destroy(value_memory(slot));
            constructed.reset(slot);
            clear_updated(slot);
        }

        void destroy_all() noexcept
        {
            if (m_value_plan != nullptr) {
                for (size_t slot = 0; slot < constructed.size(); ++slot) {
                    destroy_at(slot);
                }
            } else {
                constructed.reset();
            }
            clear_all_updated();
        }

        void add_slot_observer(SlotObserver *observer) { observers.add(observer); }

        void remove_slot_observer(SlotObserver *observer) { observers.remove(observer); }

        void notify_capacity(size_t old_capacity, size_t new_capacity) const {
            observers.notify_capacity(old_capacity, new_capacity);
        }

        void notify_insert(size_t slot) const { observers.notify_insert(slot); }

        void notify_remove(size_t slot) const { observers.notify_remove(slot); }

        void notify_erase(size_t slot) const { observers.notify_erase(slot); }

        void notify_clear() const { observers.notify_clear(); }

      private:
        const MemoryUtils::StoragePlan *m_value_plan{nullptr};

        void bind_plan(const MemoryUtils::StoragePlan &plan)
        {
            if (!plan.valid()) {
                throw std::logic_error("ValueSlotStore requires a valid storage plan");
            }
            if (m_value_plan == nullptr) {
                m_value_plan = &plan;
                return;
            }
            if (m_value_plan != &plan) {
                throw std::logic_error("ValueSlotStore plan must remain constant");
            }
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &require_bound_plan() const
        {
            if (m_value_plan == nullptr) {
                throw std::logic_error("ValueSlotStore requires a bound storage plan");
            }
            return *m_value_plan;
        }

        template <typename T>
        void require_type() const
        {
            (void) require_bound_plan();
            if (m_value_plan != &MemoryUtils::plan_for<T>()) {
                throw std::logic_error("ValueSlotStore plan does not match requested type");
            }
        }

        void require_unconstructed_slot(size_t slot) const
        {
            if (slot >= constructed.size()) {
                throw std::out_of_range("ValueSlotStore slot out of range");
            }
            if (has_slot(slot)) {
                throw std::logic_error("ValueSlotStore slot is already constructed");
            }
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_V2_VALUE_SLOT_STORE_H
