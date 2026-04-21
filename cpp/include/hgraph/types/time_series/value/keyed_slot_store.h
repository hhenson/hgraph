#pragma once

#include <hgraph/types/time_series/value/slot_observer.h>
#include <hgraph/util/stable_slot_storage.h>

#include <sul/dynamic_bitset.hpp>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <new>
#include <utility>
#include <vector>

namespace hgraph::detail
{
    /**
     * Value-side stable-slot storage for keyed runtimes.
     *
     * The key layer owns the slot lifecycle and key lookup semantics. This
     * store owns the per-slot value memory together with the small amount of
     * extra bookkeeping that is defined over the same stable slot ids:
     *
     * - slot-stable value payload memory
     * - value-updated flags for the current delta epoch
     * - slot observers that mirror capacity / insert / remove / erase events
     *
     * This is the TSD-facing extraction step ahead of sharing the same store
     * shape with keyed nested runtimes such as `map_`.
     */
    struct KeyedSlotStore
    {
        StableSlotStorage      value_storage{};
        sul::dynamic_bitset<>  updated{};
        std::vector<SlotObserver *> observers{};

        [[nodiscard]] size_t slot_capacity() const noexcept { return value_storage.slot_capacity(); }

        [[nodiscard]] void *value_memory(size_t slot) noexcept { return value_storage.slot_data(slot); }

        [[nodiscard]] const void *value_memory(size_t slot) const noexcept { return value_storage.slot_data(slot); }

        void reserve_to(size_t capacity, size_t stride, size_t alignment)
        {
            value_storage.reserve_to(capacity, stride, alignment);
            updated.resize(capacity);
        }

        [[nodiscard]] bool slot_updated(size_t slot) const noexcept
        {
            return slot < updated.size() && updated.test(slot);
        }

        void mark_updated(size_t slot) noexcept
        {
            if (slot < updated.size()) { updated.set(slot); }
        }

        void clear_updated(size_t slot) noexcept
        {
            if (slot < updated.size()) { updated.reset(slot); }
        }

        void clear_all_updated() noexcept { updated.reset(); }

        void add_slot_observer(SlotObserver *observer)
        {
            if (observer == nullptr) { return; }
            const auto it = std::find(observers.begin(), observers.end(), observer);
            assert(it == observers.end() && "slot observer registered twice");
            if (it == observers.end()) { observers.push_back(observer); }
        }

        void remove_slot_observer(SlotObserver *observer)
        {
            if (observer == nullptr) { return; }
            const auto it = std::find(observers.begin(), observers.end(), observer);
            assert(it != observers.end() && "removing unregistered slot observer");
            if (it == observers.end()) { return; }
            if (it != observers.end() - 1) { *it = observers.back(); }
            observers.pop_back();
        }

        void notify_capacity(size_t old_capacity, size_t new_capacity) const
        {
            for (auto *observer : observers) {
                if (observer != nullptr) { observer->on_capacity(old_capacity, new_capacity); }
            }
        }

        void notify_insert(size_t slot) const
        {
            for (auto *observer : observers) {
                if (observer != nullptr) { observer->on_insert(slot); }
            }
        }

        void notify_remove(size_t slot) const
        {
            for (auto *observer : observers) {
                if (observer != nullptr) { observer->on_remove(slot); }
            }
        }

        void notify_erase(size_t slot) const
        {
            for (auto *observer : observers) {
                if (observer != nullptr) { observer->on_erase(slot); }
            }
        }

        void notify_clear() const
        {
            for (auto *observer : observers) {
                if (observer != nullptr) { observer->on_clear(); }
            }
        }
    };

    /**
     * Typed payload layer over stable slot storage.
     *
     * This is the non-keyed form used by dynamic runtimes that still require
     * slot-stable payload addresses but do not need key-side bookkeeping.
     */
    template <typename T> struct StablePayloadStore
    {
        StableSlotStorage      storage{};
        sul::dynamic_bitset<>  constructed{};

        void reserve_to(size_t capacity)
        {
            storage.reserve_to(capacity, sizeof(T), alignof(T));
            constructed.resize(capacity);
        }

        [[nodiscard]] size_t slot_capacity() const noexcept { return storage.slot_capacity(); }

        [[nodiscard]] bool has_slot(size_t slot) const noexcept
        {
            return slot < constructed.size() && constructed.test(slot);
        }

        [[nodiscard]] T *try_slot(size_t slot) noexcept
        {
            return has_slot(slot) ? std::launder(reinterpret_cast<T *>(storage.slot_data(slot))) : nullptr;
        }

        [[nodiscard]] const T *try_slot(size_t slot) const noexcept
        {
            return has_slot(slot) ? std::launder(reinterpret_cast<const T *>(storage.slot_data(slot))) : nullptr;
        }

        template <typename... Args> T &emplace_at(size_t slot, Args &&...args)
        {
            T *value = std::launder(reinterpret_cast<T *>(storage.slot_data(slot)));
            new (value) T(std::forward<Args>(args)...);
            constructed.set(slot);
            return *value;
        }

        void destroy_at(size_t slot) noexcept
        {
            if (!has_slot(slot)) { return; }
            std::destroy_at(std::launder(reinterpret_cast<T *>(storage.slot_data(slot))));
            constructed.reset(slot);
        }
    };

    /**
     * Typed payload layer over KeyedSlotStore.
     *
     * Stable slot ids still come from the owning keyed runtime. This helper
     * only manages which of those slots currently hold a constructed payload
     * of type @p T together with typed emplacement / destruction.
     */
    template <typename T> struct KeyedPayloadStore
    {
        KeyedSlotStore        storage{};
        sul::dynamic_bitset<> constructed{};

        void reserve_to(size_t capacity)
        {
            storage.reserve_to(capacity, sizeof(T), alignof(T));
            constructed.resize(capacity);
        }

        [[nodiscard]] bool has_slot(size_t slot) const noexcept
        {
            return slot < constructed.size() && constructed.test(slot);
        }

        [[nodiscard]] T *try_slot(size_t slot) noexcept
        {
            return has_slot(slot) ? std::launder(reinterpret_cast<T *>(storage.value_memory(slot))) : nullptr;
        }

        [[nodiscard]] const T *try_slot(size_t slot) const noexcept
        {
            return has_slot(slot) ? std::launder(reinterpret_cast<const T *>(storage.value_memory(slot))) : nullptr;
        }

        template <typename... Args> T &emplace_at(size_t slot, Args &&...args)
        {
            T *value = std::launder(reinterpret_cast<T *>(storage.value_memory(slot)));
            new (value) T(std::forward<Args>(args)...);
            constructed.set(slot);
            return *value;
        }

        void destroy_at(size_t slot) noexcept
        {
            if (!has_slot(slot)) { return; }
            std::destroy_at(std::launder(reinterpret_cast<T *>(storage.value_memory(slot))));
            constructed.reset(slot);
        }
    };
}  // namespace hgraph::detail
