#ifndef HGRAPH_CPP_ROOT_VALUE_CONTAINER_STORAGE_H
#define HGRAPH_CPP_ROOT_VALUE_CONTAINER_STORAGE_H

#include <hgraph/v2/types/utils/key_slot_store.h>
#include <hgraph/v2/types/utils/value_slot_store.h>
#include <hgraph/v2/types/value/value_builder_ops.h>

#include <algorithm>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph::v2::detail
{
    [[nodiscard]] inline size_t combine_hash(size_t seed, size_t value) noexcept {
        seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
        return seed;
    }

    template <typename State> [[nodiscard]] const State &checked_state(const void *context, const char *name) {
        if (context == nullptr) { throw std::logic_error(std::string(name) + " requires lifecycle context"); }
        return *static_cast<const State *>(context);
    }

    inline void replace_value_memory(void *memory, const MemoryUtils::StoragePlan &plan, const void *src) {
        if (plan.can_copy_assign()) {
            plan.copy_assign(memory, src);
        } else {
            plan.destroy(memory);
            plan.copy_construct(memory, src);
        }
    }

    inline void replace_slot_value(ValueSlotStore &store, size_t slot, const void *src) {
        const auto *plan = store.plan();
        if (plan == nullptr) { throw std::logic_error("ValueSlotStore is not bound to a plan"); }
        if (!store.has_slot(slot)) {
            store.construct_at(slot, src);
        } else {
            replace_value_memory(store.value_memory(slot), *plan, src);
        }
        store.mark_updated(slot);
    }

    inline size_t binding_hash_key(const void *key, const void *context) {
        const auto *binding = static_cast<const ValueTypeBinding *>(context);
        if (binding == nullptr) { throw std::logic_error("Value-backed key slot hashing requires a binding"); }
        return binding->checked_ops().hash_of(key, *binding);
    }

    inline bool binding_equal_key(const void *lhs, const void *rhs, const void *context) {
        const auto *binding = static_cast<const ValueTypeBinding *>(context);
        if (binding == nullptr) { throw std::logic_error("Value-backed key slot equality requires a binding"); }
        return binding->checked_ops().equals_of(lhs, rhs, *binding);
    }

    [[nodiscard]] inline KeySlotStoreOps binding_key_slot_store_ops(const ValueTypeBinding &binding) {
        if (!binding.checked_ops().can_hash() || !binding.checked_ops().can_equal()) {
            throw std::logic_error("Set/map keys require hashable and equatable value bindings");
        }

        return KeySlotStoreOps{
            .hash    = &binding_hash_key,
            .equal   = &binding_equal_key,
            .context = &binding,
        };
    }

    template <typename Fn> inline void for_each_live_slot(const KeySlotStore &store, Fn &&fn) {
        for (size_t slot = 0; slot < store.slot_capacity(); ++slot) {
            if (store.slot_live(slot)) { fn(slot); }
        }
    }

    struct DynamicListStorage
    {
        explicit DynamicListStorage(const MemoryUtils::StoragePlan &element_plan) : values(element_plan) {}

        DynamicListStorage(DynamicListStorage &&) noexcept            = default;
        DynamicListStorage &operator=(DynamicListStorage &&) noexcept = default;

        ValueSlotStore values;
        size_t         size{0};

        [[nodiscard]] bool empty() const noexcept { return size == 0; }

        void reserve_to(size_t capacity) {
            if (capacity > values.slot_capacity()) {
                values.reserve_to(std::max<size_t>(capacity, std::max<size_t>(8, values.slot_capacity() * 2)));
            }
        }

        void clear() noexcept {
            for (size_t index = size; index > 0; --index) { values.destroy_at(index - 1); }
            size = 0;
        }

        void resize(size_t new_size) {
            if (new_size > size) {
                reserve_to(new_size);
                for (size_t index = size; index < new_size; ++index) { values.construct_at(index); }
            } else {
                for (size_t index = size; index > new_size; --index) { values.destroy_at(index - 1); }
            }
            size = new_size;
        }

        void push_back_copy(const void *src) {
            reserve_to(size + 1);
            values.construct_at(size, src);
            values.mark_updated(size);
            ++size;
        }

        void replace_at(size_t index, const void *src) {
            if (index >= size) { throw std::out_of_range("DynamicListStorage index out of range"); }
            replace_slot_value(values, index, src);
        }

        void copy_from(const DynamicListStorage &other) {
            clear();
            reserve_to(other.size);
            for (size_t index = 0; index < other.size; ++index) { values.construct_at(index, other.values.value_memory(index)); }
            size = other.size;
        }
    };

    struct SetStorage
    {
        SetStorage(const MemoryUtils::StoragePlan &key_plan, KeySlotStoreOps ops) : keys(key_plan, ops) {}

        SetStorage(SetStorage &&) noexcept            = default;
        SetStorage &operator=(SetStorage &&) noexcept = default;

        KeySlotStore keys;

        void clear() { keys.clear(); }

        void copy_from(const SetStorage &other) {
            clear();
            keys.begin_mutation();
            for_each_live_slot(other.keys, [&](size_t slot) { static_cast<void>(keys.insert(other.keys[slot])); });
            keys.end_mutation();
        }
    };

    struct MapValueObserver final : SlotObserver
    {
        explicit MapValueObserver(ValueSlotStore *values_) noexcept : values(values_) {}

        void on_capacity(size_t, size_t new_capacity) override {
            if (values != nullptr) { values->reserve_to(new_capacity); }
        }

        void on_insert(size_t slot) override {
            if (values == nullptr) { return; }
            if (!values->has_slot(slot)) { values->construct_at(slot); }
            values->notify_insert(slot);
        }

        void on_remove(size_t slot) override {
            if (values != nullptr) { values->notify_remove(slot); }
        }

        void on_erase(size_t slot) override {
            if (values == nullptr) { return; }
            values->notify_erase(slot);
            values->destroy_at(slot);
        }

        void on_clear() override {
            if (values == nullptr) { return; }
            values->notify_clear();
            values->destroy_all();
        }

        ValueSlotStore *values{nullptr};
    };

    struct MapStorage
    {
        MapStorage(const MemoryUtils::StoragePlan &key_plan, KeySlotStoreOps key_ops, const MemoryUtils::StoragePlan &value_plan)
            : keys(key_plan, key_ops), values(value_plan), value_observer(&values) {
            keys.add_slot_observer(&value_observer);
        }

        MapStorage(MapStorage &&other) noexcept
            : keys(std::move(other.keys)), values(std::move(other.values)), value_observer(&values) {
            if (std::find(keys.observers.entries().begin(), keys.observers.entries().end(), &other.value_observer) !=
                keys.observers.entries().end()) {
                keys.remove_slot_observer(&other.value_observer);
            }
            keys.add_slot_observer(&value_observer);
        }

        MapStorage &operator=(MapStorage &&other) noexcept {
            if (this != &other) {
                keys           = std::move(other.keys);
                values         = std::move(other.values);
                value_observer = MapValueObserver(&values);
                if (std::find(keys.observers.entries().begin(), keys.observers.entries().end(), &other.value_observer) !=
                    keys.observers.entries().end()) {
                    keys.remove_slot_observer(&other.value_observer);
                }
                keys.add_slot_observer(&value_observer);
            }
            return *this;
        }

        KeySlotStore     keys;
        ValueSlotStore   values;
        MapValueObserver value_observer;

        void clear() { keys.clear(); }

        [[nodiscard]] bool contains(const void *key) const { return keys.contains(key); }

        [[nodiscard]] const void *value_at(const void *key) const {
            const size_t slot = keys.find_slot(key);
            if (slot == KeySlotStore::npos) { return nullptr; }
            return values.value_memory(slot);
        }

        [[nodiscard]] void *value_at(const void *key) {
            const size_t slot = keys.find_slot(key);
            if (slot == KeySlotStore::npos) { return nullptr; }
            return values.value_memory(slot);
        }

        void set_item(const void *key, const void *value) {
            if (value == nullptr) { throw std::invalid_argument("MapStorage requires a non-null value payload"); }

            const auto result = keys.insert(key);
            replace_slot_value(values, result.slot, value);
        }

        [[nodiscard]] bool remove(const void *key) { return keys.remove(key); }

        void copy_from(const MapStorage &other) {
            clear();
            keys.begin_mutation();
            for_each_live_slot(other.keys, [&](size_t slot) {
                const auto result = keys.insert(other.keys[slot]);
                replace_slot_value(values, result.slot, other.values.value_memory(slot));
            });
            keys.end_mutation();
        }
    };

    struct CyclicBufferStorage
    {
        CyclicBufferStorage(const MemoryUtils::StoragePlan &element_plan, size_t capacity_)
            : values(element_plan), capacity(capacity_) {
            values.reserve_to(capacity);
        }

        CyclicBufferStorage(CyclicBufferStorage &&) noexcept            = default;
        CyclicBufferStorage &operator=(CyclicBufferStorage &&) noexcept = default;

        ValueSlotStore values;
        size_t         size{0};
        size_t         head{0};
        size_t         capacity{0};

        [[nodiscard]] bool empty() const noexcept { return size == 0; }
        [[nodiscard]] bool full() const noexcept { return size == capacity; }

        [[nodiscard]] size_t slot_for_index(size_t index) const {
            if (index >= size) { throw std::out_of_range("CyclicBufferStorage index out of range"); }
            return capacity == 0 ? 0 : (head + index) % capacity;
        }

        void clear() noexcept {
            for (size_t index = 0; index < size; ++index) { values.destroy_at(slot_for_index(index)); }
            size = 0;
            head = 0;
        }

        void push_back_copy(const void *src) {
            if (capacity == 0) { throw std::logic_error("CyclicBufferStorage capacity must be greater than zero"); }

            if (!full()) {
                const size_t slot = (head + size) % capacity;
                values.construct_at(slot, src);
                values.mark_updated(slot);
                ++size;
                return;
            }

            const size_t slot = head;
            replace_slot_value(values, slot, src);
            head = (head + 1) % capacity;
        }

        void copy_from(const CyclicBufferStorage &other) {
            clear();
            for (size_t index = 0; index < other.size; ++index) {
                push_back_copy(other.values.value_memory(other.slot_for_index(index)));
            }
        }
    };

    struct QueueStorage
    {
        QueueStorage(const MemoryUtils::StoragePlan &element_plan, size_t max_capacity_)
            : values(element_plan), max_capacity(max_capacity_) {}

        QueueStorage(QueueStorage &&) noexcept            = default;
        QueueStorage &operator=(QueueStorage &&) noexcept = default;

        ValueSlotStore      values;
        std::deque<size_t>  order{};
        std::vector<size_t> free_slots{};
        size_t              max_capacity{0};

        [[nodiscard]] size_t size() const noexcept { return order.size(); }
        [[nodiscard]] bool   empty() const noexcept { return order.empty(); }
        [[nodiscard]] bool   bounded() const noexcept { return max_capacity > 0; }
        [[nodiscard]] bool   full() const noexcept { return bounded() && size() >= max_capacity; }

        void reserve_slots(size_t capacity) {
            if (capacity <= values.slot_capacity()) { return; }

            const size_t old_capacity = values.slot_capacity();
            values.reserve_to(std::max<size_t>(capacity, std::max<size_t>(8, values.slot_capacity() * 2)));
            for (size_t slot = values.slot_capacity(); slot > old_capacity; --slot) { free_slots.push_back(slot - 1); }
        }

        [[nodiscard]] size_t acquire_slot() {
            if (free_slots.empty()) { reserve_slots(size() + 1); }

            const size_t slot = free_slots.back();
            free_slots.pop_back();
            return slot;
        }

        void release_slot(size_t slot) {
            values.destroy_at(slot);
            free_slots.push_back(slot);
        }

        void clear() {
            while (!order.empty()) {
                release_slot(order.front());
                order.pop_front();
            }
        }

        void push_back_copy(const void *src) {
            if (full()) {
                const size_t slot = order.front();
                order.pop_front();
                replace_slot_value(values, slot, src);
                order.push_back(slot);
                return;
            }

            const size_t slot = acquire_slot();
            values.construct_at(slot, src);
            values.mark_updated(slot);
            order.push_back(slot);
        }

        void pop_front() {
            if (order.empty()) { throw std::out_of_range("QueueStorage is empty"); }
            const size_t slot = order.front();
            order.pop_front();
            release_slot(slot);
        }

        void copy_from(const QueueStorage &other) {
            clear();
            for (const size_t slot : other.order) { push_back_copy(other.values.value_memory(slot)); }
        }
    };

    struct DynamicListState
    {
        const ValueTypeBinding *element_binding{nullptr};
    };

    struct SetState
    {
        const ValueTypeBinding *element_binding{nullptr};
        KeySlotStoreOps         key_ops{};
    };

    struct MapState
    {
        const ValueTypeBinding *key_binding{nullptr};
        const ValueTypeBinding *value_binding{nullptr};
        KeySlotStoreOps         key_ops{};
    };

    struct CyclicBufferState
    {
        const ValueTypeBinding *element_binding{nullptr};
        size_t                  capacity{0};
    };

    struct QueueState
    {
        const ValueTypeBinding *element_binding{nullptr};
        size_t                  max_capacity{0};
    };

    inline void dynamic_list_construct(void *dst, const void *context) {
        const auto &state = checked_state<DynamicListState>(context, "dynamic list");
        std::construct_at(static_cast<DynamicListStorage *>(dst), state.element_binding->checked_plan());
    }

    inline void dynamic_list_destroy(void *memory, const void *) noexcept {
        std::destroy_at(static_cast<DynamicListStorage *>(memory));
    }

    inline void dynamic_list_copy_construct(void *dst, const void *src, const void *context) {
        dynamic_list_construct(dst, context);
        static_cast<DynamicListStorage *>(dst)->copy_from(*static_cast<const DynamicListStorage *>(src));
    }

    inline void dynamic_list_move_construct(void *dst, void *src, const void *context) {
        static_cast<void>(context);
        std::construct_at(static_cast<DynamicListStorage *>(dst), std::move(*static_cast<DynamicListStorage *>(src)));
    }

    inline void dynamic_list_copy_assign(void *dst, const void *src, const void *context) {
        if (dst == src) { return; }
        const auto        &state = checked_state<DynamicListState>(context, "dynamic list");
        DynamicListStorage temp(state.element_binding->checked_plan());
        temp.copy_from(*static_cast<const DynamicListStorage *>(src));
        *static_cast<DynamicListStorage *>(dst) = std::move(temp);
    }

    inline void dynamic_list_move_assign(void *dst, void *src, const void *context) {
        static_cast<void>(context);
        if (dst == src) { return; }
        *static_cast<DynamicListStorage *>(dst) = std::move(*static_cast<DynamicListStorage *>(src));
    }

    inline void set_storage_construct(void *dst, const void *context) {
        const auto &state = checked_state<SetState>(context, "set");
        std::construct_at(static_cast<SetStorage *>(dst), state.element_binding->checked_plan(), state.key_ops);
    }

    inline void set_storage_destroy(void *memory, const void *) noexcept { std::destroy_at(static_cast<SetStorage *>(memory)); }

    inline void set_storage_copy_construct(void *dst, const void *src, const void *context) {
        set_storage_construct(dst, context);
        static_cast<SetStorage *>(dst)->copy_from(*static_cast<const SetStorage *>(src));
    }

    inline void set_storage_move_construct(void *dst, void *src, const void *context) {
        static_cast<void>(context);
        std::construct_at(static_cast<SetStorage *>(dst), std::move(*static_cast<SetStorage *>(src)));
    }

    inline void set_storage_copy_assign(void *dst, const void *src, const void *context) {
        if (dst == src) { return; }
        const auto &state = checked_state<SetState>(context, "set");
        SetStorage  temp(state.element_binding->checked_plan(), state.key_ops);
        temp.copy_from(*static_cast<const SetStorage *>(src));
        *static_cast<SetStorage *>(dst) = std::move(temp);
    }

    inline void set_storage_move_assign(void *dst, void *src, const void *context) {
        static_cast<void>(context);
        if (dst == src) { return; }
        *static_cast<SetStorage *>(dst) = std::move(*static_cast<SetStorage *>(src));
    }

    inline void map_storage_construct(void *dst, const void *context) {
        const auto &state = checked_state<MapState>(context, "map");
        std::construct_at(static_cast<MapStorage *>(dst), state.key_binding->checked_plan(), state.key_ops,
                          state.value_binding->checked_plan());
    }

    inline void map_storage_destroy(void *memory, const void *) noexcept { std::destroy_at(static_cast<MapStorage *>(memory)); }

    inline void map_storage_copy_construct(void *dst, const void *src, const void *context) {
        map_storage_construct(dst, context);
        static_cast<MapStorage *>(dst)->copy_from(*static_cast<const MapStorage *>(src));
    }

    inline void map_storage_move_construct(void *dst, void *src, const void *context) {
        static_cast<void>(context);
        std::construct_at(static_cast<MapStorage *>(dst), std::move(*static_cast<MapStorage *>(src)));
    }

    inline void map_storage_copy_assign(void *dst, const void *src, const void *context) {
        if (dst == src) { return; }
        const auto &state = checked_state<MapState>(context, "map");
        MapStorage  temp(state.key_binding->checked_plan(), state.key_ops, state.value_binding->checked_plan());
        temp.copy_from(*static_cast<const MapStorage *>(src));
        *static_cast<MapStorage *>(dst) = std::move(temp);
    }

    inline void map_storage_move_assign(void *dst, void *src, const void *context) {
        static_cast<void>(context);
        if (dst == src) { return; }
        *static_cast<MapStorage *>(dst) = std::move(*static_cast<MapStorage *>(src));
    }

    inline void cyclic_buffer_construct(void *dst, const void *context) {
        const auto &state = checked_state<CyclicBufferState>(context, "cyclic buffer");
        std::construct_at(static_cast<CyclicBufferStorage *>(dst), state.element_binding->checked_plan(), state.capacity);
    }

    inline void cyclic_buffer_destroy(void *memory, const void *) noexcept {
        std::destroy_at(static_cast<CyclicBufferStorage *>(memory));
    }

    inline void cyclic_buffer_copy_construct(void *dst, const void *src, const void *context) {
        cyclic_buffer_construct(dst, context);
        static_cast<CyclicBufferStorage *>(dst)->copy_from(*static_cast<const CyclicBufferStorage *>(src));
    }

    inline void cyclic_buffer_move_construct(void *dst, void *src, const void *context) {
        static_cast<void>(context);
        std::construct_at(static_cast<CyclicBufferStorage *>(dst), std::move(*static_cast<CyclicBufferStorage *>(src)));
    }

    inline void cyclic_buffer_copy_assign(void *dst, const void *src, const void *context) {
        if (dst == src) { return; }
        const auto         &state = checked_state<CyclicBufferState>(context, "cyclic buffer");
        CyclicBufferStorage temp(state.element_binding->checked_plan(), state.capacity);
        temp.copy_from(*static_cast<const CyclicBufferStorage *>(src));
        *static_cast<CyclicBufferStorage *>(dst) = std::move(temp);
    }

    inline void cyclic_buffer_move_assign(void *dst, void *src, const void *context) {
        static_cast<void>(context);
        if (dst == src) { return; }
        *static_cast<CyclicBufferStorage *>(dst) = std::move(*static_cast<CyclicBufferStorage *>(src));
    }

    inline void queue_storage_construct(void *dst, const void *context) {
        const auto &state = checked_state<QueueState>(context, "queue");
        std::construct_at(static_cast<QueueStorage *>(dst), state.element_binding->checked_plan(), state.max_capacity);
    }

    inline void queue_storage_destroy(void *memory, const void *) noexcept { std::destroy_at(static_cast<QueueStorage *>(memory)); }

    inline void queue_storage_copy_construct(void *dst, const void *src, const void *context) {
        queue_storage_construct(dst, context);
        static_cast<QueueStorage *>(dst)->copy_from(*static_cast<const QueueStorage *>(src));
    }

    inline void queue_storage_move_construct(void *dst, void *src, const void *context) {
        static_cast<void>(context);
        std::construct_at(static_cast<QueueStorage *>(dst), std::move(*static_cast<QueueStorage *>(src)));
    }

    inline void queue_storage_copy_assign(void *dst, const void *src, const void *context) {
        if (dst == src) { return; }
        const auto  &state = checked_state<QueueState>(context, "queue");
        QueueStorage temp(state.element_binding->checked_plan(), state.max_capacity);
        temp.copy_from(*static_cast<const QueueStorage *>(src));
        *static_cast<QueueStorage *>(dst) = std::move(temp);
    }

    inline void queue_storage_move_assign(void *dst, void *src, const void *context) {
        static_cast<void>(context);
        if (dst == src) { return; }
        *static_cast<QueueStorage *>(dst) = std::move(*static_cast<QueueStorage *>(src));
    }

    template <typename Key, typename State, typename KeyHash = std::hash<Key>> struct ValueContainerPlanRegistry
    {
        std::mutex                                                         mutex{};
        std::unordered_map<Key, const MemoryUtils::StoragePlan *, KeyHash> cache{};
        std::vector<std::unique_ptr<State>>                                states{};
        std::vector<std::unique_ptr<MemoryUtils::StoragePlan>>             plans{};

        template <typename StateFactory, typename PlanFactory>
        [[nodiscard]] const MemoryUtils::StoragePlan &intern(const Key &key, StateFactory &&state_factory,
                                                             PlanFactory &&plan_factory) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (const auto it = cache.find(key); it != cache.end()) { return *it->second; }
            }

            auto state = state_factory();
            auto plan  = plan_factory(*state);

            std::lock_guard<std::mutex> lock(mutex);
            if (const auto it = cache.find(key); it != cache.end()) { return *it->second; }

            const auto *result = plan.get();
            states.push_back(std::move(state));
            plans.push_back(std::move(plan));
            cache.emplace(key, result);
            return *result;
        }
    };

    struct UnaryBindingKey
    {
        const ValueTypeBinding *binding{nullptr};

        [[nodiscard]] bool operator==(const UnaryBindingKey &) const noexcept = default;
    };

    struct UnaryBindingKeyHash
    {
        [[nodiscard]] size_t operator()(const UnaryBindingKey &key) const noexcept {
            return std::hash<const ValueTypeBinding *>{}(key.binding);
        }
    };

    struct BinaryBindingKey
    {
        const ValueTypeBinding *first{nullptr};
        const ValueTypeBinding *second{nullptr};

        [[nodiscard]] bool operator==(const BinaryBindingKey &) const noexcept = default;
    };

    struct BinaryBindingKeyHash
    {
        [[nodiscard]] size_t operator()(const BinaryBindingKey &key) const noexcept {
            size_t seed = std::hash<const ValueTypeBinding *>{}(key.first);
            return combine_hash(seed, std::hash<const ValueTypeBinding *>{}(key.second));
        }
    };

    struct SizedBindingKey
    {
        const ValueTypeBinding *binding{nullptr};
        size_t                  size{0};

        [[nodiscard]] bool operator==(const SizedBindingKey &) const noexcept = default;
    };

    struct SizedBindingKeyHash
    {
        [[nodiscard]] size_t operator()(const SizedBindingKey &key) const noexcept {
            size_t seed = std::hash<const ValueTypeBinding *>{}(key.binding);
            return combine_hash(seed, std::hash<size_t>{}(key.size));
        }
    };

    [[nodiscard]] inline MemoryUtils::StoragePlan make_dynamic_list_plan(const DynamicListState &state) {
        static_cast<void>(state);
        return MemoryUtils::StoragePlan{
            .layout                       = MemoryUtils::layout_for<DynamicListStorage>(),
            .lifecycle                    = {.construct      = &dynamic_list_construct,
                                             .destroy        = &dynamic_list_destroy,
                                             .copy_construct = &dynamic_list_copy_construct,
                                             .move_construct = &dynamic_list_move_construct,
                                             .copy_assign    = &dynamic_list_copy_assign,
                                             .move_assign    = &dynamic_list_move_assign},
            .lifecycle_context            = &state,
            .composite_kind_tag           = MemoryUtils::CompositeKind::None,
            .trivially_destructible       = false,
            .trivially_copyable           = false,
            .trivially_move_constructible = false,
        };
    }

    [[nodiscard]] inline MemoryUtils::StoragePlan make_set_plan(const SetState &state) {
        static_cast<void>(state);
        return MemoryUtils::StoragePlan{
            .layout                       = MemoryUtils::layout_for<SetStorage>(),
            .lifecycle                    = {.construct      = &set_storage_construct,
                                             .destroy        = &set_storage_destroy,
                                             .copy_construct = &set_storage_copy_construct,
                                             .move_construct = &set_storage_move_construct,
                                             .copy_assign    = &set_storage_copy_assign,
                                             .move_assign    = &set_storage_move_assign},
            .lifecycle_context            = &state,
            .composite_kind_tag           = MemoryUtils::CompositeKind::None,
            .trivially_destructible       = false,
            .trivially_copyable           = false,
            .trivially_move_constructible = false,
        };
    }

    [[nodiscard]] inline MemoryUtils::StoragePlan make_map_plan(const MapState &state) {
        static_cast<void>(state);
        return MemoryUtils::StoragePlan{
            .layout                       = MemoryUtils::layout_for<MapStorage>(),
            .lifecycle                    = {.construct      = &map_storage_construct,
                                             .destroy        = &map_storage_destroy,
                                             .copy_construct = &map_storage_copy_construct,
                                             .move_construct = &map_storage_move_construct,
                                             .copy_assign    = &map_storage_copy_assign,
                                             .move_assign    = &map_storage_move_assign},
            .lifecycle_context            = &state,
            .composite_kind_tag           = MemoryUtils::CompositeKind::None,
            .trivially_destructible       = false,
            .trivially_copyable           = false,
            .trivially_move_constructible = false,
        };
    }

    [[nodiscard]] inline MemoryUtils::StoragePlan make_cyclic_buffer_plan(const CyclicBufferState &state) {
        return MemoryUtils::StoragePlan{
            .layout                       = MemoryUtils::layout_for<CyclicBufferStorage>(),
            .lifecycle                    = {.construct      = &cyclic_buffer_construct,
                                             .destroy        = &cyclic_buffer_destroy,
                                             .copy_construct = &cyclic_buffer_copy_construct,
                                             .move_construct = &cyclic_buffer_move_construct,
                                             .copy_assign    = &cyclic_buffer_copy_assign,
                                             .move_assign    = &cyclic_buffer_move_assign},
            .lifecycle_context            = &state,
            .composite_kind_tag           = MemoryUtils::CompositeKind::None,
            .trivially_destructible       = false,
            .trivially_copyable           = false,
            .trivially_move_constructible = false,
        };
    }

    [[nodiscard]] inline MemoryUtils::StoragePlan make_queue_plan(const QueueState &state) {
        return MemoryUtils::StoragePlan{
            .layout                       = MemoryUtils::layout_for<QueueStorage>(),
            .lifecycle                    = {.construct      = &queue_storage_construct,
                                             .destroy        = &queue_storage_destroy,
                                             .copy_construct = &queue_storage_copy_construct,
                                             .move_construct = &queue_storage_move_construct,
                                             .copy_assign    = &queue_storage_copy_assign,
                                             .move_assign    = &queue_storage_move_assign},
            .lifecycle_context            = &state,
            .composite_kind_tag           = MemoryUtils::CompositeKind::None,
            .trivially_destructible       = false,
            .trivially_copyable           = false,
            .trivially_move_constructible = false,
        };
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &dynamic_list_plan(const ValueTypeBinding &element_binding) {
        static ValueContainerPlanRegistry<UnaryBindingKey, DynamicListState, UnaryBindingKeyHash> registry;
        return registry.intern(
            UnaryBindingKey{.binding = &element_binding},
            [&] {
                return std::make_unique<DynamicListState>(DynamicListState{
                    .element_binding = &element_binding,
                });
            },
            [&](const DynamicListState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(make_dynamic_list_plan(state));
            });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &set_plan(const ValueTypeBinding &element_binding) {
        static ValueContainerPlanRegistry<UnaryBindingKey, SetState, UnaryBindingKeyHash> registry;
        return registry.intern(
            UnaryBindingKey{.binding = &element_binding},
            [&] {
                return std::make_unique<SetState>(SetState{
                    .element_binding = &element_binding,
                    .key_ops         = binding_key_slot_store_ops(element_binding),
                });
            },
            [&](const SetState &state) { return std::make_unique<MemoryUtils::StoragePlan>(make_set_plan(state)); });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &map_plan(const ValueTypeBinding &key_binding,
                                                                  const ValueTypeBinding &value_binding) {
        static ValueContainerPlanRegistry<BinaryBindingKey, MapState, BinaryBindingKeyHash> registry;
        return registry.intern(
            BinaryBindingKey{
                .first  = &key_binding,
                .second = &value_binding,
            },
            [&] {
                return std::make_unique<MapState>(MapState{
                    .key_binding   = &key_binding,
                    .value_binding = &value_binding,
                    .key_ops       = binding_key_slot_store_ops(key_binding),
                });
            },
            [&](const MapState &state) { return std::make_unique<MemoryUtils::StoragePlan>(make_map_plan(state)); });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &cyclic_buffer_plan(const ValueTypeBinding &element_binding,
                                                                            size_t                  capacity) {
        static ValueContainerPlanRegistry<SizedBindingKey, CyclicBufferState, SizedBindingKeyHash> registry;
        return registry.intern(
            SizedBindingKey{
                .binding = &element_binding,
                .size    = capacity,
            },
            [&] {
                return std::make_unique<CyclicBufferState>(CyclicBufferState{
                    .element_binding = &element_binding,
                    .capacity        = capacity,
                });
            },
            [&](const CyclicBufferState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(make_cyclic_buffer_plan(state));
            });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &queue_plan(const ValueTypeBinding &element_binding, size_t max_capacity) {
        static ValueContainerPlanRegistry<SizedBindingKey, QueueState, SizedBindingKeyHash> registry;
        return registry.intern(
            SizedBindingKey{
                .binding = &element_binding,
                .size    = max_capacity,
            },
            [&] {
                return std::make_unique<QueueState>(QueueState{
                    .element_binding = &element_binding,
                    .max_capacity    = max_capacity,
                });
            },
            [&](const QueueState &state) { return std::make_unique<MemoryUtils::StoragePlan>(make_queue_plan(state)); });
    }
}  // namespace hgraph::v2::detail

#endif  // HGRAPH_CPP_ROOT_VALUE_CONTAINER_STORAGE_H
