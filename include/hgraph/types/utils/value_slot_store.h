#ifndef HGRAPH_CPP_ROOT_V2_VALUE_SLOT_STORE_H
#define HGRAPH_CPP_ROOT_V2_VALUE_SLOT_STORE_H

#include <hgraph/types/utils/key_slot_store.h>
#include <hgraph/types/utils/slot_observer.h>
#include <hgraph/types/utils/slot_bitmap.h>
#include <hgraph/types/utils/stable_slot_store.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>

namespace hgraph
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
     * - per-slot ``updated`` flags owned and reset by the caller
     * - per-slot constructed state for live payload ownership
     * - structural observers mirroring capacity / insert / remove / erase /
     *   clear events
     *
     * The store is non-copyable but movable. Move transfers ownership of the
     * heap blocks and bookkeeping; the moved-from store holds no payloads.
     */
    struct ValueSlotStore
    {
        /** Construct an unbound store. A storage plan must be bound before reserving or constructing slots. */
        ValueSlotStore() noexcept = default;

        /**
         * Construct a store bound to ``plan``, optionally using ``allocator``
         * for the underlying heap blocks. The plan must remain valid for the
         * lifetime of the store.
         */
        ValueSlotStore(const MemoryUtils::StoragePlan &plan,
                       const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : value_storage(plan.layout, allocator)
        {
            bind_plan(plan);
        }

        ValueSlotStore(const ValueSlotStore &) = delete;
        ValueSlotStore &operator=(const ValueSlotStore &) = delete;

        /** Move construction transfers all owned slots and bookkeeping. */
        ValueSlotStore(ValueSlotStore &&other) noexcept
            : value_storage(std::move(other.value_storage))
            , updated(std::move(other.updated))
            , observers(std::move(other.observers))
            , m_value_plan(std::exchange(other.m_value_plan, nullptr))
        {
            other.updated.clear();
        }

        /** Move assignment destroys the existing payloads and adopts ``other``. */
        ValueSlotStore &operator=(ValueSlotStore &&other) noexcept
        {
            if (this != &other) {
                destroy_all();
                value_storage = std::move(other.value_storage);
                updated = std::move(other.updated);
                observers = std::move(other.observers);
                m_value_plan = std::exchange(other.m_value_plan, nullptr);
                other.updated.clear();
            }
            return *this;
        }

        /** Destructor calls ``destroy_all`` so live payloads are torn down properly. */
        ~ValueSlotStore()
        {
            destroy_all();
        }

      private:
        using StableStorage = StableSlotStore<StableSlotStateModel::ConstructedOnly>;
        StableStorage value_storage{};

      public:
        /** Per-slot caller-managed ``updated`` bit. */
        SlotBitmap updated{};
        /** Structural observer list mirrored against slot lifecycle events. */
        SlotObserverList observers{};

        /** Number of slots currently addressable. */
        [[nodiscard]] size_t slot_capacity() const noexcept { return value_storage.slot_capacity(); }
        /** Per-slot byte stride in the underlying storage. */
        [[nodiscard]] size_t stride() const noexcept { return value_storage.stride(); }
        /** Bound storage plan for the held value type, or ``nullptr`` if unbound. */
        [[nodiscard]] const MemoryUtils::StoragePlan *plan() const noexcept { return m_value_plan; }
        /** Selected stable-slot index representation. */
        [[nodiscard]] StableSlotRepresentation slot_representation() const noexcept
        {
            return value_storage.representation();
        }
        /** Allocator used for stable payload blocks. */
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept
        {
            return value_storage.allocator();
        }
        /** Data-only addresses used when publishing debugger offsets. */
        [[nodiscard]] StableSlotDebugView debug_slot_view() const noexcept { return value_storage.debug_view(); }

        /** Exact occupied/retained heap bytes owned by the value slots. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = value_storage.dynamic_storage_metrics();
            result += updated.dynamic_storage_metrics();
            result += observers.dynamic_storage_metrics();
            return result;
        }

        /**
         * Bind this store to ``plan``.
         *
         * This supports TSData storage whose element plan is known by the
         * type-erased binding but not by the default constructor used by the
         * storage lifecycle. Once bound, the plan is immutable; rebinding to
         * a different plan is an error.
         */
        void bind_plan(const MemoryUtils::StoragePlan &plan)
        {
            if (!plan.valid()) {
                throw std::logic_error("ValueSlotStore requires a valid storage plan");
            }
            if (m_value_plan == nullptr) {
                m_value_plan = &plan;
                if (!value_storage.bound())
                    value_storage.bind_layout(plan.layout);
                else
                    value_storage.bind_layout(plan.layout, value_storage.allocator());
                return;
            }
            if (m_value_plan != &plan) {
                throw std::logic_error("ValueSlotStore plan must remain constant");
            }
        }

        /** Mutable byte pointer for ``slot``; not bounds-checked. */
        [[nodiscard]] void *value_memory(size_t slot) noexcept { return value_storage.slot_memory(slot); }

        /** Const byte pointer for ``slot``; not bounds-checked. */
        [[nodiscard]] const void *value_memory(size_t slot) const noexcept { return value_storage.slot_memory(slot); }

        /** Mutable payload pointer when ``slot`` is constructed; ``nullptr`` otherwise. */
        [[nodiscard]] void *try_value_memory(size_t slot) noexcept
        {
            return value_storage.live_slot_memory(slot);
        }

        /** Const overload of ``try_value_memory``. */
        [[nodiscard]] const void *try_value_memory(size_t slot) const noexcept
        {
            return value_storage.live_slot_memory(slot);
        }

        /**
         * Grow capacity to at least ``capacity`` slots. Leaves existing
         * payloads in place; new ``updated`` bits and constructed states
         * default to ``false``.
         */
        void reserve_to(size_t capacity)
        {
            (void) require_bound_plan();
            value_storage.reserve_to(capacity);
            updated.resize(capacity);
        }

        /** Typed overload that asserts the bound plan is the canonical plan for ``T``. */
        template <typename T>
        void reserve_to(size_t capacity)
        {
            require_type<T>();
            reserve_to(capacity);
        }

        /** True if ``slot`` is currently marked updated by the caller. */
        [[nodiscard]] bool slot_updated(size_t slot) const noexcept
        {
            return slot < updated.size() && updated.test(slot);
        }

        /** True if ``slot`` currently holds a constructed payload. */
        [[nodiscard]] bool has_slot(size_t slot) const noexcept
        {
            return value_storage.constructed(slot);
        }

        /** Mark ``slot`` as updated. */
        void mark_updated(size_t slot) noexcept
        {
            if (slot < updated.size()) {
                updated.set(slot);
            }
        }

        /** Clear the ``updated`` bit for ``slot`` only. */
        void clear_updated(size_t slot) noexcept
        {
            if (slot < updated.size()) {
                updated.reset(slot);
            }
        }

        /** Reset every ``updated`` bit; preserves ``constructed`` bits. */
        void clear_all_updated() noexcept { updated.reset(); }

        /** Typed pointer to ``slot`` if it holds a ``T``; otherwise ``nullptr``. */
        template <typename T>
        [[nodiscard]] T *try_value(size_t slot)
        {
            if (!has_slot(slot)) {
                return nullptr;
            }
            require_type<T>();
            return MemoryUtils::cast<T>(value_memory(slot));
        }

        /** Const overload of ``try_value``. */
        template <typename T>
        [[nodiscard]] const T *try_value(size_t slot) const
        {
            if (!has_slot(slot)) {
                return nullptr;
            }
            require_type<T>();
            return MemoryUtils::cast<T>(value_memory(slot));
        }

        /**
         * Default-construct a payload at ``slot`` using the bound plan.
         * Throws if ``slot`` is out of range or already constructed.
         */
        void construct_at(size_t slot)
        {
            const auto &plan = require_bound_plan();
            require_unconstructed_slot(slot);
            plan.default_construct(value_memory(slot));
            value_storage.mark_constructed(slot);
        }

        /**
         * Copy-construct a payload at ``slot`` from ``src`` using the bound
         * plan. Throws if ``slot`` is out of range or already constructed.
         */
        void construct_at(size_t slot, const void *src)
        {
            const auto &plan = require_bound_plan();
            require_unconstructed_slot(slot);
            plan.copy_construct(value_memory(slot), src);
            value_storage.mark_constructed(slot);
        }

        /**
         * In-place construct a typed payload at ``slot`` with ``args...``.
         * Asserts the bound plan matches ``T`` and that ``slot`` is unused.
         * Returns a reference to the constructed object.
         */
        template <typename T, typename... Args>
        T &construct_at(size_t slot, Args &&...args)
        {
            require_type<T>();
            require_unconstructed_slot(slot);

            T *value = MemoryUtils::cast<T>(value_memory(slot));
            std::construct_at(value, std::forward<Args>(args)...);
            value_storage.mark_constructed(slot);
            return *value;
        }

        /**
         * Destroy the payload at ``slot`` if one is present. Clears both the
         * ``constructed`` and ``updated`` bits. No-op when the slot is empty
         * or no plan is bound.
         */
        void destroy_at(size_t slot) noexcept
        {
            if (!has_slot(slot) || m_value_plan == nullptr) {
                return;
            }

            m_value_plan->destroy(value_memory(slot));
            value_storage.mark_free(slot);
            clear_updated(slot);
        }

        /** Destroy every constructed payload and clear the ``updated`` bitmap. */
        void destroy_all() noexcept
        {
            if (m_value_plan != nullptr) {
                for (size_t slot = 0; slot < slot_capacity(); ++slot) {
                    destroy_at(slot);
                }
            } else {
                value_storage.reset_states();
            }
            clear_all_updated();
        }

        /** Forward to ``observers.add``. */
        void add_slot_observer(SlotObserver *observer) { observers.add(observer); }

        /** Forward to ``observers.remove``. */
        void remove_slot_observer(SlotObserver *observer) { observers.remove(observer); }

        /** Forward to ``observers.notify_capacity``. */
        void notify_capacity(size_t old_capacity, size_t new_capacity) const {
            observers.notify_capacity(old_capacity, new_capacity);
        }

        /** Forward to ``observers.notify_insert``. */
        void notify_insert(size_t slot) const { observers.notify_insert(slot); }

        /** Forward to ``observers.notify_remove``. */
        void notify_remove(size_t slot) const { observers.notify_remove(slot); }

        /** Forward to ``observers.notify_erase``. */
        void notify_erase(size_t slot) const { observers.notify_erase(slot); }

        /** Forward to ``observers.notify_clear``. */
        void notify_clear() const { observers.notify_clear(); }

      private:
        const MemoryUtils::StoragePlan *m_value_plan{nullptr};

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
            if (slot >= slot_capacity()) {
                throw std::out_of_range("ValueSlotStore slot out of range");
            }
            if (has_slot(slot)) {
                throw std::logic_error("ValueSlotStore slot is already constructed");
            }
        }
    };

    /**
     * Value-side storage whose payload lifetime mirrors a ``KeySlotStore``.
     *
     * ``ValueSlotStore`` is intentionally reusable and owns internal
     * constructed state. TSD-style storage should not expose that state as a
     * second source of truth. This wrapper registers as a key-store observer
     * and keeps the value payload constructed exactly when the key slot is
     * constructed, including pending-erase slots. The referenced key store
     * must outlive the mirror so the observer can be unregistered during
     * destruction.
     */
    class KeyMirroredValueSlotStore final : private SlotObserver
    {
      public:
        KeyMirroredValueSlotStore(KeySlotStore &keys,
                                  const MemoryUtils::StoragePlan &value_plan,
                                  const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : m_keys(keys), m_values(value_plan, allocator)
        {
            mirror_existing_keys();
            m_keys.add_slot_observer(this);
        }

        KeyMirroredValueSlotStore(const KeyMirroredValueSlotStore &)            = delete;
        KeyMirroredValueSlotStore &operator=(const KeyMirroredValueSlotStore &) = delete;
        KeyMirroredValueSlotStore(KeyMirroredValueSlotStore &&)                 = delete;
        KeyMirroredValueSlotStore &operator=(KeyMirroredValueSlotStore &&)      = delete;

        ~KeyMirroredValueSlotStore() override
        {
            m_keys.remove_slot_observer(this);
        }

        /** Key store whose constructed state owns value payload lifetime. */
        [[nodiscard]] const KeySlotStore &key_store() const noexcept { return m_keys; }
        /** Number of value slots currently addressable. */
        [[nodiscard]] size_t slot_capacity() const noexcept { return m_values.slot_capacity(); }
        /** Bound value payload plan. */
        [[nodiscard]] const MemoryUtils::StoragePlan *plan() const noexcept { return m_values.plan(); }

        /** Stable storage metadata used to publish debugger-only slot offsets. */
        [[nodiscard]] const ValueSlotStore &debug_values() const noexcept { return m_values; }

        /** Exact occupied/retained heap bytes owned by the mirrored value slots. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            return m_values.dynamic_storage_metrics();
        }

        /**
         * True when the key slot is constructed. This is the public lifetime
         * answer for mirrored storage; the wrapped ``ValueSlotStore`` state is
         * only an internal mirror.
         */
        [[nodiscard]] bool has_slot(size_t slot) const noexcept
        {
            return m_keys.slot_constructed(slot);
        }

        /** True when the internal value bitmap matches the key store. */
        [[nodiscard]] bool mirrors_key_construction() const noexcept
        {
            const size_t capacity = std::max(m_keys.slot_capacity(), m_values.slot_capacity());
            for (size_t slot = 0; slot < capacity; ++slot) {
                if (m_keys.slot_constructed(slot) != m_values.has_slot(slot)) { return false; }
            }
            return true;
        }

        /** Mutable payload memory for a constructed key slot. */
        [[nodiscard]] void *value_memory(size_t slot)
        {
            if (void *memory = m_values.try_value_memory(slot); memory != nullptr) { return memory; }
            require_mirrored_slot(slot);
            return m_values.value_memory(slot);
        }

        /** Const payload memory for a constructed key slot. */
        [[nodiscard]] const void *value_memory(size_t slot) const
        {
            if (const void *memory = m_values.try_value_memory(slot); memory != nullptr) { return memory; }
            require_mirrored_slot(slot);
            return m_values.value_memory(slot);
        }

        /** Typed pointer to a value for a constructed key slot, or null otherwise. */
        template <typename T>
        [[nodiscard]] T *try_value(size_t slot)
        {
            if (!has_slot(slot)) { return nullptr; }
            auto *value = m_values.template try_value<T>(slot);
            if (value == nullptr) { throw std::logic_error("KeyMirroredValueSlotStore mirror is missing a value slot"); }
            return value;
        }

        /** Const overload of ``try_value``. */
        template <typename T>
        [[nodiscard]] const T *try_value(size_t slot) const
        {
            if (!has_slot(slot)) { return nullptr; }
            const auto *value = m_values.template try_value<T>(slot);
            if (value == nullptr) { throw std::logic_error("KeyMirroredValueSlotStore mirror is missing a value slot"); }
            return value;
        }

        /** Mark a constructed value slot as updated. */
        void mark_updated(size_t slot)
        {
            require_mirrored_slot(slot);
            m_values.mark_updated(slot);
        }

        /** True when a constructed value slot was marked updated. */
        [[nodiscard]] bool slot_updated(size_t slot) const noexcept
        {
            return has_slot(slot) && m_values.slot_updated(slot);
        }

        /** Clear one update bit. */
        void clear_updated(size_t slot) noexcept { m_values.clear_updated(slot); }
        /** Clear every update bit. */
        void clear_all_updated() noexcept { m_values.clear_all_updated(); }

      private:
        KeySlotStore  &m_keys;
        ValueSlotStore m_values;

        void mirror_existing_keys()
        {
            m_values.reserve_to(m_keys.slot_capacity());
            for (size_t slot = 0; slot < m_keys.slot_capacity(); ++slot) {
                if (m_keys.slot_constructed(slot)) { construct_value_slot(slot); }
            }
        }

        void construct_value_slot(size_t slot)
        {
            if (!m_values.has_slot(slot)) { m_values.construct_at(slot); }
        }

        void require_mirrored_slot(size_t slot) const
        {
            if (slot >= m_keys.slot_capacity()) { throw std::out_of_range("KeyMirroredValueSlotStore slot out of range"); }
            if (!m_keys.slot_constructed(slot)) {
                throw std::logic_error("KeyMirroredValueSlotStore slot is not constructed by the key store");
            }
            if (!m_values.has_slot(slot)) {
                throw std::logic_error("KeyMirroredValueSlotStore mirror is missing a value slot");
            }
        }

        void on_capacity(size_t, size_t new_capacity) override { m_values.reserve_to(new_capacity); }

        void on_insert(size_t slot) override { construct_value_slot(slot); }

        void on_remove(size_t) override {}

        void on_erase(size_t slot) override { m_values.destroy_at(slot); }

        void on_clear() override { m_values.destroy_all(); }
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_V2_VALUE_SLOT_STORE_H
