#ifndef HGRAPH_CPP_ROOT_V2_KEY_SLOT_STORE_H
#define HGRAPH_CPP_ROOT_V2_KEY_SLOT_STORE_H

#include <hgraph/util/scope.h>
#include <hgraph/types/utils/slot_observer.h>
#include <hgraph/types/utils/stable_slot_store.h>
#include <hgraph/types/value/value_view.h>

#include <ankerl/unordered_dense.h>
#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Type-erased hash and equality operations for keys held in a
     * ``KeySlotStore``.
     *
     * Both function pointers receive a key as ``const void *`` and an
     * optional opaque ``context`` carried alongside the ops. Use
     * ``key_slot_store_ops_for<T>()`` to derive ops from a concrete type's
     * ``std::hash`` and ``operator==``.
     */
    struct KeySlotStoreOps
    {
        using hash_fn  = size_t (*)(const void *, const void *);
        using equal_fn = bool (*)(const void *, const void *, const void *);

        /** Hash hook; receives ``(key_pointer, context)``. */
        hash_fn     hash{nullptr};
        /** Equality hook; receives ``(lhs_pointer, rhs_pointer, context)``. */
        equal_fn    equal{nullptr};
        /** Opaque context forwarded to both hooks; may be null. */
        const void *context{nullptr};

        /** Hash ``key`` via the bound hook; throws if no hook is configured. */
        [[nodiscard]] size_t hash_key(const void *key) const {
            if (hash == nullptr) { throw std::logic_error("KeySlotStore requires a hash hook"); }
            return hash(key, context);
        }

        /** Compare ``lhs`` and ``rhs`` via the bound hook; throws if no hook is configured. */
        [[nodiscard]] bool equal_keys(const void *lhs, const void *rhs) const {
            if (equal == nullptr) { throw std::logic_error("KeySlotStore requires an equality hook"); }
            return equal(lhs, rhs, context);
        }
    };

    namespace detail
    {
        template <typename T>
        concept KeyHashable = requires(const T &value) {
            { std::hash<T>{}(value) } -> std::convertible_to<size_t>;
        };

        template <typename T>
        concept KeyEquatable = requires(const T &lhs, const T &rhs) {
            { lhs == rhs } -> std::convertible_to<bool>;
        };

        template <typename T> [[nodiscard]] size_t typed_key_hash(const void *key, const void *) {
            return std::hash<T>{}(*MemoryUtils::cast<T>(key));
        }

        template <typename T> [[nodiscard]] bool typed_key_equal(const void *lhs, const void *rhs, const void *) {
            return *MemoryUtils::cast<T>(lhs) == *MemoryUtils::cast<T>(rhs);
        }
    }  // namespace detail

    /**
     * Build ``KeySlotStoreOps`` for a concrete type ``T`` using its
     * ``std::hash<T>`` specialisation and ``operator==``. Both must be
     * available at compile time.
     */
    template <typename T> [[nodiscard]] constexpr KeySlotStoreOps key_slot_store_ops_for() noexcept {
        static_assert(detail::KeyHashable<T>, "KeySlotStore typed ops require std::hash<T>");
        static_assert(detail::KeyEquatable<T>, "KeySlotStore typed ops require operator==");

        return KeySlotStoreOps{
            .hash  = &detail::typed_key_hash<T>,
            .equal = &detail::typed_key_equal<T>,
        };
    }

    /**
     * Stable slot-backed key storage with delayed erase semantics.
     *
     * `KeySlotStore` owns homogeneous keys in stable slot memory and provides
     * key-to-slot lookup through an internal hash index. Logical removal is
     * split from physical erase:
     *
     * - ``constructed(slot)`` means a key object still exists in slot memory
     * - ``live(slot)`` means that constructed key is currently present
     * - constructed and not live means the key is pending physical erase
     *
     * The lifecycle representation is selected from the payload alignment:
     * pointer-aligned keys use tagged slot pointers, while weaker alignments
     * use the compact bitmap implementation.
     *
     * Pending removals remain addressable by slot and key until the owner
     * explicitly flushes them with `erase_pending()`. The store deliberately
     * does not define mutation epochs; callers such as TSData decide when an
     * epoch rolls over.
     *
     * Example:
     * ```c++
     * KeySlotStore store(MemoryUtils::plan_for<std::int32_t>(), key_slot_store_ops_for<std::int32_t>());
     * store.insert(3);
     *
     * store.remove(3);
     *
     * // The key is no longer live, but it still exists until the next batch.
     * assert(!store.slot_live(0));
     * assert(store.slot_constructed(0));
     * assert(store.slot_pending_erase(0));
     *
     * store.erase_pending();  // flushes pending removals from the prior batch
     * assert(!store.slot_constructed(0));
     * ```
     */
    struct KeySlotStore
    {
        /** Sentinel slot value returned for "not found". */
        static constexpr size_t npos = static_cast<size_t>(-1);

        /** Result of an ``insert`` call. */
        struct InsertResult
        {
            /** Slot id assigned to the key (``npos`` only on failure paths that throw before reaching the result). */
            size_t slot{npos};
            /** True when this call introduced (or resurrected) the key; false when the live key was already present. */
            bool   inserted{false};
            /** True only when this call constructed a new physical key slot. */
            bool   constructed{false};
        };

      private:
        using StableStorage = StableSlotStore<StableSlotStateModel::ConstructedAndLive>;
        StableStorage key_storage{};

      public:
        /**
         * Structural observers kept in sync with insert, remove, erase, clear,
         * and capacity events.
         */
        SlotObserverList observers{};

        /**
         * Bind the store to ``plan`` and ``ops``. ``plan`` must remain valid
         * for the store's lifetime; the optional ``allocator`` is held by
         * the underlying ``StableSlotStore``.
         */
        KeySlotStore(const MemoryUtils::StoragePlan &plan, KeySlotStoreOps ops,
                     const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : key_storage(plan.layout, allocator), m_key_plan(&plan), m_ops(ops) {
            validate_plan();
            validate_ops();
            rebuild_index();
        }

        explicit KeySlotStore(ValueTypeRef binding,
                              const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : KeySlotStore(
                  binding.checked_plan(),
                  KeySlotStoreOps{
                      .hash = [](const void *key, const void *context) {
                          return static_cast<const ValueOps *>(context)->hash(key);
                      },
                      .equal = [](const void *lhs, const void *rhs, const void *context) {
                          return static_cast<const ValueOps *>(context)->equals(lhs, rhs);
                      },
                      .context = &binding.ops_ref(),
                  }, allocator) {
            m_value_binding = binding;
        }

        KeySlotStore(const KeySlotStore &)            = delete;
        KeySlotStore &operator=(const KeySlotStore &) = delete;

        /**
         * Move construction transfers slots, bookkeeping, AND the lookup index
         * (O(1), allocation-free — honestly ``noexcept``): the index functors
         * point at the heap-stable ``IndexBackPtr`` cell, which moves with the
         * store and is re-pointed with one write. The moved-from store keeps no
         * index and must only be destroyed or assigned into.
         */
        KeySlotStore(KeySlotStore &&other) noexcept
            : key_storage(std::move(other.key_storage)), observers(std::move(other.observers)),
              m_size(std::exchange(other.m_size, 0)),
              m_pending_erase_count(std::exchange(other.m_pending_erase_count, 0)),
              m_key_plan(std::exchange(other.m_key_plan, nullptr)), m_ops(other.m_ops),
              m_value_binding(std::exchange(other.m_value_binding, {})),
              m_free_slots(std::move(other.m_free_slots)),
              m_pending_erase_slots(std::move(other.m_pending_erase_slots)),
              m_index_owner(std::move(other.m_index_owner)),
              m_index(std::move(other.m_index)) {
            if (m_index_owner != nullptr) { m_index_owner->store = this; }
            other.m_free_slots.clear();
        }

        /** Move assignment destroys current contents and adopts ``other`` (O(1), ``noexcept``). */
        KeySlotStore &operator=(KeySlotStore &&other) noexcept {
            if (this != &other) {
                hard_clear();
                key_storage           = std::move(other.key_storage);
                observers             = std::move(other.observers);
                m_size                = std::exchange(other.m_size, 0);
                m_pending_erase_count = std::exchange(other.m_pending_erase_count, 0);
                m_key_plan            = std::exchange(other.m_key_plan, nullptr);
                m_ops                 = other.m_ops;
                m_value_binding       = std::exchange(other.m_value_binding, {});
                m_free_slots          = std::move(other.m_free_slots);
                m_pending_erase_slots = std::move(other.m_pending_erase_slots);
                // Destroy the current index while its allocation tracker is still alive.
                m_index               = std::move(other.m_index);
                m_index_owner         = std::move(other.m_index_owner);
                if (m_index_owner != nullptr) { m_index_owner->store = this; }
                other.m_free_slots.clear();
            }
            return *this;
        }

        /** Destructor destroys every constructed key, including pending-erase ones. */
        ~KeySlotStore() { hard_clear(); }

        /** Number of currently live keys. */
        [[nodiscard]] size_t                           size() const noexcept { return m_size; }
        /** Number of slots currently addressable. */
        [[nodiscard]] size_t                           slot_capacity() const noexcept { return key_storage.slot_capacity(); }
        /** Number of constructed keys that are no longer live, awaiting physical erase. */
        [[nodiscard]] size_t                           pending_erase_count() const noexcept { return m_pending_erase_count; }
        /** Bound storage plan; never null after successful construction. */
        [[nodiscard]] const MemoryUtils::StoragePlan  *plan() const noexcept { return m_key_plan; }
        /** Allocator carried through to the underlying ``StableSlotStore``. */
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept { return key_storage.allocator(); }
        /** True when at least one slot is awaiting physical erase. */
        [[nodiscard]] bool                             has_pending_erase() const noexcept { return m_pending_erase_count != 0; }
        /** Selected stable-slot index representation. */
        [[nodiscard]] StableSlotRepresentation slot_representation() const noexcept
        {
            return key_storage.representation();
        }
        /** Data-only addresses used when publishing debugger offsets. */
        [[nodiscard]] StableSlotDebugView debug_slot_view() const noexcept { return key_storage.debug_view(); }
        /** Slots logically removed in the current batch. Stale resurrected entries may be present. */
        [[nodiscard]] std::span<const size_t> pending_erase_slots() const noexcept
        {
            return m_pending_erase_slots;
        }

        /** Exact occupied/retained heap bytes owned by keys, indices, state, and bookkeeping. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = key_storage.dynamic_storage_metrics();
            result += observers.dynamic_storage_metrics();
            result += vector_metrics(m_free_slots);
            result += vector_metrics(m_pending_erase_slots);

            if (m_index_owner != nullptr)
            {
                result.live_bytes += sizeof(IndexBackPtr);
                result.reserved_bytes += sizeof(IndexBackPtr);
            }
            if (m_index != nullptr)
            {
                const size_t live_index_entries = m_index->size();
                result.live_bytes += sizeof(IndexSet) +
                                     live_index_entries *
                                         (sizeof(typename IndexSet::value_type) + sizeof(typename IndexSet::bucket_type));
                result.reserved_bytes += sizeof(IndexSet) + m_index_owner->allocated_bytes;
            }
            return result;
        }

        /**
         * Return whether slot memory still contains a constructed key object.
         */
        [[nodiscard]] bool slot_constructed(size_t slot) const noexcept {
            return key_storage.constructed(slot);
        }

        /**
         * Return whether the constructed key at `slot` is logically present.
         */
        [[nodiscard]] bool slot_live(size_t slot) const noexcept { return key_storage.live(slot); }

        /**
         * Return whether a removed key is awaiting physical erase.
         */
        [[nodiscard]] bool slot_pending_erase(size_t slot) const noexcept {
            return key_storage.non_live_slot_memory(slot) != nullptr;
        }

        /**
         * Return the key payload stored at `slot`.
         *
         * This accessor only requires the slot to still be constructed. It
         * intentionally ignores live-ness so callers can inspect pending-erase
         * keys before they are flushed.
         *
         * Throws if `slot` is out of range or has already been physically
         * erased.
         */
        [[nodiscard]] void *operator[](size_t slot) {
            if (void *memory = key_memory(slot); memory != nullptr) { return memory; }
            require_constructed_slot(slot);
            return key_storage.slot_memory(slot);
        }

        /**
         * Const overload of the constructed-slot accessor.
         */
        [[nodiscard]] const void *operator[](size_t slot) const {
            if (const void *memory = key_memory(slot); memory != nullptr) { return memory; }
            require_constructed_slot(slot);
            return key_storage.slot_memory(slot);
        }

        /** Pointer to ``slot``'s payload if constructed; ``nullptr`` otherwise. */
        [[nodiscard]] void *key_memory(size_t slot) noexcept {
            if (void *memory = key_storage.live_slot_memory(slot); memory != nullptr) { return memory; }
            return slot_constructed(slot) ? key_storage.slot_memory(slot) : nullptr;
        }

        /** Const overload of ``key_memory``. */
        [[nodiscard]] const void *key_memory(size_t slot) const noexcept {
            if (const void *memory = key_storage.live_slot_memory(slot); memory != nullptr) { return memory; }
            return slot_constructed(slot) ? key_storage.slot_memory(slot) : nullptr;
        }

        /**
         * Find the slot for a constructed key regardless of live state.
         *
         * This lookup still returns a slot for keys that were removed in the
         * current batch but have not yet been physically erased.
         *
         * Example:
         * After `remove(3)`, `find_stored_slot(&three)` still returns the old
         * slot until `erase_pending()`.
         */
        [[nodiscard]] size_t find_stored_slot(const void *key) const {
            if (m_index == nullptr) { return npos; }
            const auto it = m_index->find(key);
            return it == m_index->end() ? npos : *it;
        }

        [[nodiscard]] size_t find_stored_slot(const ValueView &key) const {
            require_value_binding();
            if (!key.has_value() || m_index == nullptr) { return npos; }
            const auto it = m_index->find(key);
            return it == m_index->end() ? npos : *it;
        }

        /**
         * Find the slot only if the key is currently live.
         *
         * This is the membership-facing lookup. Pending-erase keys are treated
         * as absent even though their payload may still be constructed and
         * discoverable via `find_stored_slot()`.
         */
        [[nodiscard]] size_t find_slot(const void *key) const {
            const size_t slot = find_stored_slot(key);
            return slot != npos && slot_live(slot) ? slot : npos;
        }

        [[nodiscard]] size_t find_slot(const ValueView &key) const {
            const size_t slot = find_stored_slot(key);
            return slot != npos && slot_live(slot) ? slot : npos;
        }

        /** True when ``key`` is currently a live member of the set. */
        [[nodiscard]] bool contains(const void *key) const { return find_slot(key) != npos; }
        [[nodiscard]] bool contains(const ValueView &key) const { return find_slot(key) != npos; }

        /** Typed pointer to the key in ``slot`` if constructed; null otherwise. Asserts the bound plan matches ``T``. */
        template <typename T> [[nodiscard]] T *try_key(size_t slot) {
            if (!slot_constructed(slot)) { return nullptr; }
            require_type<T>();
            return MemoryUtils::cast<T>(key_storage.slot_memory(slot));
        }

        /** Const overload of ``try_key``. */
        template <typename T> [[nodiscard]] const T *try_key(size_t slot) const {
            if (!slot_constructed(slot)) { return nullptr; }
            require_type<T>();
            return MemoryUtils::cast<T>(key_storage.slot_memory(slot));
        }

        /**
         * Grow capacity to at least ``capacity`` slots. New slots are added
         * to the free-slot pool and the lookup index is reserved
         * accordingly. Notifies registered observers via ``on_capacity``.
         */
        void reserve_to(size_t capacity) {
            if (capacity <= slot_capacity()) { return; }

            const size_t old_capacity = slot_capacity();
            key_storage.reserve_to(capacity);
            m_free_slots.reserve(m_free_slots.size() + capacity - old_capacity);
            m_index->reserve(capacity);
            for (size_t slot = capacity; slot > old_capacity; --slot) { m_free_slots.push_back(slot - 1); }
            observers.notify_capacity(old_capacity, capacity);
        }

        /** Typed overload of ``reserve_to`` that asserts the bound plan matches ``T``. */
        template <typename T> void reserve_to(size_t capacity) {
            require_type<T>();
            reserve_to(capacity);
        }

      private:
        [[nodiscard]] auto rollback_new_slot(size_t slot) {
            return ::hgraph::make_scope_exit([this, slot]() noexcept {
                m_key_plan->destroy(key_storage.slot_memory(slot));
                key_storage.mark_free(slot);
                m_free_slots.push_back(slot);
            });
        }

      public:

        /** Typed overload of ``find_stored_slot`` that asserts the bound plan matches ``T``. */
        template <typename T> [[nodiscard]] size_t find_stored_slot(const T &key) const {
            require_type<T>();
            return find_stored_slot(static_cast<const void *>(std::addressof(key)));
        }

        /** Typed overload of ``find_slot`` that asserts the bound plan matches ``T``. */
        template <typename T> [[nodiscard]] size_t find_slot(const T &key) const {
            require_type<T>();
            return find_slot(static_cast<const void *>(std::addressof(key)));
        }

        /** Typed overload of ``contains`` that asserts the bound plan matches ``T``. */
        template <typename T> [[nodiscard]] bool contains(const T &key) const {
            require_type<T>();
            return contains(static_cast<const void *>(std::addressof(key)));
        }

        /**
         * Insert a key if it is not already live.
         *
         * If the same key is pending erase, insertion resurrects the existing
         * slot rather than allocating a new one.
         */
        [[nodiscard]] InsertResult insert(const void *key) {
            if (key == nullptr) { throw std::invalid_argument("KeySlotStore insert requires a non-null key"); }

            if (const size_t existing = find_stored_slot(static_cast<const void *>(key)); existing != npos) {
                return reuse_existing_slot(existing);
            }

            const size_t slot = acquire_free_slot();
            auto rollback_free = ::hgraph::make_scope_exit([&]() noexcept { m_free_slots.push_back(slot); });
            m_key_plan->copy_construct(key_storage.slot_memory(slot), key);
            key_storage.mark_staged(slot);
            auto rollback = rollback_new_slot(slot);
            rollback_free.release();
            m_index->insert(slot);
            static_cast<void>(key_storage.mark_live(slot));
            ++m_size;
            rollback.release();
            observers.notify_insert(slot);
            return {.slot = slot, .inserted = true, .constructed = true};
        }

        [[nodiscard]] InsertResult insert(const ValueView &key) {
            require_value_binding();
            if (!key.has_value()) { throw std::invalid_argument("KeySlotStore insert requires a live key"); }
            if (const size_t existing = find_stored_slot(key); existing != npos) {
                return reuse_existing_slot(existing);
            }

            const size_t slot = acquire_free_slot();
            void *destination = key_storage.slot_memory(slot);
            auto rollback_free = ::hgraph::make_scope_exit([&]() noexcept { m_free_slots.push_back(slot); });
            m_value_binding.default_construct_at(destination);
            auto rollback_value = ::hgraph::make_scope_exit([&]() noexcept { m_value_binding.destroy_at(destination); });
            m_value_binding.ops_ref().copy_assign_from(
                m_value_binding, destination, key.binding(), key.data());

            key_storage.mark_staged(slot);
            auto rollback = rollback_new_slot(slot);
            rollback_value.release();
            rollback_free.release();
            m_index->insert(slot);
            static_cast<void>(key_storage.mark_live(slot));
            ++m_size;
            rollback.release();
            observers.notify_insert(slot);
            return {.slot = slot, .inserted = true, .constructed = true};
        }

        /**
         * Move-insert a key if it is not already live.
         *
         * Lookup is performed before the move. If an equivalent key already
         * exists, the source is left untouched and the existing slot is
         * returned. New slots are move-constructed from ``key``.
         */
        [[nodiscard]] InsertResult insert_move(void *key) {
            if (key == nullptr) { throw std::invalid_argument("KeySlotStore move insert requires a non-null key"); }

            if (const size_t existing = find_stored_slot(static_cast<const void *>(key)); existing != npos) {
                return reuse_existing_slot(existing);
            }

            if (!m_key_plan->can_move_construct()) {
                throw std::logic_error("KeySlotStore move insert requires move-constructible keys");
            }
            const size_t slot = acquire_free_slot();
            auto rollback_free = ::hgraph::make_scope_exit([&]() noexcept { m_free_slots.push_back(slot); });
            m_key_plan->move_construct(key_storage.slot_memory(slot), key);
            key_storage.mark_staged(slot);
            auto rollback = rollback_new_slot(slot);
            rollback_free.release();
            m_index->insert(slot);
            static_cast<void>(key_storage.mark_live(slot));
            ++m_size;
            rollback.release();
            observers.notify_insert(slot);
            return {.slot = slot, .inserted = true, .constructed = true};
        }

        /** Typed overload of ``insert`` that asserts the bound plan matches ``T``. */
        template <typename T> [[nodiscard]] InsertResult insert(const T &key) {
            require_type<T>();
            return insert(static_cast<const void *>(std::addressof(key)));
        }

        /** Logically remove ``key``. Returns ``true`` if a live key was removed. */
        [[nodiscard]] bool remove(const void *key) {
            const size_t slot = find_stored_slot(key);
            return slot != npos && remove_slot(slot);
        }

        /** Typed overload of ``remove`` that asserts the bound plan matches ``T``. */
        template <typename T> [[nodiscard]] bool remove(const T &key) {
            require_type<T>();
            return remove(static_cast<const void *>(std::addressof(key)));
        }

        /**
         * Logically remove a live key while deferring physical destruction.
         *
         * After removal the slot remains constructed and addressable until the
         * pending erase set is flushed.
         */
        [[nodiscard]] bool remove_slot(size_t slot) {
            if (!key_storage.mark_pending_erase(slot)) { return false; }

            observers.notify_remove(slot);
            m_pending_erase_slots.push_back(slot);
            ++m_pending_erase_count;
            --m_size;
            return true;
        }

        /**
         * Physically erase every removed key still pending destruction.
         *
         * This destroys the underlying key objects, removes them from the
         * lookup index, and returns their slots to the internal free-slot pool.
         */
        void erase_pending() noexcept {
            if (m_key_plan == nullptr || m_pending_erase_count == 0) { return; }

            for (const size_t slot : m_pending_erase_slots) {
                void *memory = key_storage.non_live_slot_memory(slot);
                if (memory == nullptr) { continue; }

                observers.notify_erase(slot);
                m_index->erase(slot);
                m_key_plan->destroy(memory);
                key_storage.mark_free(slot);
                m_free_slots.push_back(slot);
            }

            m_pending_erase_count = 0;
            m_pending_erase_slots.clear();
        }

        /** Notify ``on_clear`` and destroy every constructed key. Capacity is preserved. */
        void clear() {
            observers.notify_clear();
            hard_clear();
        }

        /** Forward to ``observers.add``. */
        void add_slot_observer(SlotObserver *observer) { observers.add(observer); }

        /** Forward to ``observers.remove``. */
        void remove_slot_observer(SlotObserver *observer) { observers.remove(observer); }

      private:
        /**
         * Stable indirection cell for the index functors. The functors are
         * copied into the ankerl set, so they must not hold ``this`` directly:
         * a moved store would leave them pointing at the old object. Instead
         * they point at this heap-allocated cell, which moves with the store
         * (``unique_ptr``) and is re-pointed with a single write — keeping the
         * move operations honestly ``noexcept`` and O(1) (no index rebuild).
         */
        struct IndexBackPtr
        {
            const KeySlotStore *store{nullptr};
            size_t              allocated_bytes{0};
        };

        /** Allocator shared by the dense values and bucket arrays so retained bytes are exact. */
        template <typename T>
        struct IndexAllocator
        {
            using value_type = T;
            using propagate_on_container_move_assignment = std::true_type;
            using is_always_equal = std::false_type;

            IndexBackPtr *owner{nullptr};

            constexpr IndexAllocator() noexcept = default;
            explicit constexpr IndexAllocator(IndexBackPtr *owner_) noexcept : owner(owner_) {}

            template <typename U>
            constexpr IndexAllocator(const IndexAllocator<U> &other) noexcept : owner(other.owner) {}

            [[nodiscard]] T *allocate(size_t count)
            {
                T *result = std::allocator<T>{}.allocate(count);
                if (owner != nullptr) { owner->allocated_bytes += count * sizeof(T); }
                return result;
            }

            void deallocate(T *memory, size_t count) noexcept
            {
                if (owner != nullptr) { owner->allocated_bytes -= count * sizeof(T); }
                std::allocator<T>{}.deallocate(memory, count);
            }

            template <typename U>
            [[nodiscard]] constexpr bool operator==(const IndexAllocator<U> &other) const noexcept
            {
                return owner == other.owner;
            }

            template <typename>
            friend struct IndexAllocator;
        };

        struct IndexHash
        {
            using is_transparent = void;
            using is_avalanching = void;

            const IndexBackPtr *owner{nullptr};

            [[nodiscard]] const KeySlotStore *store() const noexcept { return owner != nullptr ? owner->store : nullptr; }

            [[nodiscard]] static size_t mix(size_t hash) noexcept
            {
                return ankerl::unordered_dense::hash<size_t>{}(hash);
            }

            [[nodiscard]] size_t operator()(size_t slot) const {
                const KeySlotStore *s = store();
                return s != nullptr ? mix(s->hash_at_slot(slot)) : 0U;
            }

            [[nodiscard]] size_t operator()(const void *key) const {
                const KeySlotStore *s = store();
                return s != nullptr ? mix(s->m_ops.hash_key(key)) : 0U;
            }

            [[nodiscard]] size_t operator()(const ValueView &key) const { return mix(key.hash()); }
        };

        struct IndexEqual
        {
            using is_transparent = void;

            const IndexBackPtr *owner{nullptr};

            [[nodiscard]] const KeySlotStore *store() const noexcept { return owner != nullptr ? owner->store : nullptr; }

            [[nodiscard]] bool operator()(size_t lhs, size_t rhs) const {
                if (lhs == rhs) { return true; }
                const KeySlotStore *s = store();
                return s != nullptr && s->m_ops.equal_keys(s->key_memory(lhs), s->key_memory(rhs));
            }

            [[nodiscard]] bool operator()(size_t slot, const void *key) const {
                const KeySlotStore *s = store();
                return s != nullptr && s->m_ops.equal_keys(s->key_memory(slot), key);
            }

            [[nodiscard]] bool operator()(const void *key, size_t slot) const { return (*this)(slot, key); }

            [[nodiscard]] bool operator()(size_t slot, const ValueView &key) const {
                const KeySlotStore *s = store();
                return s != nullptr && ValueView{s->m_value_binding, s->key_memory(slot)}.equals(key);
            }

            [[nodiscard]] bool operator()(const ValueView &key, size_t slot) const { return (*this)(slot, key); }
        };

        using IndexSet = ankerl::unordered_dense::set<size_t, IndexHash, IndexEqual, IndexAllocator<size_t>>;

        template <typename T>
        [[nodiscard]] static DynamicStorageMetrics vector_metrics(const std::vector<T> &values) noexcept
        {
            return {
                .live_bytes = values.size() * sizeof(T),
                .reserved_bytes = values.capacity() * sizeof(T),
            };
        }

        [[nodiscard]] size_t hash_at_slot(size_t slot) const { return m_ops.hash_key(key_memory(slot)); }

        [[nodiscard]] const MemoryUtils::StoragePlan &require_bound_plan() const {
            if (m_key_plan == nullptr) { throw std::logic_error("KeySlotStore requires a bound storage plan"); }
            return *m_key_plan;
        }

        void require_constructed_slot(size_t slot) const {
            if (slot >= slot_capacity()) { throw std::out_of_range("KeySlotStore slot out of range"); }
            if (!slot_constructed(slot)) { throw std::logic_error("KeySlotStore slot is not constructed"); }
        }

        template <typename T> void require_type() const {
            if (&require_bound_plan() != &MemoryUtils::plan_for<T>()) {
                throw std::logic_error("KeySlotStore plan does not match requested type");
            }
        }

        void validate_plan() const {
            if (m_key_plan == nullptr || !m_key_plan->valid()) {
                throw std::logic_error("KeySlotStore requires a valid storage plan");
            }
            if (!m_key_plan->can_copy_construct()) { throw std::logic_error("KeySlotStore requires copy-constructible keys"); }
        }

        void validate_ops() const {
            if (m_ops.hash == nullptr || m_ops.equal == nullptr) {
                throw std::logic_error("KeySlotStore requires hash and equality hooks");
            }
        }

        void require_value_binding() const {
            if (!m_value_binding) {
                throw std::logic_error("KeySlotStore ValueView API requires a value binding");
            }
        }

        [[nodiscard]] InsertResult reuse_existing_slot(size_t slot) {
            if (!key_storage.mark_live(slot)) { return {.slot = slot, .inserted = false, .constructed = false}; }
            --m_pending_erase_count;
            if (m_pending_erase_count == 0) { m_pending_erase_slots.clear(); }
            ++m_size;
            observers.notify_insert(slot);
            return {.slot = slot, .inserted = true, .constructed = false};
        }

        [[nodiscard]] size_t acquire_free_slot() {
            if (m_free_slots.empty()) {
                reserve_to(std::max<size_t>(m_size + 1, std::max<size_t>(8, slot_capacity() * 2)));
            }
            const size_t slot = m_free_slots.back();
            m_free_slots.pop_back();
            return slot;
        }

        [[nodiscard]] std::unique_ptr<IndexSet> make_index() const {
            return std::make_unique<IndexSet>(0, IndexHash{.owner = m_index_owner.get()},
                                              IndexEqual{.owner = m_index_owner.get()},
                                              IndexAllocator<size_t>{m_index_owner.get()});
        }

        void rebuild_index() {
            if (m_index_owner == nullptr) { m_index_owner = std::make_unique<IndexBackPtr>(); }
            m_index_owner->store = this;
            m_index = make_index();
            for (size_t slot = 0; slot < slot_capacity(); ++slot) {
                if (slot_constructed(slot)) { m_index->insert(slot); }
            }
        }

        void hard_clear() noexcept {
            if (m_key_plan != nullptr) {
                for (size_t slot = 0; slot < slot_capacity(); ++slot) {
                    if (slot_constructed(slot)) { m_key_plan->destroy(key_storage.slot_memory(slot)); }
                }
            }

            if (m_index != nullptr) { m_index->clear(); }

            m_size                = 0;
            m_pending_erase_count = 0;
            m_pending_erase_slots.clear();
            key_storage.reset_states();
            m_free_slots.clear();
            for (size_t slot = slot_capacity(); slot > 0; --slot) { m_free_slots.push_back(slot - 1); }
        }

        size_t                          m_size{0};
        size_t                          m_pending_erase_count{0};
        const MemoryUtils::StoragePlan *m_key_plan{nullptr};
        KeySlotStoreOps                 m_ops{};
        ValueTypeRef                    m_value_binding{};
        std::vector<size_t>             m_free_slots{};
        std::vector<size_t>             m_pending_erase_slots{};
        std::unique_ptr<IndexBackPtr>   m_index_owner{};
        std::unique_ptr<IndexSet>       m_index{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_V2_KEY_SLOT_STORE_H
