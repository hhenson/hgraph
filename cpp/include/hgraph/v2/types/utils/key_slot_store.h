#ifndef HGRAPH_CPP_ROOT_V2_KEY_SLOT_STORE_H
#define HGRAPH_CPP_ROOT_V2_KEY_SLOT_STORE_H

#include <hgraph/util/scope.h>
#include <hgraph/v2/types/utils/slot_observer.h>
#include <hgraph/v2/types/utils/stable_slot_storage.h>

#include <ankerl/unordered_dense.h>
#include <sul/dynamic_bitset.hpp>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::v2
{
    struct KeySlotStoreOps
    {
        using hash_fn  = size_t (*)(const void *, const void *);
        using equal_fn = bool (*)(const void *, const void *, const void *);

        hash_fn     hash{nullptr};
        equal_fn    equal{nullptr};
        const void *context{nullptr};

        [[nodiscard]] size_t hash_key(const void *key) const {
            if (hash == nullptr) { throw std::logic_error("KeySlotStore requires a hash hook"); }
            return hash(key, context);
        }

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
     * - `constructed[slot]` means a key object still exists in slot memory
     * - `live[slot]` means that constructed key is currently present
     * - `constructed && !live` means the key is pending physical erase
     *
     * Pending removals remain addressable by slot and key until they are
     * flushed, either explicitly with `erase_pending()` or automatically when
     * the next outermost mutation begins.
     *
     * Example:
     * ```c++
     * KeySlotStore store(MemoryUtils::plan_for<int>(), key_slot_store_ops_for<int>());
     * store.insert(3);
     *
     * store.begin_mutation();
     * store.remove(3);
     * store.end_mutation();
     *
     * // The key is no longer live, but it still exists until the next batch.
     * assert(!store.slot_live(0));
     * assert(store.slot_constructed(0));
     * assert(store.slot_pending_erase(0));
     *
     * store.begin_mutation();  // flushes pending removals from the prior batch
     * assert(!store.slot_constructed(0));
     * ```
     */
    struct KeySlotStore
    {
        static constexpr size_t npos = static_cast<size_t>(-1);

        struct InsertResult
        {
            size_t slot{npos};
            bool   inserted{false};
        };

        /**
         * Non-moving slot memory for the key payloads.
         */
        StableSlotStorage key_storage{};
        /**
         * Physical ownership bitset. If set, a key object exists in slot
         * memory and may still be inspected even when no longer live.
         */
        sul::dynamic_bitset<> constructed{};
        /**
         * Logical membership bitset. If set, the constructed key currently
         * participates in lookup and iteration.
         */
        sul::dynamic_bitset<> live{};
        /**
         * Structural observers kept in sync with insert, remove, erase, clear,
         * and capacity events.
         */
        SlotObserverList observers{};

        KeySlotStore(const MemoryUtils::StoragePlan &plan, KeySlotStoreOps ops,
                     const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : key_storage(allocator), m_key_plan(&plan), m_ops(ops) {
            validate_plan();
            validate_ops();
            rebuild_index();
        }

        KeySlotStore(const KeySlotStore &)            = delete;
        KeySlotStore &operator=(const KeySlotStore &) = delete;

        KeySlotStore(KeySlotStore &&other) noexcept
            : key_storage(std::move(other.key_storage)), constructed(std::move(other.constructed)), live(std::move(other.live)),
              observers(std::move(other.observers)), m_size(std::exchange(other.m_size, 0)),
              m_pending_erase_count(std::exchange(other.m_pending_erase_count, 0)),
              m_mutation_depth(std::exchange(other.m_mutation_depth, 0)), m_key_plan(std::exchange(other.m_key_plan, nullptr)),
              m_ops(other.m_ops), m_free_slots(std::move(other.m_free_slots)) {
            rebuild_index();
            other.constructed.clear();
            other.live.clear();
            other.m_free_slots.clear();
        }

        KeySlotStore &operator=(KeySlotStore &&other) noexcept {
            if (this != &other) {
                hard_clear();
                key_storage           = std::move(other.key_storage);
                constructed           = std::move(other.constructed);
                live                  = std::move(other.live);
                observers             = std::move(other.observers);
                m_size                = std::exchange(other.m_size, 0);
                m_pending_erase_count = std::exchange(other.m_pending_erase_count, 0);
                m_mutation_depth      = std::exchange(other.m_mutation_depth, 0);
                m_key_plan            = std::exchange(other.m_key_plan, nullptr);
                m_ops                 = other.m_ops;
                m_free_slots          = std::move(other.m_free_slots);
                rebuild_index();
                other.constructed.clear();
                other.live.clear();
                other.m_free_slots.clear();
            }
            return *this;
        }

        ~KeySlotStore() { hard_clear(); }

        [[nodiscard]] size_t                           size() const noexcept { return m_size; }
        [[nodiscard]] size_t                           slot_capacity() const noexcept { return key_storage.slot_capacity(); }
        [[nodiscard]] size_t                           pending_erase_count() const noexcept { return m_pending_erase_count; }
        [[nodiscard]] size_t                           mutation_depth() const noexcept { return m_mutation_depth; }
        [[nodiscard]] const MemoryUtils::StoragePlan  *plan() const noexcept { return m_key_plan; }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept { return key_storage.allocator(); }
        [[nodiscard]] bool                             has_pending_erase() const noexcept { return m_pending_erase_count != 0; }

        /**
         * Return whether slot memory still contains a constructed key object.
         */
        [[nodiscard]] bool slot_constructed(size_t slot) const noexcept {
            return slot < constructed.size() && constructed.test(slot);
        }

        /**
         * Return whether the constructed key at `slot` is logically present.
         */
        [[nodiscard]] bool slot_live(size_t slot) const noexcept { return slot < live.size() && live.test(slot); }

        /**
         * Return whether a removed key is awaiting physical erase.
         */
        [[nodiscard]] bool slot_pending_erase(size_t slot) const noexcept { return slot_constructed(slot) && !slot_live(slot); }

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
            require_constructed_slot(slot);
            return key_storage.slot_data(slot);
        }

        /**
         * Const overload of the constructed-slot accessor.
         */
        [[nodiscard]] const void *operator[](size_t slot) const {
            require_constructed_slot(slot);
            return key_storage.slot_data(slot);
        }

        [[nodiscard]] void *key_memory(size_t slot) noexcept {
            return slot_constructed(slot) ? key_storage.slot_data(slot) : nullptr;
        }

        [[nodiscard]] const void *key_memory(size_t slot) const noexcept {
            return slot_constructed(slot) ? key_storage.slot_data(slot) : nullptr;
        }

        /**
         * Find the slot for a constructed key regardless of live state.
         *
         * This lookup still returns a slot for keys that were removed in the
         * current batch but have not yet been physically erased.
         *
         * Example:
         * After `remove(3)`, `find_stored_slot(&three)` still returns the old
         * slot until `erase_pending()` or the next outermost
         * `begin_mutation()`.
         */
        [[nodiscard]] size_t find_stored_slot(const void *key) const {
            if (m_index == nullptr) { return npos; }
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

        [[nodiscard]] bool contains(const void *key) const { return find_slot(key) != npos; }

        template <typename T> [[nodiscard]] T *try_key(size_t slot) {
            if (!slot_constructed(slot)) { return nullptr; }
            require_type<T>();
            return MemoryUtils::cast<T>(key_storage.slot_data(slot));
        }

        template <typename T> [[nodiscard]] const T *try_key(size_t slot) const {
            if (!slot_constructed(slot)) { return nullptr; }
            require_type<T>();
            return MemoryUtils::cast<T>(key_storage.slot_data(slot));
        }

        void reserve_to(size_t capacity) {
            if (capacity <= slot_capacity()) { return; }

            const size_t old_capacity = slot_capacity();
            key_storage.reserve_to(capacity, m_key_plan->layout.size, m_key_plan->layout.alignment);
            constructed.resize(capacity);
            live.resize(capacity);
            m_free_slots.reserve(m_free_slots.size() + capacity - old_capacity);
            m_index->reserve(capacity);
            for (size_t slot = capacity; slot > old_capacity; --slot) { m_free_slots.push_back(slot - 1); }
            observers.notify_capacity(old_capacity, capacity);
        }

        template <typename T> void reserve_to(size_t capacity) {
            require_type<T>();
            reserve_to(capacity);
        }

        /**
         * Enter a batch mutation scope.
         *
         * The outermost begin flushes removals left pending by the previous
         * batch, so current-batch changes can still inspect their own removed
         * slots until the batch is closed.
         */
        void begin_mutation() noexcept {
            if (m_mutation_depth++ == 0) { erase_pending(); }
        }

        /**
         * Leave a batch mutation scope.
         *
         * Pending removals are intentionally retained here and are only
         * erased at the next outermost `begin_mutation()` or by an explicit
         * `erase_pending()` call.
         */
        void end_mutation() {
            if (m_mutation_depth == 0) { throw std::logic_error("KeySlotStore mutation depth underflow"); }

            --m_mutation_depth;
        }

        template <typename T> [[nodiscard]] size_t find_stored_slot(const T &key) const {
            require_type<T>();
            return find_stored_slot(static_cast<const void *>(std::addressof(key)));
        }

        template <typename T> [[nodiscard]] size_t find_slot(const T &key) const {
            require_type<T>();
            return find_slot(static_cast<const void *>(std::addressof(key)));
        }

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

            if (const size_t existing = find_stored_slot(key); existing != npos) {
                if (slot_live(existing)) { return {.slot = existing, .inserted = false}; }

                live.set(existing);
                --m_pending_erase_count;
                ++m_size;
                observers.notify_insert(existing);
                return {.slot = existing, .inserted = true};
            }

            if (m_free_slots.empty() && has_pending_erase() && m_mutation_depth == 0) { erase_pending(); }
            if (m_free_slots.empty()) { reserve_to(std::max<size_t>(m_size + 1, std::max<size_t>(8, slot_capacity() * 2))); }

            const size_t slot = m_free_slots.back();
            m_free_slots.pop_back();

            auto rollback_slot = ::hgraph::make_scope_exit([&]() noexcept { m_free_slots.push_back(slot); });
            m_key_plan->copy_construct(key_storage.slot_data(slot), key);
            rollback_slot.release();

            constructed.set(slot);
            live.set(slot);
            ++m_size;
            m_index->insert(slot);
            observers.notify_insert(slot);
            return {.slot = slot, .inserted = true};
        }

        template <typename T> [[nodiscard]] InsertResult insert(const T &key) {
            require_type<T>();
            return insert(static_cast<const void *>(std::addressof(key)));
        }

        [[nodiscard]] bool remove(const void *key) {
            const size_t slot = find_slot(key);
            return slot != npos && remove_slot(slot);
        }

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
            if (!slot_live(slot)) { return false; }

            observers.notify_remove(slot);
            live.reset(slot);
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

            for (size_t slot = 0; slot < slot_capacity(); ++slot) {
                if (!slot_pending_erase(slot)) { continue; }

                observers.notify_erase(slot);
                m_index->erase(slot);
                m_key_plan->destroy(key_storage.slot_data(slot));
                constructed.reset(slot);
                m_free_slots.push_back(slot);
            }

            m_pending_erase_count = 0;
        }

        void clear() {
            observers.notify_clear();
            hard_clear();
        }

        void add_slot_observer(SlotObserver *observer) { observers.add(observer); }

        void remove_slot_observer(SlotObserver *observer) { observers.remove(observer); }

      private:
        struct IndexHash
        {
            using is_transparent = void;
            using is_avalanching = void;

            const KeySlotStore *store{nullptr};

            [[nodiscard]] size_t operator()(size_t slot) const { return store != nullptr ? store->hash_at_slot(slot) : 0U; }

            [[nodiscard]] size_t operator()(const void *key) const { return store != nullptr ? store->m_ops.hash_key(key) : 0U; }
        };

        struct IndexEqual
        {
            using is_transparent = void;

            const KeySlotStore *store{nullptr};

            [[nodiscard]] bool operator()(size_t lhs, size_t rhs) const {
                if (lhs == rhs) { return true; }
                return store != nullptr && store->m_ops.equal_keys(store->key_memory(lhs), store->key_memory(rhs));
            }

            [[nodiscard]] bool operator()(size_t slot, const void *key) const {
                return store != nullptr && store->m_ops.equal_keys(store->key_memory(slot), key);
            }

            [[nodiscard]] bool operator()(const void *key, size_t slot) const { return (*this)(slot, key); }
        };

        using IndexSet = ankerl::unordered_dense::set<size_t, IndexHash, IndexEqual>;

        [[nodiscard]] size_t hash_at_slot(size_t slot) const { return m_ops.hash_key(key_memory(slot)); }

        [[nodiscard]] const MemoryUtils::StoragePlan &require_bound_plan() const {
            if (m_key_plan == nullptr) { throw std::logic_error("KeySlotStore requires a bound storage plan"); }
            return *m_key_plan;
        }

        void require_constructed_slot(size_t slot) const {
            if (slot >= constructed.size()) { throw std::out_of_range("KeySlotStore slot out of range"); }
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

        [[nodiscard]] std::unique_ptr<IndexSet> make_index() const {
            return std::make_unique<IndexSet>(0, IndexHash{.store = this}, IndexEqual{.store = this});
        }

        void rebuild_index() {
            m_index = make_index();
            for (size_t slot = 0; slot < slot_capacity(); ++slot) {
                if (slot_constructed(slot)) { m_index->insert(slot); }
            }
        }

        void hard_clear() noexcept {
            if (m_key_plan != nullptr) {
                for (size_t slot = 0; slot < slot_capacity(); ++slot) {
                    if (slot_constructed(slot)) { m_key_plan->destroy(key_storage.slot_data(slot)); }
                }
            }

            if (m_index != nullptr) { m_index->clear(); }

            m_size                = 0;
            m_pending_erase_count = 0;
            m_mutation_depth      = 0;
            constructed.reset();
            live.reset();
            m_free_slots.clear();
            for (size_t slot = slot_capacity(); slot > 0; --slot) { m_free_slots.push_back(slot - 1); }
        }

        size_t                          m_size{0};
        size_t                          m_pending_erase_count{0};
        size_t                          m_mutation_depth{0};
        const MemoryUtils::StoragePlan *m_key_plan{nullptr};
        KeySlotStoreOps                 m_ops{};
        std::vector<size_t>             m_free_slots{};
        std::unique_ptr<IndexSet>       m_index{};
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_V2_KEY_SLOT_STORE_H
