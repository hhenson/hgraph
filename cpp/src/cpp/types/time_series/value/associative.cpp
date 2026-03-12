#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/associative.h>
#include <hgraph/types/time_series/value/state.h>

#include <ankerl/unordered_dense.h>
#include <sul/dynamic_bitset.hpp>

#include <algorithm>
#include <concepts>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace hgraph
{

    namespace detail
    {

        struct SetKeyIndexHash
        {
            using is_transparent = void;
            using is_avalanching = void;

            const struct SetStorageBase *state{nullptr};
            const ViewDispatch    *dispatch{nullptr};
            size_t                 stride{0};

            [[nodiscard]] uint64_t operator()(size_t slot) const;
            [[nodiscard]] uint64_t operator()(const void *key) const;
        };

        struct SetKeyIndexEqual
        {
            using is_transparent = void;

            const struct SetStorageBase *state{nullptr};
            const ViewDispatch    *dispatch{nullptr};
            size_t                 stride{0};

            [[nodiscard]] bool operator()(size_t lhs, size_t rhs) const;
            [[nodiscard]] bool operator()(size_t slot, const void *key) const;
            [[nodiscard]] bool operator()(const void *key, size_t slot) const;
        };

        struct SetStorageBase
        {
            using IndexSet = ankerl::unordered_dense::set<size_t, SetKeyIndexHash, SetKeyIndexEqual>;
            /**
             * Key-to-slot lookup over occupied slots.
             *
             * The index is kept over occupied slots, not only live slots, so a
             * same-epoch remove/add of the same key can revive the retained
             * payload in place without a second lookup structure.
             */
            std::unique_ptr<IndexSet> index{};
            /**
             * Live element count.
             *
             * The container keeps stable slots, so `size` counts only live
             * entries, not occupied or removed slots.
             */
            size_t size{0};
            /**
             * Number of allocated slots.
             */
            size_t capacity{0};
            /**
             * Occupied-slot bitset used to track slots with constructed payload.
             *
             * Removed elements stay occupied until clear or slot reuse so the
             * associative layer can preserve removed payloads internally.
             */
            sul::dynamic_bitset<> occupied{};
            /**
             * Slots available for reuse.
             */
            std::vector<size_t> free_list{};
            /**
             * Live-slot bitset used for membership and public iteration.
             *
             * This sits directly above the element storage because those two
             * structures are read together whenever live entries are scanned.
             */
            sul::dynamic_bitset<> alive{};
            /**
             * Contiguous raw element storage indexed by slot.
             */
            std::byte *elements{nullptr};
        };

        struct PlainSetState : SetStorageBase
        {
        };

        struct DeltaSetState : SetStorageBase
        {
            /**
             * Slots introduced in the current mutation epoch.
             *
             * This records the net added delta only. If a key is added and
             * then removed in the same epoch, the slot is released immediately
             * and is no longer marked as either added or removed.
             */
            sul::dynamic_bitset<> added{};
            /**
             * Recently removed slots.
             *
             * Removed payloads remain available by slot until the next
             * outermost mutation scope begins. This bitset also carries the
             * net removed-delta surface exposed by the public views.
             */
            sul::dynamic_bitset<> removed{};
            /**
             * Active mutation-scope depth for this container.
             *
             * Nested mutation scopes are allowed so helper functions can open a
             * local scope while participating in a larger mutation operation.
             * Removed payloads are released only when the outermost scope
             * begins.
             */
            size_t mutation_depth{0};
        };

        struct PlainMapState
        {
            PlainSetState keys{};
            std::byte    *values{nullptr};
        };

        struct DeltaMapState
        {
            DeltaSetState keys{};
            std::byte    *values{nullptr};
            /**
             * Slots whose values were updated in the current mutation epoch
             * without the key being newly added in that same epoch.
             */
            sul::dynamic_bitset<> updated{};
            /**
             * Active mutation-scope depth for this container.
             *
             * Nested mutation scopes are allowed so helper functions can open a
             * local scope while participating in a larger mutation operation.
             * Removed key/value payloads are released only when the outermost
             * scope begins.
             */
            size_t mutation_depth{0};
        };

        inline uint64_t SetKeyIndexHash::operator()(size_t slot) const
        {
            return dispatch != nullptr ? dispatch->hash(state->elements + slot * stride) : 0U;
        }

        inline uint64_t SetKeyIndexHash::operator()(const void *key) const
        {
            return dispatch != nullptr ? dispatch->hash(key) : 0U;
        }

        inline bool SetKeyIndexEqual::operator()(size_t lhs, size_t rhs) const
        {
            if (lhs == rhs) { return true; }
            return dispatch != nullptr &&
                   std::is_eq(dispatch->compare(state->elements + lhs * stride, state->elements + rhs * stride));
        }

        inline bool SetKeyIndexEqual::operator()(size_t slot, const void *key) const
        {
            return dispatch != nullptr && std::is_eq(dispatch->compare(state->elements + slot * stride, key));
        }

        inline bool SetKeyIndexEqual::operator()(const void *key, size_t slot) const
        {
            return (*this)(slot, key);
        }

        /**
         * Common stable-slot key management for sets and maps.
         *
         * This helper keeps the keyed-container logic in one place so set and
         * map do not silently diverge in lookup, slot lifecycle, or remove
         * semantics. The state still stores only data; schema-derived behavior
         * remains in the dispatch/builder side.
         */
        struct KeySlotStorage
        {
            struct InsertResult
            {
                size_t slot{npos};
                bool   inserted{false};
            };

            KeySlotStorage(const ViewDispatch &dispatch, const ValueBuilder &builder) noexcept
                : m_dispatch(dispatch),
                  m_builder(builder),
                  m_stride(stride_for(builder)),
                  m_requires_destroy(builder.requires_destroy())
            {
            }

            [[nodiscard]] const ViewDispatch &dispatch() const noexcept { return m_dispatch.get(); }
            [[nodiscard]] const ValueBuilder &builder() const noexcept { return m_builder.get(); }
            [[nodiscard]] size_t stride() const noexcept { return m_stride; }

            template <typename TState> void initialise(TState &state) const
            {
                state.index = std::make_unique<SetStorageBase::IndexSet>(
                    0,
                    SetKeyIndexHash{.state = &state, .dispatch = &dispatch(), .stride = stride()},
                    SetKeyIndexEqual{.state = &state, .dispatch = &dispatch(), .stride = stride()});
            }

            template <typename TState> void rebind_index(TState &state) const
            {
                initialise(state);
                for (size_t slot = 0; slot < state.capacity; ++slot) {
                    if (state.occupied.test(slot)) { state.index->insert(slot); }
                }
            }

            template <typename TState> [[nodiscard]] void *slot_data(TState &state, size_t slot) const noexcept
            {
                return state.elements + slot * stride();
            }

            template <typename TState> [[nodiscard]] const void *slot_data(const TState &state, size_t slot) const noexcept
            {
                return state.elements + slot * stride();
            }

            template <typename TState> [[nodiscard]] size_t live_slot_at(const TState &state, size_t live_index) const
            {
                size_t seen = 0;
                for (size_t slot = 0; slot < state.capacity; ++slot) {
                    if (!state.alive.test(slot)) { continue; }
                    if (seen == live_index) { return slot; }
                    ++seen;
                }
                throw std::out_of_range("Associative slot index out of range");
            }

            template <typename TState> [[nodiscard]] size_t find_slot(const TState &state, const void *key) const
            {
                if (state.index == nullptr) { return npos; }
                const auto it = state.index->find(key);
                return it == state.index->end() ? npos : *it;
            }

            template <typename TState> [[nodiscard]] bool contains(const TState &state, const void *key) const
            {
                return find_live_slot(state, key) != npos;
            }

            template <typename TState> void reserve(TState &state, size_t min_capacity) const
            {
                if (min_capacity <= state.capacity) { return; }

                const size_t new_capacity = std::max<size_t>(min_capacity, std::max<size_t>(8, state.capacity * 2));
                std::byte *new_elements = static_cast<std::byte *>(
                    ::operator new(new_capacity * stride(), std::align_val_t{builder().alignment()}));

                std::vector<size_t> moved_slots;
                moved_slots.reserve(state.capacity);
                try {
                    for (size_t slot = 0; slot < state.capacity; ++slot) {
                        if (!state.occupied.test(slot)) { continue; }
                        builder().move_construct(new_elements + slot * stride(), state.elements + slot * stride(), builder());
                        moved_slots.push_back(slot);
                    }
                } catch (...) {
                    for (const size_t slot : moved_slots) {
                        destroy_payload(new_elements + slot * stride());
                    }
                    ::operator delete(new_elements, std::align_val_t{builder().alignment()});
                    throw;
                }

                destroy_occupied_payloads(state);
                if (state.elements != nullptr) {
                    ::operator delete(state.elements, std::align_val_t{builder().alignment()});
                }

                const size_t old_capacity = state.capacity;
                state.elements = new_elements;
                state.capacity = new_capacity;
                state.alive.resize(new_capacity);
                state.occupied.resize(new_capacity);
                if constexpr (std::is_same_v<TState, DeltaSetState>) {
                    state.added.resize(new_capacity);
                    state.removed.resize(new_capacity);
                }
                for (size_t slot = new_capacity; slot > old_capacity; --slot) {
                    state.free_list.push_back(slot - 1);
                }
            }

            template <typename TState> [[nodiscard]] size_t find_live_slot(const TState &state, const void *key) const
            {
                const size_t slot = find_slot(state, key);
                return slot != npos && state.alive.test(slot) ? slot : npos;
            }

            [[nodiscard]] InsertResult insert(PlainSetState &state, const void *key) const
            {
                if (const size_t existing = find_slot(state, key); existing != npos) {
                    return state.alive.test(existing) ? InsertResult{.slot = existing, .inserted = false}
                                                      : InsertResult{.slot = existing, .inserted = false};
                }

                if (state.free_list.empty()) { reserve(state, state.size + 1); }
                const size_t slot = state.free_list.back();
                state.free_list.pop_back();

                void *dst = slot_data(state, slot);
                if (state.occupied.test(slot)) {
                    dispatch().assign(dst, key);
                } else {
                    builder().construct(dst);
                    dispatch().assign(dst, key);
                    state.occupied.set(slot);
                }

                state.alive.set(slot);
                ++state.size;
                state.index->insert(slot);
                return {.slot = slot, .inserted = true};
            }

            [[nodiscard]] InsertResult insert(DeltaSetState &state, const void *key) const
            {
                if (const size_t existing = find_slot(state, key); existing != npos) {
                    if (state.alive.test(existing)) { return {.slot = existing, .inserted = false}; }
                    state.alive.set(existing);
                    state.removed.reset(existing);
                    ++state.size;
                    return {.slot = existing, .inserted = true};
                }

                if (state.free_list.empty()) { reserve(state, state.size + 1); }
                const size_t slot = state.free_list.back();
                state.free_list.pop_back();

                void *dst = slot_data(state, slot);
                if (state.occupied.test(slot)) {
                    dispatch().assign(dst, key);
                } else {
                    builder().construct(dst);
                    dispatch().assign(dst, key);
                    state.occupied.set(slot);
                }

                state.alive.set(slot);
                state.added.set(slot);
                state.removed.reset(slot);
                ++state.size;
                state.index->insert(slot);
                return {.slot = slot, .inserted = true};
            }

            template <typename TState> [[nodiscard]] bool remove(TState &state, const void *key) const
            {
                const size_t slot = find_live_slot(state, key);
                if (slot == npos) { return false; }
                remove_slot(state, slot);
                return true;
            }

            void remove_slot(PlainSetState &state, size_t slot) const
            {
                if (slot >= state.capacity || !state.alive.test(slot)) {
                    throw std::out_of_range("Associative remove_slot on non-live slot");
                }

                state.alive.reset(slot);
                state.index->erase(slot);
                destroy_payload(slot_data(state, slot));
                state.occupied.reset(slot);
                state.free_list.push_back(slot);
                --state.size;
            }

            void remove_slot(DeltaSetState &state, size_t slot) const
            {
                if (slot >= state.capacity || !state.alive.test(slot)) {
                    throw std::out_of_range("Associative remove_slot on non-live slot");
                }

                if (state.added.test(slot)) {
                    destroy_payload(slot_data(state, slot));
                    state.alive.reset(slot);
                    state.occupied.reset(slot);
                    state.added.reset(slot);
                    state.removed.reset(slot);
                    state.index->erase(slot);
                    state.free_list.push_back(slot);
                    --state.size;
                    return;
                }

                state.alive.reset(slot);
                state.removed.set(slot);
                --state.size;
            }

            void release_removed(DeltaSetState &state) const noexcept
            {
                for (size_t slot = 0; slot < state.capacity; ++slot) {
                    if (!state.removed.test(slot)) { continue; }
                    state.index->erase(slot);
                    destroy_payload(slot_data(state, slot));
                    state.occupied.reset(slot);
                    state.removed.reset(slot);
                    state.free_list.push_back(slot);
                }
                state.added.reset();
            }

            template <typename TState> void clear(TState &state) const noexcept
            {
                destroy_occupied_payloads(state);
                if (state.index != nullptr) { state.index->clear(); }
                state.size = 0;
                state.alive.reset();
                state.occupied.reset();
                if constexpr (std::is_same_v<TState, DeltaSetState>) {
                    state.added.reset();
                    state.removed.reset();
                }
                state.free_list.clear();
                for (size_t slot = state.capacity; slot > 0; --slot) {
                    state.free_list.push_back(slot - 1);
                }
            }

            template <typename TState> void destroy_occupied_payloads(TState &state) const noexcept
            {
                if (!m_requires_destroy) { return; }
                for (size_t slot = 0; slot < state.capacity; ++slot) {
                    if (state.occupied.test(slot)) {
                        builder().destroy(slot_data(state, slot));
                    }
                }
            }

            void destroy_payload(void *slot) const noexcept
            {
                if (m_requires_destroy) { builder().destroy(slot); }
            }

          private:
            [[nodiscard]] static size_t stride_for(const ValueBuilder &builder) noexcept
            {
                const size_t alignment = builder.alignment();
                const size_t size = builder.size();
                return ((size + alignment - 1) / alignment) * alignment;
            }

            static constexpr size_t npos = static_cast<size_t>(-1);

            std::reference_wrapper<const ViewDispatch> m_dispatch;
            std::reference_wrapper<const ValueBuilder> m_builder;
            size_t                                     m_stride;
            bool                                       m_requires_destroy;
        };

        template <MutationTracking TTracking> struct SetDispatch final : SetViewDispatch
        {
            static constexpr MutationTracking tracking_mode = TTracking;
            static constexpr bool tracks_deltas_v = TTracking == MutationTracking::Delta;
            using SetState = std::conditional_t<tracks_deltas_v, DeltaSetState, PlainSetState>;

            explicit SetDispatch(const value::TypeMeta &schema)
                : m_schema(schema),
                  m_element_builder(ValueBuilderFactory::checked_builder_for(schema.element_type, TTracking)),
                  m_keys(m_element_builder.get().dispatch(), m_element_builder.get())
            {
                if (schema.element_type == nullptr) {
                    throw std::runtime_error("Set schema requires an element schema");
                }
            }

            [[nodiscard]] bool tracks_deltas() const noexcept { return tracks_deltas_v; }

            [[nodiscard]] size_t size(const void *data) const noexcept override { return state(data)->size; }
            [[nodiscard]] size_t slot_capacity(const void *data) const noexcept override { return state(data)->capacity; }
            [[nodiscard]] const value::TypeMeta &element_schema() const noexcept override { return *m_schema.get().element_type; }
            [[nodiscard]] const ViewDispatch &element_dispatch() const noexcept override { return m_keys.dispatch(); }

            void begin_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    SetState *set = state(data);
                    if (set->mutation_depth++ == 0) { m_keys.release_removed(*set); }
                }
            }

            void end_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    SetState *set = state(data);
                    if (set->mutation_depth == 0) {
                        throw std::runtime_error("Set mutation depth underflow");
                    }
                    --set->mutation_depth;
                }
            }

            [[nodiscard]] void *element_data(void *data, size_t index) const override
            {
                auto *set = state(data);
                return m_keys.slot_data(*set, m_keys.live_slot_at(*set, index));
            }

            [[nodiscard]] const void *element_data(const void *data, size_t index) const override
            {
                const auto *set = state(data);
                return m_keys.slot_data(*set, m_keys.live_slot_at(*set, index));
            }

            [[nodiscard]] bool slot_occupied(const void *data, size_t slot) const noexcept override
            {
                const auto *set = state(data);
                return slot < set->capacity && set->occupied.test(slot);
            }

            [[nodiscard]] bool slot_added(const void *data, size_t slot) const noexcept override
            {
                if constexpr (tracks_deltas_v) {
                    return slot < state(data)->capacity && state(data)->added.test(slot);
                } else {
                    static_cast<void>(data);
                    static_cast<void>(slot);
                    return false;
                }
            }

            [[nodiscard]] bool slot_removed(const void *data, size_t slot) const noexcept override
            {
                if constexpr (tracks_deltas_v) {
                    return slot < state(data)->capacity && state(data)->removed.test(slot);
                } else {
                    static_cast<void>(data);
                    static_cast<void>(slot);
                    return false;
                }
            }

            [[nodiscard]] void *slot_data(void *data, size_t slot) const override
            {
                auto *set = state(data);
                if (slot >= set->capacity || !set->occupied.test(slot)) {
                    throw std::out_of_range("Set slot out of range");
                }
                return m_keys.slot_data(*set, slot);
            }

            [[nodiscard]] const void *slot_data(const void *data, size_t slot) const override
            {
                const auto *set = state(data);
                if (slot >= set->capacity || !set->occupied.test(slot)) {
                    throw std::out_of_range("Set slot out of range");
                }
                return m_keys.slot_data(*set, slot);
            }

            [[nodiscard]] bool contains(const void *data, const void *element) const override
            {
                return m_keys.contains(*state(data), element);
            }

            [[nodiscard]] bool add(void *data, const void *element) const override
            {
                return m_keys.insert(*state(data), element).inserted;
            }

            [[nodiscard]] bool remove(void *data, const void *element) const override
            {
                return m_keys.remove(*state(data), element);
            }

            void clear(void *data) const override
            {
                while (state(data)->size > 0) {
                    const size_t slot = m_keys.live_slot_at(*state(data), 0);
                    m_keys.remove_slot(*state(data), slot);
                }
            }

            [[nodiscard]] size_t hash(const void *data) const override
            {
                const auto *set = state(data);
                size_t result = 0;
                for (size_t slot = 0; slot < set->capacity; ++slot) {
                    if (!set->alive.test(slot)) { continue; }
                    result ^= element_dispatch().hash(m_keys.slot_data(*set, slot));
                }
                return result;
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                const auto *set = state(data);
                std::string result = "{";
                bool first = true;
                for (size_t slot = 0; slot < set->capacity; ++slot) {
                    if (!set->alive.test(slot)) { continue; }
                    if (!first) { result += ", "; }
                    first = false;
                    result += element_dispatch().to_string(m_keys.slot_data(*set, slot));
                }
                result += "}";
                return result;
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                const auto *a = state(lhs);
                const auto *b = state(rhs);
                if (a->size != b->size) { return std::partial_ordering::unordered; }
                for (size_t slot = 0; slot < a->capacity; ++slot) {
                    if (!a->alive.test(slot)) { continue; }
                    if (!m_keys.contains(*b, m_keys.slot_data(*a, slot))) {
                        return std::partial_ordering::unordered;
                    }
                }
                return std::partial_ordering::equivalent;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                nb::set result;
                const auto *set = state(data);
                for (size_t slot = 0; slot < set->capacity; ++slot) {
                    if (!set->alive.test(slot)) { continue; }
                    result.add(element_dispatch().to_python(m_keys.slot_data(*set, slot), &element_schema()));
                }
                return nb::frozenset(result);
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                if (!nb::isinstance<nb::set>(src) && !nb::isinstance<nb::frozenset>(src) &&
                    !nb::isinstance<nb::list>(src) && !nb::isinstance<nb::tuple>(src)) {
                    throw std::runtime_error("Set value expects a set, frozenset, list, or tuple");
                }

                hard_clear(dst);
                nb::iterator it = nb::iter(src);
                while (it != nb::iterator::sentinel()) {
                    nb::handle item = *it;
                    if (item.is_none()) {
                        throw std::runtime_error("Set value does not allow None elements");
                    }

                    void *temp = m_element_builder.get().allocate();
                    try {
                        m_element_builder.get().construct(temp);
                        element_dispatch().from_python(temp, nb::borrow<nb::object>(item), &element_schema());
                        static_cast<void>(add(dst, temp));
                    } catch (...) {
                        if (m_element_builder.get().requires_destroy()) {
                            m_element_builder.get().destroy(temp);
                        }
                        m_element_builder.get().deallocate(temp);
                        throw;
                    }

                    if (m_element_builder.get().requires_destroy()) {
                        m_element_builder.get().destroy(temp);
                    }
                    m_element_builder.get().deallocate(temp);
                    ++it;
                }
            }

            void assign(void *dst, const void *src) const override
            {
                hard_clear(dst);
                const auto *set = state(src);
                for (size_t slot = 0; slot < set->capacity; ++slot) {
                    if (!set->alive.test(slot)) { continue; }
                    static_cast<void>(add(dst, m_keys.slot_data(*set, slot)));
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Set value set_from_cpp is not implemented");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Set value move_from_cpp is not implemented");
            }

            void construct(void *memory) const
            {
                std::construct_at(state(memory));
                m_keys.initialise(*state(memory));
            }

            void destroy(void *memory) const noexcept
            {
                auto *set = state(memory);
                hard_clear(*set);
                if (set->elements != nullptr) {
                    ::operator delete(set->elements, std::align_val_t{m_element_builder.get().alignment()});
                }
                std::destroy_at(state(memory));
            }

            void copy_construct(void *dst, const void *src) const
            {
                construct(dst);
                assign(dst, src);
            }

            void move_construct(void *dst, void *src) const
            {
                std::construct_at(state(dst), std::move(*state(src)));
                m_keys.rebind_index(*state(dst));
                state(src)->elements = nullptr;
                state(src)->size = 0;
                state(src)->capacity = 0;
                state(src)->alive.clear();
                state(src)->occupied.clear();
                state(src)->free_list.clear();
                state(src)->index.reset();
                if constexpr (tracks_deltas_v) {
                    state(src)->added.clear();
                    state(src)->removed.clear();
                    state(src)->mutation_depth = 0;
                }
            }

          private:
            void hard_clear(void *memory) const noexcept
            {
                hard_clear(*state(memory));
            }

            template <typename TState> void hard_clear(TState &state) const noexcept
            {
                m_keys.clear(state);
            }

            [[nodiscard]] SetState *state(void *memory) const noexcept
            {
                return std::launder(reinterpret_cast<SetState *>(memory));
            }

            [[nodiscard]] const SetState *state(const void *memory) const noexcept
            {
                return std::launder(reinterpret_cast<const SetState *>(memory));
            }

            std::reference_wrapper<const value::TypeMeta> m_schema;
            std::reference_wrapper<const ValueBuilder>    m_element_builder;
            KeySlotStorage                                m_keys;
        };

        template <MutationTracking TTracking> struct MapDispatch final : MapViewDispatch
        {
            static constexpr MutationTracking tracking_mode = TTracking;
            static constexpr bool tracks_deltas_v = TTracking == MutationTracking::Delta;
            using MapState = std::conditional_t<tracks_deltas_v, DeltaMapState, PlainMapState>;
            using KeyState = std::conditional_t<tracks_deltas_v, DeltaSetState, PlainSetState>;

            explicit MapDispatch(const value::TypeMeta &schema)
                : m_schema(schema),
                  m_key_builder(ValueBuilderFactory::checked_builder_for(schema.key_type, TTracking)),
                  m_value_builder(ValueBuilderFactory::checked_builder_for(schema.element_type, TTracking)),
                  m_keys(m_key_builder.get().dispatch(), m_key_builder.get()),
                  m_value_stride(stride_for(m_value_builder.get())),
                  m_value_requires_destroy(m_value_builder.get().requires_destroy())
            {
                if (schema.key_type == nullptr || schema.element_type == nullptr) {
                    throw std::runtime_error("Map schema requires key and value schemas");
                }
            }

            [[nodiscard]] bool tracks_deltas() const noexcept { return tracks_deltas_v; }

            [[nodiscard]] size_t size(const void *data) const noexcept override { return keys(data).size; }
            [[nodiscard]] size_t slot_capacity(const void *data) const noexcept override { return keys(data).capacity; }
            [[nodiscard]] const value::TypeMeta &key_schema() const noexcept override { return *m_schema.get().key_type; }
            [[nodiscard]] const value::TypeMeta &value_schema() const noexcept override { return *m_schema.get().element_type; }
            [[nodiscard]] const ViewDispatch &key_dispatch() const noexcept override { return m_keys.dispatch(); }
            [[nodiscard]] const ViewDispatch &value_dispatch() const noexcept override { return m_value_builder.get().dispatch(); }

            void begin_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    MapState *map = state(data);
                    if (map->mutation_depth++ == 0) { release_removed(*map); }
                }
            }

            void end_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    MapState *map = state(data);
                    if (map->mutation_depth == 0) {
                        throw std::runtime_error("Map mutation depth underflow");
                    }
                    --map->mutation_depth;
                }
            }

            [[nodiscard]] size_t find(const void *data, const void *key) const override
            {
                return m_keys.find_live_slot(keys(data), key);
            }

            [[nodiscard]] bool slot_occupied(const void *data, size_t slot) const noexcept override
            {
                const auto &key_state = keys(data);
                return slot < key_state.capacity && key_state.occupied.test(slot);
            }

            [[nodiscard]] bool slot_added(const void *data, size_t slot) const noexcept override
            {
                if constexpr (tracks_deltas_v) {
                    return slot < state(data)->keys.capacity && state(data)->keys.added.test(slot);
                } else {
                    static_cast<void>(data);
                    static_cast<void>(slot);
                    return false;
                }
            }

            [[nodiscard]] bool slot_removed(const void *data, size_t slot) const noexcept override
            {
                if constexpr (tracks_deltas_v) {
                    return slot < state(data)->keys.capacity && state(data)->keys.removed.test(slot);
                } else {
                    static_cast<void>(data);
                    static_cast<void>(slot);
                    return false;
                }
            }

            [[nodiscard]] bool slot_updated(const void *data, size_t slot) const noexcept override
            {
                if constexpr (tracks_deltas_v) {
                    return slot < state(data)->keys.capacity && state(data)->updated.test(slot);
                } else {
                    static_cast<void>(data);
                    static_cast<void>(slot);
                    return false;
                }
            }

            [[nodiscard]] void *key_data(void *data, size_t index) const override
            {
                auto &key_state = keys(data);
                if (index >= key_state.capacity || !key_state.occupied.test(index)) {
                    throw std::out_of_range("Map key index out of range");
                }
                return m_keys.slot_data(key_state, index);
            }

            [[nodiscard]] const void *key_data(const void *data, size_t index) const override
            {
                const auto &key_state = keys(data);
                if (index >= key_state.capacity || !key_state.occupied.test(index)) {
                    throw std::out_of_range("Map key index out of range");
                }
                return m_keys.slot_data(key_state, index);
            }

            [[nodiscard]] void *value_data(void *data, size_t index) const override
            {
                const auto &key_state = keys(data);
                if (index >= key_state.capacity || !key_state.occupied.test(index)) {
                    throw std::out_of_range("Map value index out of range");
                }
                return values_memory(data) + index * m_value_stride;
            }

            [[nodiscard]] const void *value_data(const void *data, size_t index) const override
            {
                const auto &key_state = keys(data);
                if (index >= key_state.capacity || !key_state.occupied.test(index)) {
                    throw std::out_of_range("Map value index out of range");
                }
                return values_memory(data) + index * m_value_stride;
            }

            [[nodiscard]] bool set_item(void *data, const void *key, const void *value) const override
            {
                if (value == nullptr) {
                    throw std::invalid_argument("Map value requires a live value for every present key");
                }
                auto &key_state = keys(data);
                if (const size_t slot = m_keys.find_slot(key_state, key); slot != npos) {
                    const bool was_live = key_state.alive.test(slot);
                    if (!was_live) {
                        key_state.alive.set(slot);
                        if constexpr (tracks_deltas_v) { state(data)->keys.removed.reset(slot); }
                        ++key_state.size;
                        if constexpr (tracks_deltas_v) { state(data)->updated.set(slot); }
                    } else if constexpr (tracks_deltas_v) {
                        if (!state(data)->keys.added.test(slot)) { state(data)->updated.set(slot); }
                    }
                    key_dispatch().assign(key_data(data, slot), key);
                    value_dispatch().assign(value_data(data, slot), value);
                    return !was_live;
                }

                reserve(*state(data), key_state.size + 1);
                const auto insertion = m_keys.insert(state(data)->keys, key);
                const size_t slot = insertion.slot;
                prepare_value_slot(*state(data), slot);
                value_dispatch().assign(value_data(data, slot), value);
                return insertion.inserted;
            }

            [[nodiscard]] bool remove(void *data, const void *key) const override
            {
                const size_t slot = find(data, key);
                if (slot == npos) { return false; }
                if constexpr (tracks_deltas_v) {
                    if (state(data)->keys.added.test(slot)) {
                        destroy_value(values_memory(data) + slot * m_value_stride);
                    }
                    state(data)->updated.reset(slot);
                } else {
                    destroy_value(values_memory(data) + slot * m_value_stride);
                }
                m_keys.remove_slot(state(data)->keys, slot);
                return true;
            }

            void clear(void *data) const override
            {
                while (keys(data).size > 0) {
                    const size_t slot = m_keys.live_slot_at(keys(data), 0);
                    if constexpr (tracks_deltas_v) {
                        if (state(data)->keys.added.test(slot)) {
                            destroy_value(values_memory(data) + slot * m_value_stride);
                        }
                        state(data)->updated.reset(slot);
                    }
                    m_keys.remove_slot(state(data)->keys, slot);
                }
            }

            [[nodiscard]] size_t hash(const void *data) const override
            {
                size_t result = 0;
                for (size_t slot = 0; slot < keys(data).capacity; ++slot) {
                    if (!keys(data).alive.test(slot)) { continue; }
                    size_t pair_hash = key_dispatch().hash(key_data(data, slot));
                    pair_hash ^= value_dispatch().hash(value_data(data, slot)) << 1;
                    result ^= pair_hash;
                }
                return result;
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                std::string result = "{";
                bool first = true;
                for (size_t slot = 0; slot < keys(data).capacity; ++slot) {
                    if (!keys(data).alive.test(slot)) { continue; }
                    if (!first) { result += ", "; }
                    first = false;
                    result += key_dispatch().to_string(key_data(data, slot));
                    result += ": ";
                    result += value_dispatch().to_string(value_data(data, slot));
                }
                result += "}";
                return result;
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                if (keys(lhs).size != keys(rhs).size) { return std::partial_ordering::unordered; }

                for (size_t slot = 0; slot < keys(lhs).capacity; ++slot) {
                    if (!keys(lhs).alive.test(slot)) { continue; }
                    const size_t rhs_slot = find(rhs, key_data(lhs, slot));
                    if (rhs_slot == npos) { return std::partial_ordering::unordered; }
                    if (value_dispatch().compare(value_data(lhs, slot), value_data(rhs, rhs_slot)) !=
                        std::partial_ordering::equivalent) {
                        return std::partial_ordering::unordered;
                    }
                }
                return std::partial_ordering::equivalent;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                nb::dict result;
                for (size_t slot = 0; slot < keys(data).capacity; ++slot) {
                    if (!keys(data).alive.test(slot)) { continue; }
                    nb::object py_key = key_dispatch().to_python(key_data(data, slot), &key_schema());
                    nb::object py_value = value_dispatch().to_python(value_data(data, slot), &value_schema());
                    result[py_key] = py_value;
                }
                return result;
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                if (!nb::isinstance<nb::dict>(src) && !nb::hasattr(src, "items")) {
                    throw std::runtime_error("Map value expects a dict or dict-like object");
                }

                hard_clear(dst);
                const nb::object items = nb::hasattr(src, "items") ? src.attr("items")() : src;
                nb::iterator it = nb::iter(items);
                while (it != nb::iterator::sentinel()) {
                    const nb::tuple pair = nb::cast<nb::tuple>(*it);
                    if (pair.size() != 2) { throw std::runtime_error("Map items() must yield key/value pairs"); }
                    if (pair[0].is_none()) { throw std::runtime_error("Map value does not allow None keys"); }
                    if (pair[1].is_none()) { throw std::runtime_error("Map value does not allow None values"); }

                    void *temp_key = m_key_builder.get().allocate();
                    void *temp_value = nullptr;
                    try {
                        m_key_builder.get().construct(temp_key);
                        key_dispatch().from_python(temp_key, nb::borrow<nb::object>(pair[0]), &key_schema());

                        temp_value = m_value_builder.get().allocate();
                        m_value_builder.get().construct(temp_value);
                        value_dispatch().from_python(temp_value, nb::borrow<nb::object>(pair[1]), &value_schema());
                        static_cast<void>(set_item(dst, temp_key, temp_value));
                    } catch (...) {
                        if (temp_value != nullptr) {
                            if (m_value_builder.get().requires_destroy()) {
                                m_value_builder.get().destroy(temp_value);
                            }
                            m_value_builder.get().deallocate(temp_value);
                        }
                        if (temp_key != nullptr) {
                            if (m_key_builder.get().requires_destroy()) {
                                m_key_builder.get().destroy(temp_key);
                            }
                            m_key_builder.get().deallocate(temp_key);
                        }
                        throw;
                    }

                    if (temp_value != nullptr) {
                        if (m_value_builder.get().requires_destroy()) {
                            m_value_builder.get().destroy(temp_value);
                        }
                        m_value_builder.get().deallocate(temp_value);
                    }
                    if (m_key_builder.get().requires_destroy()) {
                        m_key_builder.get().destroy(temp_key);
                    }
                    m_key_builder.get().deallocate(temp_key);
                    ++it;
                }
            }

            void assign(void *dst, const void *src) const override
            {
                hard_clear(dst);
                for (size_t slot = 0; slot < keys(src).capacity; ++slot) {
                    if (!keys(src).alive.test(slot)) { continue; }
                    static_cast<void>(set_item(dst, key_data(src, slot), value_data(src, slot)));
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Map value set_from_cpp is not implemented");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Map value move_from_cpp is not implemented");
            }

            void construct(void *memory) const
            {
                std::construct_at(state(memory));
                m_keys.initialise(state(memory)->keys);
            }

            void destroy(void *memory) const noexcept
            {
                hard_clear(memory);
                if (keys(memory).elements != nullptr) {
                    ::operator delete(keys(memory).elements, std::align_val_t{m_key_builder.get().alignment()});
                }
                if (values_memory(memory) != nullptr) {
                    ::operator delete(values_memory(memory), std::align_val_t{m_value_builder.get().alignment()});
                }
                std::destroy_at(state(memory));
            }

            void copy_construct(void *dst, const void *src) const
            {
                construct(dst);
                assign(dst, src);
            }

            void move_construct(void *dst, void *src) const
            {
                std::construct_at(state(dst), std::move(*state(src)));
                m_keys.rebind_index(state(dst)->keys);
                state(src)->keys.elements = nullptr;
                state(src)->keys.size = 0;
                state(src)->keys.capacity = 0;
                state(src)->keys.alive.clear();
                state(src)->keys.occupied.clear();
                state(src)->keys.free_list.clear();
                state(src)->keys.index.reset();
                state(src)->values = nullptr;
                if constexpr (tracks_deltas_v) {
                    state(src)->keys.added.clear();
                    state(src)->keys.removed.clear();
                    state(src)->keys.mutation_depth = 0;
                    state(src)->updated.clear();
                    state(src)->mutation_depth = 0;
                }
            }

          private:
            static constexpr size_t npos = static_cast<size_t>(-1);

            [[nodiscard]] static size_t stride_for(const ValueBuilder &builder) noexcept
            {
                const size_t alignment = builder.alignment();
                const size_t size = builder.size();
                return ((size + alignment - 1) / alignment) * alignment;
            }

            template <typename TMapState> void reserve(TMapState &map, size_t min_capacity) const
            {
                if (min_capacity <= map.keys.capacity) { return; }

                const size_t new_capacity = std::max<size_t>(min_capacity, std::max<size_t>(8, map.keys.capacity * 2));
                std::byte *new_values = static_cast<std::byte *>(
                    ::operator new(new_capacity * m_value_stride, std::align_val_t{m_value_builder.get().alignment()}));

                std::vector<size_t> moved_slots;
                moved_slots.reserve(map.keys.capacity);
                try {
                    for (size_t slot = 0; slot < map.keys.capacity; ++slot) {
                        if (!map.keys.occupied.test(slot)) { continue; }
                        m_value_builder.get().move_construct(new_values + slot * m_value_stride,
                                                             map.values + slot * m_value_stride,
                                                             m_value_builder);
                        moved_slots.push_back(slot);
                    }
                } catch (...) {
                    for (const size_t slot : moved_slots) {
                        destroy_value(new_values + slot * m_value_stride);
                    }
                    ::operator delete(new_values, std::align_val_t{m_value_builder.get().alignment()});
                    throw;
                }

                try {
                    m_keys.reserve(map.keys, new_capacity);
                } catch (...) {
                    for (const size_t slot : moved_slots) {
                        destroy_value(new_values + slot * m_value_stride);
                    }
                    ::operator delete(new_values, std::align_val_t{m_value_builder.get().alignment()});
                    throw;
                }

                destroy_constructed_values(map);
                if (map.values != nullptr) {
                    ::operator delete(map.values, std::align_val_t{m_value_builder.get().alignment()});
                }

                map.values = new_values;
                if constexpr (std::is_same_v<TMapState, DeltaMapState>) { map.updated.resize(new_capacity); }
            }

            void release_removed(DeltaMapState &map) const noexcept
            {
                for (size_t slot = 0; slot < map.keys.capacity; ++slot) {
                    if (!map.keys.removed.test(slot)) { continue; }
                    m_keys.destroy_payload(m_keys.slot_data(map.keys, slot));
                    destroy_value(map.values + slot * m_value_stride);
                    map.keys.index->erase(slot);
                    map.keys.occupied.reset(slot);
                    map.keys.removed.reset(slot);
                    map.updated.reset(slot);
                    map.keys.free_list.push_back(slot);
                }
                map.keys.added.reset();
                map.updated.reset();
            }

            void hard_clear(void *memory) const noexcept
            {
                hard_clear_impl(*state(memory));
            }

            template <typename TMapState> void hard_clear_impl(TMapState &map) const noexcept
            {
                destroy_constructed_values(map);
                m_keys.clear(map.keys);
                if constexpr (std::is_same_v<TMapState, DeltaMapState>) { map.updated.reset(); }
            }

            template <typename TMapState> void prepare_value_slot(TMapState &map, size_t slot) const
            {
                if (slot >= map.keys.capacity) {
                    throw std::out_of_range("Map value slot out of range");
                }

                if (map.keys.occupied.test(slot)) {
                    reset_value(map, slot);
                    return;
                }

                m_value_builder.get().construct(map.values + slot * m_value_stride);
            }

            template <typename TMapState> void destroy_constructed_values(TMapState &map) const noexcept
            {
                if (!m_value_requires_destroy) { return; }
                for (size_t slot = 0; slot < map.keys.capacity; ++slot) {
                    if (map.keys.occupied.test(slot)) {
                        m_value_builder.get().destroy(map.values + slot * m_value_stride);
                    }
                }
            }

            void destroy_value(void *slot) const noexcept
            {
                if (m_value_requires_destroy) {
                    m_value_builder.get().destroy(slot);
                }
            }

            template <typename TMapState> void reset_value(TMapState &map, size_t slot) const
            {
                destroy_value(map.values + slot * m_value_stride);
                m_value_builder.get().construct(map.values + slot * m_value_stride);
            }

            [[nodiscard]] MapState *state(void *memory) const noexcept
            {
                return std::launder(reinterpret_cast<MapState *>(memory));
            }

            [[nodiscard]] const MapState *state(const void *memory) const noexcept
            {
                return std::launder(reinterpret_cast<const MapState *>(memory));
            }

            [[nodiscard]] KeyState &keys(void *memory) const noexcept { return state(memory)->keys; }

            [[nodiscard]] const KeyState &keys(const void *memory) const noexcept { return state(memory)->keys; }

            [[nodiscard]] std::byte *values_memory(void *memory) const noexcept { return state(memory)->values; }

            [[nodiscard]] const std::byte *values_memory(const void *memory) const noexcept { return state(memory)->values; }

            std::reference_wrapper<const value::TypeMeta> m_schema;
            std::reference_wrapper<const ValueBuilder>    m_key_builder;
            std::reference_wrapper<const ValueBuilder>    m_value_builder;
            KeySlotStorage                                m_keys;
            size_t                                        m_value_stride;
            bool                                          m_value_requires_destroy;
        };

        template <typename TDispatch> struct AssociativeStateOps final : StateOps
        {
            explicit AssociativeStateOps(const TDispatch &dispatch) noexcept
                : m_dispatch(dispatch)
            {
            }

            void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                if constexpr (std::same_as<TDispatch, SetDispatch<MutationTracking::Delta>> ||
                              std::same_as<TDispatch, SetDispatch<MutationTracking::Plain>>) {
                    if constexpr (TDispatch::tracking_mode == MutationTracking::Delta) {
                        builder.cache_layout(sizeof(DeltaSetState), alignof(DeltaSetState));
                    } else {
                        builder.cache_layout(sizeof(PlainSetState), alignof(PlainSetState));
                    }
                } else {
                    if constexpr (TDispatch::tracking_mode == MutationTracking::Delta) {
                        builder.cache_layout(sizeof(DeltaMapState), alignof(DeltaMapState));
                    } else {
                        builder.cache_layout(sizeof(PlainMapState), alignof(PlainMapState));
                    }
                }
                builder.cache_lifecycle(true, true, false);
            }

            [[nodiscard]] const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return m_dispatch;
            }

            [[nodiscard]] bool requires_destroy(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return true;
            }

            [[nodiscard]] bool requires_deallocate(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return true;
            }

            [[nodiscard]] bool stores_inline_in_value_handle(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return false;
            }

            void construct(void *memory) const override { m_dispatch.get().construct(memory); }
            void destroy(void *memory) const noexcept override { m_dispatch.get().destroy(memory); }
            void copy_construct(void *dst, const void *src) const override { m_dispatch.get().copy_construct(dst, src); }
            void move_construct(void *dst, void *src) const override { m_dispatch.get().move_construct(dst, src); }

            std::reference_wrapper<const TDispatch> m_dispatch;
        };

        struct CachedBuilderEntry
        {
            std::shared_ptr<const ViewDispatch> dispatch;
            std::shared_ptr<const StateOps>     state_ops;
            std::shared_ptr<const ValueBuilder> builder;
        };

        struct AssociativeBuilderKey
        {
            const value::TypeMeta *schema{nullptr};
            MutationTracking       tracking{MutationTracking::Delta};

            [[nodiscard]] bool operator==(const AssociativeBuilderKey &other) const noexcept
            {
                return schema == other.schema && tracking == other.tracking;
            }
        };

        struct AssociativeBuilderKeyHash
        {
            [[nodiscard]] size_t operator()(const AssociativeBuilderKey &key) const noexcept
            {
                return std::hash<const value::TypeMeta *>{}(key.schema) ^
                       (static_cast<size_t>(key.tracking) << 1U);
            }
        };

        const ValueBuilder *associative_builder_for(const value::TypeMeta *schema, MutationTracking tracking)
        {
            if (schema == nullptr) { return nullptr; }
            if (schema->kind != value::TypeKind::Set && schema->kind != value::TypeKind::Map) { return nullptr; }

            static std::mutex cache_mutex;
            static std::unordered_map<AssociativeBuilderKey, CachedBuilderEntry, AssociativeBuilderKeyHash> cache;

            std::lock_guard lock(cache_mutex);
            const AssociativeBuilderKey key{schema, tracking};
            if (auto it = cache.find(key); it != cache.end()) {
                return it->second.builder.get();
            }

            CachedBuilderEntry entry;
            if (schema->kind == value::TypeKind::Set) {
                if (tracking == MutationTracking::Delta) {
                    auto dispatch = std::make_shared<SetDispatch<MutationTracking::Delta>>(*schema);
                    auto state_ops =
                        std::make_shared<AssociativeStateOps<SetDispatch<MutationTracking::Delta>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                } else {
                    auto dispatch = std::make_shared<SetDispatch<MutationTracking::Plain>>(*schema);
                    auto state_ops =
                        std::make_shared<AssociativeStateOps<SetDispatch<MutationTracking::Plain>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                }
            } else {
                if (tracking == MutationTracking::Delta) {
                    auto dispatch = std::make_shared<MapDispatch<MutationTracking::Delta>>(*schema);
                    auto state_ops =
                        std::make_shared<AssociativeStateOps<MapDispatch<MutationTracking::Delta>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                } else {
                    auto dispatch = std::make_shared<MapDispatch<MutationTracking::Plain>>(*schema);
                    auto state_ops =
                        std::make_shared<AssociativeStateOps<MapDispatch<MutationTracking::Plain>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                }
            }

            auto [it, inserted] = cache.emplace(key, std::move(entry));
            static_cast<void>(inserted);
            return it->second.builder.get();
        }

    }  // namespace detail

    SetView::SetView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Set) {
            throw std::runtime_error("SetView requires a set schema");
        }
    }

    SetMutationView SetView::begin_mutation()
    {
        return SetMutationView{*this};
    }

    SetDeltaView SetView::delta()
    {
        return SetDeltaView{*this};
    }

    SetDeltaView SetView::delta() const
    {
        return SetDeltaView{*this};
    }

    void SetView::begin_mutation_scope()
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::begin_mutation on invalid view"); }
        dispatch->begin_mutation(data());
    }

    void SetView::end_mutation_scope() noexcept
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { return; }
        dispatch->end_mutation(data());
    }

    size_t SetView::size() const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::size on invalid view"); }
        return dispatch->size(data());
    }

    bool SetView::empty() const
    {
        return size() == 0;
    }

    SetDeltaView::SetDeltaView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Set) {
            throw std::runtime_error("SetDeltaView requires a set schema");
        }
    }

    Range<View> SetDeltaView::added() const
    {
        return Range<View>{this, slot_capacity(), &SetDeltaView::slot_is_added, &SetDeltaView::project_slot};
    }

    Range<View> SetDeltaView::removed() const
    {
        return Range<View>{this, slot_capacity(), &SetDeltaView::slot_is_removed, &SetDeltaView::project_slot};
    }

    size_t SetDeltaView::slot_capacity() const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetDeltaView::slot_capacity on invalid view"); }
        return dispatch->slot_capacity(data());
    }

    const value::TypeMeta *SetView::element_schema() const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::element_schema on invalid view"); }
        return &dispatch->element_schema();
    }

    View SetView::at(size_t index)
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::at on invalid view"); }
        return View{&dispatch->element_dispatch(), dispatch->element_data(data(), index), &dispatch->element_schema()};
    }

    View SetView::at(size_t index) const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::at on invalid view"); }
        return View{&dispatch->element_dispatch(),
                    const_cast<void *>(dispatch->element_data(data(), index)),
                    &dispatch->element_schema()};
    }

    bool SetDeltaView::slot_occupied(size_t slot) const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetDeltaView::slot_occupied on invalid view"); }
        return dispatch->slot_occupied(data(), slot);
    }

    bool SetDeltaView::slot_added(size_t slot) const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetDeltaView::slot_added on invalid view"); }
        return dispatch->slot_added(data(), slot);
    }

    bool SetDeltaView::slot_removed(size_t slot) const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetDeltaView::slot_removed on invalid view"); }
        return dispatch->slot_removed(data(), slot);
    }

    View SetDeltaView::at_slot(size_t slot)
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetDeltaView::at_slot on invalid view"); }
        return View{&dispatch->element_dispatch(), dispatch->slot_data(data(), slot), &dispatch->element_schema()};
    }

    View SetDeltaView::at_slot(size_t slot) const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetDeltaView::at_slot on invalid view"); }
        return View{&dispatch->element_dispatch(),
                    const_cast<void *>(dispatch->slot_data(data(), slot)),
                    &dispatch->element_schema()};
    }

    bool SetView::contains(const View &value) const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::contains on invalid view"); }
        if (!value.valid() || value.schema() != &dispatch->element_schema()) {
            throw std::invalid_argument("SetView::contains requires a valid matching-schema element");
        }
        return dispatch->contains(data(), data_of(value));
    }

    SetMutationView::SetMutationView(SetView &view)
        : SetView(view)
    {
        begin_mutation_scope();
    }

    SetMutationView::SetMutationView(SetMutationView &&other) noexcept
        : SetView(other)
    {
        m_owns_scope = other.m_owns_scope;
        other.m_owns_scope = false;
    }

    SetMutationView::~SetMutationView()
    {
        if (!m_owns_scope) { return; }
        try {
            end_mutation_scope();
        } catch (...) {
        }
    }

    bool SetMutationView::add(const View &value)
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetMutationView::add on invalid view"); }
        if (!value.valid() || value.schema() != &dispatch->element_schema()) {
            throw std::invalid_argument("SetMutationView::add requires a valid matching-schema element");
        }
        return dispatch->add(data(), data_of(value));
    }

    SetMutationView &SetMutationView::adding(const View &value)
    {
        static_cast<void>(add(value));
        return *this;
    }

    bool SetMutationView::remove(const View &value)
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetMutationView::remove on invalid view"); }
        if (!value.valid() || value.schema() != &dispatch->element_schema()) {
            throw std::invalid_argument("SetMutationView::remove requires a valid matching-schema element");
        }
        return dispatch->remove(data(), data_of(value));
    }

    SetMutationView &SetMutationView::removing(const View &value)
    {
        static_cast<void>(remove(value));
        return *this;
    }

    void SetMutationView::clear()
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetMutationView::clear on invalid view"); }
        dispatch->clear(data());
    }

    SetMutationView &SetMutationView::clearing()
    {
        clear();
        return *this;
    }

    const detail::SetViewDispatch *SetView::set_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::SetViewDispatch *>(dispatch()) : nullptr;
    }

    const detail::SetViewDispatch *SetDeltaView::set_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::SetViewDispatch *>(dispatch()) : nullptr;
    }

    bool SetDeltaView::slot_is_added(const void *context, size_t slot)
    {
        const auto *self = static_cast<const SetDeltaView *>(context);
        return self->slot_occupied(slot) && self->slot_added(slot);
    }

    bool SetDeltaView::slot_is_removed(const void *context, size_t slot)
    {
        const auto *self = static_cast<const SetDeltaView *>(context);
        return self->slot_occupied(slot) && self->slot_removed(slot);
    }

    View SetDeltaView::project_slot(const void *context, size_t slot)
    {
        const auto *self = static_cast<const SetDeltaView *>(context);
        return self->at_slot(slot);
    }

    MapView::MapView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Map) {
            throw std::runtime_error("MapView requires a map schema");
        }
    }

    MapMutationView MapView::begin_mutation()
    {
        return MapMutationView{*this};
    }

    MapDeltaView MapView::delta()
    {
        return MapDeltaView{*this};
    }

    MapDeltaView MapView::delta() const
    {
        return MapDeltaView{*this};
    }

    void MapView::begin_mutation_scope()
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::begin_mutation on invalid view"); }
        dispatch->begin_mutation(data());
    }

    void MapView::end_mutation_scope() noexcept
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { return; }
        dispatch->end_mutation(data());
    }

    size_t MapView::size() const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::size on invalid view"); }
        return dispatch->size(data());
    }

    bool MapView::empty() const
    {
        return size() == 0;
    }

    MapDeltaView::MapDeltaView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Map) {
            throw std::runtime_error("MapDeltaView requires a map schema");
        }
    }

    Range<View> MapDeltaView::added_keys() const
    {
        return Range<View>{this, slot_capacity(), &MapDeltaView::slot_is_added, &MapDeltaView::project_key};
    }

    Range<View> MapDeltaView::removed_keys() const
    {
        return Range<View>{this, slot_capacity(), &MapDeltaView::slot_is_removed, &MapDeltaView::project_key};
    }

    Range<View> MapDeltaView::updated_keys() const
    {
        return Range<View>{this, slot_capacity(), &MapDeltaView::slot_is_updated, &MapDeltaView::project_key};
    }

    Range<View> MapDeltaView::added_values() const
    {
        return Range<View>{this, slot_capacity(), &MapDeltaView::slot_is_added, &MapDeltaView::project_value};
    }

    Range<View> MapDeltaView::removed_values() const
    {
        return Range<View>{this, slot_capacity(), &MapDeltaView::slot_is_removed, &MapDeltaView::project_value};
    }

    Range<View> MapDeltaView::updated_values() const
    {
        return Range<View>{this, slot_capacity(), &MapDeltaView::slot_is_updated, &MapDeltaView::project_value};
    }

    Range<std::pair<View, View>> MapDeltaView::added_items() const
    {
        return Range<std::pair<View, View>>{this, slot_capacity(), &MapDeltaView::slot_is_added, &MapDeltaView::project_item};
    }

    Range<std::pair<View, View>> MapDeltaView::removed_items() const
    {
        return Range<std::pair<View, View>>{this, slot_capacity(), &MapDeltaView::slot_is_removed, &MapDeltaView::project_item};
    }

    Range<std::pair<View, View>> MapDeltaView::updated_items() const
    {
        return Range<std::pair<View, View>>{this, slot_capacity(), &MapDeltaView::slot_is_updated, &MapDeltaView::project_item};
    }

    size_t MapDeltaView::slot_capacity() const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapDeltaView::slot_capacity on invalid view"); }
        return dispatch->slot_capacity(data());
    }

    const value::TypeMeta *MapView::key_schema() const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::key_schema on invalid view"); }
        return &dispatch->key_schema();
    }

    const value::TypeMeta *MapView::value_schema() const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::value_schema on invalid view"); }
        return &dispatch->value_schema();
    }

    bool MapDeltaView::slot_occupied(size_t slot) const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapDeltaView::slot_occupied on invalid view"); }
        return dispatch->slot_occupied(data(), slot);
    }

    bool MapDeltaView::slot_added(size_t slot) const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapDeltaView::slot_added on invalid view"); }
        return dispatch->slot_added(data(), slot);
    }

    bool MapDeltaView::slot_removed(size_t slot) const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapDeltaView::slot_removed on invalid view"); }
        return dispatch->slot_removed(data(), slot);
    }

    View MapDeltaView::key_at_slot(size_t slot)
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapDeltaView::key_at_slot on invalid view"); }
        return View{&dispatch->key_dispatch(), dispatch->key_data(data(), slot), &dispatch->key_schema()};
    }

    View MapDeltaView::key_at_slot(size_t slot) const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapDeltaView::key_at_slot on invalid view"); }
        return View{&dispatch->key_dispatch(),
                    const_cast<void *>(dispatch->key_data(data(), slot)),
                    &dispatch->key_schema()};
    }

    View MapDeltaView::value_at_slot(size_t slot)
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapDeltaView::value_at_slot on invalid view"); }
        return View{&dispatch->value_dispatch(), dispatch->value_data(data(), slot), &dispatch->value_schema()};
    }

    View MapDeltaView::value_at_slot(size_t slot) const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapDeltaView::value_at_slot on invalid view"); }
        return View{&dispatch->value_dispatch(),
                    const_cast<void *>(dispatch->value_data(data(), slot)),
                    &dispatch->value_schema()};
    }

    bool MapView::contains(const View &key) const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::contains on invalid view"); }
        if (!key.valid() || key.schema() != &dispatch->key_schema()) {
            throw std::invalid_argument("MapView::contains requires a valid matching-schema key");
        }
        return dispatch->find(data(), data_of(key)) != static_cast<size_t>(-1);
    }

    View MapView::at(const View &key)
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::at on invalid view"); }
        if (!key.valid() || key.schema() != &dispatch->key_schema()) {
            throw std::invalid_argument("MapView::at requires a valid matching-schema key");
        }
        const size_t index = dispatch->find(data(), data_of(key));
        if (index == static_cast<size_t>(-1)) { throw std::out_of_range("MapView::at key not found"); }
        return View{&dispatch->value_dispatch(), dispatch->value_data(data(), index), &dispatch->value_schema()};
    }

    View MapView::at(const View &key) const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::at on invalid view"); }
        if (!key.valid() || key.schema() != &dispatch->key_schema()) {
            throw std::invalid_argument("MapView::at requires a valid matching-schema key");
        }
        const size_t index = dispatch->find(data(), data_of(key));
        if (index == static_cast<size_t>(-1)) { throw std::out_of_range("MapView::at key not found"); }
        return View{&dispatch->value_dispatch(),
                    const_cast<void *>(dispatch->value_data(data(), index)),
                    &dispatch->value_schema()};
    }

    MapMutationView::MapMutationView(MapView &view)
        : MapView(view)
    {
        begin_mutation_scope();
    }

    MapMutationView::MapMutationView(MapMutationView &&other) noexcept
        : MapView(other)
    {
        m_owns_scope = other.m_owns_scope;
        other.m_owns_scope = false;
    }

    MapMutationView::~MapMutationView()
    {
        if (!m_owns_scope) { return; }
        try {
            end_mutation_scope();
        } catch (...) {
        }
    }

    void MapMutationView::set(const View &key, const View &value)
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapMutationView::set on invalid view"); }
        if (!key.valid() || key.schema() != &dispatch->key_schema()) {
            throw std::invalid_argument("MapMutationView::set requires a valid matching-schema key");
        }
        if (!value.valid()) {
            throw std::invalid_argument("MapMutationView::set requires a valid value");
        }
        if (value.schema() != nullptr && value.schema() != &dispatch->value_schema()) {
            throw std::invalid_argument("MapMutationView::set requires a matching value schema");
        }
        dispatch->set_item(data(), data_of(key), data_of(value));
    }

    MapMutationView &MapMutationView::setting(const View &key, const View &value)
    {
        set(key, value);
        return *this;
    }

    bool MapMutationView::remove(const View &key)
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapMutationView::remove on invalid view"); }
        if (!key.valid() || key.schema() != &dispatch->key_schema()) {
            throw std::invalid_argument("MapMutationView::remove requires a valid matching-schema key");
        }
        return dispatch->remove(data(), data_of(key));
    }

    MapMutationView &MapMutationView::removing(const View &key)
    {
        static_cast<void>(remove(key));
        return *this;
    }

    void MapMutationView::clear()
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapMutationView::clear on invalid view"); }
        dispatch->clear(data());
    }

    MapMutationView &MapMutationView::clearing()
    {
        clear();
        return *this;
    }

    const detail::MapViewDispatch *MapView::map_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::MapViewDispatch *>(dispatch()) : nullptr;
    }

    const detail::MapViewDispatch *MapDeltaView::map_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::MapViewDispatch *>(dispatch()) : nullptr;
    }

    bool MapDeltaView::slot_is_added(const void *context, size_t slot)
    {
        const auto *self = static_cast<const MapDeltaView *>(context);
        return self->slot_occupied(slot) && self->slot_added(slot);
    }

    bool MapDeltaView::slot_is_removed(const void *context, size_t slot)
    {
        const auto *self = static_cast<const MapDeltaView *>(context);
        return self->slot_occupied(slot) && self->slot_removed(slot);
    }

    bool MapDeltaView::slot_is_updated(const void *context, size_t slot)
    {
        const auto *self = static_cast<const MapDeltaView *>(context);
        const auto *dispatch = self->map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapDeltaView::updated range on invalid view"); }
        return self->slot_occupied(slot) && dispatch->slot_updated(self->data(), slot);
    }

    View MapDeltaView::project_key(const void *context, size_t slot)
    {
        const auto *self = static_cast<const MapDeltaView *>(context);
        return self->key_at_slot(slot);
    }

    View MapDeltaView::project_value(const void *context, size_t slot)
    {
        const auto *self = static_cast<const MapDeltaView *>(context);
        return self->value_at_slot(slot);
    }

    std::pair<View, View> MapDeltaView::project_item(const void *context, size_t slot)
    {
        const auto *self = static_cast<const MapDeltaView *>(context);
        return {self->key_at_slot(slot), self->value_at_slot(slot)};
    }

}  // namespace hgraph
