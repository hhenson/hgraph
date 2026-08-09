#ifndef HGRAPH_CPP_ROOT_VALUE_MUTABLE_CONTAINER_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_MUTABLE_CONTAINER_OPS_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/value_slot_store.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/value_ops.h>

#include <sul/dynamic_bitset.hpp>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    /**
     * Structurally-mutable value-layer containers, built on the same slot-store
     * substrate the time-series layer uses (here without delta tracking). This
     * header carries the *mutable* implementation of the list kind: a growable
     * storage shape, its lifecycle/read thunks, the canonical
     * ``MutableListValueOps`` table (read surface from ``container_ops.h`` plus
     * the structural-mutation hooks), the per-instantiation storage plan, and
     * the canonical-binding accessor that interns the (schema, plan, ops)
     * triple. The mutable map lives alongside it in a later slice.
     *
     * The element binding is baked into the plan's lifecycle context so an
     * empty mutable container is still element-bound and can accept
     * ``push_back`` immediately (unlike the compact, build-once containers).
     */

    /**
     * Growable contiguous-by-index value storage for a mutable ``List``.
     *
     * Backed by a ``ValueSlotStore`` (the shared slot substrate); logical size
     * tracks the live prefix ``[0, size())``. Elements are addressed by index;
     * ``push_back`` grows capacity geometrically.
     */
    class MutableListStorage
    {
      public:
        MutableListStorage() noexcept = default;

        explicit MutableListStorage(const ValueTypeRef &element_binding)
            : element_binding_{element_binding}, slots_{element_binding.checked_plan()}
        {
        }

        MutableListStorage(const MutableListStorage &other) { copy_from(other); }

        MutableListStorage &operator=(const MutableListStorage &other)
        {
            if (this != &other) { copy_from(other); }
            return *this;
        }

        MutableListStorage(MutableListStorage &&other) noexcept
            : element_binding_{other.element_binding_}, slots_{std::move(other.slots_)}, size_{other.size_},
              validity_{std::move(other.validity_)}
        {
            other.size_ = 0;
            other.validity_.clear();
        }

        MutableListStorage &operator=(MutableListStorage &&other) noexcept
        {
            if (this != &other)
            {
                slots_           = std::move(other.slots_);
                element_binding_ = other.element_binding_;
                size_            = other.size_;
                validity_        = std::move(other.validity_);
                other.size_      = 0;
                other.validity_.clear();
            }
            return *this;
        }

        ~MutableListStorage() = default;  // ValueSlotStore destroys live payloads

        [[nodiscard]] std::size_t              size() const noexcept { return size_; }
        [[nodiscard]] bool                     empty() const noexcept { return size_ == 0; }
        [[nodiscard]] const ValueTypeRef &element_binding() const noexcept { return element_binding_; }

        [[nodiscard]] const void *element_at(std::size_t index) const
        {
            require_index(index);
            return slots_.value_memory(index);
        }
        [[nodiscard]] void *element_at(std::size_t index)
        {
            require_index(index);
            return slots_.value_memory(index);
        }

        /** Append a copy of the typed element at ``src``. */
        void push_back(ValueTypeRef source_binding, const void *src)
        {
            require_bound();
            ensure_capacity(size_ + 1);
            if (source_binding == element_binding_)
            {
                slots_.construct_at(size_, src);
            }
            else
            {
                slots_.construct_at(size_);
                auto rollback = make_scope_exit([&]() noexcept { slots_.destroy_at(size_); });
                element_binding_.ops_ref().copy_assign_from(
                    element_binding_, slots_.value_memory(size_), source_binding, src);
                rollback.release();
            }
            if (!validity_.empty()) { validity_.push_back(true); }
            ++size_;
        }

        /** Append an UNSET element (a hole - element validity,
            core_concepts.rst: an EMPTY bitset means dense/all-set; the
            first hole sizes it). The slot default-constructs so the store's
            lifetime invariants hold; reads report it unset. */
        void push_back_unset()
        {
            require_bound();
            ensure_capacity(size_ + 1);
            slots_.construct_at(size_);  // default-construct
            if (validity_.empty()) { validity_.resize(size_, true); }
            validity_.push_back(false);
            ++size_;
        }

        /** True when the element at ``index`` holds a live value. */
        [[nodiscard]] bool element_set(std::size_t index) const noexcept
        {
            return validity_.empty() || (index < validity_.size() && validity_.test(index));
        }

        /** Replace the element at ``index`` with a copy of typed ``src``. */
        void set_element(std::size_t index, ValueTypeRef source_binding, const void *src)
        {
            require_index(index);
            element_binding_.ops_ref().copy_assign_from(
                element_binding_, slots_.value_memory(index), source_binding, src);
        }

        /** Remove the element at ``index``, shifting later elements down one place. */
        void erase(std::size_t index)
        {
            require_index(index);
            if (!validity_.empty())
            {
                for (std::size_t j = index; j + 1 < validity_.size(); ++j) { validity_[j] = validity_[j + 1]; }
                validity_.resize(size_ - 1);
            }
            const auto &plan = element_binding_.checked_plan();
            for (std::size_t j = index; j + 1 < size_; ++j)
            {
                plan.move_assign(slots_.value_memory(j), slots_.value_memory(j + 1));
            }
            slots_.destroy_at(size_ - 1);  // the tail slot is now a duplicate
            --size_;
        }

        /** Drop the last element. */
        void pop_back()
        {
            if (size_ == 0) { throw std::logic_error("MutableListStorage::pop_back on empty list"); }
            slots_.destroy_at(size_ - 1);
            --size_;
            if (!validity_.empty()) { validity_.resize(size_); }
        }

        /** Destroy every element; capacity is retained for reuse. */
        void clear() noexcept
        {
            for (std::size_t index = size_; index > 0; --index) { slots_.destroy_at(index - 1); }
            size_ = 0;
            validity_.clear();
        }

        [[nodiscard]] DebugDynamicLayout debug_layout() const noexcept
        {
            const auto *base = reinterpret_cast<const std::byte *>(this);
            const auto offset_of = [base](const auto *member) {
                return static_cast<std::size_t>(reinterpret_cast<const std::byte *>(member) - base);
            };
            const StableSlotDebugView slots = slots_.debug_slot_view();
            DebugDynamicFlags flags = DebugDynamicFlags::DataIsIndirect |
                                      DebugDynamicFlags::DataIsPointerTable |
                                      DebugDynamicFlags::SlotIndexIsIndirect;
            if (slots.pointers_tagged) { flags = flags | DebugDynamicFlags::DataPointersAreTagged; }
            return DebugDynamicLayout{
                .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
                .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
                .kind = DebugDynamicKind::StableSlots,
                .flags = flags,
                .size_offset = offset_of(&size_),
                .data_offset = offset_of(slots.implementation_pointer),
                .stride = element_binding_.checked_plan().layout.size,
                .auxiliary_offset = slots.pointer_table_offset,
            };
        }

        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = slots_.dynamic_storage_metrics();
            if (!validity_.empty())
            {
                using bitset_type = sul::dynamic_bitset<>;
                result.live_bytes += validity_.num_blocks() * sizeof(typename bitset_type::block_type);
                result.reserved_bytes +=
                    (validity_.capacity() / bitset_type::bits_per_block) *
                    sizeof(typename bitset_type::block_type);
            }
            const auto &ops = element_binding_.ops_ref();
            for (std::size_t index = 0; index < size_; ++index)
            {
                result += ops.dynamic_storage_metrics(slots_.value_memory(index));
            }
            return result;
        }

      private:
        ValueTypeRef element_binding_{nullptr};
        ValueSlotStore          slots_{};
        std::size_t             size_{0};
        // Element validity (holes): EMPTY = dense/all-set; sized on the
        // first unset element (the Bundle-field-validity convention).
        sul::dynamic_bitset<>   validity_{};

        void require_bound() const
        {
            if (element_binding_ == nullptr) { throw std::logic_error("MutableListStorage is not element-bound"); }
        }
        void require_index(std::size_t index) const
        {
            if (index >= size_) { throw std::out_of_range("MutableListStorage index out of range"); }
        }
        void ensure_capacity(std::size_t needed)
        {
            const std::size_t cap = slots_.slot_capacity();
            if (needed <= cap) { return; }
            slots_.reserve_to(std::max<std::size_t>(needed, cap == 0 ? std::size_t{4} : cap * 2));
        }
        void copy_from(const MutableListStorage &other)
        {
            element_binding_ = other.element_binding_;
            size_            = 0;
            if (element_binding_ == nullptr)
            {
                slots_ = ValueSlotStore{};  // unbound; destroys any prior payloads
                return;
            }
            slots_ = ValueSlotStore{element_binding_.checked_plan()};
            slots_.reserve_to(other.size_);
            for (std::size_t index = 0; index < other.size_; ++index)
            {
                slots_.construct_at(index, other.element_at(index));
                ++size_;
            }
            validity_ = other.validity_;
        }
    };

    namespace mutable_container_detail
    {
        using MaterializedOwner = MemoryUtils::ErasedOwner<MemoryUtils::InlineStoragePolicy<>, TypeRecord>;

        [[nodiscard]] inline bool accepts_container_source(const void *, ValueTypeRef binding,
                                                           ValueTypeRef source) noexcept
        {
            return binding && source && binding.schema() == source.schema();
        }

        [[nodiscard]] inline MaterializedOwner materialize_element(ValueTypeRef target, const ValueView &source)
        {
            MaterializedOwner result{*target.record()};
            target.ops_ref().copy_assign_from(target, result.data(), source.binding(), source.data());
            return result;
        }

        struct MutableListState
        {
            ValueTypeRef element_binding{nullptr};
        };

        // -- lifecycle thunks (element binding comes from the plan's state) --
        inline void list_construct(void *dst, const void *context)
        {
            const auto &state = compact_detail::checked_state<MutableListState>(context, "mutable_list");
            if (state.element_binding == nullptr)
            {
                throw std::logic_error("mutable_list construction requires an element binding");
            }
            std::construct_at(static_cast<MutableListStorage *>(dst), state.element_binding);
        }
        inline void list_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<MutableListStorage *>(memory));
        }
        inline void list_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<MutableListStorage *>(dst), *static_cast<const MutableListStorage *>(src));
        }
        inline void list_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<MutableListStorage *>(dst), std::move(*static_cast<MutableListStorage *>(src)));
        }
        inline void list_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<MutableListStorage *>(dst) = *static_cast<const MutableListStorage *>(src);
        }
        inline void list_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<MutableListStorage *>(dst) = std::move(*static_cast<MutableListStorage *>(src));
        }

        // -- read-op thunks (same surface as the compact list, over the mutable storage) --
        inline std::size_t list_size(const void *, const void *memory) noexcept
        {
            return static_cast<const MutableListStorage *>(memory)->size();
        }
        inline const void *list_element_at(const void *, const void *memory, std::size_t index)
        {
            const auto *storage = static_cast<const MutableListStorage *>(memory);
            if (!storage->element_set(index)) { return nullptr; }   // hole: UNSET element
            return storage->element_at(index);
        }
        inline ValueTypeRef list_element_binding(const void *, const void *memory, std::size_t) noexcept
        {
            return static_cast<const MutableListStorage *>(memory)->element_binding();
        }
        inline std::size_t list_hash(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MutableListStorage *>(memory);
            if (storage->element_binding() == nullptr) { throw std::logic_error("mutable list hash requires a binding"); }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            std::size_t seed = 0;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                // UNSET elements hash with a distinct marker (element
                // validity - the Bundle-field convention).
                seed = container_ops_detail::combine_hash(
                    seed, storage->element_set(i) ? ops.hash(storage->element_at(i)) : 0x9e3779b97f4a7c15ULL);
            }
            return seed;
        }
        inline bool list_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MutableListStorage *>(lhs);
            const auto *b = static_cast<const MutableListStorage *>(rhs);
            if (a->size() != b->size()) { return false; }
            if (a->size() == 0) { return true; }
            if (a->element_binding() == nullptr || b->element_binding() == nullptr) { return false; }
            if (a->element_binding() != b->element_binding()) { return false; }
            const auto element_binding = a->element_binding();
            const auto &ops = element_binding.ops_ref();
            for (std::size_t i = 0; i < a->size(); ++i)
            {
                const bool a_set = a->element_set(i);
                if (a_set != b->element_set(i)) { return false; }
                if (!a_set) { continue; }   // both unset
                if (!ops.equals(a->element_at(i), b->element_at(i))) { return false; }
            }
            return true;
        }
        inline std::partial_ordering list_compare(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MutableListStorage *>(lhs);
            const auto *b = static_cast<const MutableListStorage *>(rhs);
            const auto a_binding = a->element_binding();
            const auto b_binding = b->element_binding();
            if (const auto order = value_ops_detail::null_order(a_binding, b_binding)) { return *order; }
            if (a_binding != b_binding) { return std::partial_ordering::unordered; }
            const auto &ops = a_binding.ops_ref();
            const auto  n   = std::min(a->size(), b->size());
            for (std::size_t i = 0; i < n; ++i)
            {
                const bool a_set = a->element_set(i);
                const bool b_set = b->element_set(i);
                if (!a_set || !b_set)
                {
                    if (a_set == b_set) { continue; }             // both unset
                    return a_set ? std::partial_ordering::greater  // unset < set
                                 : std::partial_ordering::less;
                }
                const auto c = ops.compare(a->element_at(i), b->element_at(i));
                if (c != 0) { return c; }
            }
            if (a->size() < b->size()) { return std::partial_ordering::less; }
            if (a->size() > b->size()) { return std::partial_ordering::greater; }
            return std::partial_ordering::equivalent;
        }
        inline std::string list_to_string(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MutableListStorage *>(memory);
            if (storage->element_binding() == nullptr) { return "[]"; }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            return container_ops_detail::format_delimited(
                '[', ']', storage->size(), [&](fmt::memory_buffer &out, std::size_t i) {
                    if (!storage->element_set(i))
                    {
                        fmt::format_to(std::back_inserter(out), "<unset>");
                        return;
                    }
                    fmt::format_to(std::back_inserter(out), "{}", ops.to_string(storage->element_at(i)));
                });
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        inline nb::object mutable_list_to_python(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MutableListStorage *>(memory);
            if (storage->element_binding() == nullptr) { return nb::list(); }
            const auto element_binding = storage->element_binding();
            const auto &ops = element_binding.ops_ref();
            nb::list    result;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                // UNSET elements (holes) read back as None.
                result.append(storage->element_set(i) ? ops.to_python(storage->element_at(i)) : nb::none());
            }
            return result;
        }
#endif

        // -- structural-mutation thunks --
        inline void list_push_back(const void *, void *memory, ValueTypeRef element_type,
                                   const void *element)
        {
            static_cast<MutableListStorage *>(memory)->push_back(element_type, element);
        }
        inline void list_push_back_unset(const void *, void *memory)
        {
            static_cast<MutableListStorage *>(memory)->push_back_unset();
        }
        inline void list_set_element(const void *, void *memory, std::size_t index,
                                     ValueTypeRef element_type, const void *element)
        {
            static_cast<MutableListStorage *>(memory)->set_element(index, element_type, element);
        }
        inline void list_erase(const void *, void *memory, std::size_t index)
        {
            static_cast<MutableListStorage *>(memory)->erase(index);
        }
        inline void list_pop_back(const void *, void *memory)
        {
            static_cast<MutableListStorage *>(memory)->pop_back();
        }
        inline void list_clear(const void *, void *memory)
        {
            static_cast<MutableListStorage *>(memory)->clear();
        }

        inline void list_copy_assign_from(const void *, ValueTypeRef binding, void *memory, ValueTypeRef source,
                                          const void *src)
        {
            if (binding == source)
            {
                binding.copy_assign_at(memory, src);
                return;
            }
            auto &target = *static_cast<MutableListStorage *>(memory);
            target.clear();
            const ValueView source_view{source, src};
            const auto values = source_view.as_list();
            for (std::size_t index = 0; index < values.size(); ++index)
            {
                const auto child = values.at(index);
                if (child.has_value())
                {
                    target.push_back(child.binding(), child.data());
                }
                else
                {
                    target.push_back_unset();
                }
            }
        }

        inline void list_move_assign_from(const void *context, ValueTypeRef binding, void *memory, ValueTypeRef source,
                                          void *src)
        {
            if (binding == source)
            {
                binding.move_assign_at(memory, src);
                return;
            }
            list_copy_assign_from(context, binding, memory, source, src);
        }

        inline compact_detail::CompactContainerPlanRegistry<compact_detail::UnaryBindingKey, MutableListState,
                                                            compact_detail::UnaryBindingKeyHash> &
        list_registry()
        {
            static compact_detail::CompactContainerPlanRegistry<compact_detail::UnaryBindingKey, MutableListState,
                                                                compact_detail::UnaryBindingKeyHash>
                r;
            return r;
        }
    }  // namespace mutable_container_detail

    [[nodiscard]] inline const MemoryUtils::StoragePlan &mutable_list_plan(const ValueTypeRef &element_binding)
    {
        using namespace mutable_container_detail;
        return list_registry().intern(
            compact_detail::UnaryBindingKey{.binding = element_binding},
            [&] { return std::make_unique<MutableListState>(MutableListState{.element_binding = element_binding}); },
            [&](const MutableListState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<MutableListStorage, &list_construct, &list_destroy,
                                                      &list_copy_construct, &list_move_construct, &list_copy_assign,
                                                      &list_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MutableListValueOps &mutable_list_ops() noexcept
    {
        using namespace mutable_container_detail;
        static const MutableListValueOps ops = [] {
            auto value = MutableListValueOps{
            {{{// ValueOps:
               ValueOpsKind::MutableList,
               nullptr,
               true,  // allows_mutation
               &list_hash,
               &list_equals,
               &list_compare,
               &list_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
               ,
               &mutable_container_detail::mutable_list_to_python,
               nullptr
#endif
              },
              // IndexedValueOps:
              &list_size,
              &list_element_at,
              &list_element_binding,
              &container_ops_detail::dense_make_range<&list_size, &list_element_at, &list_element_binding>,
              &container_ops_detail::dense_make_mutable_range<&list_size, &list_element_at, &list_element_binding>}},
            // MutableListValueOps:
            &list_push_back,
            &list_set_element,
            &list_erase,
            &list_pop_back,
            &list_clear,
            &list_push_back_unset,
            };
            value.dynamic_storage_metrics_impl =
                &container_ops_detail::compact_dynamic_storage_metrics<MutableListStorage>;
            value.accepts_source_impl = &accepts_container_source;
            value.copy_assign_from_impl = &list_copy_assign_from;
            value.move_assign_from_impl = &list_move_assign_from;
            return value;
        }();
        return ops;
    }

    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef mutable_list_type(const ValueTypeRef &element_binding);

    // =================================================================
    // Mutable Map
    // =================================================================

    namespace mutable_container_detail
    {
        // Adapt a key binding's value-layer ValueOps (hash/equals) to the
        // KeySlotStore hook shape (which passes (key, context) / (lhs, rhs,
        // context)). The context is the key binding's ValueOps.
        inline std::size_t key_hash_adapter(const void *key, const void *context)
        {
            return static_cast<const ValueOps *>(context)->hash(key);
        }
        inline bool key_equal_adapter(const void *lhs, const void *rhs, const void *context)
        {
            return static_cast<const ValueOps *>(context)->equals(lhs, rhs);
        }
        [[nodiscard]] inline KeySlotStoreOps key_ops_for(const ValueTypeRef &key_binding)
        {
            return KeySlotStoreOps{
                .hash    = &key_hash_adapter,
                .equal   = &key_equal_adapter,
                .context = &key_binding.ops_ref(),
            };
        }
    }  // namespace mutable_container_detail

    /**
     * Structurally-mutable value-layer ``Map`` storage. Keys live in a
     * ``KeySlotStore`` (hashed/compared via the key binding's ``ValueOps``);
     * values live in a parallel ``ValueSlotStore`` co-indexed by slot. Erase is
     * immediate (the key is removed and flushed, the value destroyed at once).
     * Always key/value-bound (the plan's lifecycle context carries the
     * bindings), so it never has an unbound state.
     */
    class MutableMapStorage
    {
      public:
        MutableMapStorage(const ValueTypeRef &key_binding, const ValueTypeRef &value_binding)
            : key_binding_{key_binding}
            , value_binding_{value_binding}
            , keys_{key_binding.checked_plan(), mutable_container_detail::key_ops_for(key_binding)}
            , values_{value_binding.checked_plan()}
        {
        }

        MutableMapStorage(const MutableMapStorage &other)
            : key_binding_{other.key_binding_}
            , value_binding_{other.value_binding_}
            , keys_{other.key_binding_.checked_plan(), mutable_container_detail::key_ops_for(other.key_binding_)}
            , values_{other.value_binding_.checked_plan()}
        {
            copy_entries_from(other);
        }

        MutableMapStorage &operator=(const MutableMapStorage &other)
        {
            if (this != &other)
            {
                values_.destroy_all();
                keys_          = KeySlotStore{other.key_binding_.checked_plan(),
                                              mutable_container_detail::key_ops_for(other.key_binding_)};
                values_        = ValueSlotStore{other.value_binding_.checked_plan()};
                key_binding_   = other.key_binding_;
                value_binding_ = other.value_binding_;
                copy_entries_from(other);
            }
            return *this;
        }

        MutableMapStorage(MutableMapStorage &&) noexcept            = default;
        MutableMapStorage &operator=(MutableMapStorage &&) noexcept = default;
        ~MutableMapStorage()                                       = default;

        [[nodiscard]] std::size_t             size() const noexcept { return keys_.size(); }
        [[nodiscard]] std::size_t             slot_capacity() const noexcept { return keys_.slot_capacity(); }
        [[nodiscard]] bool                    slot_live(std::size_t slot) const noexcept { return keys_.slot_live(slot); }
        [[nodiscard]] const ValueTypeRef &key_binding() const noexcept { return key_binding_; }
        [[nodiscard]] const ValueTypeRef &value_binding() const noexcept { return value_binding_; }

        [[nodiscard]] const void *key_at(std::size_t slot) const noexcept { return keys_.key_memory(slot); }
        [[nodiscard]] const void *value_at_slot(std::size_t slot) const noexcept { return values_.value_memory(slot); }

        [[nodiscard]] bool        contains(const void *key) const { return keys_.find_slot(key) != KeySlotStore::npos; }
        [[nodiscard]] const void *value_for(const void *key) const
        {
            const std::size_t slot = keys_.find_slot(key);
            return slot == KeySlotStore::npos ? nullptr : values_.value_memory(slot);
        }

        // Ordinal (nth-live) accessors for the indexed surface; O(n).
        [[nodiscard]] const void *key_at_ordinal(std::size_t ordinal) const { return nth_live_memory(ordinal, true); }
        [[nodiscard]] const void *value_at_ordinal(std::size_t ordinal) const { return nth_live_memory(ordinal, false); }

        /** Insert ``key`` -> ``value`` (copy), or replace the value if the key is present. */
        void insert(const void *key, const void *value)
        {
            const auto result = keys_.insert(key);  // may grow key capacity
            values_.reserve_to(keys_.slot_capacity());
            if (result.inserted) { values_.construct_at(result.slot, value); }
            else { value_binding_.checked_plan().copy_assign(values_.value_memory(result.slot), value); }
        }

        /**
         * Mutable value memory for ``key``, default-constructing an (empty)
         * value entry when the key is absent. Lets callers assign the value in
         * place rather than building and copying a temporary.
         */
        void *value_or_emplace(const void *key)
        {
            const std::size_t slot = keys_.find_slot(key);
            if (slot != KeySlotStore::npos) { return values_.value_memory(slot); }
            const auto result = keys_.insert(key);
            values_.reserve_to(keys_.slot_capacity());
            values_.construct_at(result.slot);  // default-construct the value
            return values_.value_memory(result.slot);
        }

        /** Remove ``key`` (and its value) immediately. Returns true if a key was removed. */
        bool erase(const void *key)
        {
            const std::size_t slot = keys_.find_slot(key);
            if (slot == KeySlotStore::npos) { return false; }
            values_.destroy_at(slot);
            (void)keys_.remove_slot(slot);
            keys_.erase_pending();  // flush this removal immediately
            return true;
        }

        /** Remove every entry. */
        void clear()
        {
            values_.destroy_all();
            keys_.clear();
        }

        [[nodiscard]] DebugDynamicLayout debug_layout() const noexcept
        {
            const auto *base = reinterpret_cast<const std::byte *>(this);
            const auto offset_of = [base](const auto *member) {
                return static_cast<std::size_t>(reinterpret_cast<const std::byte *>(member) - base);
            };
            const StableSlotDebugView key_slots = keys_.debug_slot_view();
            const StableSlotDebugView value_slots = values_.debug_slot_view();
            DebugDynamicFlags flags = DebugDynamicFlags::DataIsIndirect |
                                      DebugDynamicFlags::KeyDataIsIndirect |
                                      DebugDynamicFlags::DataIsPointerTable |
                                      DebugDynamicFlags::KeyDataIsPointerTable |
                                      DebugDynamicFlags::HasSlotState |
                                      DebugDynamicFlags::SlotIndexIsIndirect |
                                      DebugDynamicFlags::SlotSizeIsIndirect;
            if (value_slots.pointers_tagged) { flags = flags | DebugDynamicFlags::DataPointersAreTagged; }
            if (key_slots.pointers_tagged) { flags = flags | DebugDynamicFlags::KeyPointersAreTagged; }
            if (key_slots.state_tagged) { flags = flags | DebugDynamicFlags::SlotStateIsTaggedPointer; }
            return DebugDynamicLayout{
                .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
                .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
                .kind = DebugDynamicKind::StableSlots,
                .flags = flags,
                .key_auxiliary_offset =
                    static_cast<std::uint32_t>(key_slots.pointer_table_offset),
                .size_offset = key_slots.slot_count_offset,
                .data_offset = offset_of(value_slots.implementation_pointer),
                .stride = value_binding_.checked_plan().layout.size,
                .key_data_offset = offset_of(key_slots.implementation_pointer),
                .key_stride = key_binding_.checked_plan().layout.size,
                .state_offset = key_slots.state_offset,
                .auxiliary_offset = value_slots.pointer_table_offset,
            };
        }

        [[nodiscard]] DynamicStorageMetrics key_set_dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = keys_.dynamic_storage_metrics();
            const auto &ops = key_binding_.ops_ref();
            for (std::size_t slot = 0; slot < keys_.slot_capacity(); ++slot)
            {
                if (keys_.slot_constructed(slot))
                {
                    result += ops.dynamic_storage_metrics(keys_.key_memory(slot));
                }
            }
            return result;
        }

        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = key_set_dynamic_storage_metrics();
            result += values_.dynamic_storage_metrics();
            const auto &ops = value_binding_.ops_ref();
            for (std::size_t slot = 0; slot < values_.slot_capacity(); ++slot)
            {
                if (values_.has_slot(slot))
                {
                    result += ops.dynamic_storage_metrics(values_.value_memory(slot));
                }
            }
            return result;
        }

      private:
        ValueTypeRef key_binding_{nullptr};
        ValueTypeRef value_binding_{nullptr};
        KeySlotStore            keys_;
        ValueSlotStore          values_;

        [[nodiscard]] const void *nth_live_memory(std::size_t ordinal, bool key) const
        {
            std::size_t count = 0;
            for (std::size_t slot = 0; slot < keys_.slot_capacity(); ++slot)
            {
                if (!keys_.slot_live(slot)) { continue; }
                if (count == ordinal) { return key ? keys_.key_memory(slot) : values_.value_memory(slot); }
                ++count;
            }
            throw std::out_of_range("MutableMapStorage ordinal out of range");
        }

        void copy_entries_from(const MutableMapStorage &other)
        {
            for (std::size_t slot = 0; slot < other.keys_.slot_capacity(); ++slot)
            {
                if (other.keys_.slot_live(slot)) { insert(other.keys_.key_memory(slot), other.values_.value_memory(slot)); }
            }
        }
    };

    namespace mutable_container_detail
    {
        struct MutableMapState
        {
            ValueTypeRef key_binding{nullptr};
            ValueTypeRef value_binding{nullptr};
        };

        // -- lifecycle thunks --
        inline void map_construct(void *dst, const void *context)
        {
            const auto &state = compact_detail::checked_state<MutableMapState>(context, "mutable_map");
            if (state.key_binding == nullptr || state.value_binding == nullptr)
            {
                throw std::logic_error("mutable_map construction requires key and value bindings");
            }
            std::construct_at(static_cast<MutableMapStorage *>(dst), state.key_binding, state.value_binding);
        }
        inline void map_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<MutableMapStorage *>(memory));
        }
        inline void map_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<MutableMapStorage *>(dst), *static_cast<const MutableMapStorage *>(src));
        }
        inline void map_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<MutableMapStorage *>(dst), std::move(*static_cast<MutableMapStorage *>(src)));
        }
        inline void map_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<MutableMapStorage *>(dst) = *static_cast<const MutableMapStorage *>(src);
        }
        inline void map_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<MutableMapStorage *>(dst) = std::move(*static_cast<MutableMapStorage *>(src));
        }

        // -- read thunks --
        inline std::size_t map_size(const void *, const void *m) noexcept
        {
            return static_cast<const MutableMapStorage *>(m)->size();
        }
        inline bool map_contains(const void *, const void *m, const void *key)
        {
            return static_cast<const MutableMapStorage *>(m)->contains(key);
        }
        inline const void *map_value_at(const void *, const void *m, const void *key)
        {
            return static_cast<const MutableMapStorage *>(m)->value_for(key);
        }
        inline const void *map_key_at_index(const void *, const void *m, std::size_t i)
        {
            return static_cast<const MutableMapStorage *>(m)->key_at_ordinal(i);
        }
        inline const void *map_value_at_index(const void *, const void *m, std::size_t i)
        {
            return static_cast<const MutableMapStorage *>(m)->value_at_ordinal(i);
        }
        inline ValueTypeRef map_key_binding(const void *, const void *m, std::size_t) noexcept
        {
            return static_cast<const MutableMapStorage *>(m)->key_binding();
        }
        inline ValueTypeRef map_value_binding(const void *, const void *m) noexcept
        {
            return static_cast<const MutableMapStorage *>(m)->value_binding();
        }
        inline ValueTypeRef map_value_binding_indexed(const void *, const void *m, std::size_t) noexcept
        {
            return map_value_binding(nullptr, m);
        }

        // -- predicate-based ranges (skip dead slots) --
        inline bool map_slot_live(const void *, const void *m, std::size_t slot)
        {
            return static_cast<const MutableMapStorage *>(m)->slot_live(slot);
        }
        inline ValueView map_key_projector(const void *, const void *m, std::size_t slot)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            return ValueView{s->key_binding(), s->key_at(slot)};
        }
        inline ValueView map_value_projector(const void *, const void *m, std::size_t slot)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            return ValueView{s->value_binding(), s->value_at_slot(slot)};
        }
        inline std::pair<ValueView, ValueView> map_kv_projector(const void *, const void *m, std::size_t slot)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            return {ValueView{s->key_binding(), s->key_at(slot)}, ValueView{s->value_binding(), s->value_at_slot(slot)}};
        }
        inline Range<ValueView> map_make_keys_range(const void *, const void *m)
        {
            return Range<ValueView>{nullptr, m, static_cast<const MutableMapStorage *>(m)->slot_capacity(),
                                    &map_slot_live, &map_key_projector};
        }
        inline Range<ValueView> map_make_values_range(const void *, const void *m)
        {
            return Range<ValueView>{nullptr, m, static_cast<const MutableMapStorage *>(m)->slot_capacity(),
                                    &map_slot_live, &map_value_projector};
        }
        inline KeyValueRange<ValueView, ValueView> map_make_kv_range(const void *, const void *m)
        {
            return KeyValueRange<ValueView, ValueView>{nullptr, m,
                                                       static_cast<const MutableMapStorage *>(m)->slot_capacity(),
                                                       &map_slot_live, &map_kv_projector};
        }

        // Capture bindings in locals before calling ops_ref(). That keeps the
        // ValueTypeRef alive for the whole use site and avoids dangling
        // references from chained temporary expressions.
        // -- whole-map hash/equals/compare/to_string (order-independent) --
        inline std::size_t map_hash(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            const auto  key_binding   = s->key_binding();
            const auto  value_binding = s->value_binding();
            const auto &kops          = key_binding.ops_ref();
            const auto &vops          = value_binding.ops_ref();
            std::size_t acc  = 0;  // xor of per-entry hashes -> order-independent
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (!s->slot_live(slot)) { continue; }
                acc ^= container_ops_detail::combine_hash(kops.hash(s->key_at(slot)), vops.hash(s->value_at_slot(slot)));
            }
            return acc;
        }
        inline bool map_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MutableMapStorage *>(lhs);
            const auto *b = static_cast<const MutableMapStorage *>(rhs);
            if (a->size() != b->size()) { return false; }
            const auto  a_key_binding   = a->key_binding();
            const auto  a_value_binding = a->value_binding();
            if (a_key_binding != b->key_binding() || a_value_binding != b->value_binding()) { return false; }
            const auto &vops = a_value_binding.ops_ref();
            for (std::size_t slot = 0; slot < a->slot_capacity(); ++slot)
            {
                if (!a->slot_live(slot)) { continue; }
                const void *other_value = b->value_for(a->key_at(slot));
                if (other_value == nullptr || !vops.equals(a->value_at_slot(slot), other_value)) { return false; }
            }
            return true;
        }
        inline std::partial_ordering map_compare(const void *ctx, const void *lhs, const void *rhs) noexcept
        {
            // Maps have no natural order; equal maps are equivalent, otherwise unordered.
            return map_equals(ctx, lhs, rhs) ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
        }
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        inline nb::object mutable_map_to_python(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            nb::dict    result;
            const auto  key_binding   = s->key_binding();
            const auto  value_binding = s->value_binding();
            if (key_binding == nullptr) { return result; }
            const auto &kops = key_binding.ops_ref();
            const auto &vops = value_binding.ops_ref();
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (!s->slot_live(slot)) { continue; }
                result[kops.to_python(s->key_at(slot))] = vops.to_python(s->value_at_slot(slot));
            }
            return result;
        }
#endif

        inline std::string map_to_string(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            const auto  key_binding   = s->key_binding();
            const auto  value_binding = s->value_binding();
            const auto &kops          = key_binding.ops_ref();
            const auto &vops          = value_binding.ops_ref();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (!s->slot_live(slot)) { continue; }
                if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}: {}", kops.to_string(s->key_at(slot)),
                               vops.to_string(s->value_at_slot(slot)));
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

        // -- mutation thunks --
        inline void map_insert(const void *, void *m, const void *key, const void *value)
        {
            static_cast<MutableMapStorage *>(m)->insert(key, value);
        }
        inline void map_erase(const void *, void *m, const void *key)
        {
            (void)static_cast<MutableMapStorage *>(m)->erase(key);
        }
        inline void map_clear(const void *, void *m) { static_cast<MutableMapStorage *>(m)->clear(); }
        inline void *map_value_or_emplace(const void *, void *m, const void *key)
        {
            return static_cast<MutableMapStorage *>(m)->value_or_emplace(key);
        }

        // -- key-set adapter (read-only SetView over the live keys) --
        inline std::size_t map_key_set_hash(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            const auto  key_binding = s->key_binding();
            const auto &kops        = key_binding.ops_ref();
            std::size_t acc = 0;
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (s->slot_live(slot)) { acc ^= kops.hash(s->key_at(slot)); }
            }
            return acc;
        }
        inline bool map_key_set_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MutableMapStorage *>(lhs);
            const auto *b = static_cast<const MutableMapStorage *>(rhs);
            if (a->size() != b->size()) { return false; }
            for (std::size_t slot = 0; slot < a->slot_capacity(); ++slot)
            {
                if (a->slot_live(slot) && !b->contains(a->key_at(slot))) { return false; }
            }
            return true;
        }
        inline std::partial_ordering map_key_set_compare(const void *ctx, const void *lhs, const void *rhs) noexcept
        {
            return map_key_set_equals(ctx, lhs, rhs) ? std::partial_ordering::equivalent
                                                     : std::partial_ordering::unordered;
        }
        inline std::string map_key_set_to_string(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            const auto  key_binding = s->key_binding();
            const auto &kops        = key_binding.ops_ref();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (!s->slot_live(slot)) { continue; }
                if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}", kops.to_string(s->key_at(slot)));
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

        inline compact_detail::CompactContainerPlanRegistry<compact_detail::BinaryBindingKey, MutableMapState,
                                                            compact_detail::BinaryBindingKeyHash> &
        map_registry()
        {
            static compact_detail::CompactContainerPlanRegistry<compact_detail::BinaryBindingKey, MutableMapState,
                                                                compact_detail::BinaryBindingKeyHash>
                r;
            return r;
        }
    }  // namespace mutable_container_detail

    // Forward-declared: the key-set binding's plan is the map's own plan.
    [[nodiscard]] const MemoryUtils::StoragePlan &mutable_map_plan(const ValueTypeRef &key_binding,
                                                                   const ValueTypeRef &value_binding);

    [[nodiscard]] inline const SetValueOps &mutable_map_key_set_ops() noexcept
    {
        using namespace mutable_container_detail;
        static const SetValueOps ops = [] {
            auto value = SetValueOps{
            {{ValueOpsKind::Set,
              nullptr,
              false,
              &map_key_set_hash,
              &map_key_set_equals,
              &map_key_set_compare,
              &map_key_set_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
              ,
              nullptr,
              nullptr
#endif
             },
             &map_size,
             &map_key_at_index,
             &map_key_binding,
             &map_make_keys_range,
             nullptr},
            &map_contains,
            };
            value.dynamic_storage_metrics_impl = [](
                const void *, const void *memory) noexcept {
                return static_cast<const MutableMapStorage *>(memory)
                    ->key_set_dynamic_storage_metrics();
            };
            return value;
        }();
        return ops;
    }

    [[nodiscard]] inline ValueTypeRef mutable_map_key_set_type(const ValueTypeRef &key_binding,
                                                                             const ValueTypeRef &value_binding)
    {
        const auto *set_meta = TypeRegistry::instance().set(key_binding.schema());
        return intern_value_type(*set_meta, mutable_map_plan(key_binding, value_binding),
                                        mutable_map_key_set_ops());
    }

    namespace mutable_container_detail
    {
        inline SetView map_key_set_thunk(const void *, ValueTypeRef /*map_binding*/, const void *memory)
        {
            const auto *s = static_cast<const MutableMapStorage *>(memory);
            const auto set_binding = mutable_map_key_set_type(s->key_binding(), s->value_binding());
            return SetView{ValueView{set_binding, memory}};
        }

        inline void map_copy_assign_from(const void *, ValueTypeRef binding, void *memory, ValueTypeRef source,
                                         const void *src)
        {
            if (binding == source)
            {
                binding.copy_assign_at(memory, src);
                return;
            }
            auto &target = *static_cast<MutableMapStorage *>(memory);
            target.clear();
            const ValueView source_view{source, src};
            for (const auto entry : source_view.as_map())
            {
                auto key = materialize_element(target.key_binding(), entry.first);
                auto value = materialize_element(target.value_binding(), entry.second);
                target.insert(key.data(), value.data());
            }
        }

        inline void map_move_assign_from(const void *context, ValueTypeRef binding, void *memory, ValueTypeRef source,
                                         void *src)
        {
            if (binding == source)
            {
                binding.move_assign_at(memory, src);
                return;
            }
            map_copy_assign_from(context, binding, memory, source, src);
        }
    }  // namespace mutable_container_detail

    [[nodiscard]] inline const MemoryUtils::StoragePlan &mutable_map_plan(const ValueTypeRef &key_binding,
                                                                          const ValueTypeRef &value_binding)
    {
        using namespace mutable_container_detail;
        return map_registry().intern(
            compact_detail::BinaryBindingKey{.first = key_binding, .second = value_binding},
            [&] {
                return std::make_unique<MutableMapState>(
                    MutableMapState{.key_binding = key_binding, .value_binding = value_binding});
            },
            [&](const MutableMapState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<MutableMapStorage, &map_construct, &map_destroy,
                                                      &map_copy_construct, &map_move_construct, &map_copy_assign,
                                                      &map_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MutableMapValueOps &mutable_map_ops() noexcept
    {
        using namespace mutable_container_detail;
        static const MutableMapValueOps ops = [] {
            auto value = MutableMapValueOps{
            {{{// ValueOps:
               ValueOpsKind::MutableMap,
               nullptr,
               true,  // allows_mutation
               &map_hash,
               &map_equals,
               &map_compare,
               &map_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
               ,
               &mutable_map_to_python,
               nullptr
#endif
              },
              // IndexedValueOps (key surface):
              &map_size,
              &map_key_at_index,
              &map_key_binding,
              &map_make_keys_range,
              nullptr},
             // MapValueOps:
             &map_contains,
             &map_value_at,
             &map_value_at_index,
             &map_value_binding,
             &map_make_keys_range,
             &map_make_values_range,
             &map_make_kv_range,
             &map_key_set_thunk},
            // MutableMapValueOps:
            &map_insert,
            &map_erase,
            &map_clear,
            &map_value_or_emplace,
            };
            value.dynamic_storage_metrics_impl =
                &container_ops_detail::compact_dynamic_storage_metrics<MutableMapStorage>;
            value.accepts_source_impl = &accepts_container_source;
            value.copy_assign_from_impl = &map_copy_assign_from;
            value.move_assign_from_impl = &map_move_assign_from;
            return value;
        }();
        return ops;
    }

    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef mutable_map_type(const ValueTypeRef &key_binding,
                                                              const ValueTypeRef &value_binding);

    // -----------------------------------------------------------------
    // Mutable Set — a structurally-mutable value-layer set (the key-only
    // counterpart of MutableMapStorage). Elements live in a KeySlotStore
    // hashed/compared via the element binding's ValueOps; erase is immediate.
    // -----------------------------------------------------------------

    class MutableSetStorage
    {
      public:
        explicit MutableSetStorage(const ValueTypeRef &element_binding)
            : element_binding_{element_binding}
            , keys_{element_binding.checked_plan(), mutable_container_detail::key_ops_for(element_binding)}
        {
        }

        MutableSetStorage(const MutableSetStorage &other)
            : element_binding_{other.element_binding_}
            , keys_{other.element_binding_.checked_plan(), mutable_container_detail::key_ops_for(other.element_binding_)}
        {
            copy_keys_from(other);
        }

        MutableSetStorage &operator=(const MutableSetStorage &other)
        {
            if (this != &other)
            {
                keys_           = KeySlotStore{other.element_binding_.checked_plan(),
                                               mutable_container_detail::key_ops_for(other.element_binding_)};
                element_binding_ = other.element_binding_;
                copy_keys_from(other);
            }
            return *this;
        }

        MutableSetStorage(MutableSetStorage &&) noexcept            = default;
        MutableSetStorage &operator=(MutableSetStorage &&) noexcept = default;
        ~MutableSetStorage()                                       = default;

        [[nodiscard]] std::size_t             size() const noexcept { return keys_.size(); }
        [[nodiscard]] std::size_t             slot_capacity() const noexcept { return keys_.slot_capacity(); }
        [[nodiscard]] bool                    slot_live(std::size_t slot) const noexcept { return keys_.slot_live(slot); }
        [[nodiscard]] const ValueTypeRef &element_binding() const noexcept { return element_binding_; }
        [[nodiscard]] const void             *key_at(std::size_t slot) const noexcept { return keys_.key_memory(slot); }
        [[nodiscard]] bool                    contains(const void *key) const { return keys_.find_slot(key) != KeySlotStore::npos; }
        [[nodiscard]] const void             *key_at_ordinal(std::size_t ordinal) const { return nth_live_memory(ordinal); }

        /** Add ``key`` (copy); returns true when the set changed (key was absent). */
        bool add(const void *key) { return keys_.insert(key).inserted; }

        /** Remove ``key`` immediately; returns true when a key was removed. */
        bool remove(const void *key)
        {
            const std::size_t slot = keys_.find_slot(key);
            if (slot == KeySlotStore::npos) { return false; }
            (void)keys_.remove_slot(slot);
            keys_.erase_pending();
            return true;
        }

        void clear() { keys_.clear(); }

        [[nodiscard]] DebugDynamicLayout debug_layout() const noexcept
        {
            const auto *base = reinterpret_cast<const std::byte *>(this);
            const auto offset_of = [base](const auto *member) {
                return static_cast<std::size_t>(reinterpret_cast<const std::byte *>(member) - base);
            };
            const StableSlotDebugView slots = keys_.debug_slot_view();
            DebugDynamicFlags flags = DebugDynamicFlags::DataIsIndirect |
                                      DebugDynamicFlags::DataIsPointerTable |
                                      DebugDynamicFlags::HasSlotState |
                                      DebugDynamicFlags::SlotIndexIsIndirect |
                                      DebugDynamicFlags::SlotSizeIsIndirect;
            if (slots.pointers_tagged) { flags = flags | DebugDynamicFlags::DataPointersAreTagged; }
            if (slots.state_tagged) { flags = flags | DebugDynamicFlags::SlotStateIsTaggedPointer; }
            return DebugDynamicLayout{
                .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
                .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
                .kind = DebugDynamicKind::StableSlots,
                .flags = flags,
                .size_offset = slots.slot_count_offset,
                .data_offset = offset_of(slots.implementation_pointer),
                .stride = element_binding_.checked_plan().layout.size,
                .state_offset = slots.state_offset,
                .auxiliary_offset = slots.pointer_table_offset,
            };
        }

        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = keys_.dynamic_storage_metrics();
            const auto &ops = element_binding_.ops_ref();
            for (std::size_t slot = 0; slot < keys_.slot_capacity(); ++slot)
            {
                if (keys_.slot_constructed(slot))
                {
                    result += ops.dynamic_storage_metrics(keys_.key_memory(slot));
                }
            }
            return result;
        }

      private:
        ValueTypeRef element_binding_{nullptr};
        KeySlotStore            keys_;

        [[nodiscard]] const void *nth_live_memory(std::size_t ordinal) const
        {
            std::size_t count = 0;
            for (std::size_t slot = 0; slot < keys_.slot_capacity(); ++slot)
            {
                if (!keys_.slot_live(slot)) { continue; }
                if (count == ordinal) { return keys_.key_memory(slot); }
                ++count;
            }
            throw std::out_of_range("MutableSetStorage ordinal out of range");
        }

        void copy_keys_from(const MutableSetStorage &other)
        {
            for (std::size_t slot = 0; slot < other.keys_.slot_capacity(); ++slot)
            {
                if (other.keys_.slot_live(slot)) { (void)add(other.keys_.key_memory(slot)); }
            }
        }
    };

    namespace mutable_container_detail
    {
        struct MutableSetState
        {
            ValueTypeRef element_binding{nullptr};
        };

        inline void set_construct(void *dst, const void *context)
        {
            const auto &state = compact_detail::checked_state<MutableSetState>(context, "mutable_set");
            if (state.element_binding == nullptr) { throw std::logic_error("mutable_set construction requires an element binding"); }
            std::construct_at(static_cast<MutableSetStorage *>(dst), state.element_binding);
        }
        inline void set_destroy(void *memory, const void *) noexcept { std::destroy_at(static_cast<MutableSetStorage *>(memory)); }
        inline void set_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<MutableSetStorage *>(dst), *static_cast<const MutableSetStorage *>(src));
        }
        inline void set_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<MutableSetStorage *>(dst), std::move(*static_cast<MutableSetStorage *>(src)));
        }
        inline void set_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<MutableSetStorage *>(dst) = *static_cast<const MutableSetStorage *>(src);
        }
        inline void set_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<MutableSetStorage *>(dst) = std::move(*static_cast<MutableSetStorage *>(src));
        }

        inline std::size_t set_size(const void *, const void *m) noexcept { return static_cast<const MutableSetStorage *>(m)->size(); }
        inline bool        set_contains(const void *, const void *m, const void *key) { return static_cast<const MutableSetStorage *>(m)->contains(key); }
        inline const void *set_element_at(const void *, const void *m, std::size_t i) { return static_cast<const MutableSetStorage *>(m)->key_at_ordinal(i); }
        inline ValueTypeRef set_element_binding(const void *, const void *m, std::size_t) noexcept
        {
            return static_cast<const MutableSetStorage *>(m)->element_binding();
        }
        inline bool set_slot_live(const void *, const void *m, std::size_t slot) { return static_cast<const MutableSetStorage *>(m)->slot_live(slot); }
        inline ValueView set_element_projector(const void *, const void *m, std::size_t slot)
        {
            const auto *s = static_cast<const MutableSetStorage *>(m);
            return ValueView{s->element_binding(), s->key_at(slot)};
        }
        inline Range<ValueView> set_make_range(const void *, const void *m)
        {
            return Range<ValueView>{nullptr, m, static_cast<const MutableSetStorage *>(m)->slot_capacity(),
                                    &set_slot_live, &set_element_projector};
        }

        inline std::size_t set_hash(const void *, const void *m)
        {
            const auto *s    = static_cast<const MutableSetStorage *>(m);
            const auto element_binding = s->element_binding();
            const auto &eops = element_binding.ops_ref();
            std::size_t acc  = 0;  // xor of element hashes -> order-independent
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (s->slot_live(slot)) { acc ^= eops.hash(s->key_at(slot)); }
            }
            return acc;
        }
        inline bool set_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MutableSetStorage *>(lhs);
            const auto *b = static_cast<const MutableSetStorage *>(rhs);
            if (a->size() != b->size()) { return false; }
            for (std::size_t slot = 0; slot < a->slot_capacity(); ++slot)
            {
                if (a->slot_live(slot) && !b->contains(a->key_at(slot))) { return false; }
            }
            return true;
        }
        inline std::partial_ordering set_compare(const void *ctx, const void *lhs, const void *rhs) noexcept
        {
            return set_equals(ctx, lhs, rhs) ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
        }
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        inline nb::object mutable_set_to_python(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableSetStorage *>(m);
            nb::list    items;
            if (s->element_binding() != nullptr)
            {
                const auto element_binding = s->element_binding();
                const auto &eops = element_binding.ops_ref();
                for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
                {
                    if (!s->slot_live(slot)) { continue; }
                    items.append(eops.to_python(s->key_at(slot)));
                }
            }
            return nb::steal(PyFrozenSet_New(items.ptr()));
        }
#endif

        inline std::string set_to_string(const void *, const void *m)
        {
            const auto *s    = static_cast<const MutableSetStorage *>(m);
            const auto element_binding = s->element_binding();
            const auto &eops = element_binding.ops_ref();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (!s->slot_live(slot)) { continue; }
                if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}", eops.to_string(s->key_at(slot)));
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

        inline bool set_add(const void *, void *m, const void *key) { return static_cast<MutableSetStorage *>(m)->add(key); }
        inline bool set_remove(const void *, void *m, const void *key) { return static_cast<MutableSetStorage *>(m)->remove(key); }
        inline void set_clear(const void *, void *m) { static_cast<MutableSetStorage *>(m)->clear(); }

        inline void set_copy_assign_from(const void *, ValueTypeRef binding, void *memory, ValueTypeRef source,
                                         const void *src)
        {
            if (binding == source)
            {
                binding.copy_assign_at(memory, src);
                return;
            }
            auto &target = *static_cast<MutableSetStorage *>(memory);
            target.clear();
            const ValueView source_view{source, src};
            for (const auto child : source_view.as_set())
            {
                auto value = materialize_element(target.element_binding(), child);
                static_cast<void>(target.add(value.data()));
            }
        }

        inline void set_move_assign_from(const void *context, ValueTypeRef binding, void *memory, ValueTypeRef source,
                                         void *src)
        {
            if (binding == source)
            {
                binding.move_assign_at(memory, src);
                return;
            }
            set_copy_assign_from(context, binding, memory, source, src);
        }

        inline compact_detail::CompactContainerPlanRegistry<compact_detail::UnaryBindingKey, MutableSetState,
                                                            compact_detail::UnaryBindingKeyHash> &
        set_registry()
        {
            static compact_detail::CompactContainerPlanRegistry<compact_detail::UnaryBindingKey, MutableSetState,
                                                                compact_detail::UnaryBindingKeyHash>
                r;
            return r;
        }
    }  // namespace mutable_container_detail

    [[nodiscard]] inline const MemoryUtils::StoragePlan &mutable_set_plan(const ValueTypeRef &element_binding)
    {
        using namespace mutable_container_detail;
        return set_registry().intern(
            compact_detail::UnaryBindingKey{.binding = element_binding},
            [&] { return std::make_unique<MutableSetState>(MutableSetState{.element_binding = element_binding}); },
            [&](const MutableSetState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<MutableSetStorage, &set_construct, &set_destroy,
                                                      &set_copy_construct, &set_move_construct, &set_copy_assign,
                                                      &set_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MutableSetValueOps &mutable_set_ops() noexcept
    {
        using namespace mutable_container_detail;
        static const MutableSetValueOps ops = [] {
            auto value = MutableSetValueOps{
            {{// IndexedValueOps:
              {// ValueOps:
               ValueOpsKind::MutableSet,
               nullptr,
               true,  // allows_mutation
               &set_hash,
               &set_equals,
               &set_compare,
               &set_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
               ,
               &mutable_set_to_python,
               nullptr
#endif
              },
              &set_size,
              &set_element_at,
              &set_element_binding,
              &set_make_range,
              nullptr},
             // SetValueOps:
             &set_contains},
            // MutableSetValueOps:
            &set_add,
            &set_remove,
            &set_clear,
            };
            value.dynamic_storage_metrics_impl =
                &container_ops_detail::compact_dynamic_storage_metrics<MutableSetStorage>;
            value.accepts_source_impl = &accepts_container_source;
            value.copy_assign_from_impl = &set_copy_assign_from;
            value.move_assign_from_impl = &set_move_assign_from;
            return value;
        }();
        return ops;
    }

    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef mutable_set_type(const ValueTypeRef &element_binding);

    HGRAPH_EXPORT void clear_mutable_container_plans() noexcept;
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_MUTABLE_CONTAINER_OPS_H
