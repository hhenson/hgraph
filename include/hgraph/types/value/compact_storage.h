#ifndef HGRAPH_CPP_ROOT_VALUE_COMPACT_STORAGE_H
#define HGRAPH_CPP_ROOT_VALUE_COMPACT_STORAGE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/util/scope.h>

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    class ListBuilder;

    /**
     * Compact, immutable-after-construction storage shapes for the value
     * layer's container kinds. See *Allocation, Plans and Ops > Scalar
     * Plans and Ops > Container Storage Shapes* for the design narrative.
     *
     * Each storage class:
     *
     * - is sized at construction time (no growth, no shrink),
     * - exposes only read-time access (per-element mutation is a TS-layer
     *   concern; the value-layer view is read-only),
     * - is built either through a one-shot constructor (taking a
     *   pre-assembled element span) or through the matching
     *   :class:`ValueBuilder` (see ``value_builder.h``).
     */

    // -----------------------------------------------------------------
    // Element span — a borrowed view of N source elements with the
    // matching element plan. Used as the construction input for every
    // compact storage shape so callers can supply elements either from
    // raw memory, from a builder's accumulated buffer, or from another
    // storage's element memory.
    // -----------------------------------------------------------------

    struct ElementSpan
    {
        const void *bytes{nullptr};
        std::size_t size{0};                // number of elements
        std::size_t stride{0};              // bytes between elements
        const MemoryUtils::StoragePlan *plan{nullptr};

        [[nodiscard]] const void *at(std::size_t index) const noexcept
        {
            return static_cast<const std::byte *>(bytes) + index * stride;
        }
    };

    namespace compact_detail
    {
        [[nodiscard]] inline void *allocate_element_buffer(const MemoryUtils::StoragePlan &plan, std::size_t count)
        {
            if (count == 0) { return nullptr; }
            if (plan.layout.size != 0 && count > std::numeric_limits<std::size_t>::max() / plan.layout.size)
            {
                throw std::bad_array_new_length();
            }
            return ::operator new(count * plan.layout.size, std::align_val_t{plan.layout.alignment});
        }

        inline void deallocate_element_buffer(void *buffer, const MemoryUtils::StoragePlan &plan, std::size_t count) noexcept
        {
            if (buffer == nullptr || count == 0) { return; }
            ::operator delete(buffer, std::align_val_t{plan.layout.alignment});
        }

        inline void destroy_elements_reverse(void *buffer, const MemoryUtils::StoragePlan &plan, std::size_t count) noexcept
        {
            if (buffer == nullptr) { return; }
            const std::size_t stride = plan.layout.size;
            for (std::size_t index = count; index > 0; --index)
            {
                plan.destroy(static_cast<std::byte *>(buffer) + (index - 1) * stride);
            }
        }

        inline void copy_construct_elements(void *dst_buffer, const ElementSpan &src) noexcept(false)
        {
            const std::size_t stride      = src.plan->layout.size;
            std::size_t       constructed = 0;
            auto rollback = make_scope_exit([&]() noexcept {
                destroy_elements_reverse(dst_buffer, *src.plan, constructed);
            });
            for (constructed = 0; constructed < src.size; ++constructed)
            {
                src.plan->copy_construct(static_cast<std::byte *>(dst_buffer) + constructed * stride,
                                         src.at(constructed));
            }
            rollback.release();
        }

        [[nodiscard]] inline std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        struct LookupKey
        {
            const void *key{nullptr};
        };

        struct SlotIndexContext
        {
            ValueTypeRef binding{nullptr};
            const void             *owner{nullptr};
            const void *(*at)(const void *owner, std::size_t slot) noexcept{nullptr};
            std::size_t allocated_bytes{0};
        };

        template <typename T>
        struct SlotIndexAllocator
        {
            using value_type = T;
            using propagate_on_container_move_assignment = std::true_type;
            using is_always_equal = std::false_type;

            SlotIndexContext *context{nullptr};

            constexpr SlotIndexAllocator() noexcept = default;
            explicit constexpr SlotIndexAllocator(SlotIndexContext *value) noexcept
                : context{value}
            {
            }

            template <typename U>
            constexpr SlotIndexAllocator(const SlotIndexAllocator<U> &other) noexcept
                : context{other.context}
            {
            }

            [[nodiscard]] T *allocate(std::size_t count)
            {
                T *result = std::allocator<T>{}.allocate(count);
                if (context != nullptr) { context->allocated_bytes += count * sizeof(T); }
                return result;
            }

            void deallocate(T *memory, std::size_t count) noexcept
            {
                if (context != nullptr) { context->allocated_bytes -= count * sizeof(T); }
                std::allocator<T>{}.deallocate(memory, count);
            }

            template <typename U>
            [[nodiscard]] constexpr bool operator==(const SlotIndexAllocator<U> &other) const noexcept
            {
                return context == other.context;
            }

            template <typename>
            friend struct SlotIndexAllocator;
        };

        // The hash/equal functors hold a RAW pointer to the SlotIndexContext the
        // owning SlotIndex uniquely owns on the heap: the pointee's address is
        // stable across SlotIndex moves, and single-threaded evaluation means no
        // shared ownership is needed (a shared_ptr here put atomic refcounting on
        // the per-tick value path — banned by the single-threaded rule).
        struct SlotHash
        {
            using is_transparent = void;

            const SlotIndexContext *context{nullptr};

            [[nodiscard]] std::size_t operator()(std::int32_t slot) const
            {
                return context->binding.ops_ref().hash(context->at(context->owner, static_cast<std::size_t>(slot)));
            }

            [[nodiscard]] std::size_t operator()(LookupKey key) const
            {
                return context->binding.ops_ref().hash(key.key);
            }
        };

        struct SlotEqual
        {
            using is_transparent = void;

            const SlotIndexContext *context{nullptr};

            [[nodiscard]] bool operator()(std::int32_t lhs, std::int32_t rhs) const
            {
                const auto &ops = context->binding.ops_ref();
                return ops.equals(context->at(context->owner, static_cast<std::size_t>(lhs)),
                                  context->at(context->owner, static_cast<std::size_t>(rhs)));
            }

            [[nodiscard]] bool operator()(LookupKey lhs, std::int32_t rhs) const
            {
                return context->binding.ops_ref().equals(
                    lhs.key, context->at(context->owner, static_cast<std::size_t>(rhs)));
            }

            [[nodiscard]] bool operator()(std::int32_t lhs, LookupKey rhs) const
            {
                return context->binding.ops_ref().equals(
                    context->at(context->owner, static_cast<std::size_t>(lhs)), rhs.key);
            }

            [[nodiscard]] bool operator()(LookupKey lhs, LookupKey rhs) const
            {
                return context->binding.ops_ref().equals(lhs.key, rhs.key);
            }
        };

        class SlotIndex
        {
          public:
            using slot_type = std::int32_t;
            using index_type = ankerl::unordered_dense::set<
                slot_type, SlotHash, SlotEqual, SlotIndexAllocator<slot_type>>;

            SlotIndex()
                : context_{std::make_unique<SlotIndexContext>()}
                , slots_{0, SlotHash{context_.get()}, SlotEqual{context_.get()},
                         SlotIndexAllocator<slot_type>{context_.get()}}
            {
            }

            SlotIndex(const SlotIndex &)            = delete;
            SlotIndex &operator=(const SlotIndex &) = delete;

            SlotIndex(SlotIndex &&other)
                : context_{std::move(other.context_)}
                , slots_{std::move(other.slots_)}
            {
                other.reset_empty_context();
            }

            SlotIndex &operator=(SlotIndex &&other)
            {
                if (this != &other)
                {
                    slots_   = std::move(other.slots_);
                    context_ = std::move(other.context_);
                    other.reset_empty_context();
                }
                return *this;
            }

            void reset(SlotIndexContext context, std::size_t reserve)
            {
                rebind(context);
                slots_.clear();
                slots_.reserve(reserve);
            }

            void rebind(SlotIndexContext context) noexcept
            {
                context_->binding = context.binding;
                context_->owner   = context.owner;
                context_->at      = context.at;
            }

            void clear() noexcept
            {
                slots_.clear();
                context_->binding = nullptr;
                context_->owner   = nullptr;
                context_->at      = nullptr;
            }

            [[nodiscard]] bool empty() const noexcept { return slots_.empty(); }

            [[nodiscard]] std::size_t size() const noexcept { return slots_.size(); }

            [[nodiscard]] bool insert(std::size_t slot)
            {
                return slots_.insert(checked_slot(slot)).second;
            }

            [[nodiscard]] std::optional<slot_type> find(const void *key) const
            {
                const auto it = slots_.find(LookupKey{key});
                if (it == slots_.end()) { return std::nullopt; }
                return *it;
            }

            [[nodiscard]] bool contains(const void *key) const { return find(key).has_value(); }

            [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
            {
                const auto live_index_bytes = slots_.size() *
                    (sizeof(typename index_type::value_type) +
                     sizeof(typename index_type::bucket_type));
                return {
                    .live_bytes = sizeof(SlotIndexContext) + live_index_bytes,
                    .reserved_bytes = sizeof(SlotIndexContext) + context_->allocated_bytes,
                };
            }

          private:
            [[nodiscard]] static slot_type checked_slot(std::size_t slot)
            {
                if (slot > static_cast<std::size_t>(std::numeric_limits<slot_type>::max()))
                {
                    throw std::length_error("Compact slot index exceeds supported size");
                }
                return static_cast<slot_type>(slot);
            }

            std::unique_ptr<SlotIndexContext> context_;
            index_type slots_;

            // A moved-from SlotIndex gets a fresh context + empty set so it stays
            // fully usable (reset/rebind/find) — the move transfers the uniquely
            // owned context, whose stable heap address keeps the destination
            // set's raw functor pointers valid.
            void reset_empty_context()
            {
                context_ = std::make_unique<SlotIndexContext>();
                slots_   = index_type{
                    0, SlotHash{context_.get()}, SlotEqual{context_.get()},
                    SlotIndexAllocator<slot_type>{context_.get()}};
            }
        };
    }  // namespace compact_detail

    // -----------------------------------------------------------------
    // ListStorage — sized contiguous buffer of N elements.
    // -----------------------------------------------------------------

    /**
     * Compact value-layer storage for the ``List`` kind. Holds exactly
     * the number of elements supplied at construction. Cannot be resized
     * and individual elements cannot be replaced; whole-container
     * replacement happens at the ``Value`` level.
     */
    class ListStorage
    {
      public:
        ListStorage() noexcept = default;

        ListStorage(const ValueTypeRef &element_binding, const ElementSpan &source)
            : ListStorage(element_binding, source, {})
        {
        }

        /** Construct with an element-VALIDITY bitset (element validity,
            core_concepts.rst: an EMPTY bitset means dense / all-set - the
            unknown-size counterpart to the fixed-tuple pre-allocated words).
            ``validity`` is one bool per element; empty leaves the list dense. */
        ListStorage(const ValueTypeRef &element_binding, const ElementSpan &source,
                    std::vector<bool> validity)
            : size_{source.size}, element_binding_{element_binding}
        {
            const auto &plan = element_binding.checked_plan();
            bytes_ = compact_detail::allocate_element_buffer(plan, size_);
            auto rollback = make_scope_exit([&]() noexcept {
                compact_detail::deallocate_element_buffer(bytes_, plan, size_);
            });
            compact_detail::copy_construct_elements(bytes_, source);
            rollback.release();
            if (!validity.empty())
            {
                // Only retain the bitset if it actually carries a hole; a
                // fully-set bitset stays dense (empty) for zero overhead.
                for (std::size_t index = 0; index < validity.size(); ++index)
                {
                    if (!validity[index]) { validity_ = std::move(validity); break; }
                }
            }
        }

        ListStorage(const ListStorage &other) { copy_from(other); }

        ListStorage &operator=(const ListStorage &other)
        {
            if (this != &other)
            {
                clear();
                copy_from(other);
            }
            return *this;
        }

        ListStorage(ListStorage &&other) noexcept
            : bytes_{other.bytes_}, size_{other.size_}, element_binding_{other.element_binding_}
            , validity_{std::move(other.validity_)}
        {
            other.bytes_           = nullptr;
            other.size_            = 0;
            other.element_binding_ = nullptr;
            other.validity_.clear();
        }

        ListStorage &operator=(ListStorage &&other) noexcept
        {
            if (this != &other)
            {
                clear();
                bytes_                 = other.bytes_;
                size_                  = other.size_;
                element_binding_       = other.element_binding_;
                validity_              = std::move(other.validity_);
                other.bytes_           = nullptr;
                other.size_            = 0;
                other.element_binding_ = nullptr;
                other.validity_.clear();
            }
            return *this;
        }

        ~ListStorage() { clear(); }

        [[nodiscard]] std::size_t              size() const noexcept { return size_; }
        [[nodiscard]] bool                     empty() const noexcept { return size_ == 0; }
        [[nodiscard]] ValueTypeRef element_binding() const noexcept { return element_binding_; }
        [[nodiscard]] const MemoryUtils::StoragePlan *element_plan() const noexcept
        {
            return element_binding_ ? element_binding_.plan() : nullptr;
        }

        [[nodiscard]] void *element_at(std::size_t index)
        {
            if (index >= size_) { throw std::out_of_range("ListStorage index out of range"); }
            return static_cast<std::byte *>(bytes_) + index * element_binding_.checked_plan().layout.size;
        }
        [[nodiscard]] const void *element_at(std::size_t index) const
        {
            if (index >= size_) { throw std::out_of_range("ListStorage index out of range"); }
            return static_cast<const std::byte *>(bytes_) +
                   index * element_binding_.checked_plan().layout.size;
        }

        /** True when element ``index`` holds a live value (dense when the
            validity bitset is empty). */
        [[nodiscard]] bool element_set(std::size_t index) const noexcept
        {
            return validity_.empty() || (index < validity_.size() && validity_[index]);
        }

        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            if (element_binding_ == nullptr) { return {}; }

            const auto stride = element_binding_.checked_plan().layout.size;
            DynamicStorageMetrics result{};
            const auto &ops = element_binding_.ops_ref();
            for (std::size_t index = 0; index < size_; ++index)
            {
                result += ops.dynamic_storage_metrics(element_at(index));
            }

            result.live_bytes += size_ * stride;
            result.reserved_bytes += size_ * stride;
            if (!validity_.empty())
            {
                constexpr std::size_t bits_per_byte =
                    std::numeric_limits<unsigned char>::digits;
                result.live_bytes +=
                    (validity_.size() + bits_per_byte - 1U) / bits_per_byte;
                result.reserved_bytes +=
                    (validity_.capacity() + bits_per_byte - 1U) / bits_per_byte;
            }
            return result;
        }

        [[nodiscard]] static constexpr std::size_t debug_data_offset() noexcept
        {
            return offsetof(ListStorage, bytes_);
        }
        [[nodiscard]] static constexpr std::size_t debug_size_offset() noexcept
        {
            return offsetof(ListStorage, size_);
        }

      private:
        friend class ListBuilder;

        struct AdoptElementsTag
        {
        };

        ListStorage(AdoptElementsTag, const ValueTypeRef &element_binding,
                    void *bytes, std::size_t size, std::vector<bool> validity) noexcept
            : bytes_{bytes}, size_{size}, element_binding_{element_binding},
              validity_{std::move(validity)}
        {
        }

        void clear() noexcept
        {
            if (element_binding_ == nullptr) { return; }
            const auto &plan = element_binding_.checked_plan();
            compact_detail::destroy_elements_reverse(bytes_, plan, size_);
            compact_detail::deallocate_element_buffer(bytes_, plan, size_);
            bytes_           = nullptr;
            size_            = 0;
            element_binding_ = nullptr;
            validity_.clear();
        }

        void copy_from(const ListStorage &other)
        {
            element_binding_ = other.element_binding_;
            size_            = other.size_;
            validity_        = other.validity_;
            if (element_binding_ == nullptr) { return; }
            const auto &plan = element_binding_.checked_plan();
            bytes_           = compact_detail::allocate_element_buffer(plan, size_);
            auto rollback = make_scope_exit([&]() noexcept {
                compact_detail::deallocate_element_buffer(bytes_, plan, size_);
                bytes_           = nullptr;
                size_            = 0;
                element_binding_ = nullptr;
            });
            compact_detail::copy_construct_elements(
                bytes_, ElementSpan{
                            .bytes  = other.bytes_,
                            .size   = other.size_,
                            .stride = plan.layout.size,
                            .plan   = &plan,
                        });
            rollback.release();
        }

        void                    *bytes_{nullptr};
        std::size_t              size_{0};
        ValueTypeRef element_binding_{nullptr};
        std::vector<bool>        validity_{};   // empty = dense (all-set)
    };

    // -----------------------------------------------------------------
    // CyclicBufferStorage — sized buffer + ring-read interpretation.
    // -----------------------------------------------------------------

    /**
     * Compact value-layer storage for the ``CyclicBuffer`` kind. Holds
     * exactly the elements supplied at construction in arrival order and
     * remembers the logical *head* offset so the read view can walk them
     * in ring order. The buffer is immutable once constructed; the
     * "rolling overwrite" semantics belong to the slot-store-based
     * time-series variant.
     */
    class CyclicBufferStorage
    {
      public:
        CyclicBufferStorage() noexcept = default;

        CyclicBufferStorage(const ValueTypeRef &element_binding, const ElementSpan &source, std::size_t head)
            : storage_{element_binding, source}, head_{source.size == 0 ? 0 : head % source.size}
        {
        }

        [[nodiscard]] std::size_t              size() const noexcept { return storage_.size(); }
        [[nodiscard]] bool                     empty() const noexcept { return storage_.empty(); }
        [[nodiscard]] std::size_t              head() const noexcept { return head_; }
        [[nodiscard]] ValueTypeRef element_binding() const noexcept { return storage_.element_binding(); }

        [[nodiscard]] std::size_t physical_index(std::size_t logical_index) const
        {
            const std::size_t n = storage_.size();
            if (logical_index >= n) { throw std::out_of_range("CyclicBufferStorage index out of range"); }
            return n == 0 ? 0 : (head_ + logical_index) % n;
        }

        [[nodiscard]] void *element_at(std::size_t logical_index)
        {
            return storage_.element_at(physical_index(logical_index));
        }
        [[nodiscard]] const void *element_at(std::size_t logical_index) const
        {
            return storage_.element_at(physical_index(logical_index));
        }

        /** Dense container: every element is live (holes are a
            list-only concern - element validity). */
        [[nodiscard]] bool element_set(std::size_t) const noexcept { return true; }

        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            return storage_.dynamic_storage_metrics();
        }

        [[nodiscard]] static constexpr std::size_t debug_data_offset() noexcept
        {
            return offsetof(CyclicBufferStorage, storage_) + ListStorage::debug_data_offset();
        }
        [[nodiscard]] static constexpr std::size_t debug_size_offset() noexcept
        {
            return offsetof(CyclicBufferStorage, storage_) + ListStorage::debug_size_offset();
        }
        [[nodiscard]] static constexpr std::size_t debug_head_offset() noexcept
        {
            return offsetof(CyclicBufferStorage, head_);
        }

      private:
        ListStorage storage_{};
        std::size_t head_{0};
    };

    // -----------------------------------------------------------------
    // QueueStorage — sized buffer in arrival order.
    // -----------------------------------------------------------------

    /**
     * Compact value-layer storage for the ``Queue`` kind. Holds the
     * queued elements in arrival order; the read view walks them
     * front-to-back. Immutable after construction.
     */
    class QueueStorage
    {
      public:
        QueueStorage() noexcept = default;

        QueueStorage(const ValueTypeRef &element_binding, const ElementSpan &source)
            : storage_{element_binding, source}
        {
        }

        [[nodiscard]] std::size_t             size() const noexcept { return storage_.size(); }
        [[nodiscard]] bool                    empty() const noexcept { return storage_.empty(); }
        [[nodiscard]] ValueTypeRef element_binding() const noexcept { return storage_.element_binding(); }

        [[nodiscard]] void       *front() { return storage_.element_at(0); }
        [[nodiscard]] const void *front() const { return storage_.element_at(0); }

        [[nodiscard]] void       *element_at(std::size_t index) { return storage_.element_at(index); }
        [[nodiscard]] const void *element_at(std::size_t index) const { return storage_.element_at(index); }

        /** Dense container: every element is live (holes are a
            list-only concern - element validity). */
        [[nodiscard]] bool element_set(std::size_t) const noexcept { return true; }

        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            return storage_.dynamic_storage_metrics();
        }

        [[nodiscard]] static constexpr std::size_t debug_data_offset() noexcept
        {
            return offsetof(QueueStorage, storage_) + ListStorage::debug_data_offset();
        }
        [[nodiscard]] static constexpr std::size_t debug_size_offset() noexcept
        {
            return offsetof(QueueStorage, storage_) + ListStorage::debug_size_offset();
        }

      private:
        ListStorage storage_{};
    };

    // -----------------------------------------------------------------
    // SetStorage — content-keyed hash set, populated once at
    // construction.
    // -----------------------------------------------------------------

    /**
     * Compact value-layer storage for the ``Set`` kind. Holds the keys
     * in a contiguous buffer plus a dense slot index for lookup. Hash
     * and equality are driven by the bound element ops via the borrowed
     * binding.
     *
     * Two sets are *equal* when they contain the same keys regardless of
     * insertion order; the underlying buffer's order is preserved as
     * supplied at construction so iteration is deterministic for that
     * single instance.
     */
    class SetStorage
    {
      public:
        SetStorage() noexcept = default;

        SetStorage(const ValueTypeRef &element_binding, const ElementSpan &source)
            : element_binding_{element_binding}, storage_{element_binding, source}
        {
            ensure_ops();
            build_index();
        }

        SetStorage(const SetStorage &other) { copy_from(other); }
        SetStorage &operator=(const SetStorage &other)
        {
            if (this != &other)
            {
                index_.clear();
                storage_ = ListStorage{};
                copy_from(other);
            }
            return *this;
        }

        SetStorage(SetStorage &&other)
            : element_binding_{other.element_binding_}
            , storage_{std::move(other.storage_)}
            , index_{std::move(other.index_)}
        {
            index_.rebind(slot_context());
            other.element_binding_ = nullptr;
        }

        SetStorage &operator=(SetStorage &&other)
        {
            if (this != &other)
            {
                element_binding_ = other.element_binding_;
                storage_         = std::move(other.storage_);
                index_           = std::move(other.index_);
                index_.rebind(slot_context());
                other.element_binding_ = nullptr;
            }
            return *this;
        }

        [[nodiscard]] std::size_t              size() const noexcept { return storage_.size(); }
        [[nodiscard]] bool                     empty() const noexcept { return storage_.empty(); }
        [[nodiscard]] ValueTypeRef element_binding() const noexcept { return element_binding_; }

        [[nodiscard]] const void *element_at(std::size_t index) const { return storage_.element_at(index); }

        [[nodiscard]] bool contains(const void *key) const
        {
            if (storage_.empty() || element_binding_ == nullptr) { return false; }
            return index_.contains(key);
        }

        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = storage_.dynamic_storage_metrics();
            result += index_.dynamic_storage_metrics();
            return result;
        }

        [[nodiscard]] DebugDynamicLayout debug_layout(std::size_t stride) const noexcept
        {
            const auto *base = reinterpret_cast<const std::byte *>(this);
            const std::size_t storage_offset = static_cast<std::size_t>(
                reinterpret_cast<const std::byte *>(&storage_) - base);
            return DebugDynamicLayout{
                .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
                .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
                .kind = DebugDynamicKind::Contiguous,
                .flags = DebugDynamicFlags::DataIsIndirect,
                .size_offset = storage_offset + ListStorage::debug_size_offset(),
                .data_offset = storage_offset + ListStorage::debug_data_offset(),
                .stride = stride,
            };
        }

      private:
        void ensure_ops() const
        {
            if (!element_binding_ || element_binding_.schema() == nullptr ||
                !element_binding_.schema()->is_hashable() || !element_binding_.schema()->is_equatable())
            {
                throw std::logic_error("SetStorage requires a hashable and equatable element binding");
            }
        }

        void build_index()
        {
            index_.reset(slot_context(), storage_.size());
            for (std::size_t i = 0; i < storage_.size(); ++i)
            {
                if (!index_.insert(i)) { throw std::invalid_argument("SetStorage contains duplicate keys"); }
            }
        }

        void copy_from(const SetStorage &other)
        {
            element_binding_ = other.element_binding_;
            if (element_binding_ == nullptr) { return; }
            const auto &plan = element_binding_.checked_plan();
            storage_ = ListStorage{
                element_binding_,
                ElementSpan{
                    .bytes  = other.storage_.size() == 0 ? nullptr : other.storage_.element_at(0),
                    .size   = other.storage_.size(),
                    .stride = plan.layout.size,
                    .plan   = &plan,
                },
            };
            build_index();
        }

        [[nodiscard]] compact_detail::SlotIndexContext slot_context() const noexcept
        {
            return compact_detail::SlotIndexContext{
                .binding = element_binding_,
                .owner   = this,
                .at      = &SetStorage::slot_at_context,
            };
        }

        [[nodiscard]] static const void *slot_at_context(const void *owner, std::size_t slot) noexcept
        {
            return static_cast<const SetStorage *>(owner)->storage_.element_at(slot);
        }

        ValueTypeRef element_binding_{nullptr};
        ListStorage               storage_{};
        compact_detail::SlotIndex index_{};
    };

    // -----------------------------------------------------------------
    // MapStorage — content-keyed hash map.
    // -----------------------------------------------------------------

    /**
     * Compact value-layer storage for the ``Map`` kind. Pairs a key
     * buffer with a value buffer indexed by the same slot ids. Keys and
     * values are stored as parallel contiguous arrays; lookup goes
     * through a dense slot index driven by the bound key ops.
     */
    class MapStorage
    {
      public:
        MapStorage() noexcept = default;

        MapStorage(const ValueTypeRef &key_binding, const ValueTypeRef &value_binding,
                   const ElementSpan &keys_source, const ElementSpan &values_source,
                   std::vector<bool> value_validity = {})
            : key_binding_{key_binding}
            , value_binding_{value_binding}
            , keys_{key_binding, keys_source}
            , values_{value_binding, values_source, std::move(value_validity)}
        {
            if (keys_source.size != values_source.size)
            {
                throw std::invalid_argument("MapStorage requires keys and values of the same size");
            }
            ensure_key_ops();
            build_index();
        }

        MapStorage(const MapStorage &other) { copy_from(other); }
        MapStorage &operator=(const MapStorage &other)
        {
            if (this != &other)
            {
                index_.clear();
                keys_   = ListStorage{};
                values_ = ListStorage{};
                copy_from(other);
            }
            return *this;
        }

        MapStorage(MapStorage &&other)
            : key_binding_{other.key_binding_}
            , value_binding_{other.value_binding_}
            , keys_{std::move(other.keys_)}
            , values_{std::move(other.values_)}
            , index_{std::move(other.index_)}
        {
            index_.rebind(slot_context());
            other.key_binding_   = nullptr;
            other.value_binding_ = nullptr;
        }

        MapStorage &operator=(MapStorage &&other)
        {
            if (this != &other)
            {
                key_binding_   = other.key_binding_;
                value_binding_ = other.value_binding_;
                keys_          = std::move(other.keys_);
                values_        = std::move(other.values_);
                index_         = std::move(other.index_);
                index_.rebind(slot_context());
                other.key_binding_   = nullptr;
                other.value_binding_ = nullptr;
            }
            return *this;
        }

        [[nodiscard]] std::size_t             size() const noexcept { return keys_.size(); }
        [[nodiscard]] bool                    empty() const noexcept { return keys_.empty(); }
        [[nodiscard]] ValueTypeRef key_binding() const noexcept { return key_binding_; }
        [[nodiscard]] ValueTypeRef value_binding() const noexcept { return value_binding_; }

        [[nodiscard]] const void *key_at(std::size_t slot) const { return keys_.element_at(slot); }
        /** True when the entry at ``slot`` carries a value (value HOLES are
            None-valued mapping entries; element validity). */
        [[nodiscard]] bool value_set(std::size_t slot) const noexcept { return values_.element_set(slot); }
        [[nodiscard]] void *value_at_index(std::size_t slot) { return values_.element_at(slot); }
        [[nodiscard]] const void *value_at_index(std::size_t slot) const
        {
            return values_.element_set(slot) ? values_.element_at(slot) : nullptr;
        }

        [[nodiscard]] std::int32_t find_slot(const void *key) const
        {
            if (keys_.empty() || key_binding_ == nullptr) { return -1; }
            return index_.find(key).value_or(-1);
        }

        [[nodiscard]] bool contains(const void *key) const { return find_slot(key) != -1; }

        [[nodiscard]] DynamicStorageMetrics key_set_dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = keys_.dynamic_storage_metrics();
            result += index_.dynamic_storage_metrics();
            return result;
        }

        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = key_set_dynamic_storage_metrics();
            result += values_.dynamic_storage_metrics();
            return result;
        }

        [[nodiscard]] const void *value_at(const void *key) const
        {
            const auto slot = find_slot(key);
            if (slot == -1) { return nullptr; }
            return value_at_index(static_cast<std::size_t>(slot));
        }
        [[nodiscard]] void *value_at(const void *key)
        {
            const auto slot = find_slot(key);
            return slot == -1 ? nullptr : values_.element_at(static_cast<std::size_t>(slot));
        }

      private:
        void ensure_key_ops() const
        {
            if (!key_binding_ || key_binding_.schema() == nullptr ||
                !key_binding_.schema()->is_hashable() || !key_binding_.schema()->is_equatable())
            {
                throw std::logic_error("MapStorage requires a hashable and equatable key binding");
            }
        }

        void build_index()
        {
            index_.reset(slot_context(), keys_.size());
            for (std::size_t i = 0; i < keys_.size(); ++i)
            {
                if (!index_.insert(i)) { throw std::invalid_argument("MapStorage contains duplicate keys"); }
            }
        }

        void copy_from(const MapStorage &other)
        {
            key_binding_   = other.key_binding_;
            value_binding_ = other.value_binding_;
            if (key_binding_ == nullptr || value_binding_ == nullptr) { return; }
            const auto &kp = key_binding_.checked_plan();
            const auto &vp = value_binding_.checked_plan();
            keys_          = ListStorage{key_binding_,
                                ElementSpan{.bytes  = other.keys_.size() == 0 ? nullptr : other.keys_.element_at(0),
                                            .size   = other.keys_.size(),
                                            .stride = kp.layout.size,
                                            .plan   = &kp}};
            std::vector<bool> value_validity;
            value_validity.reserve(other.values_.size());
            for (std::size_t slot = 0; slot < other.values_.size(); ++slot)
            {
                value_validity.push_back(other.values_.element_set(slot));
            }
            values_        = ListStorage{value_binding_,
                                  ElementSpan{.bytes = other.values_.size() == 0 ? nullptr : other.values_.element_at(0),
                                              .size   = other.values_.size(),
                                              .stride = vp.layout.size,
                                              .plan   = &vp},
                                  std::move(value_validity)};
            build_index();
        }

        [[nodiscard]] compact_detail::SlotIndexContext slot_context() const noexcept
        {
            return compact_detail::SlotIndexContext{
                .binding = key_binding_,
                .owner   = this,
                .at      = &MapStorage::key_at_context,
            };
        }

        [[nodiscard]] static const void *key_at_context(const void *owner, std::size_t slot) noexcept
        {
            return static_cast<const MapStorage *>(owner)->keys_.element_at(slot);
        }

        ValueTypeRef key_binding_{nullptr};
        ValueTypeRef value_binding_{nullptr};
        ListStorage               keys_{};
        ListStorage               values_{};
        compact_detail::SlotIndex index_{};
    };

    // -----------------------------------------------------------------
    // Lifecycle context state structs.
    // -----------------------------------------------------------------

    struct ListState
    {
        ValueTypeRef element_binding{nullptr};
    };

    struct SetState
    {
        ValueTypeRef element_binding{nullptr};
    };

    struct MapState
    {
        ValueTypeRef key_binding{nullptr};
        ValueTypeRef value_binding{nullptr};
    };

    struct CyclicBufferState
    {
        ValueTypeRef element_binding{nullptr};
        std::size_t             capacity{0};
    };

    struct QueueState
    {
        ValueTypeRef element_binding{nullptr};
        std::size_t             max_capacity{0};
    };

    namespace compact_detail
    {
        template <typename State>
        [[nodiscard]] const State &checked_state(const void *context, const char *name)
        {
            if (context == nullptr)
            {
                throw std::logic_error(std::string(name) + " requires lifecycle context");
            }
            return *static_cast<const State *>(context);
        }

        // Lifecycle thunks for each storage shape. ``construct`` /
        // ``copy_construct`` / ``move_construct`` etc. — the StoragePlan
        // wires these into LifecycleOps.

        inline void list_construct(void *dst, const void *context)
        {
            const auto &state = checked_state<ListState>(context, "list");
            std::construct_at(static_cast<ListStorage *>(dst));
            (void)state;  // empty list construction needs no element span
        }

        inline void list_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<ListStorage *>(memory));
        }

        inline void list_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<ListStorage *>(dst), *static_cast<const ListStorage *>(src));
        }

        inline void list_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<ListStorage *>(dst), std::move(*static_cast<ListStorage *>(src)));
        }

        inline void list_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<ListStorage *>(dst) = *static_cast<const ListStorage *>(src);
        }

        inline void list_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<ListStorage *>(dst) = std::move(*static_cast<ListStorage *>(src));
        }

        inline void cyclic_buffer_construct(void *dst, const void *context)
        {
            const auto &state = checked_state<CyclicBufferState>(context, "cyclic buffer");
            std::construct_at(static_cast<CyclicBufferStorage *>(dst));
            (void)state;
        }

        inline void cyclic_buffer_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<CyclicBufferStorage *>(memory));
        }

        inline void cyclic_buffer_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<CyclicBufferStorage *>(dst),
                              *static_cast<const CyclicBufferStorage *>(src));
        }

        inline void cyclic_buffer_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<CyclicBufferStorage *>(dst),
                              std::move(*static_cast<CyclicBufferStorage *>(src)));
        }

        inline void cyclic_buffer_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<CyclicBufferStorage *>(dst) = *static_cast<const CyclicBufferStorage *>(src);
        }

        inline void cyclic_buffer_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<CyclicBufferStorage *>(dst) = std::move(*static_cast<CyclicBufferStorage *>(src));
        }

        inline void queue_construct(void *dst, const void *context)
        {
            const auto &state = checked_state<QueueState>(context, "queue");
            std::construct_at(static_cast<QueueStorage *>(dst));
            (void)state;
        }

        inline void queue_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<QueueStorage *>(memory));
        }

        inline void queue_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<QueueStorage *>(dst), *static_cast<const QueueStorage *>(src));
        }

        inline void queue_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<QueueStorage *>(dst), std::move(*static_cast<QueueStorage *>(src)));
        }

        inline void queue_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<QueueStorage *>(dst) = *static_cast<const QueueStorage *>(src);
        }

        inline void queue_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<QueueStorage *>(dst) = std::move(*static_cast<QueueStorage *>(src));
        }

        inline void set_construct(void *dst, const void *context)
        {
            const auto &state = checked_state<SetState>(context, "set");
            std::construct_at(static_cast<SetStorage *>(dst));
            (void)state;
        }

        inline void set_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<SetStorage *>(memory));
        }

        inline void set_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<SetStorage *>(dst), *static_cast<const SetStorage *>(src));
        }

        inline void set_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<SetStorage *>(dst), std::move(*static_cast<SetStorage *>(src)));
        }

        inline void set_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<SetStorage *>(dst) = *static_cast<const SetStorage *>(src);
        }

        inline void set_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<SetStorage *>(dst) = std::move(*static_cast<SetStorage *>(src));
        }

        inline void map_construct(void *dst, const void *context)
        {
            const auto &state = checked_state<MapState>(context, "map");
            std::construct_at(static_cast<MapStorage *>(dst));
            (void)state;
        }

        inline void map_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<MapStorage *>(memory));
        }

        inline void map_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<MapStorage *>(dst), *static_cast<const MapStorage *>(src));
        }

        inline void map_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<MapStorage *>(dst), std::move(*static_cast<MapStorage *>(src)));
        }

        inline void map_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<MapStorage *>(dst) = *static_cast<const MapStorage *>(src);
        }

        inline void map_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<MapStorage *>(dst) = std::move(*static_cast<MapStorage *>(src));
        }

        // -------- plan factory: per-instantiation interning -----------

        template <typename Key, typename State, typename KeyHash = std::hash<Key>>
        struct CompactContainerPlanRegistry
        {
            /** State + plan co-owned per key; the plan's lifecycle_context
                points at the state, so both live behind stable pointers. */
            struct Entry
            {
                std::unique_ptr<State>                    state;
                std::unique_ptr<MemoryUtils::StoragePlan> plan;
            };

            InternTable<Key, Entry, KeyHash> table{};

            template <typename StateFactory, typename PlanFactory>
            [[nodiscard]] const MemoryUtils::StoragePlan &intern(const Key &key, StateFactory &&state_factory,
                                                                 PlanFactory &&plan_factory)
            {
                return *table
                            .intern(key,
                                    [&] {
                                        auto state = state_factory();
                                        auto plan  = plan_factory(*state);
                                        return Entry{std::move(state), std::move(plan)};
                                    })
                            .plan;
            }

            void clear() noexcept { table.clear(); }
        };

        struct UnaryBindingKey
        {
            ValueTypeRef binding{nullptr};
            [[nodiscard]] bool operator==(const UnaryBindingKey &) const noexcept = default;
        };
        struct UnaryBindingKeyHash
        {
            [[nodiscard]] std::size_t operator()(const UnaryBindingKey &k) const noexcept
            {
                return std::hash<ValueTypeRef>{}(k.binding);
            }
        };

        struct BinaryBindingKey
        {
            ValueTypeRef first{nullptr};
            ValueTypeRef second{nullptr};
            [[nodiscard]] bool operator==(const BinaryBindingKey &) const noexcept = default;
        };
        struct BinaryBindingKeyHash
        {
            [[nodiscard]] std::size_t operator()(const BinaryBindingKey &k) const noexcept
            {
                std::size_t seed = std::hash<ValueTypeRef>{}(k.first);
                return combine_hash(seed, std::hash<ValueTypeRef>{}(k.second));
            }
        };

        struct SizedBindingKey
        {
            ValueTypeRef binding{nullptr};
            std::size_t             size{0};
            [[nodiscard]] bool operator==(const SizedBindingKey &) const noexcept = default;
        };
        struct SizedBindingKeyHash
        {
            [[nodiscard]] std::size_t operator()(const SizedBindingKey &k) const noexcept
            {
                std::size_t seed = std::hash<ValueTypeRef>{}(k.binding);
                return combine_hash(seed, std::hash<std::size_t>{}(k.size));
            }
        };

        template <typename Storage, auto Construct, auto Destroy, auto CopyConstruct, auto MoveConstruct,
                  auto CopyAssign, auto MoveAssign, typename State>
        [[nodiscard]] inline MemoryUtils::StoragePlan make_storage_plan(const State &state)
        {
            return MemoryUtils::StoragePlan{
                .layout                       = MemoryUtils::layout_for<Storage>(),
                .lifecycle                    = {.construct      = Construct,
                                                 .destroy        = Destroy,
                                                 .copy_construct = CopyConstruct,
                                                 .move_construct = MoveConstruct,
                                                 .copy_assign    = CopyAssign,
                                                 .move_assign    = MoveAssign},
                .lifecycle_context            = &state,
                .composite_kind_tag           = MemoryUtils::CompositeKind::None,
                .trivially_destructible       = false,
                .trivially_copyable           = false,
                .trivially_move_constructible = false,
            };
        }

        // Per-kind interning registries.
        inline CompactContainerPlanRegistry<UnaryBindingKey, ListState, UnaryBindingKeyHash> &list_registry()
        {
            static CompactContainerPlanRegistry<UnaryBindingKey, ListState, UnaryBindingKeyHash> r;
            return r;
        }
        inline CompactContainerPlanRegistry<UnaryBindingKey, SetState, UnaryBindingKeyHash> &set_registry()
        {
            static CompactContainerPlanRegistry<UnaryBindingKey, SetState, UnaryBindingKeyHash> r;
            return r;
        }
        inline CompactContainerPlanRegistry<BinaryBindingKey, MapState, BinaryBindingKeyHash> &map_registry()
        {
            static CompactContainerPlanRegistry<BinaryBindingKey, MapState, BinaryBindingKeyHash> r;
            return r;
        }
        inline CompactContainerPlanRegistry<SizedBindingKey, CyclicBufferState, SizedBindingKeyHash> &
        cyclic_buffer_registry()
        {
            static CompactContainerPlanRegistry<SizedBindingKey, CyclicBufferState, SizedBindingKeyHash> r;
            return r;
        }
        inline CompactContainerPlanRegistry<SizedBindingKey, QueueState, SizedBindingKeyHash> &queue_registry()
        {
            static CompactContainerPlanRegistry<SizedBindingKey, QueueState, SizedBindingKeyHash> r;
            return r;
        }
    }  // namespace compact_detail

    // -----------------------------------------------------------------
    // Public plan accessors. ``ValuePlanFactory`` calls into these to
    // synthesise the plan for a container schema.
    // -----------------------------------------------------------------

    [[nodiscard]] inline const MemoryUtils::StoragePlan &compact_list_plan(const ValueTypeRef &element_binding)
    {
        return compact_detail::list_registry().intern(
            compact_detail::UnaryBindingKey{.binding = element_binding},
            [&] { return std::make_unique<ListState>(ListState{.element_binding = element_binding}); },
            [&](const ListState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<ListStorage,
                                                       &compact_detail::list_construct,
                                                       &compact_detail::list_destroy,
                                                       &compact_detail::list_copy_construct,
                                                       &compact_detail::list_move_construct,
                                                       &compact_detail::list_copy_assign,
                                                       &compact_detail::list_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &compact_set_plan(const ValueTypeRef &element_binding)
    {
        return compact_detail::set_registry().intern(
            compact_detail::UnaryBindingKey{.binding = element_binding},
            [&] { return std::make_unique<SetState>(SetState{.element_binding = element_binding}); },
            [&](const SetState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<SetStorage,
                                                       &compact_detail::set_construct,
                                                       &compact_detail::set_destroy,
                                                       &compact_detail::set_copy_construct,
                                                       &compact_detail::set_move_construct,
                                                       &compact_detail::set_copy_assign,
                                                       &compact_detail::set_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &compact_map_plan(const ValueTypeRef &key_binding,
                                                                          const ValueTypeRef &value_binding)
    {
        return compact_detail::map_registry().intern(
            compact_detail::BinaryBindingKey{.first = key_binding, .second = value_binding},
            [&] {
                return std::make_unique<MapState>(MapState{.key_binding   = key_binding,
                                                            .value_binding = value_binding});
            },
            [&](const MapState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<MapStorage,
                                                       &compact_detail::map_construct,
                                                       &compact_detail::map_destroy,
                                                       &compact_detail::map_copy_construct,
                                                       &compact_detail::map_move_construct,
                                                       &compact_detail::map_copy_assign,
                                                       &compact_detail::map_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &
    compact_cyclic_buffer_plan(const ValueTypeRef &element_binding, std::size_t capacity)
    {
        return compact_detail::cyclic_buffer_registry().intern(
            compact_detail::SizedBindingKey{.binding = element_binding, .size = capacity},
            [&] {
                return std::make_unique<CyclicBufferState>(
                    CyclicBufferState{.element_binding = element_binding, .capacity = capacity});
            },
            [&](const CyclicBufferState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<CyclicBufferStorage,
                                                       &compact_detail::cyclic_buffer_construct,
                                                       &compact_detail::cyclic_buffer_destroy,
                                                       &compact_detail::cyclic_buffer_copy_construct,
                                                       &compact_detail::cyclic_buffer_move_construct,
                                                       &compact_detail::cyclic_buffer_copy_assign,
                                                       &compact_detail::cyclic_buffer_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &compact_queue_plan(const ValueTypeRef &element_binding,
                                                                            std::size_t             max_capacity)
    {
        return compact_detail::queue_registry().intern(
            compact_detail::SizedBindingKey{.binding = element_binding, .size = max_capacity},
            [&] {
                return std::make_unique<QueueState>(
                    QueueState{.element_binding = element_binding, .max_capacity = max_capacity});
            },
            [&](const QueueState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<QueueStorage,
                                                       &compact_detail::queue_construct,
                                                       &compact_detail::queue_destroy,
                                                       &compact_detail::queue_copy_construct,
                                                       &compact_detail::queue_move_construct,
                                                       &compact_detail::queue_copy_assign,
                                                       &compact_detail::queue_move_assign>(state));
            });
    }

    /**
     * Drop every interned compact-container plan and its lifecycle-context
     * state. Test-only helper used by the registry-reset listener;
     * pointers previously returned by ``compact_*_plan`` become invalid.
     */
    HGRAPH_EXPORT void clear_compact_container_plans() noexcept;
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_COMPACT_STORAGE_H
