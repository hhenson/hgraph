#ifndef HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_H
#define HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/storage_metrics.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/utils/stable_slot_store_fwd.h>

#include <cstddef>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    /** Data-only erased implementation offsets used by debugger descriptors. */
    struct StableSlotDebugView
    {
        const void *implementation_pointer{nullptr};
        std::size_t slot_count_offset{0};
        std::size_t pointer_table_offset{0};
        std::size_t state_offset{0};
        bool pointers_tagged{false};
        bool state_tagged{false};
    };

    /**
     * Passive behaviour table for one stable-slot representation.
     *
     * The first argument of every hook is the erased implementation memory,
     * matching the runtime's established ops-table pattern. Concrete tagged
     * and bitmap types populate canonical tables behind the implementation
     * boundary; semantic owners call only through this contract.
     */
    struct StableSlotStoreOps
    {
        StableSlotRepresentation representation{StableSlotRepresentation::Unbound};

        MemoryUtils::StorageLayout (*layout_impl)(const void *implementation) noexcept{nullptr};
        const MemoryUtils::AllocatorOps *(*allocator_impl)(const void *implementation) noexcept{nullptr};
        std::size_t (*capacity_impl)(const void *implementation) noexcept{nullptr};
        std::size_t (*stride_impl)(const void *implementation) noexcept{nullptr};
        std::size_t (*block_count_impl)(const void *implementation) noexcept{nullptr};

        void (*reserve_to_impl)(void *implementation, std::size_t capacity){nullptr};
        const void *(*slot_memory_impl)(const void *implementation, std::size_t slot) noexcept{nullptr};
        const void *(*live_slot_memory_impl)(const void *implementation, std::size_t slot) noexcept{nullptr};
        const void *(*non_live_slot_memory_impl)(const void *implementation, std::size_t slot) noexcept{nullptr};
        bool (*constructed_impl)(const void *implementation, std::size_t slot) noexcept{nullptr};
        bool (*live_impl)(const void *implementation, std::size_t slot) noexcept{nullptr};

        void (*mark_staged_impl)(void *implementation, std::size_t slot) noexcept{nullptr};
        bool (*mark_live_impl)(void *implementation, std::size_t slot) noexcept{nullptr};
        bool (*mark_pending_impl)(void *implementation, std::size_t slot) noexcept{nullptr};
        void (*mark_free_impl)(void *implementation, std::size_t slot) noexcept{nullptr};
        void (*reset_states_impl)(void *implementation) noexcept{nullptr};

        std::size_t (*constructed_count_impl)(const void *implementation) noexcept{nullptr};
        DynamicStorageMetrics (*dynamic_storage_metrics_impl)(const void *implementation) noexcept{nullptr};
        StableSlotDebugView (*debug_view_impl)(const void *implementation) noexcept{nullptr};
    };

    namespace detail
    {
        using StableSlotStoreImplementationOwner = MemoryUtils::ErasedOwner<>;

        /** Canonical no-op table used by default and moved-from stores. */
        [[nodiscard]] HGRAPH_EXPORT const StableSlotStoreOps &noop_stable_slot_store_ops() noexcept;

        /** Owning result returned by the implementation-only strategy factory. */
        struct StableSlotStoreImplementation
        {
            StableSlotStoreImplementationOwner owner{};
            const StableSlotStoreOps *ops{&noop_stable_slot_store_ops()};

            StableSlotStoreImplementation() noexcept = default;
            StableSlotStoreImplementation(const StableSlotStoreImplementation &) = delete;
            StableSlotStoreImplementation &operator=(const StableSlotStoreImplementation &) = delete;
            StableSlotStoreImplementation(StableSlotStoreImplementation &&) noexcept = default;
            StableSlotStoreImplementation &operator=(StableSlotStoreImplementation &&) noexcept = default;
        };

        /** Select and construct the concrete strategy from immutable layout metadata. */
        HGRAPH_EXPORT StableSlotStoreImplementation make_stable_slot_store_implementation(
            StableSlotStateModel model,
            MemoryUtils::StorageLayout layout,
            const MemoryUtils::AllocatorOps &allocator);
    }  // namespace detail

    /**
     * Payload-type-erased stable slot storage with an alignment-selected index.
     *
     * Pointer-aligned payloads carry lifecycle state in two low pointer bits.
     * Weaker alignments retain the existing raw pointer table plus the bitmap
     * planes required by ``Model``. Selection happens once when the layout is
     * bound and never changes for the lifetime of the store.
     *
     * Behaviour and ownership are independent: ``StableSlotStoreOps`` erases
     * representation behaviour while ``ErasedOwner`` owns the selected
     * implementation according to its storage plan.
     */
    template <StableSlotStateModel Model>
    class StableSlotStore
    {
      public:
        StableSlotStore() noexcept = default;

        StableSlotStore(MemoryUtils::StorageLayout layout,
                        const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
        {
            bind_layout(layout, allocator);
        }

        StableSlotStore(const StableSlotStore &) = delete;
        StableSlotStore &operator=(const StableSlotStore &) = delete;

        StableSlotStore(StableSlotStore &&other) noexcept
            : implementation_(std::move(other.implementation_)),
              ops_(std::exchange(other.ops_, &detail::noop_stable_slot_store_ops()))
        {
        }

        StableSlotStore &operator=(StableSlotStore &&other) noexcept
        {
            if (this != &other)
            {
                implementation_ = std::move(other.implementation_);
                ops_ = std::exchange(other.ops_, &detail::noop_stable_slot_store_ops());
            }
            return *this;
        }

        void bind_layout(MemoryUtils::StorageLayout layout,
                         const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
        {
            if (!layout.valid() || layout.size == 0)
            {
                throw std::logic_error("StableSlotStore requires a non-empty valid layout");
            }
            if (bound())
            {
                const auto existing = ops_->layout_impl(implementation());
                if (existing.size != layout.size || existing.alignment != layout.alignment ||
                    ops_->allocator_impl(implementation()) != &allocator)
                {
                    throw std::logic_error("StableSlotStore layout and allocator must remain constant");
                }
                return;
            }

            auto selected = detail::make_stable_slot_store_implementation(Model, layout, allocator);
            implementation_ = std::move(selected.owner);
            ops_ = selected.ops;
        }

        [[nodiscard]] StableSlotRepresentation representation() const noexcept
        {
            return ops_->representation;
        }
        [[nodiscard]] bool bound() const noexcept
        {
            return ops_ != &detail::noop_stable_slot_store_ops();
        }
        [[nodiscard]] std::size_t slot_capacity() const noexcept
        {
            return ops_->capacity_impl(implementation());
        }
        [[nodiscard]] std::size_t stride() const noexcept
        {
            return ops_->stride_impl(implementation());
        }
        [[nodiscard]] std::size_t block_count() const noexcept
        {
            return ops_->block_count_impl(implementation());
        }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept
        {
            return *ops_->allocator_impl(implementation());
        }

        void reserve_to(std::size_t capacity)
        {
            require_bound();
            ops_->reserve_to_impl(implementation(), capacity);
        }

        [[nodiscard]] void *slot_memory(std::size_t slot) noexcept
        {
            return const_cast<void *>(static_cast<const StableSlotStore &>(*this).slot_memory(slot));
        }
        [[nodiscard]] const void *slot_memory(std::size_t slot) const noexcept
        {
            return ops_->slot_memory_impl(implementation(), slot);
        }
        [[nodiscard]] void *live_slot_memory(std::size_t slot) noexcept
        {
            return const_cast<void *>(static_cast<const StableSlotStore &>(*this).live_slot_memory(slot));
        }
        [[nodiscard]] const void *live_slot_memory(std::size_t slot) const noexcept
        {
            return ops_->live_slot_memory_impl(implementation(), slot);
        }
        /** Constructed, non-live slot memory (pending erase or transiently staged). */
        [[nodiscard]] void *non_live_slot_memory(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return const_cast<void *>(static_cast<const StableSlotStore &>(*this).non_live_slot_memory(slot));
        }
        [[nodiscard]] const void *non_live_slot_memory(std::size_t slot) const noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return ops_->non_live_slot_memory_impl(implementation(), slot);
        }
        [[nodiscard]] bool constructed(std::size_t slot) const noexcept
        {
            return ops_->constructed_impl(implementation(), slot);
        }
        [[nodiscard]] bool live(std::size_t slot) const noexcept
        {
            return ops_->live_impl(implementation(), slot);
        }

        void mark_staged(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            ops_->mark_staged_impl(implementation(), slot);
        }
        void mark_constructed(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedOnly)
        {
            static_cast<void>(ops_->mark_live_impl(implementation(), slot));
        }
        [[nodiscard]] bool mark_live(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return ops_->mark_live_impl(implementation(), slot);
        }
        [[nodiscard]] bool mark_pending_erase(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return ops_->mark_pending_impl(implementation(), slot);
        }
        void mark_free(std::size_t slot) noexcept
        {
            ops_->mark_free_impl(implementation(), slot);
        }
        void reset_states() noexcept
        {
            ops_->reset_states_impl(implementation());
        }

        [[nodiscard]] std::size_t constructed_count() const noexcept
        {
            return ops_->constructed_count_impl(implementation());
        }
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = ops_->dynamic_storage_metrics_impl(implementation());
            const auto *plan = implementation_.plan();
            if (plan != nullptr)
            {
                result.live_bytes += plan->layout.size;
                result.reserved_bytes += plan->layout.size;
            }
            return result;
        }

        [[nodiscard]] StableSlotDebugView debug_view() const noexcept
        {
            StableSlotDebugView result = ops_->debug_view_impl(implementation());
            if (implementation_.stores_heap())
            {
                result.implementation_pointer = MemoryUtils::advance(
                    static_cast<const void *>(&implementation_),
                    detail::StableSlotStoreImplementationOwner::debug_storage_offset());
            }
            return result;
        }

        void swap(StableSlotStore &other) noexcept
        {
            using std::swap;
            swap(implementation_, other.implementation_);
            swap(ops_, other.ops_);
        }

      private:
        detail::StableSlotStoreImplementationOwner implementation_{};
        const StableSlotStoreOps *ops_{&detail::noop_stable_slot_store_ops()};

        [[nodiscard]] void *implementation() noexcept { return implementation_.data(); }
        [[nodiscard]] const void *implementation() const noexcept { return implementation_.data(); }

        void require_bound() const
        {
            if (ops_ == &detail::noop_stable_slot_store_ops())
            {
                throw std::logic_error("StableSlotStore has no bound layout");
            }
        }
    };

    static_assert(sizeof(StableSlotStore<StableSlotStateModel::ConstructedOnly>) <= 4 * sizeof(void *));
    static_assert(sizeof(StableSlotStore<StableSlotStateModel::ConstructedAndLive>) <= 4 * sizeof(void *));
}  // namespace hgraph

#endif  // HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_H
