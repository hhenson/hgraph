#include <hgraph/types/utils/impl/stable_slot_store_impl.h>

#include <utility>

namespace hgraph
{
    namespace
    {
        template <StableSlotStateModel Model, typename Implementation, bool Tagged>
        [[nodiscard]] const StableSlotStoreOps &stable_slot_store_ops_for() noexcept
        {
            static const StableSlotStoreOps ops{
                .representation = Tagged ? StableSlotRepresentation::TaggedPointer
                                         : StableSlotRepresentation::Bitmap,
                .layout_impl = [](const void *implementation) noexcept {
                    return MemoryUtils::cast<Implementation>(implementation)->layout();
                },
                .allocator_impl = [](const void *implementation) noexcept {
                    return &MemoryUtils::cast<Implementation>(implementation)->allocator();
                },
                .capacity_impl = [](const void *implementation) noexcept {
                    return MemoryUtils::cast<Implementation>(implementation)->capacity();
                },
                .stride_impl = [](const void *implementation) noexcept {
                    return MemoryUtils::cast<Implementation>(implementation)->stride();
                },
                .block_count_impl = [](const void *implementation) noexcept {
                    return MemoryUtils::cast<Implementation>(implementation)->block_count();
                },
                .reserve_to_impl = [](void *implementation, std::size_t capacity) {
                    MemoryUtils::cast<Implementation>(implementation)->reserve_to(capacity);
                },
                .slot_memory_impl = [](const void *implementation, std::size_t slot) noexcept -> const void * {
                    return MemoryUtils::cast<Implementation>(implementation)->slot_memory(slot);
                },
                .live_slot_memory_impl = [](const void *implementation, std::size_t slot) noexcept -> const void * {
                    return MemoryUtils::cast<Implementation>(implementation)->live_slot_memory(slot);
                },
                .non_live_slot_memory_impl = [](const void *implementation, std::size_t slot) noexcept -> const void * {
                    if constexpr (Model == StableSlotStateModel::ConstructedAndLive)
                        return MemoryUtils::cast<Implementation>(implementation)->non_live_slot_memory(slot);
                    else
                        return nullptr;
                },
                .constructed_impl = [](const void *implementation, std::size_t slot) noexcept {
                    return MemoryUtils::cast<Implementation>(implementation)->constructed(slot);
                },
                .live_impl = [](const void *implementation, std::size_t slot) noexcept {
                    return MemoryUtils::cast<Implementation>(implementation)->live(slot);
                },
                .mark_staged_impl = [](void *implementation, std::size_t slot) noexcept {
                    MemoryUtils::cast<Implementation>(implementation)->mark_staged(slot);
                },
                .mark_live_impl = [](void *implementation, std::size_t slot) noexcept {
                    return MemoryUtils::cast<Implementation>(implementation)->mark_live(slot);
                },
                .mark_pending_impl = [](void *implementation, std::size_t slot) noexcept {
                    if constexpr (Model == StableSlotStateModel::ConstructedAndLive)
                        return MemoryUtils::cast<Implementation>(implementation)->mark_pending(slot);
                    else
                        return false;
                },
                .mark_free_impl = [](void *implementation, std::size_t slot) noexcept {
                    MemoryUtils::cast<Implementation>(implementation)->mark_free(slot);
                },
                .reset_states_impl = [](void *implementation) noexcept {
                    MemoryUtils::cast<Implementation>(implementation)->reset_states();
                },
                .constructed_count_impl = [](const void *implementation) noexcept {
                    return MemoryUtils::cast<Implementation>(implementation)->constructed_count();
                },
                .dynamic_storage_metrics_impl = [](const void *implementation) noexcept {
                    return MemoryUtils::cast<Implementation>(implementation)->dynamic_storage_metrics();
                },
                .debug_view_impl = [](const void *implementation) noexcept {
                    const auto *typed = MemoryUtils::cast<Implementation>(implementation);
                    const auto *base = static_cast<const std::byte *>(implementation);
                    const auto offset_of = [base](const void *address) {
                        return static_cast<std::size_t>(
                            static_cast<const std::byte *>(address) - base);
                    };
                    return StableSlotDebugView{
                        .slot_count_offset = offset_of(typed->debug_capacity_address()),
                        .pointer_table_offset = offset_of(typed->debug_slots_address()),
                        .state_offset = offset_of(typed->debug_state_address()),
                        .pointers_tagged = Tagged,
                        .state_tagged = Tagged,
                    };
                },
            };
            return ops;
        }

        template <StableSlotStateModel Model, typename Implementation, bool Tagged>
        [[nodiscard]] detail::StableSlotStoreImplementation make_implementation(
            MemoryUtils::StorageLayout layout,
            const MemoryUtils::AllocatorOps &allocator)
        {
            detail::StableSlotStoreImplementation result;
            result.owner = detail::StableSlotStoreImplementationOwner{
                MemoryUtils::plan_for<Implementation>(), allocator};
            result.owner.as<Implementation>()->bind(layout, allocator);
            result.ops = &stable_slot_store_ops_for<Model, Implementation, Tagged>();
            return result;
        }
    }  // namespace

    detail::StableSlotStoreImplementation detail::make_stable_slot_store_implementation(
        StableSlotStateModel model,
        MemoryUtils::StorageLayout layout,
        const MemoryUtils::AllocatorOps &allocator)
    {
        if (!layout.valid() || layout.size == 0)
        {
            throw std::logic_error("StableSlotStore requires a non-empty valid layout");
        }

        if (layout.alignment >= alignof(std::uintptr_t))
        {
            if (model == StableSlotStateModel::ConstructedOnly)
            {
                using Implementation = TaggedPointerStableSlotStoreImpl<StableSlotStateModel::ConstructedOnly>;
                return make_implementation<StableSlotStateModel::ConstructedOnly, Implementation, true>(layout, allocator);
            }
            using Implementation = TaggedPointerStableSlotStoreImpl<StableSlotStateModel::ConstructedAndLive>;
            return make_implementation<StableSlotStateModel::ConstructedAndLive, Implementation, true>(layout, allocator);
        }

        if (model == StableSlotStateModel::ConstructedOnly)
        {
            using Implementation = BitmapStableSlotStoreImpl<StableSlotStateModel::ConstructedOnly>;
            return make_implementation<StableSlotStateModel::ConstructedOnly, Implementation, false>(layout, allocator);
        }
        using Implementation = BitmapStableSlotStoreImpl<StableSlotStateModel::ConstructedAndLive>;
        return make_implementation<StableSlotStateModel::ConstructedAndLive, Implementation, false>(layout, allocator);
    }
}  // namespace hgraph
