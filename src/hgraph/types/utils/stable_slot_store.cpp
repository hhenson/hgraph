#include <hgraph/types/utils/stable_slot_store.h>

#include <cassert>
#include <memory>
#include <stdexcept>

namespace hgraph
{
    namespace
    {
        using Tag = detail::StableSlotStoreImplementationTag;

        template <typename Implementation>
        [[nodiscard]] std::uintptr_t make_implementation(MemoryUtils::StorageLayout layout,
                                                         const MemoryUtils::AllocatorOps &allocator, Tag tag)
        {
            static_assert(alignof(Implementation) > detail::STABLE_SLOT_STORE_IMPLEMENTATION_TAG_MASK);
            const auto &plan = MemoryUtils::plan_for<Implementation>();
            void *storage = allocator.allocate_storage(plan.layout);
            try {
                std::construct_at(static_cast<Implementation *>(storage), layout, allocator);
            } catch (...) {
                allocator.deallocate_storage(storage, plan.layout);
                throw;
            }

            const auto address = reinterpret_cast<std::uintptr_t>(storage);
            assert((address & detail::STABLE_SLOT_STORE_IMPLEMENTATION_TAG_MASK) == 0);
            return address | static_cast<std::uintptr_t>(tag);
        }

        template <typename Implementation> void destroy_implementation(std::uintptr_t handle) noexcept
        {
            auto *implementation =
                reinterpret_cast<Implementation *>(handle & ~detail::STABLE_SLOT_STORE_IMPLEMENTATION_TAG_MASK);
            const MemoryUtils::AllocatorOps *allocator = &implementation->allocator();
            const auto layout = MemoryUtils::plan_for<Implementation>().layout;
            std::destroy_at(implementation);
            allocator->deallocate_storage(implementation, layout);
        }

        template <StableSlotStateModel Model>
        [[nodiscard]] std::uintptr_t make_for_model(MemoryUtils::StorageLayout layout,
                                                    const MemoryUtils::AllocatorOps &allocator)
        {
            if (layout.alignment >= alignof(std::uintptr_t)) {
                using Implementation = detail::TaggedPointerStableSlotStoreImpl<Model>;
                return make_implementation<Implementation>(layout, allocator, Tag::TaggedPointer);
            }

            using Implementation = detail::BitmapStableSlotStoreImpl<Model>;
            return make_implementation<Implementation>(layout, allocator, Tag::Bitmap);
        }

        template <StableSlotStateModel Model> void destroy_for_model(std::uintptr_t handle) noexcept
        {
            const auto tag = static_cast<Tag>(handle & detail::STABLE_SLOT_STORE_IMPLEMENTATION_TAG_MASK);
            switch (tag) {
            case Tag::TaggedPointer:
                destroy_implementation<detail::TaggedPointerStableSlotStoreImpl<Model>>(handle);
                return;
            case Tag::Bitmap:
                destroy_implementation<detail::BitmapStableSlotStoreImpl<Model>>(handle);
                return;
            case Tag::Nop:
                return;
            }
        }
    } // namespace

    std::uintptr_t detail::make_stable_slot_store_implementation(StableSlotStateModel model,
                                                                 MemoryUtils::StorageLayout layout,
                                                                 const MemoryUtils::AllocatorOps &allocator)
    {
        if (!layout.valid() || layout.size == 0) {
            throw std::logic_error("StableSlotStore requires a non-empty valid layout");
        }

        if (model == StableSlotStateModel::ConstructedOnly)
            return make_for_model<StableSlotStateModel::ConstructedOnly>(layout, allocator);
        return make_for_model<StableSlotStateModel::ConstructedAndLive>(layout, allocator);
    }

    void detail::destroy_stable_slot_store_implementation(StableSlotStateModel model, std::uintptr_t handle) noexcept
    {
        if (model == StableSlotStateModel::ConstructedOnly)
            destroy_for_model<StableSlotStateModel::ConstructedOnly>(handle);
        else
            destroy_for_model<StableSlotStateModel::ConstructedAndLive>(handle);
    }
} // namespace hgraph
