#ifndef HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_H
#define HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_H

#include <hgraph/types/utils/impl/stable_slot_store_impl.h>

#include <cstddef>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

namespace hgraph
{
    /** Data-only addresses used to publish stable-slot debugger offsets. */
    struct StableSlotDebugView
    {
        const void *slot_count{nullptr};
        const void *pointer_table{nullptr};
        const void *state{nullptr};
        bool pointers_tagged{false};
        bool state_tagged{false};
    };

    /**
     * Payload-type-erased stable slot storage with an alignment-selected index.
     *
     * Pointer-aligned payloads carry lifecycle state in two low pointer bits.
     * Weaker alignments retain the existing raw pointer table plus the bitmap
     * planes required by ``Model``. Selection happens once when the layout is
     * bound and never changes for the lifetime of the store.
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
        StableSlotStore(StableSlotStore &&) noexcept = default;
        StableSlotStore &operator=(StableSlotStore &&) noexcept = default;

        void bind_layout(MemoryUtils::StorageLayout layout,
                         const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
        {
            if (!layout.valid() || layout.size == 0)
            {
                throw std::logic_error("StableSlotStore requires a non-empty valid layout");
            }
            if (representation() != StableSlotRepresentation::Unbound)
            {
                visit([&](const auto &impl) {
                    const auto existing = impl.layout();
                    if (existing.size != layout.size || existing.alignment != layout.alignment ||
                        &impl.allocator() != &allocator)
                    {
                        throw std::logic_error("StableSlotStore layout and allocator must remain constant");
                    }
                });
                return;
            }

            if (layout.alignment >= alignof(std::uintptr_t))
                implementation_.template emplace<TaggedImpl>(layout, allocator);
            else
                implementation_.template emplace<BitmapImpl>(layout, allocator);
        }

        [[nodiscard]] StableSlotRepresentation representation() const noexcept
        {
            if (std::holds_alternative<TaggedImpl>(implementation_))
                return StableSlotRepresentation::TaggedPointer;
            if (std::holds_alternative<BitmapImpl>(implementation_))
                return StableSlotRepresentation::Bitmap;
            return StableSlotRepresentation::Unbound;
        }
        [[nodiscard]] bool bound() const noexcept
        {
            return representation() != StableSlotRepresentation::Unbound;
        }
        [[nodiscard]] std::size_t slot_capacity() const noexcept
        {
            return bound() ? visit([](const auto &impl) { return impl.capacity(); }) : 0;
        }
        [[nodiscard]] std::size_t stride() const noexcept
        {
            return bound() ? visit([](const auto &impl) { return impl.stride(); }) : 0;
        }
        [[nodiscard]] std::size_t block_count() const noexcept
        {
            return bound() ? visit([](const auto &impl) { return impl.block_count(); }) : 0;
        }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept
        {
            return bound() ? visit([](const auto &impl) -> const MemoryUtils::AllocatorOps & {
                return impl.allocator();
            }) : MemoryUtils::allocator();
        }

        void reserve_to(std::size_t capacity)
        {
            require_bound();
            visit([&](auto &impl) { impl.reserve_to(capacity); });
        }

        [[nodiscard]] void *slot_memory(std::size_t slot) noexcept
        {
            return bound() ? visit([&](auto &impl) { return impl.slot_memory(slot); }) : nullptr;
        }
        [[nodiscard]] const void *slot_memory(std::size_t slot) const noexcept
        {
            return bound() ? visit([&](const auto &impl) { return impl.slot_memory(slot); }) : nullptr;
        }
        [[nodiscard]] void *live_slot_memory(std::size_t slot) noexcept
        {
            return bound() ? visit([&](auto &impl) { return impl.live_slot_memory(slot); }) : nullptr;
        }
        [[nodiscard]] const void *live_slot_memory(std::size_t slot) const noexcept
        {
            return bound() ? visit([&](const auto &impl) { return impl.live_slot_memory(slot); }) : nullptr;
        }
        /** Constructed, non-live slot memory (pending erase or transiently staged). */
        [[nodiscard]] void *non_live_slot_memory(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return bound() ? visit([&](auto &impl) { return impl.non_live_slot_memory(slot); }) : nullptr;
        }
        [[nodiscard]] const void *non_live_slot_memory(std::size_t slot) const noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return bound() ? visit([&](const auto &impl) { return impl.non_live_slot_memory(slot); }) : nullptr;
        }
        [[nodiscard]] bool constructed(std::size_t slot) const noexcept
        {
            return bound() && visit([&](const auto &impl) { return impl.constructed(slot); });
        }
        [[nodiscard]] bool live(std::size_t slot) const noexcept
        {
            return bound() && visit([&](const auto &impl) { return impl.live(slot); });
        }

        void mark_staged(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            visit([&](auto &impl) { impl.mark_staged(slot); });
        }
        void mark_constructed(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedOnly)
        {
            static_cast<void>(visit([&](auto &impl) { return impl.mark_live(slot); }));
        }
        [[nodiscard]] bool mark_live(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return visit([&](auto &impl) { return impl.mark_live(slot); });
        }
        [[nodiscard]] bool mark_pending_erase(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return visit([&](auto &impl) { return impl.mark_pending(slot); });
        }
        void mark_free(std::size_t slot) noexcept
        {
            visit([&](auto &impl) { impl.mark_free(slot); });
        }
        void reset_states() noexcept
        {
            if (bound()) { visit([](auto &impl) { impl.reset_states(); }); }
        }

        [[nodiscard]] std::size_t constructed_count() const noexcept
        {
            return bound() ? visit([](const auto &impl) { return impl.constructed_count(); }) : 0;
        }
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            return bound() ? visit([](const auto &impl) { return impl.dynamic_storage_metrics(); })
                           : DynamicStorageMetrics{};
        }

        [[nodiscard]] StableSlotDebugView debug_view() const noexcept
        {
            if (!bound()) { return {}; }
            return visit([&](const auto &impl) {
                constexpr bool tagged = std::is_same_v<std::remove_cvref_t<decltype(impl)>, TaggedImpl>;
                return StableSlotDebugView{
                    .slot_count = impl.debug_capacity_address(),
                    .pointer_table = impl.debug_slots_address(),
                    .state = impl.debug_state_address(),
                    .pointers_tagged = tagged,
                    .state_tagged = tagged,
                };
            });
        }

        void swap(StableSlotStore &other) noexcept { implementation_.swap(other.implementation_); }

      private:
        using TaggedImpl = detail::TaggedPointerStableSlotStoreImpl<Model>;
        using BitmapImpl = detail::BitmapStableSlotStoreImpl<Model>;
        using Implementation = std::variant<std::monostate, TaggedImpl, BitmapImpl>;

        Implementation implementation_{};

        template <typename Visitor>
        decltype(auto) visit(Visitor &&visitor)
        {
            if (auto *tagged = std::get_if<TaggedImpl>(&implementation_); tagged != nullptr)
                return std::forward<Visitor>(visitor)(*tagged);
            if (auto *bitmap = std::get_if<BitmapImpl>(&implementation_); bitmap != nullptr)
                return std::forward<Visitor>(visitor)(*bitmap);
            throw std::logic_error("StableSlotStore has no bound layout");
        }

        template <typename Visitor>
        decltype(auto) visit(Visitor &&visitor) const
        {
            if (const auto *tagged = std::get_if<TaggedImpl>(&implementation_); tagged != nullptr)
                return std::forward<Visitor>(visitor)(*tagged);
            if (const auto *bitmap = std::get_if<BitmapImpl>(&implementation_); bitmap != nullptr)
                return std::forward<Visitor>(visitor)(*bitmap);
            throw std::logic_error("StableSlotStore has no bound layout");
        }

        void require_bound() const
        {
            if (!bound()) { throw std::logic_error("StableSlotStore has no bound layout"); }
        }
    };

    static_assert(sizeof(StableSlotStore<StableSlotStateModel::ConstructedOnly>) <= 12 * sizeof(void *));
    static_assert(sizeof(StableSlotStore<StableSlotStateModel::ConstructedAndLive>) <= 15 * sizeof(void *));
}  // namespace hgraph

#endif  // HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_H
