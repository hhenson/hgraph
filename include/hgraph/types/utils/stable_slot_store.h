#ifndef HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_H
#define HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/storage_metrics.h>
#include <hgraph/types/utils/impl/stable_slot_store_impl.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/utils/stable_slot_store_fwd.h>

#include <cstddef>
#include <cstdint>
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

    namespace detail
    {
        /**
         * Private representation tag carried by the erased implementation pointer.
         *
         * The aligned implementation deliberately uses tag zero so its pointer can
         * be consumed without masking on the common live-slot path.  The nop tag is
         * a non-null logical state even though it has no implementation allocation.
         */
        enum class StableSlotStoreImplementationTag : std::uintptr_t
        {
            TaggedPointer = 0,
            Bitmap = 1,
            Nop = 2,
        };

        inline constexpr std::uintptr_t STABLE_SLOT_STORE_IMPLEMENTATION_TAG_MASK{0x3};
        inline constexpr std::uintptr_t NOP_STABLE_SLOT_STORE_HANDLE{
            static_cast<std::uintptr_t>(StableSlotStoreImplementationTag::Nop)};

        /** Select, allocate, and construct a concrete strategy. */
        [[nodiscard]] HGRAPH_EXPORT std::uintptr_t
        make_stable_slot_store_implementation(StableSlotStateModel model, MemoryUtils::StorageLayout layout,
                                              const MemoryUtils::AllocatorOps &allocator);

        /** Destroy and deallocate a concrete strategy selected by its handle tag. */
        HGRAPH_EXPORT void destroy_stable_slot_store_implementation(StableSlotStateModel model,
                                                                    std::uintptr_t handle) noexcept;
    } // namespace detail

    /**
     * Payload-type-erased stable slot storage with an alignment-selected index.
     *
     * Pointer-aligned payloads carry lifecycle state in two low slot-pointer
     * bits. Weaker alignments retain a raw pointer table plus the bitmap planes
     * required by ``Model``. Selection happens once when the layout is bound and
     * never changes for the lifetime of the store.
     *
     * The facade owns one tagged implementation pointer. Its low bits select the
     * canonical nop, tagged-pointer, or bitmap strategy; each public operation
     * uses an inline representation switch so the compiler can optimize the
     * concrete hot path. Concrete storage types remain in the implementation
     * header and semantic owners interact only with this erased surface.
     */
    template <StableSlotStateModel Model> class StableSlotStore
    {
        using TaggedImplementation = detail::TaggedPointerStableSlotStoreImpl<Model>;
        using BitmapImplementation = detail::BitmapStableSlotStoreImpl<Model>;
        using ImplementationTag = detail::StableSlotStoreImplementationTag;

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
            : implementation_(std::exchange(other.implementation_, detail::NOP_STABLE_SLOT_STORE_HANDLE))
        {
        }

        StableSlotStore &operator=(StableSlotStore &&other) noexcept
        {
            if (this != &other) {
                detail::destroy_stable_slot_store_implementation(Model, implementation_);
                implementation_ = std::exchange(other.implementation_, detail::NOP_STABLE_SLOT_STORE_HANDLE);
            }
            return *this;
        }

        ~StableSlotStore() { detail::destroy_stable_slot_store_implementation(Model, implementation_); }

        void bind_layout(MemoryUtils::StorageLayout layout,
                         const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
        {
            if (!layout.valid() || layout.size == 0) {
                throw std::logic_error("StableSlotStore requires a non-empty valid layout");
            }
            if (bound()) {
                const auto existing = bound_layout();
                if (existing.size != layout.size || existing.alignment != layout.alignment ||
                    &this->allocator() != &allocator) {
                    throw std::logic_error("StableSlotStore layout and allocator must remain constant");
                }
                return;
            }

            implementation_ = detail::make_stable_slot_store_implementation(Model, layout, allocator);
        }

        [[nodiscard]] StableSlotRepresentation representation() const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return StableSlotRepresentation::TaggedPointer;
            case ImplementationTag::Bitmap:
                return StableSlotRepresentation::Bitmap;
            case ImplementationTag::Nop:
                return StableSlotRepresentation::Unbound;
            }
            return StableSlotRepresentation::Unbound;
        }

        [[nodiscard]] bool bound() const noexcept { return implementation_tag() != ImplementationTag::Nop; }

        [[nodiscard]] std::size_t slot_capacity() const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->capacity();
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->capacity();
            case ImplementationTag::Nop:
                return 0;
            }
            return 0;
        }

        [[nodiscard]] std::size_t stride() const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->stride();
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->stride();
            case ImplementationTag::Nop:
                return 0;
            }
            return 0;
        }

        [[nodiscard]] std::size_t block_count() const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->block_count();
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->block_count();
            case ImplementationTag::Nop:
                return 0;
            }
            return 0;
        }

        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->allocator();
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->allocator();
            case ImplementationTag::Nop:
                return MemoryUtils::allocator();
            }
            return MemoryUtils::allocator();
        }

        void reserve_to(std::size_t capacity)
        {
            require_bound();
            if (implementation_tag() == ImplementationTag::TaggedPointer)
                tagged_implementation()->reserve_to(capacity);
            else
                bitmap_implementation()->reserve_to(capacity);
        }

        [[nodiscard]] void *slot_memory(std::size_t slot) noexcept
        {
            return const_cast<void *>(static_cast<const StableSlotStore &>(*this).slot_memory(slot));
        }

        [[nodiscard]] const void *slot_memory(std::size_t slot) const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->slot_memory(slot);
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->slot_memory(slot);
            case ImplementationTag::Nop:
                return nullptr;
            }
            return nullptr;
        }

        [[nodiscard]] void *live_slot_memory(std::size_t slot) noexcept
        {
            return const_cast<void *>(static_cast<const StableSlotStore &>(*this).live_slot_memory(slot));
        }

        [[nodiscard]] const void *live_slot_memory(std::size_t slot) const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->live_slot_memory(slot);
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->live_slot_memory(slot);
            case ImplementationTag::Nop:
                return nullptr;
            }
            return nullptr;
        }

        /** Constructed, non-live slot memory (pending erase or transiently staged).
         */
        [[nodiscard]] void *non_live_slot_memory(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return const_cast<void *>(static_cast<const StableSlotStore &>(*this).non_live_slot_memory(slot));
        }

        [[nodiscard]] const void *non_live_slot_memory(std::size_t slot) const noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->non_live_slot_memory(slot);
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->non_live_slot_memory(slot);
            case ImplementationTag::Nop:
                return nullptr;
            }
            return nullptr;
        }

        [[nodiscard]] bool constructed(std::size_t slot) const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->constructed(slot);
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->constructed(slot);
            case ImplementationTag::Nop:
                return false;
            }
            return false;
        }

        [[nodiscard]] bool live(std::size_t slot) const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->live(slot);
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->live(slot);
            case ImplementationTag::Nop:
                return false;
            }
            return false;
        }

        void mark_staged(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            if (implementation_tag() == ImplementationTag::TaggedPointer)
                tagged_implementation()->mark_staged(slot);
            else if (implementation_tag() == ImplementationTag::Bitmap)
                bitmap_implementation()->mark_staged(slot);
        }

        void mark_constructed(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedOnly)
        {
            static_cast<void>(mark_live_impl(slot));
        }

        [[nodiscard]] bool mark_live(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return mark_live_impl(slot);
        }

        [[nodiscard]] bool mark_pending_erase(std::size_t slot) noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->mark_pending(slot);
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->mark_pending(slot);
            case ImplementationTag::Nop:
                return false;
            }
            return false;
        }

        void mark_free(std::size_t slot) noexcept
        {
            if (implementation_tag() == ImplementationTag::TaggedPointer)
                tagged_implementation()->mark_free(slot);
            else if (implementation_tag() == ImplementationTag::Bitmap)
                bitmap_implementation()->mark_free(slot);
        }

        void reset_states() noexcept
        {
            if (implementation_tag() == ImplementationTag::TaggedPointer)
                tagged_implementation()->reset_states();
            else if (implementation_tag() == ImplementationTag::Bitmap)
                bitmap_implementation()->reset_states();
        }

        [[nodiscard]] std::size_t constructed_count() const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->constructed_count();
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->constructed_count();
            case ImplementationTag::Nop:
                return 0;
            }
            return 0;
        }

        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer: {
                auto result = tagged_implementation()->dynamic_storage_metrics();
                result.live_bytes += sizeof(TaggedImplementation);
                result.reserved_bytes += sizeof(TaggedImplementation);
                return result;
            }
            case ImplementationTag::Bitmap: {
                auto result = bitmap_implementation()->dynamic_storage_metrics();
                result.live_bytes += sizeof(BitmapImplementation);
                result.reserved_bytes += sizeof(BitmapImplementation);
                return result;
            }
            case ImplementationTag::Nop:
                return {};
            }
            return {};
        }

        [[nodiscard]] StableSlotDebugView debug_view() const noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return implementation_debug_view(tagged_implementation(), true);
            case ImplementationTag::Bitmap:
                return implementation_debug_view(bitmap_implementation(), false);
            case ImplementationTag::Nop:
                return {};
            }
            return {};
        }

        void swap(StableSlotStore &other) noexcept
        {
            using std::swap;
            swap(implementation_, other.implementation_);
        }

      private:
        std::uintptr_t implementation_{detail::NOP_STABLE_SLOT_STORE_HANDLE};

        [[nodiscard]] ImplementationTag implementation_tag() const noexcept
        {
            return static_cast<ImplementationTag>(implementation_ & detail::STABLE_SLOT_STORE_IMPLEMENTATION_TAG_MASK);
        }

        [[nodiscard]] TaggedImplementation *tagged_implementation() noexcept
        {
            return reinterpret_cast<TaggedImplementation *>(implementation_);
        }

        [[nodiscard]] const TaggedImplementation *tagged_implementation() const noexcept
        {
            return reinterpret_cast<const TaggedImplementation *>(implementation_);
        }

        [[nodiscard]] BitmapImplementation *bitmap_implementation() noexcept
        {
            return reinterpret_cast<BitmapImplementation *>(implementation_ &
                                                            ~detail::STABLE_SLOT_STORE_IMPLEMENTATION_TAG_MASK);
        }

        [[nodiscard]] const BitmapImplementation *bitmap_implementation() const noexcept
        {
            return reinterpret_cast<const BitmapImplementation *>(implementation_ &
                                                                  ~detail::STABLE_SLOT_STORE_IMPLEMENTATION_TAG_MASK);
        }

        [[nodiscard]] MemoryUtils::StorageLayout bound_layout() const noexcept
        {
            return implementation_tag() == ImplementationTag::TaggedPointer ? tagged_implementation()->layout()
                                                                            : bitmap_implementation()->layout();
        }

        [[nodiscard]] bool mark_live_impl(std::size_t slot) noexcept
        {
            switch (implementation_tag()) {
            case ImplementationTag::TaggedPointer:
                return tagged_implementation()->mark_live(slot);
            case ImplementationTag::Bitmap:
                return bitmap_implementation()->mark_live(slot);
            case ImplementationTag::Nop:
                return false;
            }
            return false;
        }

        template <typename Implementation>
        [[nodiscard]] StableSlotDebugView implementation_debug_view(const Implementation *typed,
                                                                    bool tagged) const noexcept
        {
            const auto *implementation_base = reinterpret_cast<const std::byte *>(typed);
            const auto offset_of = [implementation_base](const void *address) {
                return static_cast<std::size_t>(static_cast<const std::byte *>(address) - implementation_base);
            };
            return {
                .implementation_pointer = &implementation_,
                .slot_count_offset = offset_of(typed->debug_capacity_address()),
                .pointer_table_offset = offset_of(typed->debug_slots_address()),
                .state_offset = offset_of(typed->debug_state_address()),
                .pointers_tagged = tagged,
                .state_tagged = tagged,
            };
        }

        void require_bound() const
        {
            if (implementation_tag() == ImplementationTag::Nop) {
                throw std::logic_error("StableSlotStore has no bound layout");
            }
        }
    };

    static_assert(sizeof(StableSlotStore<StableSlotStateModel::ConstructedOnly>) == sizeof(void *));
    static_assert(sizeof(StableSlotStore<StableSlotStateModel::ConstructedAndLive>) == sizeof(void *));
} // namespace hgraph

#endif // HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_H
