#ifndef HGRAPH_TYPES_UTILS_IMPL_STABLE_SLOT_STORE_IMPL_H
#define HGRAPH_TYPES_UTILS_IMPL_STABLE_SLOT_STORE_IMPL_H

#include <hgraph/types/utils/slot_bitmap.h>
#include <hgraph/types/utils/stable_slot_storage.h>
#include <hgraph/types/utils/stable_slot_store_fwd.h>
#include <hgraph/util/tagged_ptr.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph::detail
{
    /** Common non-relocating payload blocks shared by both slot-index strategies. */
    class StableSlotBlockStorage
    {
      public:
        StableSlotBlockStorage() noexcept = default;

        StableSlotBlockStorage(MemoryUtils::StorageLayout layout,
                               const MemoryUtils::AllocatorOps &allocator)
            : layout_(layout), allocator_(&allocator)
        {
            validate_layout(layout_);
        }

        StableSlotBlockStorage(const StableSlotBlockStorage &) = delete;
        StableSlotBlockStorage &operator=(const StableSlotBlockStorage &) = delete;
        StableSlotBlockStorage(StableSlotBlockStorage &&) noexcept = default;
        StableSlotBlockStorage &operator=(StableSlotBlockStorage &&) noexcept = default;

        void bind(MemoryUtils::StorageLayout layout,
                  const MemoryUtils::AllocatorOps &allocator)
        {
            validate_layout(layout);
            if (!bound())
            {
                layout_ = layout;
                allocator_ = &allocator;
                return;
            }
            if (layout.size != layout_.size || layout.alignment != layout_.alignment ||
                allocator_ != &allocator)
            {
                throw std::logic_error("StableSlotStore layout and allocator must remain constant");
            }
        }

        [[nodiscard]] bool bound() const noexcept { return layout_.valid() && layout_.size != 0; }
        [[nodiscard]] MemoryUtils::StorageLayout layout() const noexcept { return layout_; }
        [[nodiscard]] std::size_t stride() const noexcept
        {
            return StableSlotBlock::stride_for(layout_.size, layout_.alignment);
        }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept { return *allocator_; }
        [[nodiscard]] std::size_t block_count() const noexcept { return blocks_.size(); }

        void prepare_append()
        {
            require_bound();
            blocks_.reserve(blocks_.size() + 1);
        }

        [[nodiscard]] StableSlotBlock allocate_block(std::size_t first_slot,
                                                     std::size_t slot_count) const
        {
            require_bound();
            return StableSlotBlock::allocate(
                first_slot, slot_count, layout_.size, layout_.alignment, allocator());
        }

        void append(StableSlotBlock block) noexcept
        {
            if (block.slot_count != 0) { blocks_.push_back(std::move(block)); }
        }

        void clear() noexcept { blocks_.clear(); }

        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics(
            std::size_t slot_capacity,
            std::size_t constructed_count) const noexcept
        {
            const std::size_t pointer_bytes = slot_capacity * sizeof(void *);
            return {
                .live_bytes = pointer_bytes + blocks_.size() * sizeof(StableSlotBlock) +
                              constructed_count * stride(),
                .reserved_bytes = pointer_bytes + blocks_.capacity() * sizeof(StableSlotBlock) +
                                  slot_capacity * stride(),
            };
        }

      private:
        std::vector<StableSlotBlock> blocks_{};
        MemoryUtils::StorageLayout layout_{};
        const MemoryUtils::AllocatorOps *allocator_{&MemoryUtils::allocator()};

        static void validate_layout(MemoryUtils::StorageLayout layout)
        {
            if (!layout.valid() || layout.size == 0)
            {
                throw std::logic_error("StableSlotStore requires a non-empty power-of-two-aligned layout");
            }
        }

        void require_bound() const
        {
            if (!bound()) { throw std::logic_error("StableSlotStore has no bound layout"); }
        }
    };

    struct EmptyStableSlotBitmap
    {
    };

    inline SlotBitmap resized_bitmap_copy(const SlotBitmap &source, std::size_t size)
    {
        SlotBitmap result;
        result.resize(size);
        const std::size_t preserved_words = std::min(source.word_count(), result.word_count());
        for (std::size_t index = 0; index < preserved_words; ++index)
        {
            result.words[index] = source.words[index];
        }
        return result;
    }

    /** Existing pointer table plus one or two lifecycle bitmaps. */
    template <StableSlotStateModel Model>
    class BitmapStableSlotStoreImpl
    {
      public:
        using LiveBitmap = std::conditional_t<Model == StableSlotStateModel::ConstructedAndLive,
                                              SlotBitmap,
                                              EmptyStableSlotBitmap>;

        BitmapStableSlotStoreImpl() noexcept = default;

        BitmapStableSlotStoreImpl(MemoryUtils::StorageLayout layout,
                                  const MemoryUtils::AllocatorOps &allocator)
            : blocks_(layout, allocator)
        {
        }

        void bind(MemoryUtils::StorageLayout layout,
                  const MemoryUtils::AllocatorOps &allocator)
        {
            blocks_.bind(layout, allocator);
        }

        BitmapStableSlotStoreImpl(const BitmapStableSlotStoreImpl &) = delete;
        BitmapStableSlotStoreImpl &operator=(const BitmapStableSlotStoreImpl &) = delete;
        BitmapStableSlotStoreImpl(BitmapStableSlotStoreImpl &&other) noexcept
            : blocks_(std::move(other.blocks_)), slots_(std::exchange(other.slots_, nullptr)),
              slot_count_(std::exchange(other.slot_count_, 0)),
              constructed_(std::move(other.constructed_)), live_(std::move(other.live_))
        {
        }
        BitmapStableSlotStoreImpl &operator=(BitmapStableSlotStoreImpl &&other) noexcept
        {
            if (this != &other)
            {
                delete[] slots_;
                blocks_ = std::move(other.blocks_);
                slots_ = std::exchange(other.slots_, nullptr);
                slot_count_ = std::exchange(other.slot_count_, 0);
                constructed_ = std::move(other.constructed_);
                live_ = std::move(other.live_);
            }
            return *this;
        }
        ~BitmapStableSlotStoreImpl() { delete[] slots_; }

        [[nodiscard]] MemoryUtils::StorageLayout layout() const noexcept { return blocks_.layout(); }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept { return blocks_.allocator(); }
        [[nodiscard]] std::size_t capacity() const noexcept { return slot_count_; }
        [[nodiscard]] std::size_t stride() const noexcept { return blocks_.stride(); }
        [[nodiscard]] std::size_t block_count() const noexcept { return blocks_.block_count(); }

        void reserve_to(std::size_t capacity)
        {
            if (capacity <= slot_count_) { return; }

            blocks_.prepare_append();
            auto replacement = std::make_unique<std::byte *[]>(capacity);
            if (slot_count_ != 0) { std::copy_n(slots_, slot_count_, replacement.get()); }
            SlotBitmap next_constructed = resized_bitmap_copy(constructed_, capacity);
            LiveBitmap next_live = resized_live_bitmap(capacity);
            StableSlotBlock block = blocks_.allocate_block(slot_count_, capacity - slot_count_);
            for (std::size_t slot = slot_count_; slot < capacity; ++slot)
            {
                replacement[slot] = block.slot_data(slot);
            }

            blocks_.append(std::move(block));
            delete[] slots_;
            slots_ = replacement.release();
            slot_count_ = capacity;
            constructed_ = std::move(next_constructed);
            live_ = std::move(next_live);
        }

        [[nodiscard]] void *slot_memory(std::size_t slot) const noexcept
        {
            return slot < slot_count_ ? slots_[slot] : nullptr;
        }
        [[nodiscard]] void *live_slot_memory(std::size_t slot) const noexcept
        {
            return live(slot) ? slot_memory(slot) : nullptr;
        }
        [[nodiscard]] void *non_live_slot_memory(std::size_t slot) const noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return constructed(slot) && !live(slot) ? slot_memory(slot) : nullptr;
        }
        [[nodiscard]] bool constructed(std::size_t slot) const noexcept
        {
            return constructed_.test(slot);
        }
        [[nodiscard]] bool live(std::size_t slot) const noexcept
        {
            if constexpr (Model == StableSlotStateModel::ConstructedOnly)
                return constructed(slot);
            else
                return live_.test(slot);
        }

        void mark_staged(std::size_t slot) noexcept
        {
            constructed_.set(slot);
            if constexpr (Model == StableSlotStateModel::ConstructedAndLive) { live_.reset(slot); }
        }
        [[nodiscard]] bool mark_live(std::size_t slot) noexcept
        {
            if constexpr (Model == StableSlotStateModel::ConstructedAndLive)
            {
                if (!constructed(slot) || live(slot)) { return false; }
            }
            else if (constructed(slot))
            {
                return false;
            }
            constructed_.set(slot);
            if constexpr (Model == StableSlotStateModel::ConstructedAndLive) { live_.set(slot); }
            return true;
        }
        [[nodiscard]] bool mark_pending(std::size_t slot) noexcept
        {
            static_assert(Model == StableSlotStateModel::ConstructedAndLive);
            if (!live(slot)) { return false; }
            live_.reset(slot);
            return true;
        }
        void mark_free(std::size_t slot) noexcept
        {
            constructed_.reset(slot);
            if constexpr (Model == StableSlotStateModel::ConstructedAndLive) { live_.reset(slot); }
        }
        void reset_states() noexcept
        {
            constructed_.reset();
            if constexpr (Model == StableSlotStateModel::ConstructedAndLive) { live_.reset(); }
        }

        [[nodiscard]] std::size_t constructed_count() const noexcept { return constructed_.count(); }
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result = blocks_.dynamic_storage_metrics(slot_count_, constructed_count());
            result += constructed_.dynamic_storage_metrics();
            if constexpr (Model == StableSlotStateModel::ConstructedAndLive)
            {
                result += live_.dynamic_storage_metrics();
            }
            return result;
        }

        [[nodiscard]] const void *debug_capacity_address() const noexcept { return &slot_count_; }
        [[nodiscard]] const void *debug_slots_address() const noexcept { return &slots_; }
        [[nodiscard]] const void *debug_state_address() const noexcept
        {
            if constexpr (Model == StableSlotStateModel::ConstructedAndLive)
                return &live_;
            else
                return &constructed_;
        }

      private:
        StableSlotBlockStorage blocks_{};
        std::byte **slots_{nullptr};
        std::size_t slot_count_{0};
        SlotBitmap constructed_{};
#if defined(_MSC_VER)
        [[msvc::no_unique_address]]
#else
        [[no_unique_address]]
#endif
        LiveBitmap live_{};

        [[nodiscard]] LiveBitmap resized_live_bitmap(std::size_t size) const
        {
            if constexpr (Model == StableSlotStateModel::ConstructedAndLive)
                return resized_bitmap_copy(live_, size);
            else
                return {};
        }
    };

    enum class TaggedStableSlotState : std::uint8_t
    {
        Live = 0,
        PendingErase = 1,
        Staged = 2,
        Free = 3,
    };

    /** Slot pointer table whose two low alignment bits carry lifecycle state. */
    template <StableSlotStateModel Model>
    class TaggedPointerStableSlotStoreImpl
    {
      public:
        using SlotPointer = erased_tagged_ptr<alignof(std::uintptr_t), 2, TaggedStableSlotState>;

        TaggedPointerStableSlotStoreImpl() noexcept = default;
        TaggedPointerStableSlotStoreImpl(MemoryUtils::StorageLayout layout,
                                         const MemoryUtils::AllocatorOps &allocator)
            : blocks_(layout, allocator)
        {
            if (layout.alignment < alignof(std::uintptr_t))
            {
                throw std::logic_error("Tagged stable slots require pointer-aligned payloads");
            }
        }

        void bind(MemoryUtils::StorageLayout layout,
                  const MemoryUtils::AllocatorOps &allocator)
        {
            if (layout.alignment < alignof(std::uintptr_t))
            {
                throw std::logic_error("Tagged stable slots require pointer-aligned payloads");
            }
            blocks_.bind(layout, allocator);
        }

        TaggedPointerStableSlotStoreImpl(const TaggedPointerStableSlotStoreImpl &) = delete;
        TaggedPointerStableSlotStoreImpl &operator=(const TaggedPointerStableSlotStoreImpl &) = delete;
        TaggedPointerStableSlotStoreImpl(TaggedPointerStableSlotStoreImpl &&other) noexcept
            : blocks_(std::move(other.blocks_)), slots_(std::exchange(other.slots_, nullptr)),
              slot_count_(std::exchange(other.slot_count_, 0))
        {
        }
        TaggedPointerStableSlotStoreImpl &operator=(TaggedPointerStableSlotStoreImpl &&other) noexcept
        {
            if (this != &other)
            {
                delete[] slots_;
                blocks_ = std::move(other.blocks_);
                slots_ = std::exchange(other.slots_, nullptr);
                slot_count_ = std::exchange(other.slot_count_, 0);
            }
            return *this;
        }
        ~TaggedPointerStableSlotStoreImpl() { delete[] slots_; }

        [[nodiscard]] MemoryUtils::StorageLayout layout() const noexcept { return blocks_.layout(); }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept { return blocks_.allocator(); }
        [[nodiscard]] std::size_t capacity() const noexcept { return slot_count_; }
        [[nodiscard]] std::size_t stride() const noexcept { return blocks_.stride(); }
        [[nodiscard]] std::size_t block_count() const noexcept { return blocks_.block_count(); }

        void reserve_to(std::size_t capacity)
        {
            if (capacity <= slot_count_) { return; }

            blocks_.prepare_append();
            auto replacement = std::make_unique<SlotPointer[]>(capacity);
            if (slot_count_ != 0) { std::copy_n(slots_, slot_count_, replacement.get()); }
            StableSlotBlock block = blocks_.allocate_block(slot_count_, capacity - slot_count_);
            for (std::size_t slot = slot_count_; slot < capacity; ++slot)
            {
                replacement[slot].set(block.slot_data(slot), TaggedStableSlotState::Free);
            }

            blocks_.append(std::move(block));
            delete[] slots_;
            slots_ = replacement.release();
            slot_count_ = capacity;
        }

        [[nodiscard]] void *slot_memory(std::size_t slot) const noexcept
        {
            return slot < slot_count_ ? slots_[slot].ptr() : nullptr;
        }
        [[nodiscard]] void *live_slot_memory(std::size_t slot) const noexcept
        {
            if (!live(slot)) { return nullptr; }
            return reinterpret_cast<void *>(slots_[slot].raw_bits());
        }
        [[nodiscard]] void *non_live_slot_memory(std::size_t slot) const noexcept
            requires(Model == StableSlotStateModel::ConstructedAndLive)
        {
            return constructed(slot) && !live(slot) ? slot_memory(slot) : nullptr;
        }
        [[nodiscard]] bool constructed(std::size_t slot) const noexcept
        {
            return slot < slot_count_ && !slots_[slot].has_enum(TaggedStableSlotState::Free);
        }
        [[nodiscard]] bool live(std::size_t slot) const noexcept
        {
            return slot < slot_count_ && slots_[slot].has_enum(TaggedStableSlotState::Live);
        }

        void mark_staged(std::size_t slot) noexcept
        {
            slots_[slot].set_tag(TaggedStableSlotState::Staged);
        }
        [[nodiscard]] bool mark_live(std::size_t slot) noexcept
        {
            if constexpr (Model == StableSlotStateModel::ConstructedAndLive)
            {
                if (!constructed(slot) || live(slot)) { return false; }
            }
            else if (constructed(slot))
            {
                return false;
            }
            slots_[slot].set_tag(TaggedStableSlotState::Live);
            return true;
        }
        [[nodiscard]] bool mark_pending(std::size_t slot) noexcept
        {
            static_assert(Model == StableSlotStateModel::ConstructedAndLive);
            if (!live(slot)) { return false; }
            slots_[slot].set_tag(TaggedStableSlotState::PendingErase);
            return true;
        }
        void mark_free(std::size_t slot) noexcept
        {
            slots_[slot].set_tag(TaggedStableSlotState::Free);
        }
        void reset_states() noexcept
        {
            for (std::size_t slot = 0; slot < slot_count_; ++slot) { mark_free(slot); }
        }

        [[nodiscard]] std::size_t constructed_count() const noexcept
        {
            std::size_t result = 0;
            for (std::size_t slot = 0; slot < slot_count_; ++slot)
            {
                result += constructed(slot) ? 1U : 0U;
            }
            return result;
        }
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            return blocks_.dynamic_storage_metrics(slot_count_, constructed_count());
        }

        [[nodiscard]] const void *debug_capacity_address() const noexcept { return &slot_count_; }
        [[nodiscard]] const void *debug_slots_address() const noexcept { return &slots_; }
        [[nodiscard]] const void *debug_state_address() const noexcept { return &slots_; }

      private:
        StableSlotBlockStorage blocks_{};
        SlotPointer *slots_{nullptr};
        std::size_t slot_count_{0};
    };

    static_assert(sizeof(TaggedPointerStableSlotStoreImpl<StableSlotStateModel::ConstructedOnly>) <=
                  8 * sizeof(void *));
    static_assert(sizeof(BitmapStableSlotStoreImpl<StableSlotStateModel::ConstructedOnly>) <=
                  11 * sizeof(void *));
    static_assert(sizeof(BitmapStableSlotStoreImpl<StableSlotStateModel::ConstructedAndLive>) <=
                  14 * sizeof(void *));
}  // namespace hgraph::detail

#endif  // HGRAPH_TYPES_UTILS_IMPL_STABLE_SLOT_STORE_IMPL_H
