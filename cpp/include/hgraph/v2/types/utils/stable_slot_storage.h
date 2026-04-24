#ifndef HGRAPH_CPP_ROOT_V2_STABLE_SLOT_STORAGE_H
#define HGRAPH_CPP_ROOT_V2_STABLE_SLOT_STORAGE_H

#include <hgraph/v2/types/utils/memory_utils.h>

#include <bit>
#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <vector>

namespace hgraph::v2
{
    struct StableSlotStorageDeleter
    {
        const MemoryUtils::AllocatorOps *allocator{&MemoryUtils::allocator()};
        MemoryUtils::StorageLayout layout{};

        void operator()(std::byte *storage) const noexcept
        {
            if (storage != nullptr && allocator != nullptr) {
                allocator->deallocate_storage(storage, layout);
            }
        }
    };

    /**
     * One owned heap block in a chained stable-slot allocation.
     *
     * Growth appends new blocks without relocating existing ones, which keeps
     * previously published slot addresses stable for the lifetime of the
     * storage.
     */
    struct StableSlotBlock
    {
        using Storage = std::unique_ptr<std::byte, StableSlotStorageDeleter>;

        Storage storage{};
        size_t first_slot{0};
        size_t slot_count{0};
        size_t stride{0};

        [[nodiscard]] static constexpr size_t stride_for(size_t slot_size, size_t alignment) noexcept
        {
            if (alignment <= 1) {
                return slot_size;
            }
            const size_t mask = alignment - 1;
            return (slot_size + mask) & ~mask;
        }

        [[nodiscard]] static StableSlotBlock allocate(size_t first_slot,
                                                      size_t slot_count,
                                                      size_t slot_size,
                                                      size_t alignment,
                                                      const MemoryUtils::AllocatorOps &allocator)
        {
            if (slot_size == 0) {
                throw std::logic_error("StableSlotStorage requires slot_size > 0");
            }
            if (alignment == 0 || !std::has_single_bit(alignment)) {
                throw std::logic_error("StableSlotStorage requires alignment to be a power of two");
            }

            StableSlotBlock block;
            block.first_slot = first_slot;
            block.slot_count = slot_count;
            block.stride = stride_for(slot_size, alignment);

            if (slot_count == 0) {
                return block;
            }

            const MemoryUtils::StorageLayout layout{
                .size = slot_count * block.stride,
                .alignment = alignment,
            };

            block.storage = Storage(static_cast<std::byte *>(allocator.allocate_storage(layout)),
                                    StableSlotStorageDeleter{
                                        .allocator = &allocator,
                                        .layout = layout,
                                    });
            return block;
        }

        [[nodiscard]] std::byte *slot_data(size_t slot) const noexcept
        {
            if (!storage || slot < first_slot || slot >= first_slot + slot_count) {
                return nullptr;
            }
            return storage.get() + (slot - first_slot) * stride;
        }
    };

    /**
     * Double-indexed stable slot storage.
     *
     * `slots` is the logical slot-id to payload-address table. `blocks` owns
     * the chained heap allocations that back those addresses. Growing the
     * storage appends a block and extends the top-level slot table, so
     * previously issued slot pointers never move.
     */
    struct StableSlotStorage
    {
        StableSlotStorage() noexcept = default;

        explicit StableSlotStorage(const MemoryUtils::AllocatorOps &allocator) noexcept
            : m_allocator(&allocator)
        {
        }

        /**
         * This is the index of slot-to-memory-pointer for slot. This points into one of the blocks
         * stored in ``blocks``.
         */
        std::vector<std::byte *> slots{};
        std::vector<StableSlotBlock> blocks{};
        size_t slot_size{0};
        size_t slot_alignment{0};
        size_t slot_stride{0};

        [[nodiscard]] size_t slot_capacity() const noexcept { return slots.size(); }
        [[nodiscard]] size_t stride() const noexcept { return slot_stride; }
        [[nodiscard]] size_t element_size() const noexcept { return slot_size; }
        [[nodiscard]] size_t alignment() const noexcept { return slot_alignment; }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept { return *m_allocator; }

        [[nodiscard]] std::byte *slot_data(size_t slot) const noexcept
        {
            return slot < slots.size() ? slots[slot] : nullptr;
        }

        void reserve_to(size_t new_capacity, size_t new_slot_size, size_t new_slot_alignment)
        {
            bind_layout(new_slot_size, new_slot_alignment);

            if (new_capacity <= slots.size()) {
                return;
            }

            const size_t old_capacity = slots.size();
            slots.resize(new_capacity, nullptr);
            blocks.reserve(blocks.size() + 1);

            StableSlotBlock block = StableSlotBlock::allocate(
                old_capacity, new_capacity - old_capacity, slot_size, slot_alignment, allocator());
            for (size_t slot = old_capacity; slot < new_capacity; ++slot) {
                slots[slot] = block.slot_data(slot);
            }

            if (block.slot_count != 0) {
                blocks.push_back(std::move(block));
            }
        }

        void clear() noexcept
        {
            slots.clear();
            blocks.clear();
            slot_size = 0;
            slot_alignment = 0;
            slot_stride = 0;
        }

      private:
        const MemoryUtils::AllocatorOps *m_allocator{&MemoryUtils::allocator()};

        void bind_layout(size_t new_slot_size, size_t new_slot_alignment)
        {
            if (new_slot_size == 0) {
                throw std::logic_error("StableSlotStorage requires slot_size > 0");
            }
            if (new_slot_alignment == 0 || !std::has_single_bit(new_slot_alignment)) {
                throw std::logic_error("StableSlotStorage requires alignment to be a power of two");
            }

            if (slot_size == 0) {
                slot_size = new_slot_size;
                slot_alignment = new_slot_alignment;
                slot_stride = StableSlotBlock::stride_for(new_slot_size, new_slot_alignment);
                return;
            }

            if (slot_size != new_slot_size || slot_alignment != new_slot_alignment) {
                throw std::logic_error("StableSlotStorage layout must remain constant");
            }
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_V2_STABLE_SLOT_STORAGE_H
