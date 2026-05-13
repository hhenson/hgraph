#pragma once

#include <cstddef>
#include <memory>
#include <new>
#include <vector>

namespace hgraph
{
    struct StableSlotStorageDeleter
    {
        size_t alignment{alignof(std::max_align_t)};

        void operator()(std::byte *storage) const noexcept
        {
            if (storage != nullptr) { ::operator delete(storage, std::align_val_t{alignment}); }
        }
    };

    /**
     * One heap block in a chained stable-slot allocation.
     *
     * Each growth appends a new block and records one pointer per slot in the
     * top-level slot table. Existing slot payload addresses therefore never
     * move when capacity grows.
     */
    struct StableSlotBlock
    {
        using Storage = std::unique_ptr<std::byte, StableSlotStorageDeleter>;

        Storage storage{};
        size_t  first_slot{0};
        size_t  slot_count{0};

        [[nodiscard]] static StableSlotBlock allocate(size_t first_slot, size_t slot_count, size_t stride, size_t alignment)
        {
            StableSlotBlock block;
            block.first_slot = first_slot;
            block.slot_count = slot_count;
            if (slot_count == 0) { return block; }
            block.storage = Storage(
                static_cast<std::byte *>(::operator new(slot_count * stride, std::align_val_t{alignment})),
                StableSlotStorageDeleter{alignment});
            return block;
        }

        [[nodiscard]] std::byte *slot_data(size_t slot, size_t stride) const noexcept
        {
            if (!storage || slot < first_slot || slot >= first_slot + slot_count) { return nullptr; }
            return storage.get() + (slot - first_slot) * stride;
        }
    };

    /**
     * Double-indexed stable slot storage.
     *
     * The first index is the logical slot id -> payload pointer table.
     * The second index is the chained heap blocks that own the actual payload
     * memory. This is the key non-moving property needed by stable-slot keyed
     * containers such as delta `TSD` storage and future keyed nested runtimes.
     */
    struct StableSlotStorage
    {
        std::vector<std::byte *>   slots{};
        std::vector<StableSlotBlock> blocks{};

        [[nodiscard]] size_t slot_capacity() const noexcept { return slots.size(); }

        [[nodiscard]] std::byte *slot_data(size_t slot) const noexcept
        {
            return slot < slots.size() ? slots[slot] : nullptr;
        }

        void reserve_to(size_t new_capacity, size_t stride, size_t alignment)
        {
            if (new_capacity <= slots.size()) { return; }

            const size_t old_capacity = slots.size();
            slots.resize(new_capacity, nullptr);
            blocks.reserve(blocks.size() + 1);

            StableSlotBlock block = StableSlotBlock::allocate(old_capacity, new_capacity - old_capacity, stride, alignment);
            for (size_t slot = old_capacity; slot < new_capacity; ++slot) {
                slots[slot] = block.slot_data(slot, stride);
            }

            if (block.slot_count != 0) { blocks.push_back(std::move(block)); }
        }

        void clear() noexcept
        {
            slots.clear();
            blocks.clear();
        }
    };
}  // namespace hgraph
