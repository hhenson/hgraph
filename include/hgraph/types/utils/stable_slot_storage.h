#ifndef HGRAPH_CPP_ROOT_V2_STABLE_SLOT_STORAGE_H
#define HGRAPH_CPP_ROOT_V2_STABLE_SLOT_STORAGE_H

#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/storage_metrics.h>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Custom deleter for the heap blocks held by ``StableSlotBlock``.
     *
     * Captures the allocator and storage layout at allocation time so the
     * matching ``deallocate_storage`` call can be issued without the slot
     * storage having to remember which allocator it used.
     */
    struct StableSlotStorageDeleter
    {
        /** Allocator used for the originating allocation. */
        const MemoryUtils::AllocatorOps *allocator{&MemoryUtils::allocator()};
        /** Layout passed to the allocator and required for the matching deallocation. */
        MemoryUtils::StorageLayout layout{};

        /** Deallocate ``storage`` if non-null. */
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
     * storage. Each block knows the slot range it backs and the byte stride
     * used between slots.
     */
    struct StableSlotBlock
    {
        /** ``unique_ptr`` over the heap allocation, customised with the layout-aware deleter. */
        using Storage = std::unique_ptr<std::byte, StableSlotStorageDeleter>;

        /** Owned heap allocation backing the slots in this block. */
        Storage storage{};
        /** Slot id assigned to the first slot in this block. */
        size_t first_slot{0};
        /** Number of contiguous slots covered by this block. */
        size_t slot_count{0};
        /** Byte distance between consecutive slot starts. */
        size_t stride{0};

        /**
         * Round ``slot_size`` up to a multiple of ``alignment`` so adjacent
         * slots remain aligned within the block. Returns ``slot_size``
         * unchanged when ``alignment <= 1``.
         */
        [[nodiscard]] static constexpr size_t stride_for(size_t slot_size, size_t alignment) noexcept
        {
            if (alignment <= 1) {
                return slot_size;
            }
            const size_t mask = alignment - 1;
            return (slot_size + mask) & ~mask;
        }

        /**
         * Allocate a single block covering ``slot_count`` slots starting at
         * ``first_slot``, using ``allocator`` for the heap allocation.
         *
         * Throws ``std::logic_error`` if ``slot_size`` is zero or
         * ``alignment`` is not a power of two. A block with ``slot_count``
         * zero allocates nothing and is returned with a null storage handle.
         */
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

        /**
         * Pointer to the byte representation of ``slot``, or ``nullptr`` if
         * the slot is outside this block's range.
         */
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
     * ``slots`` is the logical slot-id to payload-address table; ``blocks``
     * owns the chained heap allocations behind those addresses. Growing the
     * storage appends a block and extends the top-level slot table, so
     * previously issued slot pointers never move.
     *
     * The slot layout (size + alignment) is fixed on first reservation; later
     * calls to ``reserve_to`` with different parameters throw.
     */
    struct StableSlotStorage
    {
        /** Construct empty storage using the default allocator. */
        StableSlotStorage() noexcept = default;

        /** Construct empty storage using ``allocator``. */
        explicit StableSlotStorage(const MemoryUtils::AllocatorOps &allocator) noexcept
            : m_allocator(&allocator)
        {
        }

        StableSlotStorage(const StableSlotStorage &) = delete;
        StableSlotStorage &operator=(const StableSlotStorage &) = delete;

        StableSlotStorage(StableSlotStorage &&other) noexcept
            : slots(std::exchange(other.slots, nullptr)), slot_count(std::exchange(other.slot_count, 0)),
              blocks(std::move(other.blocks)), slot_size(std::exchange(other.slot_size, 0)),
              slot_alignment(std::exchange(other.slot_alignment, 0)),
              slot_stride(std::exchange(other.slot_stride, 0)), m_allocator(other.m_allocator)
        {
        }

        StableSlotStorage &operator=(StableSlotStorage &&other) noexcept
        {
            if (this != &other)
            {
                clear();
                slots = std::exchange(other.slots, nullptr);
                slot_count = std::exchange(other.slot_count, 0);
                blocks = std::move(other.blocks);
                slot_size = std::exchange(other.slot_size, 0);
                slot_alignment = std::exchange(other.slot_alignment, 0);
                slot_stride = std::exchange(other.slot_stride, 0);
                m_allocator = other.m_allocator;
            }
            return *this;
        }

        ~StableSlotStorage() { clear(); }

        /**
         * Slot-id → byte-pointer index. Each pointer points into one of the
         * blocks owned by ``blocks``; entries never move once published.
         */
        std::byte **slots{nullptr};
        /** Number of pointers in ``slots``. */
        size_t slot_count{0};
        /** Chained heap blocks backing the slot-id index. */
        std::vector<StableSlotBlock> blocks{};
        /** Bound element size, set on first reservation. */
        size_t slot_size{0};
        /** Bound element alignment, set on first reservation. */
        size_t slot_alignment{0};
        /** Per-slot byte stride, equal to ``stride_for(slot_size, slot_alignment)``. */
        size_t slot_stride{0};

        /** Number of slots currently addressable via ``slot_data``. */
        [[nodiscard]] size_t slot_capacity() const noexcept { return slot_count; }
        /** Per-slot byte stride. */
        [[nodiscard]] size_t stride() const noexcept { return slot_stride; }
        /** Bound element size. */
        [[nodiscard]] size_t element_size() const noexcept { return slot_size; }
        /** Bound element alignment. */
        [[nodiscard]] size_t alignment() const noexcept { return slot_alignment; }
        /** Allocator used to back this storage. */
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept { return *m_allocator; }

        /** Exact occupied/retained heap bytes for this slot allocation. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics(size_t constructed_count) const noexcept
        {
            const size_t pointer_bytes = slot_count * sizeof(std::byte *);
            const size_t live_block_metadata = blocks.size() * sizeof(StableSlotBlock);
            const size_t reserved_block_metadata = blocks.capacity() * sizeof(StableSlotBlock);
            return {
                .live_bytes = pointer_bytes + live_block_metadata + constructed_count * slot_stride,
                .reserved_bytes = pointer_bytes + reserved_block_metadata + slot_count * slot_stride,
            };
        }

        /** Pointer for ``slot``, or ``nullptr`` if the slot is out of range. */
        [[nodiscard]] std::byte *slot_data(size_t slot) const noexcept
        {
            return slot < slot_count ? slots[slot] : nullptr;
        }

        /**
         * Grow storage so at least ``new_capacity`` slots are addressable.
         *
         * On the first call, binds the slot layout. Subsequent calls must
         * pass the same ``new_slot_size`` / ``new_slot_alignment`` or this
         * throws. No-op if ``new_capacity`` is at most the current capacity.
         */
        void reserve_to(size_t new_capacity, size_t new_slot_size, size_t new_slot_alignment)
        {
            bind_layout(new_slot_size, new_slot_alignment);

            if (new_capacity <= slot_count) {
                return;
            }

            const size_t old_capacity = slot_count;
            blocks.reserve(blocks.size() + 1);

            auto replacement = std::make_unique<std::byte *[]>(new_capacity);
            if (old_capacity != 0) { std::copy_n(slots, old_capacity, replacement.get()); }

            StableSlotBlock block = StableSlotBlock::allocate(
                old_capacity, new_capacity - old_capacity, slot_size, slot_alignment, allocator());
            for (size_t slot = old_capacity; slot < new_capacity; ++slot) {
                replacement[slot] = block.slot_data(slot);
            }

            if (block.slot_count != 0) {
                blocks.push_back(std::move(block));
            }
            delete[] slots;
            slots = replacement.release();
            slot_count = new_capacity;
        }

        /** Drop every block and reset the bound layout. Existing slot pointers become invalid. */
        void clear() noexcept
        {
            delete[] slots;
            slots = nullptr;
            slot_count = 0;
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
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_V2_STABLE_SLOT_STORAGE_H
