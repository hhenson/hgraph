#ifndef HGRAPH_RUNTIME_NESTED_GRAPH_STORAGE_H
#define HGRAPH_RUNTIME_NESTED_GRAPH_STORAGE_H

#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/utils/stable_slot_store.h>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <limits>
#include <memory>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    /**
     * Stable slots containing an entry header and caller-owned nested-graph
     * payload in one allocation block.
     *
     * Capacity growth appends blocks, so both the entry and graph addresses for
     * existing slots remain stable. Entry destruction owns the GraphValue
     * handle and therefore destroys the externally-placed graph before the raw
     * slot memory is released.
     */
    template <typename Entry>
    class InPlaceGraphSlotStore
    {
      public:
        InPlaceGraphSlotStore() noexcept = default;

        explicit InPlaceGraphSlotStore(
            MemoryUtils::StorageLayout graph_layout,
            const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
        {
            bind_graph_layout(graph_layout, allocator);
        }

        InPlaceGraphSlotStore(const InPlaceGraphSlotStore &) = delete;
        InPlaceGraphSlotStore &operator=(const InPlaceGraphSlotStore &) = delete;
        InPlaceGraphSlotStore(InPlaceGraphSlotStore &&) = delete;
        InPlaceGraphSlotStore &operator=(InPlaceGraphSlotStore &&) = delete;

        ~InPlaceGraphSlotStore() { destroy_all(); }

        void swap(InPlaceGraphSlotStore &other) noexcept
        {
            using std::swap;
            swap(storage_, other.storage_);
            swap(graph_layout_, other.graph_layout_);
            swap(slot_layout_, other.slot_layout_);
            swap(graph_offset_, other.graph_offset_);
            swap(bound_, other.bound_);
        }

        void bind_graph_layout(MemoryUtils::StorageLayout graph_layout)
        {
            bind_graph_layout(graph_layout, storage_.allocator());
        }

        void bind_graph_layout(MemoryUtils::StorageLayout graph_layout,
                               const MemoryUtils::AllocatorOps &allocator)
        {
            if (!graph_layout.valid() || graph_layout.size == 0) {
                throw std::logic_error("InPlaceGraphSlotStore requires a non-empty graph layout");
            }
            if (bound_) {
                if (graph_layout.size != graph_layout_.size ||
                    graph_layout.alignment != graph_layout_.alignment) {
                    throw std::logic_error("InPlaceGraphSlotStore graph layout must remain constant");
                }
                storage_.bind_layout(slot_layout_, allocator);
                return;
            }

            graph_layout_ = graph_layout;
            slot_layout_.alignment = std::max(alignof(Entry), graph_layout.alignment);
            if (!std::has_single_bit(slot_layout_.alignment)) {
                throw std::logic_error("InPlaceGraphSlotStore requires power-of-two alignment");
            }

            graph_offset_ = checked_align_to(sizeof(Entry), graph_layout.alignment);
            if (graph_layout.size > std::numeric_limits<size_t>::max() - graph_offset_) {
                throw std::overflow_error("InPlaceGraphSlotStore slot size overflow");
            }
            slot_layout_.size = checked_align_to(graph_offset_ + graph_layout.size, slot_layout_.alignment);
            storage_.bind_layout(slot_layout_, allocator);
            bound_ = true;
        }

        [[nodiscard]] bool bound() const noexcept { return bound_; }
        [[nodiscard]] size_t slot_capacity() const noexcept { return storage_.slot_capacity(); }
        [[nodiscard]] size_t block_count() const noexcept { return storage_.block_count(); }
        [[nodiscard]] MemoryUtils::StorageLayout graph_layout() const noexcept { return graph_layout_; }
        [[nodiscard]] MemoryUtils::StorageLayout slot_layout() const noexcept { return slot_layout_; }
        [[nodiscard]] size_t graph_offset() const noexcept { return graph_offset_; }
        [[nodiscard]] bool has_entries() const noexcept { return storage_.constructed_count() != 0; }
        [[nodiscard]] size_t entry_count() const noexcept
        {
            return storage_.constructed_count();
        }
        [[nodiscard]] size_t live_bytes() const noexcept
        {
            return entry_count() * slot_layout_.size;
        }
        [[nodiscard]] size_t reserved_bytes() const noexcept
        {
            return storage_.dynamic_storage_metrics().reserved_bytes;
        }

        void reserve_to(size_t capacity)
        {
            require_bound();
            if (capacity <= storage_.slot_capacity()) { return; }
            storage_.reserve_to(capacity);
        }

        [[nodiscard]] bool has_entry(size_t slot) const noexcept
        {
            return storage_.constructed(slot);
        }

        [[nodiscard]] Entry *entry_at(size_t slot) noexcept
        {
            return has_entry(slot) ? MemoryUtils::cast<Entry>(storage_.slot_memory(slot)) : nullptr;
        }

        [[nodiscard]] const Entry *entry_at(size_t slot) const noexcept
        {
            return has_entry(slot) ? MemoryUtils::cast<Entry>(storage_.slot_memory(slot)) : nullptr;
        }

        template <typename... Args>
        Entry &construct_at(size_t slot, Args &&...args)
        {
            require_available_slot(slot);
            Entry *entry = MemoryUtils::cast<Entry>(storage_.slot_memory(slot));
            std::construct_at(entry, std::forward<Args>(args)...);
            storage_.mark_constructed(slot);
            return *entry;
        }

        void destroy_at(size_t slot) noexcept
        {
            Entry *entry = entry_at(slot);
            if (entry == nullptr) { return; }
            std::destroy_at(entry);
            storage_.mark_free(slot);
        }

        void destroy_all() noexcept
        {
            for (size_t slot = 0; slot < slot_capacity(); ++slot) { destroy_at(slot); }
        }

        [[nodiscard]] void *graph_memory(size_t slot)
        {
            require_slot(slot);
            return MemoryUtils::advance(storage_.slot_memory(slot), graph_offset_);
        }

        [[nodiscard]] const void *graph_memory(size_t slot) const
        {
            require_slot(slot);
            return MemoryUtils::advance(storage_.slot_memory(slot), graph_offset_);
        }

        [[nodiscard]] DebugDynamicLayout debug_layout(std::size_t owner_offset,
                                                      std::size_t graph_pointer_offset,
                                                      bool keys_are_owners) const noexcept
        {
            const auto *base = reinterpret_cast<const std::byte *>(this);
            const auto offset_of = [base, owner_offset](const auto *member) {
                return owner_offset + static_cast<std::size_t>(
                                          reinterpret_cast<const std::byte *>(member) - base);
            };
            DebugDynamicFlags flags = DebugDynamicFlags::DataIsIndirect |
                                      DebugDynamicFlags::DataIsPointerTable |
                                      DebugDynamicFlags::HasSlotState |
                                      DebugDynamicFlags::ElementsArePointers |
                                      DebugDynamicFlags::SlotIndexIsIndirect |
                                      DebugDynamicFlags::SlotSizeIsIndirect;
            const StableSlotDebugView slots = storage_.debug_view();
            if (slots.pointers_tagged) { flags = flags | DebugDynamicFlags::DataPointersAreTagged; }
            if (slots.state_tagged) { flags = flags | DebugDynamicFlags::SlotStateIsTaggedPointer; }
            if (keys_are_owners)
            {
                flags = flags | DebugDynamicFlags::KeyDataIsIndirect |
                        DebugDynamicFlags::KeyDataIsPointerTable |
                        DebugDynamicFlags::KeysAreOwners;
                if (slots.pointers_tagged) { flags = flags | DebugDynamicFlags::KeyPointersAreTagged; }
            }
            return DebugDynamicLayout{
                .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
                .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
                .kind = DebugDynamicKind::StableSlots,
                .flags = flags,
                .key_auxiliary_offset =
                    keys_are_owners ? static_cast<std::uint32_t>(slots.pointer_table_offset) : 0,
                .size_offset = slots.slot_count_offset,
                .data_offset = offset_of(slots.implementation_pointer),
                .stride = slot_layout_.size,
                .key_data_offset = keys_are_owners ? offset_of(slots.implementation_pointer) : 0,
                .key_stride = keys_are_owners ? std::size_t{1} : 0,
                .state_offset = slots.state_offset,
                .auxiliary_offset = slots.pointer_table_offset,
                .entry_offset = graph_pointer_offset,
            };
        }

      private:
        StableSlotStore<StableSlotStateModel::ConstructedOnly> storage_{};
        MemoryUtils::StorageLayout    graph_layout_{};
        MemoryUtils::StorageLayout    slot_layout_{};
        size_t                        graph_offset_{0};
        bool                          bound_{false};

        [[nodiscard]] static size_t checked_align_to(size_t offset, size_t alignment)
        {
            if (alignment <= 1) { return offset; }
            const size_t mask = alignment - 1;
            if (offset > std::numeric_limits<size_t>::max() - mask) {
                throw std::overflow_error("InPlaceGraphSlotStore aligned size overflow");
            }
            return (offset + mask) & ~mask;
        }

        void require_bound() const
        {
            if (!bound_) { throw std::logic_error("InPlaceGraphSlotStore has no graph layout"); }
        }

        void require_slot(size_t slot) const
        {
            require_bound();
            if (slot >= slot_capacity()) { throw std::out_of_range("InPlaceGraphSlotStore slot out of range"); }
        }

        void require_available_slot(size_t slot) const
        {
            require_slot(slot);
            if (has_entry(slot)) { throw std::logic_error("InPlaceGraphSlotStore slot is already constructed"); }
        }
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NESTED_GRAPH_STORAGE_H
