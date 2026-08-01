#ifndef HGRAPH_TESTS_STABLE_SLOT_REPRESENTATION_PROTOTYPE_H
#define HGRAPH_TESTS_STABLE_SLOT_REPRESENTATION_PROTOTYPE_H

#include <hgraph/types/utils/slot_bitmap.h>
#include <hgraph/types/utils/stable_slot_storage.h>
#include <hgraph/util/tagged_ptr.h>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph::experimental {
enum class SlotState : std::uintptr_t {
  Live = 0b00,
  PendingErase = 0b01,
  Staged = 0b10,
  Free = 0b11,
};

enum class SlotRepresentationKind : std::uint8_t {
  BitmapPointer,
  TaggedPointer,
  ParentTrackedTrivial,
  ExplicitStateByte,
};

struct SlotRepresentationChoice {
  SlotRepresentationKind kind{SlotRepresentationKind::TaggedPointer};
  std::size_t payload_stride{0};
  std::size_t lifecycle_bits_per_slot{0};
};

[[nodiscard]] constexpr std::size_t
aligned_size(std::size_t size, std::size_t alignment) noexcept {
  if (alignment <= 1) {
    return size;
  }
  const std::size_t mask = alignment - 1;
  return (size + mask) & ~mask;
}

[[nodiscard]] constexpr SlotRepresentationChoice choose_slot_representation(
    std::size_t payload_size, std::size_t payload_alignment,
    bool trivially_destructible,
    std::size_t tag_alignment = alignof(std::uintptr_t)) noexcept {
  // Pointer-table bytes are common to all four candidates. Compare the
  // differing payload stride and lifecycle bits using eighths of a byte so
  // the compact bitmap baseline is not accidentally rounded up to a byte.
  SlotRepresentationChoice best{
      SlotRepresentationKind::BitmapPointer,
      aligned_size(payload_size, payload_alignment),
      2,
  };
  const auto consider = [&best](SlotRepresentationChoice candidate) constexpr {
    const std::size_t best_bits =
        best.payload_stride * 8 + best.lifecycle_bits_per_slot;
    const std::size_t candidate_bits =
        candidate.payload_stride * 8 + candidate.lifecycle_bits_per_slot;
    if (candidate_bits < best_bits) {
      best = candidate;
    }
  };

  const std::size_t tagged_alignment =
      std::max(payload_alignment, tag_alignment);
  const std::size_t tagged_stride =
      aligned_size(payload_size, tagged_alignment);
  consider({SlotRepresentationKind::TaggedPointer, tagged_stride, 0});
  if (trivially_destructible) {
    consider({SlotRepresentationKind::ParentTrackedTrivial,
              aligned_size(payload_size, payload_alignment), 1});
  } else {
    consider({SlotRepresentationKind::ExplicitStateByte,
              aligned_size(payload_size + 1, payload_alignment), 0});
  }
  return best;
}

struct PrototypeMemoryReport {
  std::size_t payload_bytes{0};
  std::size_t slot_index_bytes{0};
  std::size_t lifecycle_index_bytes{0};
  std::size_t block_descriptor_bytes{0};
  std::size_t payload_allocations{0};
  std::size_t slot_index_allocations{0};
  std::size_t slot_management_allocations{0};
  std::size_t block_descriptor_allocations{0};

  [[nodiscard]] constexpr std::size_t total_bytes() const noexcept {
    return payload_bytes + slot_index_bytes + lifecycle_index_bytes +
           block_descriptor_bytes;
  }

  [[nodiscard]] constexpr std::size_t total_allocations() const noexcept {
    return payload_allocations + slot_index_allocations +
           slot_management_allocations + block_descriptor_allocations;
  }
};

class PrototypePayloadBlocks {
public:
  PrototypePayloadBlocks(
      std::size_t slot_size, std::size_t slot_alignment,
      const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
      : slot_size_(slot_size), slot_alignment_(slot_alignment),
        allocator_(&allocator) {
    if (slot_size_ == 0) {
      throw std::logic_error("prototype slot size must be positive");
    }
  }

  PrototypePayloadBlocks(const PrototypePayloadBlocks &) = delete;
  PrototypePayloadBlocks &operator=(const PrototypePayloadBlocks &) = delete;
  PrototypePayloadBlocks(PrototypePayloadBlocks &&) = delete;
  PrototypePayloadBlocks &operator=(PrototypePayloadBlocks &&) = delete;

  [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }
  [[nodiscard]] std::size_t stride() const noexcept {
    return StableSlotBlock::stride_for(slot_size_, slot_alignment_);
  }

  template <typename Initializer>
  void reserve_to(std::size_t capacity, Initializer &&initializer) {
    if (capacity <= capacity_) {
      return;
    }
    blocks_.reserve(blocks_.size() + 1);
    StableSlotBlock block =
        StableSlotBlock::allocate(capacity_, capacity - capacity_, slot_size_,
                                  slot_alignment_, *allocator_);
    std::forward<Initializer>(initializer)(block);
    blocks_.push_back(std::move(block));
    capacity_ = capacity;
  }

  [[nodiscard]] std::byte *raw_slot(std::size_t slot) const {
    if (slot >= capacity_) {
      throw std::out_of_range("prototype slot out of range");
    }
    const auto it =
        std::upper_bound(blocks_.begin(), blocks_.end(), slot,
                         [](std::size_t value, const StableSlotBlock &block) {
                           return value < block.first_slot;
                         });
    if (it == blocks_.begin()) {
      throw std::logic_error("prototype slot block lookup failed");
    }
    return std::prev(it)->slot_data(slot);
  }

  [[nodiscard]] std::size_t payload_bytes() const noexcept {
    std::size_t result = 0;
    for (const auto &block : blocks_) {
      result += block.slot_count * block.stride;
    }
    return result;
  }

  [[nodiscard]] std::size_t descriptor_bytes() const noexcept {
    return blocks_.capacity() * sizeof(StableSlotBlock);
  }
  [[nodiscard]] std::size_t block_count() const noexcept {
    return blocks_.size();
  }
  [[nodiscard]] std::size_t descriptor_allocations() const noexcept {
    return blocks_.capacity() == 0 ? 0 : 1;
  }

private:
  std::vector<StableSlotBlock> blocks_{};
  std::size_t capacity_{0};
  std::size_t slot_size_{0};
  std::size_t slot_alignment_{0};
  const MemoryUtils::AllocatorOps *allocator_{nullptr};
};

template <typename T> class BitmapPointerRepresentation {
public:
  [[nodiscard]] static constexpr std::size_t storage_slot_size() noexcept {
    return sizeof(T);
  }
  [[nodiscard]] static constexpr std::size_t storage_alignment() noexcept {
    return alignof(T);
  }

  void reserve_entries(std::size_t old_capacity, std::size_t new_capacity,
                       const StableSlotBlock &block) {
    pointers_.resize(new_capacity);
    constructed_.resize(new_capacity);
    live_.resize(new_capacity);
    for (std::size_t slot = old_capacity; slot < new_capacity; ++slot) {
      pointers_[slot] = MemoryUtils::cast<T>(block.slot_data(slot));
    }
  }

  [[nodiscard]] T *free_memory(std::size_t slot,
                               const PrototypePayloadBlocks &) const noexcept {
    return pointers_[slot];
  }
  [[nodiscard]] T *memory(std::size_t slot) const noexcept {
    return pointers_[slot];
  }
  [[nodiscard]] SlotState state(std::size_t slot) const noexcept {
    if (!constructed_.test(slot)) {
      return SlotState::Free;
    }
    return live_.test(slot) ? SlotState::Live : SlotState::PendingErase;
  }
  [[nodiscard]] bool constructed(std::size_t slot) const noexcept {
    return constructed_.test(slot);
  }
  [[nodiscard]] bool live(std::size_t slot) const noexcept {
    return live_.test(slot);
  }
  [[nodiscard]] bool pending(std::size_t slot) const noexcept {
    return constructed_.test(slot) && !live_.test(slot);
  }

  void mark_staged(std::size_t slot, T *) noexcept { constructed_.set(slot); }
  void mark_live(std::size_t slot) noexcept { live_.set(slot); }
  void mark_pending(std::size_t slot) noexcept { live_.reset(slot); }
  void mark_free(std::size_t slot) noexcept {
    live_.reset(slot);
    constructed_.reset(slot);
  }

  [[nodiscard]] std::size_t index_bytes() const noexcept {
    return pointers_.capacity() * sizeof(T *) +
           constructed_.word_capacity * sizeof(std::uint64_t) +
           live_.word_capacity * sizeof(std::uint64_t);
  }
  [[nodiscard]] std::size_t index_allocations() const noexcept {
    return (pointers_.capacity() == 0 ? 0 : 1) +
           (constructed_.word_capacity == 0 ? 0 : 1) +
           (live_.word_capacity == 0 ? 0 : 1);
  }

private:
  std::vector<T *> pointers_{};
  SlotBitmap constructed_{};
  SlotBitmap live_{};
};

template <typename T, std::size_t TagAlignment = alignof(std::uintptr_t)>
class TaggedPointerRepresentation {
public:
  static_assert(TagAlignment >= 4 && std::has_single_bit(TagAlignment));
  using SlotPointer = erased_tagged_ptr<TagAlignment, 2, SlotState>;

  [[nodiscard]] static constexpr std::size_t storage_slot_size() noexcept {
    return sizeof(T);
  }
  [[nodiscard]] static constexpr std::size_t storage_alignment() noexcept {
    return std::max(alignof(T), TagAlignment);
  }

  void reserve_entries(std::size_t old_capacity, std::size_t new_capacity,
                       const StableSlotBlock &block) {
    pointers_.resize(new_capacity);
    for (std::size_t slot = old_capacity; slot < new_capacity; ++slot) {
      pointers_[slot].set(MemoryUtils::cast<T>(block.slot_data(slot)),
                          SlotState::Free);
    }
  }

  [[nodiscard]] T *free_memory(std::size_t slot,
                               const PrototypePayloadBlocks &) const noexcept {
    return pointers_[slot].template as<T>();
  }
  [[nodiscard]] T *memory(std::size_t slot) const noexcept {
    const auto &pointer = pointers_[slot];
    if (pointer.has_enum(SlotState::Live)) {
      return reinterpret_cast<T *>(pointer.raw_bits());
    }
    return pointer.template as<T>();
  }
  [[nodiscard]] SlotState state(std::size_t slot) const noexcept {
    return pointers_[slot].enum_value();
  }
  [[nodiscard]] bool constructed(std::size_t slot) const noexcept {
    const auto value = state(slot);
    return value != SlotState::Free;
  }
  [[nodiscard]] bool live(std::size_t slot) const noexcept {
    return state(slot) == SlotState::Live;
  }
  [[nodiscard]] bool pending(std::size_t slot) const noexcept {
    return state(slot) == SlotState::PendingErase;
  }

  void mark_staged(std::size_t slot, T *) noexcept {
    pointers_[slot].set_tag(SlotState::Staged);
  }
  void mark_live(std::size_t slot) noexcept {
    pointers_[slot].set_tag(SlotState::Live);
  }
  void mark_pending(std::size_t slot) noexcept {
    pointers_[slot].set_tag(SlotState::PendingErase);
  }
  void mark_free(std::size_t slot) noexcept {
    pointers_[slot].set_tag(SlotState::Free);
  }

  [[nodiscard]] std::size_t index_bytes() const noexcept {
    return pointers_.capacity() * sizeof(SlotPointer);
  }
  [[nodiscard]] std::size_t index_allocations() const noexcept {
    return pointers_.capacity() == 0 ? 0 : 1;
  }

private:
  std::vector<SlotPointer> pointers_{};
};

template <typename T> class ParentTrackedTrivialRepresentation {
public:
  static_assert(std::is_trivially_destructible_v<T>);

  [[nodiscard]] static constexpr std::size_t storage_slot_size() noexcept {
    return sizeof(T);
  }
  [[nodiscard]] static constexpr std::size_t storage_alignment() noexcept {
    return alignof(T);
  }

  void reserve_entries(std::size_t, std::size_t new_capacity,
                       const StableSlotBlock &) {
    pointers_.resize(new_capacity, nullptr);
    live_.resize(new_capacity);
  }

  [[nodiscard]] T *free_memory(std::size_t slot,
                               const PrototypePayloadBlocks &blocks) const {
    return MemoryUtils::cast<T>(blocks.raw_slot(slot));
  }
  [[nodiscard]] T *memory(std::size_t slot) const noexcept {
    return pointers_[slot];
  }
  [[nodiscard]] SlotState state(std::size_t slot) const noexcept {
    if (pointers_[slot] == nullptr) {
      return SlotState::Free;
    }
    return live_.test(slot) ? SlotState::Live : SlotState::PendingErase;
  }
  [[nodiscard]] bool constructed(std::size_t slot) const noexcept {
    return pointers_[slot] != nullptr;
  }
  [[nodiscard]] bool live(std::size_t slot) const noexcept {
    return live_.test(slot);
  }
  [[nodiscard]] bool pending(std::size_t slot) const noexcept {
    return pointers_[slot] != nullptr && !live_.test(slot);
  }

  void mark_staged(std::size_t slot, T *memory) noexcept {
    pointers_[slot] = memory;
  }
  void mark_live(std::size_t slot) noexcept { live_.set(slot); }
  void mark_pending(std::size_t slot) noexcept { live_.reset(slot); }
  void mark_free(std::size_t slot) noexcept {
    live_.reset(slot);
    pointers_[slot] = nullptr;
  }

  [[nodiscard]] std::size_t index_bytes() const noexcept {
    return pointers_.capacity() * sizeof(T *) +
           live_.word_capacity * sizeof(std::uint64_t);
  }
  [[nodiscard]] std::size_t index_allocations() const noexcept {
    return (pointers_.capacity() == 0 ? 0 : 1) +
           (live_.word_capacity == 0 ? 0 : 1);
  }

private:
  std::vector<T *> pointers_{};
  SlotBitmap live_{};
};

template <typename T> class StateByteRepresentation {
public:
  [[nodiscard]] static constexpr std::size_t storage_slot_size() noexcept {
    return sizeof(T) + 1;
  }
  [[nodiscard]] static constexpr std::size_t storage_alignment() noexcept {
    return alignof(T);
  }

  void reserve_entries(std::size_t old_capacity, std::size_t new_capacity,
                       const StableSlotBlock &block) {
    pointers_.resize(new_capacity);
    for (std::size_t slot = old_capacity; slot < new_capacity; ++slot) {
      pointers_[slot] = MemoryUtils::cast<T>(block.slot_data(slot));
      write_state(slot, SlotState::Free);
    }
  }

  [[nodiscard]] T *free_memory(std::size_t slot,
                               const PrototypePayloadBlocks &) const noexcept {
    return pointers_[slot];
  }
  [[nodiscard]] T *memory(std::size_t slot) const noexcept {
    return pointers_[slot];
  }
  [[nodiscard]] SlotState state(std::size_t slot) const noexcept {
    return static_cast<SlotState>(
        std::to_integer<std::uint8_t>(*state_memory(slot)));
  }
  [[nodiscard]] bool constructed(std::size_t slot) const noexcept {
    return state(slot) != SlotState::Free;
  }
  [[nodiscard]] bool live(std::size_t slot) const noexcept {
    return state(slot) == SlotState::Live;
  }
  [[nodiscard]] bool pending(std::size_t slot) const noexcept {
    return state(slot) == SlotState::PendingErase;
  }

  void mark_staged(std::size_t slot, T *) noexcept {
    write_state(slot, SlotState::Staged);
  }
  void mark_live(std::size_t slot) noexcept {
    write_state(slot, SlotState::Live);
  }
  void mark_pending(std::size_t slot) noexcept {
    write_state(slot, SlotState::PendingErase);
  }
  void mark_free(std::size_t slot) noexcept {
    write_state(slot, SlotState::Free);
  }

  [[nodiscard]] std::size_t index_bytes() const noexcept {
    return pointers_.capacity() * sizeof(T *);
  }
  [[nodiscard]] std::size_t index_allocations() const noexcept {
    return pointers_.capacity() == 0 ? 0 : 1;
  }

private:
  [[nodiscard]] std::byte *state_memory(std::size_t slot) const noexcept {
    return reinterpret_cast<std::byte *>(pointers_[slot]) + sizeof(T);
  }

  void write_state(std::size_t slot, SlotState state) noexcept {
    *state_memory(slot) =
        static_cast<std::byte>(static_cast<std::uint8_t>(state));
  }

  std::vector<T *> pointers_{};
};

template <typename T, typename Representation> class PrototypeStableSlotStore {
public:
  using value_type = T;

  explicit PrototypeStableSlotStore(std::size_t initial_capacity = 0)
      : blocks_(Representation::storage_slot_size(),
                Representation::storage_alignment()) {
    if (initial_capacity != 0) {
      reserve_to(initial_capacity);
    }
  }

  PrototypeStableSlotStore(const PrototypeStableSlotStore &) = delete;
  PrototypeStableSlotStore &
  operator=(const PrototypeStableSlotStore &) = delete;
  PrototypeStableSlotStore(PrototypeStableSlotStore &&) = delete;
  PrototypeStableSlotStore &operator=(PrototypeStableSlotStore &&) = delete;

  ~PrototypeStableSlotStore() { destroy_all(); }

  [[nodiscard]] std::size_t capacity() const noexcept {
    return blocks_.capacity();
  }
  [[nodiscard]] std::size_t size() const noexcept { return live_count_; }
  [[nodiscard]] std::size_t pending_erase_count() const noexcept {
    return pending_count_;
  }
  [[nodiscard]] std::size_t payload_stride() const noexcept {
    return blocks_.stride();
  }

  void reserve_to(std::size_t capacity) {
    if (capacity <= blocks_.capacity()) {
      return;
    }
    const std::size_t old_capacity = blocks_.capacity();
    free_slots_.reserve(capacity);
    pending_slots_.reserve(capacity);
    blocks_.reserve_to(capacity, [&](const StableSlotBlock &block) {
      representation_.reserve_entries(old_capacity, capacity, block);
    });
    for (std::size_t slot = capacity; slot > old_capacity; --slot) {
      free_slots_.push_back(slot - 1);
    }
  }

  template <typename Publish, typename... Args>
  std::size_t emplace_with_publish(Publish &&publish, Args &&...args) {
    if (free_slots_.empty()) {
      reserve_to(
          std::max<std::size_t>(live_count_ + pending_count_ + 1,
                                std::max<std::size_t>(8, capacity() * 2)));
    }
    const std::size_t slot = free_slots_.back();
    free_slots_.pop_back();
    T *memory = representation_.free_memory(slot, blocks_);
    try {
      std::construct_at(memory, std::forward<Args>(args)...);
    } catch (...) {
      free_slots_.push_back(slot);
      throw;
    }
    representation_.mark_staged(slot, memory);
    try {
      std::forward<Publish>(publish)(slot, memory);
    } catch (...) {
      std::destroy_at(memory);
      representation_.mark_free(slot);
      free_slots_.push_back(slot);
      throw;
    }
    representation_.mark_live(slot);
    ++live_count_;
    return slot;
  }

  template <typename... Args> std::size_t emplace(Args &&...args) {
    return emplace_with_publish([](std::size_t, T *) {},
                                std::forward<Args>(args)...);
  }

  bool remove(std::size_t slot) {
    if (slot >= capacity() || !representation_.live(slot)) {
      return false;
    }
    pending_slots_.push_back(slot);
    representation_.mark_pending(slot);
    --live_count_;
    ++pending_count_;
    return true;
  }

  bool resurrect(std::size_t slot) {
    if (slot >= capacity() || !representation_.pending(slot)) {
      return false;
    }
    representation_.mark_live(slot);
    ++live_count_;
    --pending_count_;
    if (pending_count_ == 0) {
      pending_slots_.clear();
    }
    return true;
  }

  void erase_pending() noexcept {
    for (const std::size_t slot : pending_slots_) {
      if (!representation_.pending(slot)) {
        continue;
      }
      std::destroy_at(representation_.memory(slot));
      representation_.mark_free(slot);
      free_slots_.push_back(slot);
    }
    pending_slots_.clear();
    pending_count_ = 0;
  }

  [[nodiscard]] SlotState state(std::size_t slot) const {
    require_slot(slot);
    return representation_.state(slot);
  }
  [[nodiscard]] bool live(std::size_t slot) const noexcept {
    return slot < capacity() && representation_.live(slot);
  }
  [[nodiscard]] bool constructed(std::size_t slot) const noexcept {
    return slot < capacity() && representation_.constructed(slot);
  }
  [[nodiscard]] T *memory(std::size_t slot) noexcept {
    return constructed(slot) ? representation_.memory(slot) : nullptr;
  }
  [[nodiscard]] const T *memory(std::size_t slot) const noexcept {
    return constructed(slot) ? representation_.memory(slot) : nullptr;
  }
  [[nodiscard]] T *memory_unchecked(std::size_t slot) noexcept {
    return representation_.memory(slot);
  }
  [[nodiscard]] const T *memory_unchecked(std::size_t slot) const noexcept {
    return representation_.memory(slot);
  }

  [[nodiscard]] PrototypeMemoryReport memory_report() const noexcept {
    return PrototypeMemoryReport{
        .payload_bytes = blocks_.payload_bytes(),
        .slot_index_bytes = representation_.index_bytes(),
        .lifecycle_index_bytes =
            free_slots_.capacity() * sizeof(std::size_t) +
            pending_slots_.capacity() * sizeof(std::size_t),
        .block_descriptor_bytes = blocks_.descriptor_bytes(),
        .payload_allocations = blocks_.block_count(),
        .slot_index_allocations = representation_.index_allocations(),
        .slot_management_allocations =
            (free_slots_.capacity() == 0 ? std::size_t{0} : std::size_t{1}) +
            (pending_slots_.capacity() == 0 ? std::size_t{0} : std::size_t{1}),
        .block_descriptor_allocations = blocks_.descriptor_allocations(),
    };
  }

private:
  Representation representation_{};
  PrototypePayloadBlocks blocks_;
  std::vector<std::size_t> free_slots_{};
  std::vector<std::size_t> pending_slots_{};
  std::size_t live_count_{0};
  std::size_t pending_count_{0};

  void require_slot(std::size_t slot) const {
    if (slot >= capacity()) {
      throw std::out_of_range("prototype slot out of range");
    }
  }

  void destroy_all() noexcept {
    for (std::size_t slot = 0; slot < capacity(); ++slot) {
      if (representation_.constructed(slot)) {
        std::destroy_at(representation_.memory(slot));
        representation_.mark_free(slot);
      }
    }
    live_count_ = 0;
    pending_count_ = 0;
  }
};
} // namespace hgraph::experimental

#endif // HGRAPH_TESTS_STABLE_SLOT_REPRESENTATION_PROTOTYPE_H
