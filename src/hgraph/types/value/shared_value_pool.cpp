#include <hgraph/types/value/shared_value_pool.h>
#include <hgraph/util/scope.h>

#include "impl/lock_free_freelist.h"

#include <hgraph/types/utils/memory_utils.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph {
namespace value_impl {
namespace {
class SharedSizeClassPool;
constexpr std::uint32_t unshareable_mask{std::uint32_t{1} << 31U};
constexpr std::uint32_t strong_reference_count_mask{~unshareable_mask};
}

struct SharedValueAllocation {
  // Allocator-side ABA protection. These references never represent
  // a Shared<T> owner and remain live only while a free-list reader
  // may inspect ``free_list_next``.
  std::atomic<std::uint32_t> free_list_refs{0};
  std::atomic<SharedValueAllocation *> free_list_next{nullptr};

  // Value ownership. A slot is private while this is zero and is
  // published only after its payload has been fully constructed.
  std::atomic<std::uint32_t> strong_refs{0};
  SharedSizeClassPool *owner{nullptr};
  const TypeRecord *record{nullptr};
};

namespace {
[[nodiscard]] constexpr std::size_t align_up(std::size_t value,
                                             std::size_t alignment) noexcept {
  return (value + alignment - 1U) & ~(alignment - 1U);
}

[[nodiscard]] std::size_t checked_add(std::size_t lhs, std::size_t rhs) {
  if (rhs > std::numeric_limits<std::size_t>::max() - lhs) {
    throw std::bad_array_new_length{};
  }
  return lhs + rhs;
}

[[nodiscard]] std::size_t checked_multiply(std::size_t lhs, std::size_t rhs) {
  if (lhs != 0 && rhs > std::numeric_limits<std::size_t>::max() / lhs) {
    throw std::bad_array_new_length{};
  }
  return lhs * rhs;
}

class SharedSizeClassPool {
public:
  SharedSizeClassPool(std::size_t payload_capacity,
                      std::size_t payload_alignment)
      : alignment_{std::max(alignof(SharedValueAllocation), payload_alignment)},
        payload_offset_{
            align_up(sizeof(SharedValueAllocation), payload_alignment)},
        stride_{align_up(checked_add(payload_offset_, payload_capacity),
                         alignment_)} {
    const auto target_bytes = std::size_t{64} * 1024U;
    slots_per_slab_ =
        std::clamp(target_bytes / stride_, std::size_t{1}, std::size_t{64});
  }

  SharedSizeClassPool(const SharedSizeClassPool &) = delete;
  SharedSizeClassPool &operator=(const SharedSizeClassPool &) = delete;

  ~SharedSizeClassPool() {
    for (const auto &slab : slabs_) {
      for (std::size_t index = 0; index < slab.count; ++index) {
        allocation_at(slab.memory, index)->~SharedValueAllocation();
      }
      MemoryUtils::allocator().deallocate_storage(slab.memory, slab.layout);
    }
  }

  [[nodiscard]] SharedValueAllocation *acquire(const TypeRecord &record) {
    SharedValueAllocation *allocation = free_list_.try_get();
    if (allocation == nullptr) {
      std::lock_guard lock(grow_mutex_);
      allocation = free_list_.try_get();
      if (allocation == nullptr) {
        allocation = grow();
      }
    }

    assert(allocation->strong_refs.load(std::memory_order_relaxed) == 0);
    assert(allocation->record == nullptr);
    allocation->record = &record;
    live_values_.fetch_add(1, std::memory_order_relaxed);
    return allocation;
  }

  void recycle(SharedValueAllocation *allocation) noexcept {
    allocation->record = nullptr;
    live_values_.fetch_sub(1, std::memory_order_relaxed);
    free_list_.add(allocation);
  }

  [[nodiscard]] std::byte *
  payload(SharedValueAllocation &allocation) const noexcept {
    return reinterpret_cast<std::byte *>(&allocation) + payload_offset_;
  }

  [[nodiscard]] const std::byte *
  payload(const SharedValueAllocation &allocation) const noexcept {
    return reinterpret_cast<const std::byte *>(&allocation) + payload_offset_;
  }

  [[nodiscard]] SharedValuePoolMetrics metrics() const noexcept {
    return SharedValuePoolMetrics{
        .size_classes = 1,
        .slabs = slabs_count_.load(std::memory_order_relaxed),
        .capacity = capacity_.load(std::memory_order_relaxed),
        .live_values = live_values_.load(std::memory_order_relaxed),
        .reserved_bytes = reserved_bytes_.load(std::memory_order_relaxed),
    };
  }

private:
  struct Slab {
    void *memory{nullptr};
    MemoryUtils::StorageLayout layout{};
    std::size_t count{0};
  };

  [[nodiscard]] SharedValueAllocation *
  allocation_at(void *memory, std::size_t index) const noexcept {
    return reinterpret_cast<SharedValueAllocation *>(
        static_cast<std::byte *>(memory) + index * stride_);
  }

  [[nodiscard]] SharedValueAllocation *grow() {
    const MemoryUtils::StorageLayout layout{
        .size = checked_multiply(stride_, slots_per_slab_),
        .alignment = alignment_,
    };
    void *memory = MemoryUtils::allocator().allocate_storage(layout);
    std::size_t constructed = 0;
    auto rollback = UnwindCleanupGuard([&] {
      for (std::size_t index = 0; index < constructed; ++index) {
        allocation_at(memory, index)->~SharedValueAllocation();
      }
      MemoryUtils::allocator().deallocate_storage(memory, layout);
    });
    for (; constructed < slots_per_slab_; ++constructed) {
      ::new (allocation_at(memory, constructed))
          SharedValueAllocation{.owner = this};
    }
    slabs_.push_back(Slab{memory, layout, slots_per_slab_});
    rollback.release();

    slabs_count_.fetch_add(1, std::memory_order_relaxed);
    capacity_.fetch_add(slots_per_slab_, std::memory_order_relaxed);
    reserved_bytes_.fetch_add(layout.size, std::memory_order_relaxed);

    // Return one slot directly. The remainder may be consumed
    // concurrently as each node is published onto the list.
    for (std::size_t index = 1; index < slots_per_slab_; ++index) {
      free_list_.add(allocation_at(memory, index));
    }
    return allocation_at(memory, 0);
  }

  const std::size_t alignment_;
  const std::size_t payload_offset_;
  const std::size_t stride_;
  std::size_t slots_per_slab_{1};

  LockFreeFreeList<SharedValueAllocation> free_list_{};
  std::mutex grow_mutex_{};
  std::vector<Slab> slabs_{};
  std::atomic<std::size_t> slabs_count_{0};
  std::atomic<std::size_t> capacity_{0};
  std::atomic<std::size_t> live_values_{0};
  std::atomic<std::size_t> reserved_bytes_{0};
};

class SharedValueArena {
public:
  static constexpr std::size_t CLASS_BITS =
      std::numeric_limits<std::size_t>::digits;
  static constexpr std::size_t CLASS_COUNT = CLASS_BITS * CLASS_BITS;

  [[nodiscard]] SharedSizeClassPool &
  pool_for(const MemoryUtils::StorageLayout &layout) {
    if (layout.alignment == 0 || !std::has_single_bit(layout.alignment)) {
      throw std::invalid_argument(
          "Shared<T> requires a power-of-two-aligned payload plan");
    }

    // Empty composites have a valid zero-byte plan. Give them an addressable
    // payload location by sharing the minimum arena class; their concrete plan
    // remains zero-sized and its lifecycle still performs no field work.
    const std::size_t normalized_size = std::max(layout.size, std::size_t{1});
    const std::size_t size_exponent =
        normalized_size <= 1 ? 0 : std::bit_width(normalized_size - 1U);
    const std::size_t alignment_exponent = std::countr_zero(layout.alignment);
    if (size_exponent >= CLASS_BITS || alignment_exponent >= CLASS_BITS) {
      throw std::length_error(
          "Shared<T> payload layout exceeds the arena class table");
    }
    const std::size_t index = size_exponent * CLASS_BITS + alignment_exponent;

    if (auto *pool = classes_[index].load(std::memory_order_acquire);
        pool != nullptr) {
      return *pool;
    }

    std::lock_guard lock(mutex_);
    if (auto *pool = classes_[index].load(std::memory_order_relaxed);
        pool != nullptr) {
      return *pool;
    }
    const std::size_t payload_capacity = std::size_t{1} << size_exponent;
    auto created = std::make_unique<SharedSizeClassPool>(payload_capacity,
                                                         layout.alignment);
    auto *result = created.get();
    pools_.push_back(std::move(created));
    classes_[index].store(result, std::memory_order_release);
    return *result;
  }

  [[nodiscard]] SharedValuePoolMetrics metrics() const noexcept {
    std::lock_guard lock(mutex_);
    SharedValuePoolMetrics result{};
    for (const auto &pool : pools_) {
      const auto current = pool->metrics();
      result.size_classes += current.size_classes;
      result.slabs += current.slabs;
      result.capacity += current.capacity;
      result.live_values += current.live_values;
      result.reserved_bytes += current.reserved_bytes;
    }
    return result;
  }

  void reset() noexcept {
    std::lock_guard lock(mutex_);
    for (const auto &pool : pools_) {
      if (pool->metrics().live_values != 0) {
        std::terminate();
      }
    }
    for (auto &entry : classes_) {
      entry.store(nullptr, std::memory_order_relaxed);
    }
    pools_.clear();
  }

private:
  mutable std::mutex mutex_{};
  std::array<std::atomic<SharedSizeClassPool *>, CLASS_COUNT> classes_{};
  std::vector<std::unique_ptr<SharedSizeClassPool>> pools_{};
};

[[nodiscard]] SharedValueArena &shared_value_arena() noexcept {
  // Type records and static owners can outlive ordinary static
  // teardown. Match their immortal singleton lifetime.
  static auto *arena = new SharedValueArena();
  return *arena;
}
} // namespace

SharedValueAllocation *acquire_shared_value(ValueTypeRef binding) {
  if (!binding) {
    throw std::invalid_argument("Shared<T> requires a concrete value binding");
  }
  return shared_value_arena()
      .pool_for(binding.checked_plan().layout)
      .acquire(*binding.record());
}

void publish_shared_value(SharedValueAllocation *allocation) noexcept {
  if (allocation == nullptr || allocation->record == nullptr ||
      allocation->strong_refs.exchange(1, std::memory_order_release) != 0) {
    std::terminate();
  }
}

void abandon_shared_value(SharedValueAllocation *allocation) noexcept {
  if (allocation == nullptr) {
    return;
  }
  if (allocation->strong_refs.load(std::memory_order_relaxed) != 0) {
    std::terminate();
  }
  allocation->owner->recycle(allocation);
}

void retain_shared_value(SharedValueAllocation *allocation) noexcept {
  if (allocation == nullptr) {
    return;
  }
  if (!try_retain_shareable_shared_value(allocation)) {
    std::terminate();
  }
}

bool try_retain_shareable_shared_value(
    SharedValueAllocation *allocation) noexcept {
  if (allocation == nullptr) {
    return false;
  }
  auto state = allocation->strong_refs.load(std::memory_order_relaxed);
  while ((state & unshareable_mask) == 0) {
    const auto refs = state & strong_reference_count_mask;
    if (refs == 0) {
      return false;
    }
    if (refs == strong_reference_count_mask) {
      std::terminate();
    }
    if (allocation->strong_refs.compare_exchange_weak(
            state, state + 1U, std::memory_order_relaxed,
            std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

bool make_shared_value_unshareable(
    SharedValueAllocation *allocation) noexcept {
  if (allocation == nullptr) {
    return false;
  }
  auto state = allocation->strong_refs.load(std::memory_order_acquire);
  for (;;) {
    if (state == (unshareable_mask | 1U)) {
      return true;
    }
    if (state != 1U) {
      return false;
    }
    if (allocation->strong_refs.compare_exchange_weak(
            state, unshareable_mask | 1U, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return true;
    }
  }
}

void *unshareable_shared_value_memory(
    SharedValueAllocation &allocation) noexcept {
  if (allocation.strong_refs.load(std::memory_order_acquire) !=
      (unshareable_mask | 1U)) {
    std::terminate();
  }
  return allocation.owner->payload(allocation);
}

void release_shared_value(SharedValueAllocation *allocation) noexcept {
  if (allocation == nullptr) {
    return;
  }
  auto state = allocation->strong_refs.load(std::memory_order_relaxed);
  for (;;) {
    const auto refs = state & strong_reference_count_mask;
    if (refs == 0) {
      std::terminate();
    }
    const auto replacement =
        refs == 1U ? std::uint32_t{0}
                   : (state & unshareable_mask) | (refs - 1U);
    if (allocation->strong_refs.compare_exchange_weak(
            state, replacement, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      if (refs != 1U) {
        return;
      }
      break;
    }
  }

  const TypeRecord *record = allocation->record;
  record->plan->destroy(allocation->owner->payload(*allocation));
  allocation->owner->recycle(allocation);
}

ValueTypeRef shared_value_type(const SharedValueAllocation &allocation) {
  if (allocation.record == nullptr) {
    throw std::logic_error("shared value allocation is not published");
  }
  return ValueTypeRef::checked(AnyPtr::typed_null(*allocation.record));
}

const void *
shared_value_memory(const SharedValueAllocation &allocation) noexcept {
  return allocation.owner->payload(allocation);
}

void *mutable_unpublished_shared_value_memory(
    SharedValueAllocation &allocation) noexcept {
  if (allocation.strong_refs.load(std::memory_order_relaxed) != 0) {
    std::terminate();
  }
  return allocation.owner->payload(allocation);
}

std::uint32_t
shared_value_use_count(const SharedValueAllocation &allocation) noexcept {
  return allocation.strong_refs.load(std::memory_order_relaxed) &
         strong_reference_count_mask;
}

void reset_shared_value_pool() noexcept { shared_value_arena().reset(); }
} // namespace value_impl

SharedValuePoolMetrics shared_value_pool_metrics() noexcept {
  return value_impl::shared_value_arena().metrics();
}
} // namespace hgraph
