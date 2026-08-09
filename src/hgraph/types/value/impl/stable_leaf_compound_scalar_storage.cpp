#include <hgraph/types/value/compound_scalar_storage.h>

#include <hgraph/types/utils/stable_slot_store.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph {
namespace {
constexpr std::uint32_t unshareable_mask{std::uint32_t{1} << 31U};
constexpr std::uint32_t reference_count_mask{~unshareable_mask};

[[nodiscard]] constexpr std::size_t align_up(std::size_t value,
                                             std::size_t alignment) noexcept {
  return (value + alignment - 1U) & ~(alignment - 1U);
}

class StableLeafPool;
class StableLeafStorage;

struct LeafSlotHeader {
  StableLeafPool *owner{nullptr};
  std::uint32_t slot{0};
  std::uint32_t reference_state{0};
};

static_assert(std::is_trivially_copyable_v<LeafSlotHeader>);

[[nodiscard]] LeafSlotHeader &header_for(void *payload) noexcept {
  return *reinterpret_cast<LeafSlotHeader *>(static_cast<std::byte *>(payload) -
                                             sizeof(LeafSlotHeader));
}

[[nodiscard]] const LeafSlotHeader &header_for(const void *payload) noexcept {
  return *reinterpret_cast<const LeafSlotHeader *>(
      static_cast<const std::byte *>(payload) - sizeof(LeafSlotHeader));
}

class StableLeafPool {
public:
  StableLeafPool(StableLeafStorage &storage, ValueTypeRef leaf)
      : storage_(&storage), leaf_(leaf), payload_offset_([&]() {
          if (!leaf_) {
            throw std::invalid_argument(
                "compound scalar leaf pool requires a bound leaf type");
          }
          return align_up(sizeof(LeafSlotHeader),
                          leaf_.checked_plan().layout.alignment);
        }()),
        slot_layout_{
            .size = payload_offset_ + leaf_.checked_plan().layout.size,
            .alignment = std::max(alignof(LeafSlotHeader),
                                  leaf_.checked_plan().layout.alignment),
        },
        slots_(slot_layout_) {
    if (payload_offset_ < sizeof(LeafSlotHeader))
      throw std::logic_error("compound scalar leaf payload offset is invalid");
  }

  StableLeafPool(const StableLeafPool &) = delete;
  StableLeafPool &operator=(const StableLeafPool &) = delete;

  ~StableLeafPool() {
    for (std::size_t slot = 0; slot < slots_.slot_capacity(); ++slot) {
      if (!slots_.constructed(slot))
        continue;
      leaf_.destroy_at(payload_at(slot));
      slots_.mark_free(slot);
    }
  }

  [[nodiscard]] ValueTypeRef leaf() const noexcept { return leaf_; }
  [[nodiscard]] const StableLeafStorage *storage() const noexcept {
    return storage_;
  }
  [[nodiscard]] std::size_t capacity() const noexcept {
    return slots_.slot_capacity();
  }
  [[nodiscard]] std::size_t live_count() const noexcept {
    return slots_.constructed_count();
  }

  [[nodiscard]] void *default_payload() {
    return allocate_constructed(
        [&](void *payload) { leaf_.default_construct_at(payload); });
  }

  [[nodiscard]] void *copy_value(ValueTypeRef source,
                                 const void *source_memory) {
    if (!source || source_memory == nullptr)
      throw std::invalid_argument(
          "compound scalar copy requires a live source");
    return allocate_constructed([&](void *payload) {
      if (source == leaf_) {
        leaf_.copy_construct_at(payload, source_memory);
        return;
      }
      leaf_.default_construct_at(payload);
      auto destroy =
          make_scope_exit([&]() noexcept { leaf_.destroy_at(payload); });
      leaf_.ops_ref().copy_assign_from(leaf_, payload, source, source_memory);
      destroy.release();
    });
  }

  [[nodiscard]] void *move_value(ValueTypeRef source, void *source_memory) {
    if (!source || source_memory == nullptr)
      throw std::invalid_argument(
          "compound scalar move requires a live source");
    return allocate_constructed([&](void *payload) {
      if (source == leaf_) {
        leaf_.move_construct_at(payload, source_memory);
        return;
      }
      leaf_.default_construct_at(payload);
      auto destroy =
          make_scope_exit([&]() noexcept { leaf_.destroy_at(payload); });
      leaf_.ops_ref().move_assign_from(leaf_, payload, source, source_memory);
      destroy.release();
    });
  }

  [[nodiscard]] void *retain_or_copy(const void *payload) {
    const auto &header = header_for(payload);
    if ((header.reference_state & unshareable_mask) != 0)
      return copy_value(leaf_, payload);
    retain_shareable(const_cast<void *>(payload));
    return const_cast<void *>(payload);
  }

  void release(void *payload) noexcept {
    if (payload == nullptr)
      return;
    auto &header = header_for(payload);
    const std::uint32_t count = header.reference_state & reference_count_mask;
    if (count == 0) {
      std::terminate();
    }
    header.reference_state =
        (header.reference_state & unshareable_mask) | (count - 1U);
    if (count != 1U)
      return;

    const std::size_t slot = header.slot;
    if (!slots_.mark_pending_erase(slot))
      std::terminate();
    leaf_.destroy_at(payload);
    slots_.mark_free(slot);
    free_slots_.push_back(static_cast<std::uint32_t>(slot));
  }

  [[nodiscard]] void *writable(void *payload) {
    auto &header = header_for(payload);
    const std::uint32_t count = header.reference_state & reference_count_mask;
    if (count == 0)
      throw std::logic_error(
          "compound scalar writable projection has no live reference");
    if (count == 1U) {
      header.reference_state |= unshareable_mask;
      return payload;
    }

    void *replacement = copy_value(leaf_, payload);
    header_for(replacement).reference_state |= unshareable_mask;
    release(payload);
    return replacement;
  }

  [[nodiscard]] DynamicStorageMetrics metrics() const noexcept {
    DynamicStorageMetrics result = slots_.dynamic_storage_metrics();
    result.live_bytes += free_slots_.size() * sizeof(std::uint32_t);
    result.reserved_bytes += free_slots_.capacity() * sizeof(std::uint32_t);
    for (std::size_t slot = 0; slot < slots_.slot_capacity(); ++slot) {
      if (!slots_.constructed(slot))
        continue;
      result += leaf_.ops_ref().dynamic_storage_metrics(payload_at(slot));
    }
    return result;
  }

private:
  template <typename Construct>
  [[nodiscard]] void *allocate_constructed(Construct &&construct) {
    const std::size_t slot = acquire_slot();
    void *slot_memory = slots_.slot_memory(slot);
    void *payload = static_cast<std::byte *>(slot_memory) + payload_offset_;
    auto *header = reinterpret_cast<LeafSlotHeader *>(
        static_cast<std::byte *>(payload) - sizeof(LeafSlotHeader));
    std::construct_at(header, LeafSlotHeader{
                                  .owner = this,
                                  .slot = static_cast<std::uint32_t>(slot),
                                  .reference_state = 1U,
                              });
    auto rollback = make_scope_exit([&]() noexcept {
      std::destroy_at(header);
      slots_.mark_free(slot);
      free_slots_.push_back(static_cast<std::uint32_t>(slot));
    });
    std::forward<Construct>(construct)(payload);
    slots_.mark_staged(slot);
    if (!slots_.mark_live(slot))
      throw std::logic_error("compound scalar slot could not become live");
    rollback.release();
    return payload;
  }

  [[nodiscard]] std::size_t acquire_slot() {
    if (!free_slots_.empty()) {
      const std::size_t slot = free_slots_.back();
      free_slots_.pop_back();
      if (slots_.constructed(slot))
        throw std::logic_error(
            "compound scalar free list contains a live slot");
      return slot;
    }

    const std::size_t old_capacity = slots_.slot_capacity();
    if (old_capacity >= std::numeric_limits<std::uint32_t>::max())
      throw std::length_error(
          "compound scalar leaf pool exhausted 32-bit slot ids");
    const std::size_t requested =
        old_capacity == 0
            ? 1
            : std::min<std::size_t>(std::numeric_limits<std::uint32_t>::max(),
                                    old_capacity * 2U);
    slots_.reserve_to(requested);
    free_slots_.reserve(requested - old_capacity - 1U + free_slots_.size());
    for (std::size_t slot = requested; slot > old_capacity + 1U; --slot)
      free_slots_.push_back(static_cast<std::uint32_t>(slot - 1U));
    return old_capacity;
  }

  void retain_shareable(void *payload) {
    auto &header = header_for(payload);
    if ((header.reference_state & unshareable_mask) != 0)
      throw std::logic_error(
          "cannot retain an unshareable compound scalar slot");
    const std::uint32_t count = header.reference_state & reference_count_mask;
    if (count == reference_count_mask)
      throw std::overflow_error("compound scalar reference count overflow");
    header.reference_state = count + 1U;
  }

  [[nodiscard]] void *payload_at(std::size_t slot) noexcept {
    return static_cast<std::byte *>(slots_.slot_memory(slot)) + payload_offset_;
  }
  [[nodiscard]] const void *payload_at(std::size_t slot) const noexcept {
    return static_cast<const std::byte *>(slots_.slot_memory(slot)) +
           payload_offset_;
  }

  StableLeafStorage *storage_{nullptr};
  ValueTypeRef leaf_{};
  std::size_t payload_offset_{0};
  MemoryUtils::StorageLayout slot_layout_{};
  StableSlotStore<StableSlotStateModel::ConstructedAndLive> slots_{};
  std::vector<std::uint32_t> free_slots_{};
};

class StableLeafStorage {
public:
  [[nodiscard]] void *default_value(ValueTypeRef leaf) {
    return pool_for(leaf).default_payload();
  }

  [[nodiscard]] void *copy_value(ValueTypeRef leaf, ValueTypeRef source,
                                 const void *source_memory) {
    return pool_for(leaf).copy_value(source, source_memory);
  }

  [[nodiscard]] void *move_value(ValueTypeRef leaf, ValueTypeRef source,
                                 void *source_memory) {
    return pool_for(leaf).move_value(source, source_memory);
  }

  [[nodiscard]] DynamicStorageMetrics metrics() const noexcept {
    DynamicStorageMetrics result{};
    const std::size_t registry_bytes =
        sizeof(StableLeafStorage) + pools_.bucket_count() * sizeof(void *) +
        pools_.size() * sizeof(typename decltype(pools_)::value_type) +
        pools_.size() * sizeof(StableLeafPool);
    result.live_bytes += registry_bytes;
    result.reserved_bytes += registry_bytes;
    for (const auto &[_, pool] : pools_)
      result += pool->metrics();
    return result;
  }

  [[nodiscard]] CompoundScalarStorageInspection inspect() const noexcept {
    CompoundScalarStorageInspection result{.leaf_pool_count = pools_.size()};
    for (const auto &[_, pool] : pools_) {
      result.live_slot_count += pool->live_count();
      result.slot_capacity += pool->capacity();
    }
    return result;
  }

  [[nodiscard]] bool owns(const void *payload) const noexcept {
    return payload != nullptr && header_for(payload).owner->storage() == this;
  }

private:
  [[nodiscard]] StableLeafPool &pool_for(ValueTypeRef leaf) {
    if (!leaf)
      throw std::invalid_argument(
          "compound scalar storage requires a leaf binding");
    if (const auto found = pools_.find(leaf.record()); found != pools_.end())
      return *found->second;
    auto pool = std::make_unique<StableLeafPool>(*this, leaf);
    auto *result = pool.get();
    pools_.emplace(leaf.record(), std::move(pool));
    return *result;
  }

  std::unordered_map<const TypeRecord *, std::unique_ptr<StableLeafPool>>
      pools_{};
};

[[noreturn]] void unavailable_storage() {
  throw std::logic_error("graph-local compound scalar storage is not active");
}

void nop_destroy(void *) noexcept {}
void *nop_default(void *, ValueTypeRef) { unavailable_storage(); }
void *nop_copy(void *, ValueTypeRef, ValueTypeRef, const void *) {
  unavailable_storage();
}
void *nop_move(void *, ValueTypeRef, ValueTypeRef, void *) {
  unavailable_storage();
}
bool nop_owns(const void *, const void *) noexcept { return false; }
DynamicStorageMetrics nop_metrics(const void *) noexcept { return {}; }
CompoundScalarStorageInspection nop_inspect(const void *) noexcept {
  return {};
}

const CompoundScalarStorageOps &nop_ops() noexcept {
  static const CompoundScalarStorageOps ops{
      .destroy_impl = &nop_destroy,
      .default_value_impl = &nop_default,
      .copy_value_impl = &nop_copy,
      .move_value_impl = &nop_move,
      .owns_impl = &nop_owns,
      .metrics_impl = &nop_metrics,
      .inspect_impl = &nop_inspect,
  };
  return ops;
}

[[nodiscard]] StableLeafStorage *&stable_storage(void *context) noexcept {
  return *static_cast<StableLeafStorage **>(context);
}

[[nodiscard]] const StableLeafStorage *
stable_storage(const void *context) noexcept {
  return *static_cast<StableLeafStorage *const *>(context);
}

[[nodiscard]] StableLeafStorage &ensure_stable_storage(void *context) {
  auto &storage = stable_storage(context);
  if (storage == nullptr)
    storage = new StableLeafStorage{};
  return *storage;
}

void stable_destroy(void *context) noexcept {
  delete std::exchange(stable_storage(context), nullptr);
}
void *stable_default(void *context, ValueTypeRef leaf) {
  return ensure_stable_storage(context).default_value(leaf);
}
void *stable_copy(void *context, ValueTypeRef leaf, ValueTypeRef source,
                  const void *source_memory) {
  return ensure_stable_storage(context).copy_value(leaf, source, source_memory);
}
void *stable_move(void *context, ValueTypeRef leaf, ValueTypeRef source,
                  void *source_memory) {
  return ensure_stable_storage(context).move_value(leaf, source, source_memory);
}
bool stable_owns(const void *context, const void *payload) noexcept {
  const auto *storage = stable_storage(context);
  return storage != nullptr && storage->owns(payload);
}
DynamicStorageMetrics stable_metrics(const void *context) noexcept {
  const auto *storage = stable_storage(context);
  return storage != nullptr ? storage->metrics() : DynamicStorageMetrics{};
}
CompoundScalarStorageInspection stable_inspect(const void *context) noexcept {
  const auto *storage = stable_storage(context);
  return storage != nullptr ? storage->inspect()
                            : CompoundScalarStorageInspection{};
}

const CompoundScalarStorageOps &stable_ops() noexcept {
  static const CompoundScalarStorageOps ops{
      .destroy_impl = &stable_destroy,
      .default_value_impl = &stable_default,
      .copy_value_impl = &stable_copy,
      .move_value_impl = &stable_move,
      .owns_impl = &stable_owns,
      .metrics_impl = &stable_metrics,
      .inspect_impl = &stable_inspect,
  };
  return ops;
}

thread_local CompoundScalarStorageView active_storage{};
} // namespace

CompoundScalarStorageView::CompoundScalarStorageView() noexcept
    : ops_(&nop_ops()) {}

CompoundScalarStorageView::CompoundScalarStorageView(
    void *context, const CompoundScalarStorageOps *ops) noexcept
    : context_(context), ops_(ops != nullptr ? ops : &nop_ops()) {}

bool CompoundScalarStorageView::available() const noexcept {
  return ops_ != &nop_ops();
}

void *CompoundScalarStorageView::default_value(ValueTypeRef leaf) const {
  return ops_->default_value_impl(context_, leaf);
}

void *CompoundScalarStorageView::copy_value(ValueTypeRef leaf,
                                            ValueTypeRef source,
                                            const void *source_memory) const {
  return ops_->copy_value_impl(context_, leaf, source, source_memory);
}

void *CompoundScalarStorageView::move_value(ValueTypeRef leaf,
                                            ValueTypeRef source,
                                            void *source_memory) const {
  return ops_->move_value_impl(context_, leaf, source, source_memory);
}

bool CompoundScalarStorageView::owns(const void *payload) const noexcept {
  return ops_->owns_impl(context_, payload);
}

DynamicStorageMetrics CompoundScalarStorageView::metrics() const noexcept {
  return ops_->metrics_impl(context_);
}

CompoundScalarStorageInspection
CompoundScalarStorageView::inspect() const noexcept {
  return ops_->inspect_impl(context_);
}

bool CompoundScalarStorageView::same_storage(
    CompoundScalarStorageView other) const noexcept {
  return context_ == other.context_ && ops_ == other.ops_;
}

CompoundScalarStorage::CompoundScalarStorage() noexcept : ops_(&nop_ops()) {}

CompoundScalarStorage::CompoundScalarStorage(
    void *context, const CompoundScalarStorageOps *ops) noexcept
    : context_(context), ops_(ops != nullptr ? ops : &nop_ops()) {}

CompoundScalarStorage::~CompoundScalarStorage() { reset(); }

CompoundScalarStorage::CompoundScalarStorage(
    CompoundScalarStorage &&other) noexcept
    : context_(std::exchange(other.context_, nullptr)),
      ops_(std::exchange(other.ops_, &nop_ops())) {}

CompoundScalarStorage &
CompoundScalarStorage::operator=(CompoundScalarStorage &&other) noexcept {
  if (this != &other) {
    reset();
    context_ = std::exchange(other.context_, nullptr);
    ops_ = std::exchange(other.ops_, &nop_ops());
  }
  return *this;
}

CompoundScalarStorage CompoundScalarStorage::make_default() {
  return CompoundScalarStorage{nullptr, &stable_ops()};
}

CompoundScalarStorageView CompoundScalarStorage::view() noexcept {
  if (ops_ == &nop_ops())
    return {};
  return CompoundScalarStorageView{&context_, ops_};
}

CompoundScalarStorageView CompoundScalarStorage::view() const noexcept {
  return const_cast<CompoundScalarStorage *>(this)->view();
}

void CompoundScalarStorage::reset() noexcept {
  ops_->destroy_impl(&context_);
  context_ = nullptr;
  ops_ = &nop_ops();
}

CompoundScalarStorageScope::CompoundScalarStorageScope(
    CompoundScalarStorageView storage) noexcept
    : previous_(active_storage) {
  active_storage = storage;
}

CompoundScalarStorageScope::~CompoundScalarStorageScope() noexcept {
  active_storage = previous_;
}

CompoundScalarStorageView active_compound_scalar_storage() noexcept {
  return active_storage;
}

ValueTypeRef pooled_compound_scalar_leaf_type(const void *payload) noexcept {
  return payload != nullptr ? header_for(payload).owner->leaf()
                            : ValueTypeRef{};
}

void *retain_or_copy_pooled_compound_scalar(const void *payload) {
  if (payload == nullptr)
    throw std::invalid_argument("cannot retain a null pooled compound scalar");
  const auto active = active_compound_scalar_storage();
  if (!active.owns(payload)) {
    const auto leaf = pooled_compound_scalar_leaf_type(payload);
    return active.copy_value(leaf, leaf, payload);
  }
  return header_for(payload).owner->retain_or_copy(payload);
}

void release_pooled_compound_scalar(void *payload) noexcept {
  if (payload != nullptr)
    header_for(payload).owner->release(payload);
}

void *writable_pooled_compound_scalar(void *payload) {
  if (payload == nullptr)
    throw std::invalid_argument("cannot mutate a null pooled compound scalar");
  return header_for(payload).owner->writable(payload);
}
} // namespace hgraph
