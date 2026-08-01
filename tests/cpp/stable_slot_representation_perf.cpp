#include "stable_slot_representation_prototype.h"

#include <hgraph/types/utils/stable_slot_store.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
using namespace hgraph::experimental;

volatile std::uint64_t retained_value{0};

struct alignas(8) AlignedTracked8 {
  std::uint64_t stored{0};

  explicit AlignedTracked8(std::uint64_t value) : stored(value) {}
  ~AlignedTracked8() {}

  [[nodiscard]] std::uint64_t value() const noexcept { return stored; }
};

struct PackedTrivial9 {
  std::byte bytes[9]{};

  explicit PackedTrivial9(std::uint64_t value) {
    bytes[0] = static_cast<std::byte>(value & 0xffU);
  }

  [[nodiscard]] std::uint64_t value() const noexcept {
    return std::to_integer<std::uint8_t>(bytes[0]);
  }
};

struct PackedTracked9 {
  std::byte bytes[9]{};

  explicit PackedTracked9(std::uint64_t value) {
    bytes[0] = static_cast<std::byte>(value & 0xffU);
  }
  ~PackedTracked9() {}

  [[nodiscard]] std::uint64_t value() const noexcept {
    return std::to_integer<std::uint8_t>(bytes[0]);
  }
};

static_assert(sizeof(AlignedTracked8) == 8);
static_assert(alignof(AlignedTracked8) == 8);
static_assert(sizeof(PackedTrivial9) == 9);
static_assert(alignof(PackedTrivial9) == 1);
static_assert(std::is_trivially_destructible_v<PackedTrivial9>);
static_assert(sizeof(PackedTracked9) == 9);
static_assert(alignof(PackedTracked9) == 1);
static_assert(!std::is_trivially_destructible_v<PackedTracked9>);

[[nodiscard]] std::size_t env_size(const char *name, std::size_t fallback,
                                   std::size_t minimum = 1) {
  const char *value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    return fallback;
  }

  char *end = nullptr;
  const auto parsed = std::strtoull(value, &end, 10);
  if (end == value || *end != '\0' ||
      parsed > std::numeric_limits<std::size_t>::max()) {
    throw std::invalid_argument(std::string{name} +
                                " must be a non-negative integer");
  }
  return std::max<std::size_t>(static_cast<std::size_t>(parsed), minimum);
}

[[nodiscard]] bool selected(std::string_view name) noexcept {
  const char *filter = std::getenv("HGRAPH_STABLE_SLOT_PERF_FILTER");
  return filter == nullptr || *filter == '\0' || name.contains(filter);
}

template <typename T> [[nodiscard]] T median(std::vector<T> values) {
  std::ranges::sort(values);
  return values[values.size() / 2];
}

[[nodiscard]] double percentile(std::vector<double> values, double quantile) {
  std::ranges::sort(values);
  const double position = quantile * static_cast<double>(values.size() - 1);
  const auto lower = static_cast<std::size_t>(position);
  const auto upper = std::min(lower + 1, values.size() - 1);
  const double fraction = position - static_cast<double>(lower);
  return values[lower] + (values[upper] - values[lower]) * fraction;
}

void retain(std::uint64_t value) noexcept {
  retained_value = value;
  std::atomic_signal_fence(std::memory_order_seq_cst);
}

template <typename Operation>
void run_benchmark(std::string_view name, std::size_t default_iterations,
                   std::size_t work_items_per_iteration, std::size_t samples,
                   Operation &&operation) {
  if (!selected(name)) {
    return;
  }

  const std::size_t override_iterations =
      env_size("HGRAPH_STABLE_SLOT_PERF_ITERATIONS", 0, 0);
  const std::size_t iterations =
      override_iterations == 0 ? default_iterations : override_iterations;
  for (std::size_t warmup = 0; warmup < 3; ++warmup) {
    retain(operation());
  }

  std::vector<double> elapsed_per_item;
  std::vector<std::uint64_t> checksums;
  elapsed_per_item.reserve(samples);
  checksums.reserve(samples);
  for (std::size_t sample = 0; sample < samples; ++sample) {
    std::uint64_t checksum = 0;
    const auto start = std::chrono::steady_clock::now();
    for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
      checksum += operation();
    }
    const auto elapsed = std::chrono::duration<double, std::nano>(
                             std::chrono::steady_clock::now() - start)
                             .count();
    retain(checksum);
    elapsed_per_item.push_back(
        elapsed / static_cast<double>(iterations * work_items_per_iteration));
    checksums.push_back(checksum);
  }

  if (!std::ranges::all_of(checksums, [&](std::uint64_t value) {
        return value == checksums.front();
      })) {
    throw std::runtime_error(std::string{name} +
                             " produced an unstable checksum");
  }

  std::cout << "benchmark"
            << " name=" << name << " samples=" << samples
            << " iterations=" << iterations
            << " work_items_per_iteration=" << work_items_per_iteration
            << " median_ns_per_item=" << median(elapsed_per_item)
            << " p10_ns_per_item=" << percentile(elapsed_per_item, 0.10)
            << " p90_ns_per_item=" << percentile(elapsed_per_item, 0.90)
            << " checksum=" << checksums.front() << '\n';
}

template <typename Store> void fill(Store &store, std::size_t capacity) {
  for (std::size_t slot = 0; slot < capacity; ++slot) {
    const std::size_t inserted = store.emplace((slot % 251U) + 1U);
    if (inserted != slot) {
      throw std::runtime_error("prototype assigned an unexpected slot");
    }
  }
}

template <typename T> class ProductionStableSlotStore {
public:
  using value_type = T;

  explicit ProductionStableSlotStore(std::size_t initial_capacity = 0)
      : store_(hgraph::MemoryUtils::StorageLayout{sizeof(T), alignof(T)}) {
    if (initial_capacity != 0) {
      reserve_to(initial_capacity);
    }
  }

  ProductionStableSlotStore(const ProductionStableSlotStore &) = delete;
  ProductionStableSlotStore &
  operator=(const ProductionStableSlotStore &) = delete;

  ~ProductionStableSlotStore() { destroy_all(); }

  [[nodiscard]] std::size_t capacity() const noexcept {
    return store_.slot_capacity();
  }
  [[nodiscard]] std::size_t payload_stride() const noexcept {
    return store_.stride();
  }

  void reserve_to(std::size_t capacity) {
    if (capacity <= store_.slot_capacity()) {
      return;
    }
    const std::size_t old_capacity = store_.slot_capacity();
    free_slots_.reserve(capacity);
    pending_slots_.reserve(capacity);
    store_.reserve_to(capacity);
    for (std::size_t slot = capacity; slot > old_capacity; --slot) {
      free_slots_.push_back(slot - 1);
    }
  }

  template <typename... Args> std::size_t emplace(Args &&...args) {
    if (free_slots_.empty()) {
      reserve_to(std::max<std::size_t>(
          live_count_ + pending_count_ + 1,
          std::max<std::size_t>(8, capacity() * 2)));
    }
    const std::size_t slot = free_slots_.back();
    free_slots_.pop_back();
    T *memory = hgraph::MemoryUtils::cast<T>(store_.slot_memory(slot));
    try {
      std::construct_at(memory, std::forward<Args>(args)...);
    } catch (...) {
      free_slots_.push_back(slot);
      throw;
    }
    store_.mark_staged(slot);
    static_cast<void>(store_.mark_live(slot));
    ++live_count_;
    return slot;
  }

  bool remove(std::size_t slot) {
    if (!store_.mark_pending_erase(slot)) {
      return false;
    }
    pending_slots_.push_back(slot);
    --live_count_;
    ++pending_count_;
    return true;
  }

  bool resurrect(std::size_t slot) {
    if (!store_.mark_live(slot)) {
      return false;
    }
    ++live_count_;
    --pending_count_;
    if (pending_count_ == 0) {
      pending_slots_.clear();
    }
    return true;
  }

  void erase_pending() noexcept {
    for (const std::size_t slot : pending_slots_) {
      void *memory = store_.non_live_slot_memory(slot);
      if (memory == nullptr) {
        continue;
      }
      std::destroy_at(hgraph::MemoryUtils::cast<T>(memory));
      store_.mark_free(slot);
      free_slots_.push_back(slot);
    }
    pending_slots_.clear();
    pending_count_ = 0;
  }

  [[nodiscard]] bool live(std::size_t slot) const noexcept {
    return store_.live(slot);
  }
  [[nodiscard]] T *memory_unchecked(std::size_t slot) noexcept {
    if (void *memory = store_.live_slot_memory(slot); memory != nullptr) {
      return hgraph::MemoryUtils::cast<T>(memory);
    }
    return hgraph::MemoryUtils::cast<T>(store_.slot_memory(slot));
  }
  [[nodiscard]] const T *memory_unchecked(std::size_t slot) const noexcept {
    if (const void *memory = store_.live_slot_memory(slot); memory != nullptr) {
      return hgraph::MemoryUtils::cast<T>(memory);
    }
    return hgraph::MemoryUtils::cast<T>(store_.slot_memory(slot));
  }

  [[nodiscard]] PrototypeMemoryReport memory_report() const noexcept {
    const std::size_t bitmap_bytes =
        store_.representation() == hgraph::StableSlotRepresentation::Bitmap
            ? 2 * ((capacity() + hgraph::SlotBitmap::bits_per_word - 1) /
                   hgraph::SlotBitmap::bits_per_word) *
                  sizeof(std::uint64_t)
            : 0;
    return PrototypeMemoryReport{
        .payload_bytes = capacity() * store_.stride(),
        .slot_index_bytes = capacity() * sizeof(void *) + bitmap_bytes,
        .lifecycle_index_bytes =
            free_slots_.capacity() * sizeof(std::size_t) +
            pending_slots_.capacity() * sizeof(std::size_t),
        .block_descriptor_bytes =
            store_.block_count() * sizeof(hgraph::StableSlotBlock),
        .payload_allocations = store_.block_count(),
        .slot_index_allocations =
            capacity() == 0
                ? std::size_t{0}
                : std::size_t{1} +
                      (bitmap_bytes == 0 ? std::size_t{0} : std::size_t{2}),
        .slot_management_allocations =
            (free_slots_.capacity() == 0 ? std::size_t{0} : std::size_t{1}) +
            (pending_slots_.capacity() == 0 ? std::size_t{0}
                                            : std::size_t{1}),
        .block_descriptor_allocations =
            store_.block_count() == 0 ? std::size_t{0} : std::size_t{1},
    };
  }

private:
  using Store = hgraph::StableSlotStore<
      hgraph::StableSlotStateModel::ConstructedAndLive>;

  Store store_;
  std::vector<std::size_t> free_slots_{};
  std::vector<std::size_t> pending_slots_{};
  std::size_t live_count_{0};
  std::size_t pending_count_{0};

  void destroy_all() noexcept {
    for (std::size_t slot = 0; slot < capacity(); ++slot) {
      if (store_.constructed(slot)) {
        std::destroy_at(hgraph::MemoryUtils::cast<T>(store_.slot_memory(slot)));
        store_.mark_free(slot);
      }
    }
  }
};

[[nodiscard]] std::vector<std::size_t> random_slots(std::size_t capacity) {
  std::vector<std::size_t> slots(capacity);
  std::uint64_t state = 0x9e3779b97f4a7c15ULL;
  for (std::size_t &slot : slots) {
    state ^= state >> 12U;
    state ^= state << 25U;
    state ^= state >> 27U;
    slot = static_cast<std::size_t>((state * 0x2545f4914f6cdd1dULL) % capacity);
  }
  return slots;
}

template <typename Store>
void run_store_suite_impl(std::string_view representation_name,
                          std::size_t capacity, std::size_t samples,
                          const std::vector<std::size_t> &random_order) {
  using Value = typename Store::value_type;
  Store store(capacity);
  fill(store, capacity);

  const PrototypeMemoryReport report = store.memory_report();
  const std::size_t representation_bytes = report.payload_bytes +
                                           report.slot_index_bytes +
                                           report.block_descriptor_bytes;
  std::cout << "memory"
            << " representation=" << representation_name
            << " payload_size=" << sizeof(Value)
            << " payload_alignment=" << alignof(Value)
            << " capacity=" << capacity
            << " payload_stride=" << store.payload_stride()
            << " payload_bytes=" << report.payload_bytes
            << " slot_index_bytes=" << report.slot_index_bytes
            << " slot_management_bytes=" << report.lifecycle_index_bytes
            << " block_descriptor_bytes=" << report.block_descriptor_bytes
            << " representation_bytes=" << representation_bytes
            << " representation_bytes_per_slot="
            << static_cast<double>(representation_bytes) /
                   static_cast<double>(capacity)
            << " total_bytes=" << report.total_bytes()
            << " total_bytes_per_slot="
            << static_cast<double>(report.total_bytes()) /
                   static_cast<double>(capacity)
            << " payload_allocations=" << report.payload_allocations
            << " slot_index_allocations=" << report.slot_index_allocations
            << " slot_management_allocations="
            << report.slot_management_allocations
            << " block_descriptor_allocations="
            << report.block_descriptor_allocations
            << " total_allocations=" << report.total_allocations() << '\n';

  const std::string prefix{representation_name};
  const std::size_t scan_iterations =
      std::max<std::size_t>(1, 4'000'000 / capacity);

  run_benchmark(prefix + ".sequential_live_read", scan_iterations, capacity,
                samples, [&] {
                  std::uint64_t checksum = 0;
                  for (std::size_t slot = 0; slot < capacity; ++slot) {
                    if (store.live(slot)) {
                      checksum += store.memory_unchecked(slot)->value();
                    }
                  }
                  return checksum;
                });

  run_benchmark(prefix + ".random_live_read", scan_iterations, capacity,
                samples, [&] {
                  std::uint64_t checksum = 0;
                  for (const std::size_t slot : random_order) {
                    if (store.live(slot)) {
                      checksum += store.memory_unchecked(slot)->value();
                    }
                  }
                  return checksum;
                });

  std::size_t toggle_slot = capacity / 2;
  run_benchmark(prefix + ".remove_resurrect", 500'000, 2, samples, [&] {
    if (!store.remove(toggle_slot) || !store.resurrect(toggle_slot)) {
      throw std::runtime_error("remove/resurrect lifecycle failed");
    }
    return store.memory_unchecked(toggle_slot)->value();
  });

  run_benchmark(prefix + ".erase_reinsert", 100'000, 2, samples, [&] {
    if (!store.remove(toggle_slot)) {
      throw std::runtime_error("remove before erase failed");
    }
    store.erase_pending();
    const std::size_t inserted = store.emplace(73);
    if (inserted != toggle_slot) {
      throw std::runtime_error("erase did not recycle the same slot");
    }
    return store.memory_unchecked(toggle_slot)->value();
  });
}

template <typename Value, typename Representation>
void run_store_suite(std::string_view representation_name, std::size_t capacity,
                     std::size_t samples,
                     const std::vector<std::size_t> &random_order) {
  run_store_suite_impl<PrototypeStableSlotStore<Value, Representation>>(
      representation_name, capacity, samples, random_order);
}
} // namespace

int main() {
  try {
    const std::size_t capacity =
        env_size("HGRAPH_STABLE_SLOT_PERF_CAPACITY", 65'536);
    const std::size_t samples = env_size("HGRAPH_STABLE_SLOT_PERF_SAMPLES", 15);
    const auto random_order = random_slots(capacity);

    std::cout << std::fixed << std::setprecision(3);
    std::cout << "config capacity=" << capacity << " samples=" << samples
              << '\n';

    run_store_suite<AlignedTracked8,
                    BitmapPointerRepresentation<AlignedTracked8>>(
        "aligned8.bitmap", capacity, samples, random_order);
    run_store_suite<AlignedTracked8,
                    TaggedPointerRepresentation<AlignedTracked8, 8>>(
        "aligned8.tagged8", capacity, samples, random_order);
    run_store_suite_impl<ProductionStableSlotStore<AlignedTracked8>>(
        "aligned8.production", capacity, samples, random_order);

    run_store_suite<PackedTrivial9,
                    BitmapPointerRepresentation<PackedTrivial9>>(
        "packed_trivial9.bitmap", capacity, samples, random_order);
    run_store_suite<PackedTrivial9,
                    ParentTrackedTrivialRepresentation<PackedTrivial9>>(
        "packed_trivial9.parent", capacity, samples, random_order);
    run_store_suite<PackedTrivial9,
                    TaggedPointerRepresentation<PackedTrivial9, 8>>(
        "packed_trivial9.tagged8", capacity, samples, random_order);

    run_store_suite<PackedTracked9,
                    BitmapPointerRepresentation<PackedTracked9>>(
        "packed_tracked9.bitmap", capacity, samples, random_order);
    run_store_suite_impl<ProductionStableSlotStore<PackedTracked9>>(
        "packed_tracked9.production", capacity, samples, random_order);
    run_store_suite<PackedTracked9, StateByteRepresentation<PackedTracked9>>(
        "packed_tracked9.state_byte", capacity, samples, random_order);
    run_store_suite<PackedTracked9,
                    TaggedPointerRepresentation<PackedTracked9, 4>>(
        "packed_tracked9.tagged4", capacity, samples, random_order);
    run_store_suite<PackedTracked9,
                    TaggedPointerRepresentation<PackedTracked9, 8>>(
        "packed_tracked9.tagged8", capacity, samples, random_order);
  } catch (const std::exception &error) {
    std::cerr << "stable-slot representation benchmark failed: " << error.what()
              << '\n';
    return 1;
  }
  return 0;
}
