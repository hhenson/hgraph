#include <catch2/catch_test_macros.hpp>

#include "stable_slot_representation_prototype.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <type_traits>

namespace {
using namespace hgraph::experimental;

struct alignas(8) TrackedValue {
  static inline int constructed_count{0};
  static inline int destroyed_count{0};

  std::uint64_t value{0};

  explicit TrackedValue(std::uint64_t value_) : value(value_) {
    ++constructed_count;
  }
  ~TrackedValue() { ++destroyed_count; }

  static void reset() noexcept {
    constructed_count = 0;
    destroyed_count = 0;
  }
};

struct PackedTrivial9 {
  std::array<std::byte, 9> bytes{};
};

struct PackedTracked9 {
  std::array<std::byte, 9> bytes{};

  ~PackedTracked9() {}
};

struct alignas(4) WeaklyAlignedTracked12 {
  std::array<std::byte, 12> bytes{};

  ~WeaklyAlignedTracked12() {}
};

static_assert(sizeof(PackedTrivial9) == 9);
static_assert(alignof(PackedTrivial9) == 1);
static_assert(std::is_trivially_destructible_v<PackedTrivial9>);
static_assert(sizeof(PackedTracked9) == 9);
static_assert(alignof(PackedTracked9) == 1);
static_assert(!std::is_trivially_destructible_v<PackedTracked9>);
static_assert(sizeof(WeaklyAlignedTracked12) == 12);
static_assert(alignof(WeaklyAlignedTracked12) == 4);

template <typename Representation> void check_non_trivial_lifecycle() {
  TrackedValue::reset();
  {
    PrototypeStableSlotStore<TrackedValue, Representation> store(2);
    const std::size_t slot0 = store.emplace(41);
    const std::size_t slot1 = store.emplace(42);
    REQUIRE(slot0 == 0);
    REQUIRE(slot1 == 1);
    REQUIRE(store.size() == 2);
    REQUIRE(store.state(slot0) == SlotState::Live);

    TrackedValue *const stable_address = store.memory(slot0);
    REQUIRE(stable_address != nullptr);
    CHECK(stable_address->value == 41);

    store.reserve_to(16);
    CHECK(store.memory(slot0) == stable_address);
    CHECK(store.memory(slot1)->value == 42);

    REQUIRE(store.remove(slot0));
    CHECK(store.state(slot0) == SlotState::PendingErase);
    CHECK(store.constructed(slot0));
    CHECK(store.memory(slot0) == stable_address);
    CHECK(store.pending_erase_count() == 1);

    REQUIRE(store.resurrect(slot0));
    CHECK(store.state(slot0) == SlotState::Live);
    CHECK(store.memory(slot0) == stable_address);
    CHECK(store.pending_erase_count() == 0);

    REQUIRE(store.remove(slot0));
    store.erase_pending();
    CHECK(store.state(slot0) == SlotState::Free);
    CHECK_FALSE(store.constructed(slot0));
    CHECK(store.memory(slot0) == nullptr);
    CHECK(TrackedValue::destroyed_count == 1);

    const std::size_t reused_slot = store.emplace(43);
    CHECK(reused_slot == slot0);
    CHECK(store.memory(reused_slot) == stable_address);
    CHECK(store.memory(reused_slot)->value == 43);
  }
  CHECK(TrackedValue::constructed_count == 3);
  CHECK(TrackedValue::destroyed_count == 3);
}
} // namespace

TEST_CASE("compact stable-slot chooser selects the smallest valid lifecycle "
          "representation",
          "[stable-slot-prototype][memory]") {
  using namespace hgraph::experimental;

  constexpr auto aligned = choose_slot_representation(
      sizeof(TrackedValue), alignof(TrackedValue), false, 8);
  static_assert(aligned.kind == SlotRepresentationKind::TaggedPointer);
  static_assert(aligned.payload_stride == 8);
  static_assert(aligned.lifecycle_bits_per_slot == 0);

  constexpr auto trivial = choose_slot_representation(
      sizeof(PackedTrivial9), alignof(PackedTrivial9), true, 8);
  static_assert(trivial.kind == SlotRepresentationKind::ParentTrackedTrivial);
  static_assert(trivial.payload_stride == 9);
  static_assert(trivial.lifecycle_bits_per_slot == 1);

  constexpr auto state_byte = choose_slot_representation(
      sizeof(PackedTracked9), alignof(PackedTracked9), false, 8);
  static_assert(state_byte.kind == SlotRepresentationKind::BitmapPointer);
  static_assert(state_byte.payload_stride == 9);
  static_assert(state_byte.lifecycle_bits_per_slot == 2);

  constexpr auto padded =
      choose_slot_representation(sizeof(WeaklyAlignedTracked12),
                                 alignof(WeaklyAlignedTracked12), false, 8);
  static_assert(padded.kind == SlotRepresentationKind::BitmapPointer);
  static_assert(padded.payload_stride == 12);
}

TEST_CASE("bitmap stable-slot prototype preserves lifecycle and addresses",
          "[stable-slot-prototype]") {
  check_non_trivial_lifecycle<BitmapPointerRepresentation<TrackedValue>>();
}

TEST_CASE(
    "tagged-pointer stable-slot prototype preserves lifecycle and addresses",
    "[stable-slot-prototype]") {
  using Representation = TaggedPointerRepresentation<TrackedValue>;
  static_assert(sizeof(typename Representation::SlotPointer) == sizeof(void *));
  check_non_trivial_lifecycle<Representation>();
}

TEST_CASE("state-byte stable-slot prototype preserves lifecycle and addresses",
          "[stable-slot-prototype]") {
  check_non_trivial_lifecycle<StateByteRepresentation<TrackedValue>>();
}

TEST_CASE("parent-tracked trivial prototype needs no constructed bitmap",
          "[stable-slot-prototype][memory]") {
  using namespace hgraph::experimental;
  using Representation = ParentTrackedTrivialRepresentation<PackedTrivial9>;

  PrototypeStableSlotStore<PackedTrivial9, Representation> store(4);
  const std::size_t slot = store.emplace(PackedTrivial9{});
  PackedTrivial9 *const stable_address = store.memory(slot);
  REQUIRE(stable_address != nullptr);

  store.reserve_to(32);
  CHECK(store.memory(slot) == stable_address);
  REQUIRE(store.remove(slot));
  CHECK(store.state(slot) == SlotState::PendingErase);
  REQUIRE(store.resurrect(slot));
  CHECK(store.memory(slot) == stable_address);

  const PrototypeMemoryReport report = store.memory_report();
  CHECK(store.payload_stride() == sizeof(PackedTrivial9));
  CHECK(report.slot_index_bytes >= store.capacity() * sizeof(void *));
}

TEST_CASE("compact stable-slot publication failure destroys staged values and "
          "reuses the slot",
          "[stable-slot-prototype][lifecycle]") {
  using namespace hgraph::experimental;
  using Store =
      PrototypeStableSlotStore<TrackedValue,
                               TaggedPointerRepresentation<TrackedValue>>;

  TrackedValue::reset();
  Store store(1);
  REQUIRE_THROWS_AS(store.emplace_with_publish(
                        [&store](std::size_t slot, TrackedValue *memory) {
                          CHECK(memory->value == 7);
                          CHECK(store.state(slot) == SlotState::Staged);
                          throw std::runtime_error("publish failed");
                        },
                        7),
                    std::runtime_error);

  CHECK(store.size() == 0);
  CHECK(store.state(0) == SlotState::Free);
  CHECK_FALSE(store.constructed(0));
  CHECK(TrackedValue::constructed_count == 1);
  CHECK(TrackedValue::destroyed_count == 1);

  const std::size_t slot = store.emplace(8);
  CHECK(slot == 0);
  CHECK(store.memory(slot)->value == 8);
}

TEST_CASE(
    "state byte trades one payload byte for weakly aligned non-trivial values",
    "[stable-slot-prototype][memory]") {
  using namespace hgraph::experimental;
  using ByteStore =
      PrototypeStableSlotStore<PackedTracked9,
                               StateByteRepresentation<PackedTracked9>>;
  using TaggedStore =
      PrototypeStableSlotStore<PackedTracked9,
                               TaggedPointerRepresentation<PackedTracked9, 8>>;

  ByteStore byte_store(64);
  TaggedStore tagged_store(64);

  CHECK(byte_store.payload_stride() == 10);
  CHECK(tagged_store.payload_stride() == 16);
  CHECK(byte_store.memory_report().payload_bytes == 64 * 10);
  CHECK(tagged_store.memory_report().payload_bytes == 64 * 16);
  CHECK(byte_store.memory_report().slot_index_bytes ==
        tagged_store.memory_report().slot_index_bytes);
}

TEST_CASE("tagged pointer removes both standalone lifecycle bitmaps",
          "[stable-slot-prototype][memory]") {
  using namespace hgraph::experimental;
  using Baseline =
      PrototypeStableSlotStore<TrackedValue,
                               BitmapPointerRepresentation<TrackedValue>>;
  using Tagged =
      PrototypeStableSlotStore<TrackedValue,
                               TaggedPointerRepresentation<TrackedValue>>;

  Baseline baseline(128);
  Tagged tagged(128);

  CHECK(tagged.memory_report().slot_index_bytes == 128 * sizeof(void *));
  CHECK(baseline.memory_report().slot_index_bytes ==
        tagged.memory_report().slot_index_bytes + 4 * sizeof(std::uint64_t));
  CHECK(tagged.memory_report().slot_index_allocations == 1);
  CHECK(baseline.memory_report().slot_index_allocations == 3);
}
