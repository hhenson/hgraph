#include <catch2/catch_test_macros.hpp>

#include <hgraph/runtime/nested_graph_storage.h>
#include <hgraph/types/utils/key_slot_store.h>
#include <hgraph/types/utils/slot_bitmap.h>
#include <hgraph/types/utils/stable_slot_storage.h>
#include <hgraph/types/utils/value_slot_store.h>

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

struct TrackedKey
{
    static inline int live_instances{0};
    static inline int copied{0};

    int value{0};

    explicit TrackedKey(int value_) : value(value_) { ++live_instances; }

    TrackedKey(const TrackedKey &other) : value(other.value) {
        ++live_instances;
        ++copied;
    }

    TrackedKey(TrackedKey &&other) noexcept : value(other.value) { ++live_instances; }

    ~TrackedKey() { --live_instances; }

    [[nodiscard]] bool operator==(const TrackedKey &other) const noexcept { return value == other.value; }

    static void reset() {
        live_instances = 0;
        copied         = 0;
    }
};

namespace std
{
    template <> struct hash<TrackedKey>
    {
        [[nodiscard]] size_t operator()(const TrackedKey &key) const noexcept { return hash<int>{}(key.value); }
    };
}  // namespace std

TEST_CASE("SlotBitmap preserves visible bits and clears reused capacity", "[slot-utils][debug-layout]")
{
    hgraph::SlotBitmap bits;
    bits.resize(130);
    bits.set(1);
    bits.set(65);
    bits.set(129);
    REQUIRE(bits.any());
    REQUIRE(bits.test(1));
    REQUIRE(bits.test(65));
    REQUIRE(bits.test(129));
    REQUIRE(bits.count() == 3);

    bits.resize(64);
    REQUIRE(bits.test(1));
    REQUIRE_FALSE(bits.test(65));
    bits.resize(130);
    REQUIRE(bits.test(1));
    REQUIRE_FALSE(bits.test(65));
    REQUIRE_FALSE(bits.test(129));
    REQUIRE(bits.count() == 1);

    bits.reset();
    REQUIRE_FALSE(bits.any());
    REQUIRE(bits.count() == 0);
    REQUIRE(bits.words != nullptr);
    REQUIRE(bits.bit_count == 130);
}

namespace
{
    using namespace hgraph;

    struct AllocationProbe
    {
        static inline int                                     allocations{0};
        static inline int                                     deallocations{0};
        static inline std::vector<MemoryUtils::StorageLayout> allocated_layouts{};
        static inline std::vector<MemoryUtils::StorageLayout> deallocated_layouts{};

        static void reset() {
            allocations   = 0;
            deallocations = 0;
            allocated_layouts.clear();
            deallocated_layouts.clear();
        }
    };

    void *tracked_allocate(MemoryUtils::StorageLayout layout) {
        ++AllocationProbe::allocations;
        AllocationProbe::allocated_layouts.push_back(layout);
        return ::operator new(layout.size == 0 ? 1 : layout.size, std::align_val_t{layout.alignment});
    }

    void tracked_deallocate(void *memory, MemoryUtils::StorageLayout layout) noexcept {
        ++AllocationProbe::deallocations;
        AllocationProbe::deallocated_layouts.push_back(layout);
        ::operator delete(memory, std::align_val_t{layout.alignment});
    }

    struct RecordingObserver final : SlotObserver
    {
        std::vector<std::string> events{};

        void on_capacity(size_t old_capacity, size_t new_capacity) override {
            events.push_back("capacity:" + std::to_string(old_capacity) + "->" + std::to_string(new_capacity));
        }

        void on_insert(size_t slot) override { events.push_back("insert:" + std::to_string(slot)); }

        void on_remove(size_t slot) override { events.push_back("remove:" + std::to_string(slot)); }

        void on_erase(size_t slot) override { events.push_back("erase:" + std::to_string(slot)); }

        void on_clear() override { events.push_back("clear"); }
    };

    struct SelfRemovingSlotObserver final : SlotObserver
    {
        SlotObserverList *list{nullptr};
        int               calls{0};

        explicit SelfRemovingSlotObserver(SlotObserverList *list_) : list(list_) {}

        void on_capacity(size_t, size_t) override {}

        void on_insert(size_t) override
        {
            ++calls;
            list->remove(this);
        }

        void on_remove(size_t) override {}
        void on_erase(size_t) override {}
        void on_clear() override {}
    };

    struct TrackedPayload
    {
        static inline int constructed{0};
        static inline int destroyed{0};

        int value{0};

        explicit TrackedPayload(int value_) : value(value_) { ++constructed; }

        TrackedPayload(const TrackedPayload &other) : value(other.value) { ++constructed; }

        TrackedPayload &operator=(const TrackedPayload &) = default;
        TrackedPayload &operator=(TrackedPayload &&)      = default;

        ~TrackedPayload() { ++destroyed; }

        static void reset() {
            constructed = 0;
            destroyed   = 0;
        }
    };

    struct InPlaceEntry
    {
        static inline int destroyed{0};

        int value{0};

        explicit InPlaceEntry(int value_) : value(value_) {}
        ~InPlaceEntry() { ++destroyed; }
    };

    struct ThrowingInPlaceEntry
    {
        static inline int destroyed{0};

        explicit ThrowingInPlaceEntry(bool should_throw)
        {
            if (should_throw) { throw std::runtime_error("entry construction failed"); }
        }

        ~ThrowingInPlaceEntry() { ++destroyed; }
    };

    struct alignas(std::uintptr_t) PointerAlignedSlot
    {
        std::array<std::byte, sizeof(std::uintptr_t)> bytes{};
    };

    struct WeaklyAlignedSlot
    {
        std::array<std::byte, 3> bytes{};
    };
}  // namespace

static_assert(sizeof(StableSlotStore<StableSlotStateModel::ConstructedOnly>) == sizeof(void *));
static_assert(sizeof(StableSlotStore<StableSlotStateModel::ConstructedAndLive>) == sizeof(void *));
static_assert(std::is_nothrow_move_constructible_v<
              StableSlotStore<StableSlotStateModel::ConstructedAndLive>>);
static_assert(std::is_nothrow_move_assignable_v<
              StableSlotStore<StableSlotStateModel::ConstructedAndLive>>);

TEST_CASE("stable slot store selects tagged pointers for pointer-aligned layouts", "[v2 slot utils][stable-slot-store]")
{
    StableSlotStore<StableSlotStateModel::ConstructedAndLive> storage(
        MemoryUtils::layout_for<PointerAlignedSlot>());
    REQUIRE(storage.representation() == StableSlotRepresentation::TaggedPointer);

    storage.reserve_to(2);
    void *first = storage.slot_memory(0);
    REQUIRE(first != nullptr);
    CHECK_FALSE(storage.constructed(0));
    CHECK_FALSE(storage.live(0));

    storage.mark_staged(0);
    CHECK(storage.constructed(0));
    CHECK_FALSE(storage.live(0));
    REQUIRE(storage.mark_live(0));
    CHECK(storage.live(0));
    CHECK(storage.live_slot_memory(0) == first);
    REQUIRE(storage.mark_pending_erase(0));
    CHECK(storage.constructed(0));
    CHECK_FALSE(storage.live(0));
    CHECK(storage.slot_memory(0) == first);
    CHECK(storage.non_live_slot_memory(0) == first);
    CHECK_FALSE(storage.mark_pending_erase(0));

    storage.reserve_to(8);
    CHECK(storage.slot_memory(0) == first);
    const auto debug = storage.debug_view();
    CHECK(debug.implementation_pointer != nullptr);
    CHECK(debug.pointers_tagged);
    CHECK(debug.state_tagged);
    CHECK(debug.pointer_table_offset == debug.state_offset);
    CHECK((*static_cast<const std::uintptr_t *>(debug.implementation_pointer) &
           detail::STABLE_SLOT_STORE_IMPLEMENTATION_TAG_MASK) ==
          static_cast<std::uintptr_t>(detail::StableSlotStoreImplementationTag::TaggedPointer));

    storage.mark_free(0);
    CHECK_FALSE(storage.constructed(0));
}

TEST_CASE("stable slot store defaults to canonical no-op operations", "[v2 slot utils][stable-slot-store]")
{
    StableSlotStore<StableSlotStateModel::ConstructedAndLive> storage;

    CHECK_FALSE(storage.bound());
    CHECK(storage.representation() == StableSlotRepresentation::Unbound);
    CHECK(storage.slot_capacity() == 0);
    CHECK(storage.stride() == 0);
    CHECK(storage.block_count() == 0);
    CHECK(&storage.allocator() == &MemoryUtils::allocator());
    CHECK(storage.slot_memory(3) == nullptr);
    CHECK(storage.live_slot_memory(3) == nullptr);
    CHECK(storage.non_live_slot_memory(3) == nullptr);
    CHECK_FALSE(storage.constructed(3));
    CHECK_FALSE(storage.live(3));
    CHECK(storage.constructed_count() == 0);
    CHECK(storage.dynamic_storage_metrics().live_bytes == 0);
    CHECK(storage.dynamic_storage_metrics().reserved_bytes == 0);
    CHECK(storage.debug_view().implementation_pointer == nullptr);

    storage.mark_staged(3);
    CHECK_FALSE(storage.mark_live(3));
    CHECK_FALSE(storage.mark_pending_erase(3));
    storage.mark_free(3);
    storage.reset_states();
    CHECK_THROWS_AS(storage.reserve_to(4), std::logic_error);
}

TEST_CASE("stable slot store preserves bitmap fallback for weak alignment", "[v2 slot utils][stable-slot-store]")
{
    StableSlotStore<StableSlotStateModel::ConstructedAndLive> storage(
        MemoryUtils::layout_for<WeaklyAlignedSlot>());
    REQUIRE(storage.representation() == StableSlotRepresentation::Bitmap);

    storage.reserve_to(130);
    void *first = storage.slot_memory(0);
    storage.mark_staged(0);
    REQUIRE(storage.mark_live(0));
    storage.mark_staged(65);
    REQUIRE(storage.mark_pending_erase(0));
    CHECK(storage.constructed(0));
    CHECK(storage.constructed(65));
    CHECK_FALSE(storage.live(0));
    CHECK_FALSE(storage.live(65));
    CHECK(storage.non_live_slot_memory(0) == first);
    CHECK(storage.non_live_slot_memory(65) == storage.slot_memory(65));
    CHECK(storage.constructed_count() == 2);

    storage.reserve_to(260);
    CHECK(storage.slot_memory(0) == first);
    CHECK(storage.constructed(0));
    CHECK(storage.constructed(65));
    const auto debug = storage.debug_view();
    CHECK(debug.implementation_pointer != nullptr);
    CHECK_FALSE(debug.pointers_tagged);
    CHECK_FALSE(debug.state_tagged);
    CHECK(debug.pointer_table_offset != debug.state_offset);
    CHECK((*static_cast<const std::uintptr_t *>(debug.implementation_pointer) &
           detail::STABLE_SLOT_STORE_IMPLEMENTATION_TAG_MASK) ==
          static_cast<std::uintptr_t>(detail::StableSlotStoreImplementationTag::Bitmap));
}

TEST_CASE("constructed-only stable slot stores use one lifecycle plane", "[v2 slot utils][stable-slot-store]")
{
    StableSlotStore<StableSlotStateModel::ConstructedOnly> tagged(
        MemoryUtils::layout_for<PointerAlignedSlot>());
    StableSlotStore<StableSlotStateModel::ConstructedOnly> bitmap(
        MemoryUtils::layout_for<WeaklyAlignedSlot>());
    tagged.reserve_to(8);
    bitmap.reserve_to(8);

    tagged.mark_constructed(3);
    bitmap.mark_constructed(3);
    CHECK(tagged.constructed(3));
    CHECK(bitmap.constructed(3));
    CHECK(tagged.live(3));
    CHECK(bitmap.live(3));
    CHECK(tagged.dynamic_storage_metrics().reserved_bytes >=
          tagged.dynamic_storage_metrics().live_bytes);
    CHECK(bitmap.dynamic_storage_metrics().reserved_bytes >=
          bitmap.dynamic_storage_metrics().live_bytes);

    tagged.mark_free(3);
    bitmap.mark_free(3);
    CHECK_FALSE(tagged.constructed(3));
    CHECK_FALSE(bitmap.constructed(3));
}

TEST_CASE("stable slot store moves erased strategies without moving payloads", "[v2 slot utils][stable-slot-store]")
{
    using Store = StableSlotStore<StableSlotStateModel::ConstructedAndLive>;

    Store source(MemoryUtils::layout_for<PointerAlignedSlot>());
    source.reserve_to(4);
    void *first = source.slot_memory(0);
    source.mark_staged(0);
    REQUIRE(source.mark_live(0));

    Store moved(std::move(source));
    CHECK_FALSE(source.bound());
    CHECK(source.representation() == StableSlotRepresentation::Unbound);
    CHECK(source.slot_capacity() == 0);
    CHECK(&source.allocator() == &MemoryUtils::allocator());
    source.reset_states();
    CHECK_FALSE(source.mark_live(0));
    CHECK(moved.representation() == StableSlotRepresentation::TaggedPointer);
    CHECK(moved.slot_memory(0) == first);
    CHECK(moved.live(0));

    Store destination(MemoryUtils::layout_for<WeaklyAlignedSlot>());
    destination.reserve_to(2);
    REQUIRE(destination.representation() == StableSlotRepresentation::Bitmap);
    destination = std::move(moved);

    CHECK_FALSE(moved.bound());
    CHECK(destination.representation() == StableSlotRepresentation::TaggedPointer);
    CHECK(destination.slot_memory(0) == first);
    CHECK(destination.live(0));
}

TEST_CASE("stable slot storage preserves existing slot addresses across chained growth", "[v2 slot utils]") {
    StableSlotStorage storage;

    storage.reserve_to(2, sizeof(std::int64_t), alignof(std::int64_t));
    REQUIRE(storage.slot_capacity() == 2);
    REQUIRE(storage.slot_data(0) != nullptr);
    REQUIRE(storage.slot_data(1) != nullptr);

    void *slot0 = storage.slot_data(0);
    void *slot1 = storage.slot_data(1);

    CHECK(reinterpret_cast<std::uintptr_t>(slot0) % alignof(std::int64_t) == 0U);
    CHECK(reinterpret_cast<std::uintptr_t>(slot1) % alignof(std::int64_t) == 0U);

    storage.reserve_to(8, sizeof(std::int64_t), alignof(std::int64_t));
    CHECK(storage.slot_capacity() == 8);
    CHECK(storage.slot_data(0) == slot0);
    CHECK(storage.slot_data(1) == slot1);

    REQUIRE(storage.slot_data(7) != nullptr);
    CHECK(storage.slot_data(7) != slot0);
    CHECK(storage.slot_data(7) != slot1);
}

TEST_CASE("stable slot storage reports occupied and retained allocation bytes", "[v2 slot utils][memory]")
{
    StableSlotStorage storage;
    storage.reserve_to(8, sizeof(std::int64_t), alignof(std::int64_t));

    const DynamicStorageMetrics metrics = storage.dynamic_storage_metrics(3);
    CHECK(metrics.live_bytes ==
          8 * sizeof(std::byte *) + storage.blocks.size() * sizeof(StableSlotBlock) +
              3 * storage.stride());
    CHECK(metrics.reserved_bytes ==
          8 * sizeof(std::byte *) + storage.blocks.capacity() * sizeof(StableSlotBlock) +
              8 * storage.stride());
    CHECK(metrics.reserved_bytes >= metrics.live_bytes);
}

TEST_CASE("stable slot storage rejects layout changes after binding", "[v2 slot utils]") {
    StableSlotStorage storage;
    storage.reserve_to(4, sizeof(std::uint32_t), alignof(std::uint32_t));

    REQUIRE_THROWS_AS(storage.reserve_to(8, sizeof(std::uint64_t), alignof(std::uint64_t)), std::logic_error);
}

TEST_CASE("stable slot storage allocates blocks through allocator ops", "[v2 slot utils]") {
    AllocationProbe::reset();

    const MemoryUtils::AllocatorOps allocator{
        .allocate   = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };

    {
        StableSlotStorage storage(allocator);
        storage.reserve_to(2, sizeof(std::uint32_t), alignof(std::uint32_t));
        storage.reserve_to(5, sizeof(std::uint32_t), alignof(std::uint32_t));

        REQUIRE(AllocationProbe::allocations == 2);
        REQUIRE(AllocationProbe::allocated_layouts.size() == 2);
        CHECK(AllocationProbe::allocated_layouts[0].size == sizeof(std::uint32_t) * 2);
        CHECK(AllocationProbe::allocated_layouts[0].alignment == alignof(std::uint32_t));
        CHECK(AllocationProbe::allocated_layouts[1].size == sizeof(std::uint32_t) * 3);
        CHECK(AllocationProbe::allocated_layouts[1].alignment == alignof(std::uint32_t));
        CHECK(&storage.allocator() == &allocator);
    }

    REQUIRE(AllocationProbe::deallocations == 2);
    REQUIRE(AllocationProbe::deallocated_layouts.size() == AllocationProbe::allocated_layouts.size());
    CHECK(AllocationProbe::deallocated_layouts[0].size + AllocationProbe::deallocated_layouts[1].size ==
          AllocationProbe::allocated_layouts[0].size + AllocationProbe::allocated_layouts[1].size);
    CHECK(AllocationProbe::deallocated_layouts[0].alignment == alignof(std::uint32_t));
    CHECK(AllocationProbe::deallocated_layouts[1].alignment == alignof(std::uint32_t));
}

TEST_CASE("in-place graph slots co-locate stable entries and aligned graph payloads", "[v2 slot utils]") {
    AllocationProbe::reset();
    InPlaceEntry::destroyed = 0;

    const MemoryUtils::AllocatorOps allocator{
        .allocate   = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };
    constexpr MemoryUtils::StorageLayout graph_layout{.size = 193, .alignment = 64};

    {
        InPlaceGraphSlotStore<InPlaceEntry> store(graph_layout, allocator);
        store.bind_graph_layout(graph_layout);
        store.reserve_to(2);
        auto &first  = store.construct_at(0, 11);
        auto &second = store.construct_at(1, 22);

        REQUIRE(store.block_count() == 1);
        REQUIRE(AllocationProbe::allocations == 2);  // erased strategy object + payload block
        CHECK(store.entry_at(0) == &first);
        CHECK(store.entry_at(1) == &second);
        CHECK(first.value == 11);
        CHECK(second.value == 22);
        CHECK(store.graph_offset() >= sizeof(InPlaceEntry));
        CHECK(reinterpret_cast<std::uintptr_t>(store.graph_memory(0)) % graph_layout.alignment == 0U);
        CHECK(reinterpret_cast<std::uintptr_t>(store.graph_memory(1)) % graph_layout.alignment == 0U);

        InPlaceEntry *first_address = &first;
        void *first_graph_address = store.graph_memory(0);
        store.reserve_to(5);

        CHECK(store.block_count() == 2);
        CHECK(AllocationProbe::allocations == 3);
        CHECK(store.entry_at(0) == first_address);
        CHECK(store.graph_memory(0) == first_graph_address);
        CHECK(store.slot_capacity() == 5);

        store.reserve_to(1);
        CHECK(store.slot_capacity() == 5);
        CHECK(store.entry_at(0) == first_address);
        CHECK(store.entry_at(1) == &second);

        store.destroy_at(0);
        CHECK(InPlaceEntry::destroyed == 1);
        CHECK(store.entry_at(0) == nullptr);
        CHECK(store.entry_at(1) == &second);
    }

    CHECK(InPlaceEntry::destroyed == 2);
    CHECK(AllocationProbe::deallocations == 3);
}

TEST_CASE("in-place graph slots reject layout changes and occupied construction", "[v2 slot utils]") {
    InPlaceGraphSlotStore<InPlaceEntry> store({.size = 32, .alignment = 16});
    store.reserve_to(1);
    store.construct_at(0, 7);

    REQUIRE_THROWS_AS(store.bind_graph_layout({.size = 64, .alignment = 16}), std::logic_error);
    REQUIRE_THROWS_AS(store.construct_at(0, 8), std::logic_error);
    REQUIRE_THROWS_AS(store.construct_at(1, 9), std::out_of_range);
}

TEST_CASE("in-place graph slots leave a throwing construction reusable", "[v2 slot utils]") {
    AllocationProbe::reset();
    ThrowingInPlaceEntry::destroyed = 0;

    const MemoryUtils::AllocatorOps allocator{
        .allocate   = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };
    InPlaceGraphSlotStore<ThrowingInPlaceEntry> store({.size = 64, .alignment = 16}, allocator);
    store.reserve_to(1);

    REQUIRE_THROWS_AS(store.construct_at(0, true), std::runtime_error);
    CHECK_FALSE(store.has_entry(0));
    CHECK(AllocationProbe::allocations == 2);  // erased strategy object + payload block

    store.construct_at(0, false);
    REQUIRE(store.has_entry(0));
    CHECK(AllocationProbe::allocations == 2);

    store.destroy_at(0);
    CHECK_FALSE(store.has_entry(0));
    CHECK(ThrowingInPlaceEntry::destroyed == 1);
}

TEST_CASE("in-place graph slot banks swap without relocating entries", "[v2 slot utils]") {
    InPlaceEntry::destroyed = 0;
    constexpr MemoryUtils::StorageLayout graph_layout{.size = 96, .alignment = 32};
    InPlaceGraphSlotStore<InPlaceEntry> active{graph_layout};
    InPlaceGraphSlotStore<InPlaceEntry> previous{graph_layout};
    active.reserve_to(2);
    InPlaceEntry *entry = &active.construct_at(1, 17);
    void *graph_memory = active.graph_memory(1);

    active.swap(previous);

    CHECK_FALSE(active.has_entries());
    REQUIRE(previous.entry_at(1) == entry);
    CHECK(previous.graph_memory(1) == graph_memory);
    CHECK(previous.entry_at(1)->value == 17);

    previous.destroy_all();
    CHECK(InPlaceEntry::destroyed == 1);
}

TEST_CASE("value slot store tracks updates and notifies observers", "[v2 slot utils]") {
    ValueSlotStore    store(MemoryUtils::plan_for<std::uint32_t>());
    RecordingObserver observer;

    store.add_slot_observer(&observer);
    store.reserve_to(4);
    store.notify_capacity(0, store.slot_capacity());
    store.notify_insert(1);

    REQUIRE_FALSE(store.slot_updated(1));
    REQUIRE_FALSE(store.has_slot(1));
    store.mark_updated(1);
    REQUIRE(store.slot_updated(1));
    store.clear_updated(1);
    REQUIRE_FALSE(store.slot_updated(1));

    store.notify_remove(1);
    store.notify_erase(1);
    store.notify_clear();

    CHECK(observer.events == std::vector<std::string>{"capacity:0->4", "insert:1", "remove:1", "erase:1", "clear"});

    store.remove_slot_observer(&observer);
    store.notify_insert(2);
    CHECK(observer.events.size() == 5);
}

TEST_CASE("slot observer list supports reentrant removal", "[v2 slot utils]") {
    SlotObserverList         observers;
    SelfRemovingSlotObserver first{&observers};
    RecordingObserver        second;

    observers.add(&first);
    observers.add(&second);

    observers.notify_insert(3);
    REQUIRE(first.calls == 1);
    REQUIRE(second.events == std::vector<std::string>{"insert:3"});

    observers.notify_insert(4);
    REQUIRE(first.calls == 1);
    REQUIRE(second.events == std::vector<std::string>{"insert:3", "insert:4"});
}

TEST_CASE("value slot store supports default construction before plan binding", "[v2 slot utils]") {
    ValueSlotStore store;

    REQUIRE(store.plan() == nullptr);
    REQUIRE_THROWS_AS(store.reserve_to(1), std::logic_error);

    const auto &int_plan = MemoryUtils::plan_for<std::uint32_t>();
    store.bind_plan(int_plan);
    REQUIRE(store.plan() == &int_plan);

    store.reserve_to(2);
    store.construct_at(1);
    REQUIRE(store.has_slot(1));
    store.destroy_at(1);

    store.bind_plan(int_plan);
    REQUIRE_THROWS_AS(store.bind_plan(MemoryUtils::plan_for<std::uint64_t>()), std::logic_error);
}

TEST_CASE("value slot store manages typed payload lifetime on stable slots", "[v2 slot utils]") {
    TrackedPayload::reset();

    ValueSlotStore store(MemoryUtils::plan_for<TrackedPayload>());
    store.reserve_to(2);

    void *slot0 = store.value_memory(0);
    REQUIRE(slot0 != nullptr);

    auto &first = store.construct_at<TrackedPayload>(0, 11);
    REQUIRE(&first == store.try_value<TrackedPayload>(0));
    REQUIRE(first.value == 11);
    REQUIRE(store.has_slot(0));
    REQUIRE(TrackedPayload::constructed == 1);

    store.reserve_to(6);
    CHECK(store.value_memory(0) == slot0);

    auto &second = store.construct_at<TrackedPayload>(5, 29);
    REQUIRE(&second == store.try_value<TrackedPayload>(5));
    REQUIRE(second.value == 29);
    REQUIRE(TrackedPayload::constructed == 2);

    store.destroy_at(0);
    REQUIRE_FALSE(store.has_slot(0));
    REQUIRE(TrackedPayload::destroyed == 1);

    store.destroy_all();
    REQUIRE_FALSE(store.has_slot(5));
    REQUIRE(TrackedPayload::destroyed == 2);
}

TEST_CASE("value slot store destroys live payloads on scope exit", "[v2 slot utils]") {
    TrackedPayload::reset();

    {
        ValueSlotStore store(MemoryUtils::plan_for<TrackedPayload>());
        store.reserve_to(2);
        store.construct_at<TrackedPayload>(0, 7);
        store.construct_at<TrackedPayload>(1, 13);

        REQUIRE(TrackedPayload::constructed == 2);
        REQUIRE(TrackedPayload::destroyed == 0);
    }

    REQUIRE(TrackedPayload::destroyed == 2);
}

TEST_CASE("value slot store rejects invalid emplacement", "[v2 slot utils]") {
    TrackedPayload::reset();

    ValueSlotStore store(MemoryUtils::plan_for<TrackedPayload>());
    store.reserve_to(1);

    store.construct_at<TrackedPayload>(0, 5);
    REQUIRE_THROWS_AS(store.construct_at<TrackedPayload>(0, 9), std::logic_error);
    REQUIRE_THROWS_AS(store.construct_at<TrackedPayload>(1, 11), std::out_of_range);
    REQUIRE_THROWS_AS(store.construct_at<std::string>(0, "wrong plan"), std::logic_error);
}

TEST_CASE("value slot store uses its bound plan for lifecycle operations", "[v2 slot utils]") {
    ValueSlotStore store(MemoryUtils::plan_for<std::string>());
    store.reserve_to(2);

    REQUIRE(store.plan() == &MemoryUtils::plan_for<std::string>());
    REQUIRE(store.try_value_memory(0) == nullptr);
    REQUIRE(store.try_value_memory(store.slot_capacity()) == nullptr);

    store.construct_at(0);
    REQUIRE(store.has_slot(0));
    REQUIRE(store.try_value_memory(0) == store.value_memory(0));
    REQUIRE(*store.try_value<std::string>(0) == "");

    const std::string source = "copied";
    store.construct_at(1, &source);
    REQUIRE(store.has_slot(1));
    REQUIRE(*store.try_value<std::string>(1) == "copied");

    store.destroy_all();
    REQUIRE_FALSE(store.has_slot(0));
    REQUIRE_FALSE(store.has_slot(1));
    REQUIRE(store.try_value_memory(0) == nullptr);
}

TEST_CASE("value slot stores can share a custom allocator", "[v2 slot utils]") {
    AllocationProbe::reset();

    const MemoryUtils::AllocatorOps allocator{
        .allocate   = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };

    {
        ValueSlotStore store(MemoryUtils::plan_for<TrackedPayload>(), allocator);
        store.reserve_to(3);
        store.construct_at<TrackedPayload>(1, 17);
        REQUIRE(&store.allocator() == &allocator);
        REQUIRE(AllocationProbe::allocations == 2);  // erased strategy object + payload block
    }

    REQUIRE(AllocationProbe::deallocations == 2);
}

TEST_CASE("key mirrored value slot store derives lifetime from key construction", "[v2 slot utils]") {
    KeySlotStore             keys(MemoryUtils::plan_for<std::int32_t>(), key_slot_store_ops_for<std::int32_t>());
    KeyMirroredValueSlotStore values(keys, MemoryUtils::plan_for<std::string>());

    REQUIRE(values.mirrors_key_construction());

    const auto first = keys.insert(11);
    REQUIRE(first.inserted);
    REQUIRE(values.has_slot(first.slot));
    REQUIRE(values.mirrors_key_construction());

    auto *value = values.try_value<std::string>(first.slot);
    REQUIRE(value != nullptr);
    *value = "eleven";
    values.mark_updated(first.slot);
    REQUIRE(values.slot_updated(first.slot));

    REQUIRE(keys.remove(11));
    REQUIRE(keys.slot_constructed(first.slot));
    REQUIRE_FALSE(keys.slot_live(first.slot));
    REQUIRE(values.has_slot(first.slot));
    REQUIRE(values.try_value<std::string>(first.slot) != nullptr);
    REQUIRE(*values.try_value<std::string>(first.slot) == "eleven");
    REQUIRE(*MemoryUtils::cast<std::string>(values.value_memory(first.slot)) == "eleven");
    REQUIRE(values.mirrors_key_construction());

    keys.erase_pending();
    REQUIRE_FALSE(keys.slot_constructed(first.slot));
    REQUIRE_FALSE(values.has_slot(first.slot));
    REQUIRE(values.try_value<std::string>(first.slot) == nullptr);
    REQUIRE_THROWS_AS(static_cast<void>(values.value_memory(first.slot)), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(values.value_memory(values.slot_capacity())), std::out_of_range);
    REQUIRE(values.mirrors_key_construction());
}

TEST_CASE("key slot store tracks pending removals until explicit erase", "[v2 slot utils]") {
    KeySlotStore      store(MemoryUtils::plan_for<std::int32_t>(), key_slot_store_ops_for<std::int32_t>());
    RecordingObserver observer;

    store.add_slot_observer(&observer);
    store.reserve_to<std::int32_t>(4);

    const auto first  = store.insert(11);
    const auto second = store.insert(22);

    REQUIRE(first.inserted);
    REQUIRE(second.inserted);
    REQUIRE(first.slot == 0);
    REQUIRE(second.slot == 1);
    REQUIRE(store.contains(11));
    REQUIRE(store.find_slot(22) == second.slot);
    REQUIRE(store.find_stored_slot(22) == second.slot);
    REQUIRE(store.try_key<std::int32_t>(first.slot) != nullptr);
    REQUIRE(*store.try_key<std::int32_t>(first.slot) == 11);

    REQUIRE(store.remove(22));
    REQUIRE(store.size() == 1);
    REQUIRE_FALSE(store.slot_live(second.slot));
    REQUIRE(store.slot_constructed(second.slot));
    REQUIRE(store.slot_pending_erase(second.slot));
    REQUIRE(store.pending_erase_count() == 1);
    REQUIRE(store.has_pending_erase());
    REQUIRE(store.find_stored_slot(22) == second.slot);
    REQUIRE(store.find_slot(22) == KeySlotStore::npos);
    REQUIRE_FALSE(store.contains(22));
    REQUIRE(store.try_key<std::int32_t>(second.slot) != nullptr);
    REQUIRE(*store.try_key<std::int32_t>(second.slot) == 22);

    CHECK(observer.events == std::vector<std::string>{"capacity:0->4", "insert:0", "insert:1", "remove:1"});

    REQUIRE_FALSE(store.slot_live(second.slot));
    REQUIRE(store.slot_constructed(second.slot));
    REQUIRE(store.slot_pending_erase(second.slot));
    REQUIRE(store.has_pending_erase());
    REQUIRE(store.pending_erase_count() == 1);
    REQUIRE(store.find_stored_slot(22) == second.slot);
    REQUIRE(store.try_key<std::int32_t>(second.slot) != nullptr);

    store.erase_pending();

    REQUIRE_FALSE(store.slot_live(second.slot));
    REQUIRE_FALSE(store.slot_constructed(second.slot));
    REQUIRE_FALSE(store.slot_pending_erase(second.slot));
    REQUIRE_FALSE(store.has_pending_erase());
    REQUIRE(store.pending_erase_count() == 0);
    REQUIRE(store.find_stored_slot(22) == KeySlotStore::npos);
    REQUIRE(store.try_key<std::int32_t>(second.slot) == nullptr);

    CHECK(observer.events == std::vector<std::string>{"capacity:0->4", "insert:0", "insert:1", "remove:1", "erase:1"});
}

TEST_CASE("key and mirrored value slots report index and payload capacity", "[v2 slot utils][memory]")
{
    KeySlotStore keys(MemoryUtils::plan_for<std::int32_t>(), key_slot_store_ops_for<std::int32_t>());
    KeyMirroredValueSlotStore values(keys, MemoryUtils::plan_for<std::int64_t>());

    const auto empty_keys = keys.dynamic_storage_metrics();
    const auto empty_values = values.dynamic_storage_metrics();
    CHECK(empty_keys.reserved_bytes >= empty_keys.live_bytes);
    CHECK(empty_values.reserved_bytes >= empty_values.live_bytes);

    keys.reserve_to(32);
    REQUIRE(keys.insert(11).inserted);
    REQUIRE(keys.insert(22).inserted);

    const auto populated_keys = keys.dynamic_storage_metrics();
    const auto populated_values = values.dynamic_storage_metrics();
    CHECK(populated_keys.live_bytes > empty_keys.live_bytes);
    CHECK(populated_keys.reserved_bytes > empty_keys.reserved_bytes);
    CHECK(populated_values.live_bytes > empty_values.live_bytes);
    CHECK(populated_values.reserved_bytes > empty_values.reserved_bytes);
    CHECK(populated_keys.reserved_bytes >= populated_keys.live_bytes);
    CHECK(populated_values.reserved_bytes >= populated_values.live_bytes);
}

TEST_CASE("key slot store mixes type-erased identity hashes", "[v2 slot utils][hash]")
{
    std::size_t equality_probes = 0;
    const KeySlotStoreOps ops{
        .hash = [](const void *key, const void *) -> std::size_t {
            return static_cast<std::size_t>(*MemoryUtils::cast<const std::int64_t>(key));
        },
        .equal = [](const void *lhs, const void *rhs, const void *context) -> bool {
            ++*static_cast<std::size_t *>(const_cast<void *>(context));
            return *MemoryUtils::cast<const std::int64_t>(lhs) ==
                   *MemoryUtils::cast<const std::int64_t>(rhs);
        },
        .context = &equality_probes,
    };
    KeySlotStore store(MemoryUtils::plan_for<std::int64_t>(), ops);

    constexpr std::int64_t key_count = 1024;
    store.reserve_to(static_cast<std::size_t>(key_count));
    for (std::int64_t key = 0; key < key_count; ++key)
    {
        REQUIRE(store.insert(key).inserted);
    }

    equality_probes = 0;
    for (std::int64_t key = 0; key < key_count; ++key)
    {
        REQUIRE(store.find_slot(key) != KeySlotStore::npos);
    }
    REQUIRE(equality_probes < static_cast<std::size_t>(key_count * 4));
}

TEST_CASE("key slot store rolls back a new key when indexing throws", "[v2 slot utils][hash]")
{
    bool throw_hash = true;
    const KeySlotStoreOps ops{
        .hash = [](const void *key, const void *context) -> std::size_t {
            if (*static_cast<const bool *>(context)) { throw std::runtime_error("hash failed"); }
            return std::hash<std::int64_t>{}(*MemoryUtils::cast<const std::int64_t>(key));
        },
        .equal = [](const void *lhs, const void *rhs, const void *) -> bool {
            return *MemoryUtils::cast<const std::int64_t>(lhs) ==
                   *MemoryUtils::cast<const std::int64_t>(rhs);
        },
        .context = &throw_hash,
    };
    KeySlotStore store(MemoryUtils::plan_for<std::int64_t>(), ops);
    const std::int64_t key = 42;

    REQUIRE_THROWS_AS(store.insert(key), std::runtime_error);
    REQUIRE(store.size() == 0);
    for (std::size_t slot = 0; slot < store.slot_capacity(); ++slot) {
        REQUIRE_FALSE(store.slot_constructed(slot));
    }

    throw_hash = false;
    const auto inserted = store.insert(key);
    REQUIRE(inserted.inserted);
    REQUIRE(inserted.slot == 0);
    REQUIRE(store.contains(key));
}

TEST_CASE("key slot store resurrects removed keys before erase and reuses slots after explicit erase", "[v2 slot utils]") {
    KeySlotStore store(MemoryUtils::plan_for<std::int32_t>(), key_slot_store_ops_for<std::int32_t>());

    const auto original = store.insert(3);
    REQUIRE(original.inserted);

    REQUIRE(store.remove(3));
    REQUIRE_FALSE(store.contains(3));
    REQUIRE_FALSE(store.slot_live(original.slot));
    REQUIRE(store.slot_constructed(original.slot));
    REQUIRE(store.slot_pending_erase(original.slot));
    REQUIRE(store.pending_erase_count() == 1);

    const auto resurrected = store.insert(3);
    REQUIRE(resurrected.inserted);
    REQUIRE(resurrected.slot == original.slot);
    REQUIRE(store.contains(3));
    REQUIRE(store.slot_live(original.slot));
    REQUIRE(store.slot_constructed(original.slot));
    REQUIRE_FALSE(store.slot_pending_erase(original.slot));
    REQUIRE(store.pending_erase_count() == 0);

    REQUIRE(store.remove(3));
    REQUIRE(store.slot_pending_erase(original.slot));

    store.erase_pending();
    REQUIRE_FALSE(store.slot_live(original.slot));
    REQUIRE_FALSE(store.slot_constructed(original.slot));
    REQUIRE_FALSE(store.has_pending_erase());

    const auto reused = store.insert(9);
    REQUIRE(reused.inserted);
    REQUIRE(reused.slot == original.slot);
    REQUIRE(store.contains(9));
    REQUIRE(*store.try_key<std::int32_t>(reused.slot) == 9);
}

TEST_CASE("key slot store bracket accessor reads constructed slots and throws after erase", "[v2 slot utils]") {
    KeySlotStore store(MemoryUtils::plan_for<std::int32_t>(), key_slot_store_ops_for<std::int32_t>());

    const auto inserted = store.insert(7);
    REQUIRE(*MemoryUtils::cast<int>(store[inserted.slot]) == 7);

    REQUIRE(store.remove(7));
    REQUIRE_FALSE(store.slot_live(inserted.slot));
    REQUIRE(store.slot_constructed(inserted.slot));
    REQUIRE(*MemoryUtils::cast<int>(store[inserted.slot]) == 7);

    store.erase_pending();
    REQUIRE_FALSE(store.slot_constructed(inserted.slot));
    REQUIRE_THROWS_AS(static_cast<void>(store[inserted.slot]), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(store[store.slot_capacity()]), std::out_of_range);
}

TEST_CASE("key slot store supports custom allocators with explicit pending erase", "[v2 slot utils]") {
    TrackedKey::reset();
    AllocationProbe::reset();

    const MemoryUtils::AllocatorOps allocator{
        .allocate   = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };

    {
        KeySlotStore      store(MemoryUtils::plan_for<TrackedKey>(), key_slot_store_ops_for<TrackedKey>(), allocator);
        RecordingObserver observer;
        store.add_slot_observer(&observer);
        store.reserve_to<TrackedKey>(2);

        TrackedKey alpha{11};
        REQUIRE(TrackedKey::live_instances == 1);

        const auto inserted = store.insert(alpha);
        REQUIRE(inserted.inserted);
        REQUIRE(TrackedKey::copied == 1);
        REQUIRE(TrackedKey::live_instances == 2);

        REQUIRE(store.remove(alpha));
        REQUIRE_FALSE(store.slot_live(inserted.slot));
        REQUIRE(store.slot_constructed(inserted.slot));
        REQUIRE(store.slot_pending_erase(inserted.slot));
        REQUIRE(store.find_stored_slot(alpha) == inserted.slot);
        REQUIRE(TrackedKey::live_instances == 2);

        store.erase_pending();
        REQUIRE_FALSE(store.slot_live(inserted.slot));
        REQUIRE_FALSE(store.slot_constructed(inserted.slot));
        REQUIRE(store.find_stored_slot(alpha) == KeySlotStore::npos);
        REQUIRE(TrackedKey::live_instances == 1);

        TrackedKey beta{29};
        const auto reused = store.insert(beta);
        REQUIRE(reused.inserted);
        REQUIRE(reused.slot == inserted.slot);
        REQUIRE(TrackedKey::copied == 2);
        REQUIRE(&store.allocator() == &allocator);
        REQUIRE(AllocationProbe::allocations == 2);  // erased strategy object + payload block

        store.clear();
        REQUIRE(TrackedKey::live_instances == 2);

        CHECK(observer.events == std::vector<std::string>{"capacity:0->2", "insert:0", "remove:0", "erase:0", "insert:0", "clear"});
    }

    REQUIRE(TrackedKey::live_instances == 0);
    REQUIRE(AllocationProbe::deallocations == 2);
}

TEST_CASE("key slot store survives deterministic adversarial lifecycle transitions", "[v2 slot utils][lifetime]")
{
    constexpr std::size_t key_count = 16;
    constexpr std::size_t step_count = 1024;

    enum class ModelState : std::uint8_t
    {
        Absent,
        Live,
        PendingErase,
    };

    const auto &plan = MemoryUtils::plan_for<std::int32_t>();
    const auto ops = key_slot_store_ops_for<std::int32_t>();
    KeySlotStore store(plan, ops);
    std::array<ModelState, key_count> states{};
    std::array<std::size_t, key_count> slots{};
    slots.fill(KeySlotStore::npos);

    std::uint32_t random_state = 0x6d2b79f5U;
    const auto next_random = [&] {
        random_state = random_state * 1664525U + 1013904223U;
        return random_state;
    };

    const auto validate = [&] {
        std::size_t expected_live = 0;
        std::size_t expected_pending = 0;

        for (std::size_t key_index = 0; key_index < key_count; ++key_index)
        {
            const auto key = static_cast<std::int32_t>(key_index);
            const bool live = states[key_index] == ModelState::Live;
            const bool constructed = states[key_index] != ModelState::Absent;
            expected_live += live ? 1U : 0U;
            expected_pending += states[key_index] == ModelState::PendingErase ? 1U : 0U;

            CHECK(store.contains(key) == live);
            CHECK(store.find_slot(key) == (live ? slots[key_index] : KeySlotStore::npos));
            CHECK(store.find_stored_slot(key) == (constructed ? slots[key_index] : KeySlotStore::npos));
            if (constructed)
            {
                REQUIRE(slots[key_index] < store.slot_capacity());
                REQUIRE(store.try_key<std::int32_t>(slots[key_index]) != nullptr);
                CHECK(*store.try_key<std::int32_t>(slots[key_index]) == key);
            }
        }

        CHECK(store.size() == expected_live);
        CHECK(store.pending_erase_count() == expected_pending);
        CHECK(store.has_pending_erase() == (expected_pending != 0));

        std::size_t constructed_slots = 0;
        for (std::size_t slot = 0; slot < store.slot_capacity(); ++slot)
        {
            std::size_t owner = key_count;
            for (std::size_t key_index = 0; key_index < key_count; ++key_index)
            {
                if (states[key_index] != ModelState::Absent && slots[key_index] == slot)
                {
                    REQUIRE(owner == key_count);
                    owner = key_index;
                }
            }

            const bool expected_constructed = owner != key_count;
            CHECK(store.slot_constructed(slot) == expected_constructed);
            CHECK(store.slot_live(slot) ==
                  (expected_constructed && states[owner] == ModelState::Live));
            CHECK(store.slot_pending_erase(slot) ==
                  (expected_constructed && states[owner] == ModelState::PendingErase));
            constructed_slots += expected_constructed ? 1U : 0U;
        }
        CHECK(constructed_slots == expected_live + expected_pending);
    };

    for (std::size_t step = 0; step < step_count; ++step)
    {
        const std::uint32_t random = next_random();
        const std::size_t key_index = (random >> 8U) % key_count;
        const auto key = static_cast<std::int32_t>(key_index);

        switch (random % 11U)
        {
            case 0:
            case 1:
            case 2:
            case 3:
            {
                const ModelState previous = states[key_index];
                const std::size_t previous_slot = slots[key_index];
                const auto inserted = store.insert(key);
                CHECK(inserted.inserted == (previous != ModelState::Live));
                if (previous == ModelState::PendingErase) { CHECK(inserted.slot == previous_slot); }
                states[key_index] = ModelState::Live;
                slots[key_index] = inserted.slot;
                break;
            }
            case 4:
            case 5:
            case 6:
            {
                const bool was_live = states[key_index] == ModelState::Live;
                CHECK(store.remove(key) == was_live);
                if (was_live) { states[key_index] = ModelState::PendingErase; }
                break;
            }
            case 7:
                store.erase_pending();
                for (std::size_t index = 0; index < key_count; ++index)
                {
                    if (states[index] == ModelState::PendingErase)
                    {
                        states[index] = ModelState::Absent;
                        slots[index] = KeySlotStore::npos;
                    }
                }
                break;
            case 8:
                store.reserve_to(1U + ((random >> 16U) % 48U));
                break;
            case 9:
            {
                std::array<const std::int32_t *, key_count> addresses{};
                for (std::size_t index = 0; index < key_count; ++index)
                {
                    if (states[index] != ModelState::Absent)
                    {
                        addresses[index] = store.try_key<std::int32_t>(slots[index]);
                    }
                }
                KeySlotStore moved{std::move(store)};
                for (std::size_t index = 0; index < key_count; ++index)
                {
                    if (addresses[index] != nullptr)
                    {
                        CHECK(moved.try_key<std::int32_t>(slots[index]) == addresses[index]);
                    }
                }
                store = std::move(moved);
                break;
            }
            case 10:
                if ((random & 0x7000U) == 0)
                {
                    store.clear();
                    states.fill(ModelState::Absent);
                    slots.fill(KeySlotStore::npos);
                }
                break;
        }

        validate();
    }

    store.clear();
    states.fill(ModelState::Absent);
    slots.fill(KeySlotStore::npos);
    validate();
}
