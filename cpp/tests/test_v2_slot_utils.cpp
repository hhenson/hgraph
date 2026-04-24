#include <catch2/catch_test_macros.hpp>

#include <hgraph/v2/types/utils/value_slot_store.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph::v2;

    struct AllocationProbe
    {
        static inline int allocations{0};
        static inline int deallocations{0};
        static inline std::vector<MemoryUtils::StorageLayout> allocated_layouts{};
        static inline std::vector<MemoryUtils::StorageLayout> deallocated_layouts{};

        static void reset()
        {
            allocations = 0;
            deallocations = 0;
            allocated_layouts.clear();
            deallocated_layouts.clear();
        }
    };

    void *tracked_allocate(MemoryUtils::StorageLayout layout)
    {
        ++AllocationProbe::allocations;
        AllocationProbe::allocated_layouts.push_back(layout);
        return ::operator new(layout.size == 0 ? 1 : layout.size, std::align_val_t{layout.alignment});
    }

    void tracked_deallocate(void *memory, MemoryUtils::StorageLayout layout) noexcept
    {
        ++AllocationProbe::deallocations;
        AllocationProbe::deallocated_layouts.push_back(layout);
        ::operator delete(memory, std::align_val_t{layout.alignment});
    }

    struct RecordingObserver final : SlotObserver
    {
        std::vector<std::string> events{};

        void on_capacity(size_t old_capacity, size_t new_capacity) override
        {
            events.push_back("capacity:" + std::to_string(old_capacity) + "->" + std::to_string(new_capacity));
        }

        void on_insert(size_t slot) override
        {
            events.push_back("insert:" + std::to_string(slot));
        }

        void on_remove(size_t slot) override
        {
            events.push_back("remove:" + std::to_string(slot));
        }

        void on_erase(size_t slot) override
        {
            events.push_back("erase:" + std::to_string(slot));
        }

        void on_clear() override
        {
            events.push_back("clear");
        }
    };

    struct TrackedPayload
    {
        static inline int constructed{0};
        static inline int destroyed{0};

        int value{0};

        explicit TrackedPayload(int value_)
            : value(value_)
        {
            ++constructed;
        }

        TrackedPayload(const TrackedPayload &other)
            : value(other.value)
        {
            ++constructed;
        }

        ~TrackedPayload()
        {
            ++destroyed;
        }

        static void reset()
        {
            constructed = 0;
            destroyed = 0;
        }
    };
}  // namespace

TEST_CASE("stable slot storage preserves existing slot addresses across chained growth", "[v2 slot utils]")
{
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

TEST_CASE("stable slot storage rejects layout changes after binding", "[v2 slot utils]")
{
    StableSlotStorage storage;
    storage.reserve_to(4, sizeof(std::uint32_t), alignof(std::uint32_t));

    REQUIRE_THROWS_AS(storage.reserve_to(8, sizeof(std::uint64_t), alignof(std::uint64_t)), std::logic_error);
}

TEST_CASE("stable slot storage allocates blocks through allocator ops", "[v2 slot utils]")
{
    AllocationProbe::reset();

    const MemoryUtils::AllocatorOps allocator{
        .allocate = &tracked_allocate,
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

TEST_CASE("value slot store tracks updates and notifies observers", "[v2 slot utils]")
{
    ValueSlotStore store(MemoryUtils::plan_for<std::uint32_t>());
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

    CHECK(observer.events ==
          std::vector<std::string>{"capacity:0->4", "insert:1", "remove:1", "erase:1", "clear"});

    store.remove_slot_observer(&observer);
    store.notify_insert(2);
    CHECK(observer.events.size() == 5);
}

TEST_CASE("value slot store manages typed payload lifetime on stable slots", "[v2 slot utils]")
{
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

TEST_CASE("value slot store destroys live payloads on scope exit", "[v2 slot utils]")
{
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

TEST_CASE("value slot store rejects invalid emplacement", "[v2 slot utils]")
{
    TrackedPayload::reset();

    ValueSlotStore store(MemoryUtils::plan_for<TrackedPayload>());
    store.reserve_to(1);

    store.construct_at<TrackedPayload>(0, 5);
    REQUIRE_THROWS_AS(store.construct_at<TrackedPayload>(0, 9), std::logic_error);
    REQUIRE_THROWS_AS(store.construct_at<TrackedPayload>(1, 11), std::out_of_range);
    REQUIRE_THROWS_AS(store.construct_at<std::string>(0, "wrong plan"), std::logic_error);
}

TEST_CASE("value slot store uses its bound plan for lifecycle operations", "[v2 slot utils]")
{
    ValueSlotStore store(MemoryUtils::plan_for<std::string>());
    store.reserve_to(2);

    REQUIRE(store.plan() == &MemoryUtils::plan_for<std::string>());

    store.construct_at(0);
    REQUIRE(store.has_slot(0));
    REQUIRE(*store.try_value<std::string>(0) == "");

    const std::string source = "copied";
    store.construct_at(1, &source);
    REQUIRE(store.has_slot(1));
    REQUIRE(*store.try_value<std::string>(1) == "copied");

    store.destroy_all();
    REQUIRE_FALSE(store.has_slot(0));
    REQUIRE_FALSE(store.has_slot(1));
}

TEST_CASE("value slot stores can share a custom allocator", "[v2 slot utils]")
{
    AllocationProbe::reset();

    const MemoryUtils::AllocatorOps allocator{
        .allocate = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };

    {
        ValueSlotStore store(MemoryUtils::plan_for<TrackedPayload>(), allocator);
        store.reserve_to(3);
        store.construct_at<TrackedPayload>(1, 17);
        REQUIRE(&store.value_storage.allocator() == &allocator);
        REQUIRE(AllocationProbe::allocations == 1);
    }

    REQUIRE(AllocationProbe::deallocations == 1);
}
