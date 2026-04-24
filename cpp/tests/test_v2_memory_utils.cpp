#include <catch2/catch_test_macros.hpp>

#include <hgraph/v2/types/utils/memory_utils.h>

#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

namespace
{
    using MemoryUtils = hgraph::v2::MemoryUtils;

    struct TrackedValue
    {
        static inline int default_constructed{0};
        static inline int copy_constructed{0};
        static inline int move_constructed{0};
        static inline int destroyed{0};

        int value{0};

        TrackedValue() { ++default_constructed; }
        TrackedValue(const TrackedValue &other)
            : value(other.value)
        {
            ++copy_constructed;
        }
        TrackedValue(TrackedValue &&other) noexcept
            : value(std::exchange(other.value, -1))
        {
            ++move_constructed;
        }
        ~TrackedValue() { ++destroyed; }

        static void reset()
        {
            default_constructed = 0;
            copy_constructed = 0;
            move_constructed = 0;
            destroyed = 0;
        }
    };

    struct WideInlineValue
    {
        uint64_t lhs{0};
        uint64_t rhs{0};
    };

    struct alignas(32) OverAlignedValue
    {
        uint64_t value{0};
    };

    struct LifecycleRecorder
    {
        static inline std::vector<int> events{};

        static void reset()
        {
            events.clear();
        }
    };

    template <int Id>
    struct OrderedValue
    {
        OrderedValue()
        {
            LifecycleRecorder::events.push_back(Id);
        }

        OrderedValue(const OrderedValue &)
        {
            LifecycleRecorder::events.push_back(100 + Id);
        }

        OrderedValue(OrderedValue &&) noexcept
        {
            LifecycleRecorder::events.push_back(200 + Id);
        }

        ~OrderedValue()
        {
            LifecycleRecorder::events.push_back(-Id);
        }
    };

    struct PartiallyConstructedValue
    {
        static inline int destroyed{0};

        PartiallyConstructedValue() = default;
        PartiallyConstructedValue(const PartiallyConstructedValue &) = default;
        PartiallyConstructedValue(PartiallyConstructedValue &&) noexcept = default;
        ~PartiallyConstructedValue()
        {
            ++destroyed;
        }

        static void reset()
        {
            destroyed = 0;
        }
    };

    struct ThrowsOnDefault
    {
        ThrowsOnDefault()
        {
            throw std::runtime_error("boom");
        }
    };

    struct ThrowsOnThirdDefault
    {
        static inline int constructed{0};
        static inline int destroyed{0};

        ThrowsOnThirdDefault()
        {
            if (constructed == 2) {
                throw std::runtime_error("boom");
            }
            ++constructed;
        }

        ThrowsOnThirdDefault(const ThrowsOnThirdDefault &) = default;
        ThrowsOnThirdDefault(ThrowsOnThirdDefault &&) noexcept = default;

        ~ThrowsOnThirdDefault()
        {
            ++destroyed;
        }

        static void reset()
        {
            constructed = 0;
            destroyed = 0;
        }
    };

    struct AllocationProbe
    {
        static inline int allocations{0};
        static inline int deallocations{0};
        static inline MemoryUtils::StorageLayout last_layout{};

        static void reset()
        {
            allocations = 0;
            deallocations = 0;
            last_layout = {};
        }
    };

    void *tracked_allocate(MemoryUtils::StorageLayout layout)
    {
        ++AllocationProbe::allocations;
        AllocationProbe::last_layout = layout;
        return ::operator new(layout.size == 0 ? 1 : layout.size, std::align_val_t{layout.alignment});
    }

    void tracked_deallocate(void *memory, MemoryUtils::StorageLayout layout) noexcept
    {
        ++AllocationProbe::deallocations;
        AllocationProbe::last_layout = layout;
        ::operator delete(memory, std::align_val_t{layout.alignment});
    }
}  // namespace

TEST_CASE("memory utils caches typed plans for the process lifetime", "[memory utils]")
{
    const auto &lhs = MemoryUtils::plan_for<uint32_t>();
    const auto &rhs = MemoryUtils::plan_for<uint32_t>();

    REQUIRE(&lhs == &rhs);
    REQUIRE(lhs.valid());
    REQUIRE(lhs.layout.size == sizeof(uint32_t));
    REQUIRE(lhs.layout.alignment == alignof(uint32_t));
    REQUIRE(lhs.template stores_inline<>());
    REQUIRE_FALSE(lhs.requires_destroy());
    REQUIRE_FALSE(lhs.template requires_deallocate<>());
}

TEST_CASE("memory utils packs storage handle state into pointer-sized storage", "[memory utils]")
{
    REQUIRE(sizeof(MemoryUtils::StorageHandle<>) == sizeof(void *) * 3);
}

TEST_CASE("memory utils keeps trivial pointer-sized payloads inline in owning handles", "[memory utils]")
{
    const auto &plan = MemoryUtils::plan_for<uint32_t>();

    MemoryUtils::StorageHandle<> handle(plan);
    REQUIRE(handle);
    REQUIRE(handle.plan() == &plan);
    REQUIRE(handle.is_owning());
    REQUIRE_FALSE(handle.is_reference());
    REQUIRE(handle.stores_inline());
    REQUIRE_FALSE(handle.stores_heap());

    *handle.as<uint32_t>() = 42u;
    REQUIRE(*handle.as<uint32_t>() == 42u);
    REQUIRE(reinterpret_cast<std::uintptr_t>(handle.data()) % alignof(uint32_t) == 0u);

    auto copied = handle;
    REQUIRE(copied.is_owning());
    REQUIRE(copied.stores_inline());
    REQUIRE(*copied.as<uint32_t>() == 42u);

    *handle.as<uint32_t>() = 7u;
    REQUIRE(*copied.as<uint32_t>() == 42u);

    auto moved = std::move(handle);
    REQUIRE_FALSE(handle);
    REQUIRE(*moved.as<uint32_t>() == 7u);
}

TEST_CASE("memory utils separates allocation through allocator ops", "[memory utils]")
{
    const auto &plan = MemoryUtils::plan_for<TrackedValue>();
    const MemoryUtils::AllocatorOps allocator{
        .allocate = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };

    TrackedValue::reset();
    AllocationProbe::reset();

    {
        MemoryUtils::StorageHandle<> handle(plan, allocator);
        REQUIRE(handle.stores_heap());
        REQUIRE(AllocationProbe::allocations == 1);
        REQUIRE(AllocationProbe::deallocations == 0);
        REQUIRE(AllocationProbe::last_layout.size == sizeof(TrackedValue));
        REQUIRE(AllocationProbe::last_layout.alignment == alignof(TrackedValue));
    }

    REQUIRE(AllocationProbe::allocations == 1);
    REQUIRE(AllocationProbe::deallocations == 1);
    REQUIRE(TrackedValue::destroyed == 1);
}

TEST_CASE("memory utils heap-backed handles deep-copy on copy and transfer on move", "[memory utils]")
{
    TrackedValue::reset();

    const auto &plan = MemoryUtils::plan_for<TrackedValue>();

    REQUIRE(plan.valid());
    REQUIRE_FALSE(plan.template stores_inline<>());
    REQUIRE(plan.requires_destroy());
    REQUIRE(plan.template requires_deallocate<>());

    {
        MemoryUtils::StorageHandle<> source(plan);
        REQUIRE(source.is_owning());
        REQUIRE(source.stores_heap());
        source.as<TrackedValue>()->value = 17;
        REQUIRE(TrackedValue::default_constructed == 1);

        MemoryUtils::StorageHandle<> copied = source;
        REQUIRE(copied.is_owning());
        REQUIRE(copied.stores_heap());
        REQUIRE(copied.as<TrackedValue>()->value == 17);
        REQUIRE(TrackedValue::copy_constructed == 1);

        source.as<TrackedValue>()->value = 23;
        REQUIRE(copied.as<TrackedValue>()->value == 17);

        MemoryUtils::StorageHandle<> moved = std::move(source);
        REQUIRE_FALSE(source);
        REQUIRE(moved.is_owning());
        REQUIRE(moved.as<TrackedValue>()->value == 23);
    }

    REQUIRE(TrackedValue::destroyed == 2);
}

TEST_CASE("memory utils reference handles are non-owning and copy into owning handles", "[memory utils]")
{
    TrackedValue::reset();

    const auto &plan = MemoryUtils::plan_for<TrackedValue>();

    {
        TrackedValue external;
        external.value = 41;

        auto reference = MemoryUtils::StorageHandle<>::reference(plan, &external);
        REQUIRE(reference.is_reference());
        REQUIRE_FALSE(reference.is_owning());
        REQUIRE(reference.plan() == &plan);
        REQUIRE(reference.data() == &external);

        {
            auto copied = reference;
            REQUIRE(copied.is_owning());
            REQUIRE(copied.plan() == &plan);
            REQUIRE(copied.as<TrackedValue>()->value == 41);
            REQUIRE(TrackedValue::copy_constructed == 1);

            external.value = 7;
            REQUIRE(copied.as<TrackedValue>()->value == 41);
            copied.as<TrackedValue>()->value = 99;
            REQUIRE(external.value == 7);
        }

        REQUIRE(TrackedValue::destroyed == 1);
    }

    REQUIRE(TrackedValue::destroyed == 2);
}

TEST_CASE("memory utils supports custom inline policies and aligned heap ownership", "[memory utils]")
{
    using WideInlinePolicy = MemoryUtils::InlineStoragePolicy<sizeof(WideInlineValue), alignof(WideInlineValue)>;

    const auto &wide_inline_plan = MemoryUtils::plan_for<WideInlineValue>();
    REQUIRE_FALSE(wide_inline_plan.template stores_inline<>());
    REQUIRE(wide_inline_plan.template stores_inline<WideInlinePolicy>());

    MemoryUtils::StorageHandle<WideInlinePolicy> wide_inline_handle(wide_inline_plan);
    REQUIRE(wide_inline_handle.stores_inline());
    wide_inline_handle.as<WideInlineValue>()->lhs = 3;
    wide_inline_handle.as<WideInlineValue>()->rhs = 9;
    REQUIRE(wide_inline_handle.as<WideInlineValue>()->lhs + wide_inline_handle.as<WideInlineValue>()->rhs == 12);

    const auto &over_aligned_plan = MemoryUtils::plan_for<OverAlignedValue>();
    REQUIRE_FALSE(over_aligned_plan.template stores_inline<>());
    REQUIRE_FALSE(over_aligned_plan.requires_destroy());
    REQUIRE(over_aligned_plan.template requires_deallocate<>());

    MemoryUtils::StorageHandle<> over_aligned_handle(over_aligned_plan);
    REQUIRE(over_aligned_handle.stores_heap());
    REQUIRE(reinterpret_cast<std::uintptr_t>(over_aligned_handle.data()) % alignof(OverAlignedValue) == 0u);
}

TEST_CASE("memory utils caches tuple and named tuple plans and supports nesting", "[memory utils]")
{
    const auto &point = MemoryUtils::named_tuple()
                            .add_field<uint16_t>("x")
                            .add_field<uint16_t>("y")
                            .build();

    const auto &point_again = MemoryUtils::named_tuple_plan(
        {{"x", &MemoryUtils::plan_for<uint16_t>()}, {"y", &MemoryUtils::plan_for<uint16_t>()}});

    const auto &payload = MemoryUtils::tuple()
                              .add_type<uint8_t>()
                              .add_plan(point)
                              .add_plan(MemoryUtils::named_tuple_plan({{"id", &MemoryUtils::plan_for<uint32_t>()}}))
                              .build();

    REQUIRE(&point == &point_again);
    REQUIRE(point.is_named_tuple());
    REQUIRE(point.component_count() == 2);
    REQUIRE(point.component("x").index == 0);
    REQUIRE(point.component("x").offset == 0);
    REQUIRE(point.component("y").index == 1);
    REQUIRE(point.component("y").offset == sizeof(uint16_t));
    REQUIRE(point.find_component("missing") == nullptr);

    REQUIRE(payload.is_tuple());
    REQUIRE(payload.component_count() == 3);
    REQUIRE(payload.component(0).offset == 0);
    REQUIRE(payload.component(1).plan->is_named_tuple());
    REQUIRE(payload.component(1).plan->component("x").offset == 0);
    REQUIRE(payload.component(2).plan->component("id").offset == 0);
    REQUIRE(payload.component(2).plan->component("id").name != nullptr);
}

TEST_CASE("memory utils caches array plans and exposes homogeneous array metadata", "[memory utils]")
{
    const auto &point = MemoryUtils::named_tuple()
                            .add_field<uint16_t>("x")
                            .add_field<uint16_t>("y")
                            .build();

    const auto &points = MemoryUtils::array_plan(point, 3);
    const auto &points_again = MemoryUtils::array_plan(point, 3);
    const auto &empty_values = MemoryUtils::array_plan<uint32_t>(0);
    const auto &payload = MemoryUtils::tuple().add_type<uint8_t>().add_plan(points).build();

    REQUIRE(&points == &points_again);
    REQUIRE(points.is_array());
    REQUIRE_FALSE(points.is_composite());
    REQUIRE(points.composite_kind() == MemoryUtils::CompositeKind::Array);
    REQUIRE(points.array_count() == 3);
    REQUIRE(points.array_stride() == point.layout.size);
    REQUIRE(&points.array_element_plan() == &point);
    REQUIRE(points.element_offset(0) == 0);
    REQUIRE(points.element_offset(2) == point.layout.size * 2);
    REQUIRE_THROWS_AS(points.element_offset(3), std::out_of_range);

    REQUIRE(empty_values.valid());
    REQUIRE(empty_values.is_array());
    REQUIRE(empty_values.array_count() == 0);
    REQUIRE(empty_values.layout.size == 0);

    REQUIRE(payload.component(1).plan->is_array());
    REQUIRE(payload.component(1).plan->array_count() == 3);
}

TEST_CASE("memory utils stores composite components in trailing composite-state storage", "[memory utils]")
{
    const auto &point = MemoryUtils::named_tuple()
                            .add_field<uint16_t>("x")
                            .add_field<uint16_t>("y")
                            .build();

    const auto *state = point.composite_state();
    REQUIRE(state != nullptr);
    REQUIRE(state->component_count == 2);

    const auto *state_bytes = reinterpret_cast<const std::byte *>(state);
    const auto *component_bytes = reinterpret_cast<const std::byte *>(state->components());

    REQUIRE(component_bytes == state_bytes + MemoryUtils::CompositeState::components_offset());
    REQUIRE(&point.component(0) == state->components());
}

TEST_CASE("memory utils composite builders reject invalid tuple and named tuple mixes", "[memory utils]")
{
    const auto &scalar = MemoryUtils::plan_for<uint32_t>();

    auto tuple_builder = MemoryUtils::tuple();
    REQUIRE_THROWS_AS(tuple_builder.add_field("value", scalar), std::logic_error);

    auto named_builder = MemoryUtils::named_tuple();
    REQUIRE_THROWS_AS(named_builder.add_plan(scalar), std::logic_error);
    REQUIRE_NOTHROW(named_builder.add_field("value", scalar));
    REQUIRE_THROWS_AS(named_builder.add_field("value", scalar), std::logic_error);
}

TEST_CASE("memory utils nested composite handles construct and destroy in deterministic order", "[memory utils]")
{
    LifecycleRecorder::reset();

    const auto &inner = MemoryUtils::named_tuple()
                            .add_field<OrderedValue<2>>("lhs")
                            .add_field<OrderedValue<3>>("rhs")
                            .build();

    {
        const auto &outer = MemoryUtils::tuple().add_type<OrderedValue<1>>().add_plan(inner).build();
        MemoryUtils::StorageHandle<> handle(outer);
        REQUIRE(handle.is_owning());
    }

    REQUIRE(LifecycleRecorder::events == std::vector<int>{1, 2, 3, -3, -2, -1});
}

TEST_CASE("memory utils composite handles deep-copy nested child payloads", "[memory utils]")
{
    TrackedValue::reset();

    const auto &composite = MemoryUtils::named_tuple()
                                .add_field("value", MemoryUtils::plan_for<TrackedValue>())
                                .add_field("count", MemoryUtils::plan_for<uint32_t>())
                                .build();

    {
        MemoryUtils::StorageHandle<> source(composite);

        auto *source_value = MemoryUtils::cast<TrackedValue>(
            MemoryUtils::advance(source.data(), composite.component("value").offset));
        auto *source_count = MemoryUtils::cast<uint32_t>(
            MemoryUtils::advance(source.data(), composite.component("count").offset));

        source_value->value = 23;
        *source_count = 99u;

        MemoryUtils::StorageHandle<> copied = source;
        auto *copied_value = MemoryUtils::cast<TrackedValue>(
            MemoryUtils::advance(copied.data(), composite.component("value").offset));
        auto *copied_count = MemoryUtils::cast<uint32_t>(
            MemoryUtils::advance(copied.data(), composite.component("count").offset));

        REQUIRE(copied_value->value == 23);
        REQUIRE(*copied_count == 99u);
        REQUIRE(TrackedValue::copy_constructed == 1);

        source_value->value = 77;
        *source_count = 5u;
        REQUIRE(copied_value->value == 23);
        REQUIRE(*copied_count == 99u);

        MemoryUtils::StorageHandle<> moved = std::move(source);
        REQUIRE_FALSE(source);
        REQUIRE(MemoryUtils::cast<TrackedValue>(
                    MemoryUtils::advance(moved.data(), composite.component("value").offset))
                    ->value == 77);
    }

    REQUIRE(TrackedValue::destroyed == 2);
}

TEST_CASE("memory utils array handles deep-copy element payloads", "[memory utils]")
{
    TrackedValue::reset();

    const auto &array = MemoryUtils::array_plan<TrackedValue>(3);

    {
        MemoryUtils::StorageHandle<> source(array);
        REQUIRE(source.is_owning());
        REQUIRE(source.stores_heap());
        REQUIRE(TrackedValue::default_constructed == 3);

        for (size_t index = 0; index < array.array_count(); ++index) {
            auto *value =
                MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(source.data(), array.element_offset(index)));
            value->value = static_cast<int>(index + 1) * 10;
        }

        MemoryUtils::StorageHandle<> copied = source;
        REQUIRE(TrackedValue::copy_constructed == 3);

        for (size_t index = 0; index < array.array_count(); ++index) {
            auto *value =
                MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(copied.data(), array.element_offset(index)));
            REQUIRE(value->value == static_cast<int>(index + 1) * 10);
        }

        auto *source_first = MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(source.data(), array.element_offset(0)));
        source_first->value = 99;
        REQUIRE(MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(copied.data(), array.element_offset(0)))->value ==
                10);
    }

    REQUIRE(TrackedValue::destroyed == 6);
}

TEST_CASE("memory utils composite plans clean up partial construction on owning handle creation", "[memory utils]")
{
    PartiallyConstructedValue::reset();

    const auto &composite = MemoryUtils::tuple()
                                .add_type<PartiallyConstructedValue>()
                                .add_type<ThrowsOnDefault>()
                                .build();

    REQUIRE_THROWS_AS(MemoryUtils::StorageHandle<>{composite}, std::runtime_error);
    REQUIRE(PartiallyConstructedValue::destroyed == 1);
}

TEST_CASE("memory utils array plans clean up partial construction on owning handle creation", "[memory utils]")
{
    ThrowsOnThirdDefault::reset();

    const auto &array = MemoryUtils::array_plan<ThrowsOnThirdDefault>(4);

    REQUIRE_THROWS_AS(MemoryUtils::StorageHandle<>{array}, std::runtime_error);
    REQUIRE(ThrowsOnThirdDefault::constructed == 2);
    REQUIRE(ThrowsOnThirdDefault::destroyed == 2);
}
