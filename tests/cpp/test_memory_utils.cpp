#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/utils/memory_utils.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace
{
    using MemoryUtils = hgraph::MemoryUtils;

    template <typename Owner>
    concept HasBorrowedFactory = requires(const MemoryUtils::StoragePlan &plan, void *data) {
        Owner::reference(plan, data);
    };

    struct TrackedValue
    {
        static inline int default_constructed{0};
        static inline int copy_constructed{0};
        static inline int move_constructed{0};
        static inline int copy_assigned{0};
        static inline int move_assigned{0};
        static inline int destroyed{0};

        int value{0};

        TrackedValue() { ++default_constructed; }
        TrackedValue(const TrackedValue &other) : value(other.value) { ++copy_constructed; }
        TrackedValue(TrackedValue &&other) noexcept : value(std::exchange(other.value, -1)) { ++move_constructed; }
        TrackedValue &operator=(const TrackedValue &other) {
            value = other.value;
            ++copy_assigned;
            return *this;
        }
        TrackedValue &operator=(TrackedValue &&other) noexcept {
            value = std::exchange(other.value, -1);
            ++move_assigned;
            return *this;
        }
        ~TrackedValue() { ++destroyed; }

        static void reset() {
            default_constructed = 0;
            copy_constructed    = 0;
            move_constructed    = 0;
            copy_assigned       = 0;
            move_assigned       = 0;
            destroyed           = 0;
        }
    };

    struct CopyConstructOnlyValue
    {
        int value{0};

        CopyConstructOnlyValue()                                              = default;
        CopyConstructOnlyValue(const CopyConstructOnlyValue &)                = default;
        CopyConstructOnlyValue(CopyConstructOnlyValue &&) noexcept            = default;
        CopyConstructOnlyValue &operator=(const CopyConstructOnlyValue &)     = delete;
        CopyConstructOnlyValue &operator=(CopyConstructOnlyValue &&) noexcept = delete;
    };

    struct WideInlineValue
    {
        uint64_t lhs{0};
        uint64_t rhs{0};
    };

    struct alignas(32) OverAlignedValue
    {
        std::byte storage[32]{};
    };

    struct LifecycleRecorder
    {
        static inline std::vector<int> events{};

        static void reset() { events.clear(); }
    };

    template <int Id> struct OrderedValue
    {
        OrderedValue() { LifecycleRecorder::events.push_back(Id); }

        OrderedValue(const OrderedValue &) { LifecycleRecorder::events.push_back(100 + Id); }

        OrderedValue(OrderedValue &&) noexcept { LifecycleRecorder::events.push_back(200 + Id); }

        ~OrderedValue() { LifecycleRecorder::events.push_back(-Id); }
    };

    struct PartiallyConstructedValue
    {
        static inline int destroyed{0};

        PartiallyConstructedValue()                                      = default;
        PartiallyConstructedValue(const PartiallyConstructedValue &)     = default;
        PartiallyConstructedValue(PartiallyConstructedValue &&) noexcept = default;
        ~PartiallyConstructedValue() { ++destroyed; }

        static void reset() { destroyed = 0; }
    };

    struct ThrowsOnDefault
    {
        ThrowsOnDefault() { throw std::runtime_error("boom"); }
    };

    struct ThrowsOnThirdDefault
    {
        static inline int constructed{0};
        static inline int destroyed{0};

        ThrowsOnThirdDefault() {
            if (constructed == 2) { throw std::runtime_error("boom"); }
            ++constructed;
        }

        ThrowsOnThirdDefault(const ThrowsOnThirdDefault &)     = default;
        ThrowsOnThirdDefault(ThrowsOnThirdDefault &&) noexcept = default;

        ~ThrowsOnThirdDefault() { ++destroyed; }

        static void reset() {
            constructed = 0;
            destroyed   = 0;
        }
    };

    struct AllocationProbe
    {
        static inline int                        allocations{0};
        static inline int                        deallocations{0};
        static inline MemoryUtils::StorageLayout last_layout{};

        static void reset() {
            allocations   = 0;
            deallocations = 0;
            last_layout   = {};
        }
    };

    void *tracked_allocate(MemoryUtils::StorageLayout layout) {
        ++AllocationProbe::allocations;
        AllocationProbe::last_layout = layout;
        return ::operator new(layout.size == 0 ? 1 : layout.size, std::align_val_t{layout.alignment});
    }

    void tracked_deallocate(void *memory, MemoryUtils::StorageLayout layout) noexcept {
        ++AllocationProbe::deallocations;
        AllocationProbe::last_layout = layout;
        ::operator delete(memory, std::align_val_t{layout.alignment});
    }
}  // namespace

TEST_CASE("memory utils caches typed plans for the process lifetime", "[memory utils]") {
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

TEST_CASE("memory utils interns raw caller-owned storage plans", "[memory utils]") {
    constexpr MemoryUtils::StorageLayout layout{.size = 257, .alignment = 64};

    const auto &lhs = MemoryUtils::raw_storage_plan(layout);
    const auto &rhs = MemoryUtils::raw_storage_plan(layout);

    REQUIRE(&lhs == &rhs);
    REQUIRE(lhs.valid());
    CHECK(lhs.layout.size == layout.size);
    CHECK(lhs.layout.alignment == layout.alignment);
    CHECK(lhs.can_default_construct());
    CHECK_FALSE(lhs.requires_destroy());
    CHECK_FALSE(lhs.can_copy_construct());
    CHECK_FALSE(lhs.can_move_construct());

    void *memory = ::operator new(layout.size, std::align_val_t{layout.alignment});
    lhs.default_construct(memory);
    lhs.destroy(memory);
    ::operator delete(memory, std::align_val_t{layout.alignment});
}

TEST_CASE("memory utils rejects empty or invalid raw storage layouts", "[memory utils]") {
    REQUIRE_THROWS_AS(MemoryUtils::raw_storage_plan({.size = 0, .alignment = 1}), std::logic_error);
    REQUIRE_THROWS_AS(MemoryUtils::raw_storage_plan({.size = 8, .alignment = 3}), std::logic_error);
}

TEST_CASE("memory utils packs erased owner state into three pointer words", "[memory utils]") {
    REQUIRE(sizeof(MemoryUtils::ErasedOwner<>) == sizeof(void *) * 3);
}

TEST_CASE("memory utils keeps trivial pointer-sized payloads inline in erased owners", "[memory utils]") {
    const auto &plan = MemoryUtils::plan_for<uint32_t>();

    MemoryUtils::ErasedOwner<> handle(plan);
    REQUIRE(handle);
    REQUIRE(handle.plan() == &plan);
    REQUIRE(handle.is_owning());
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

TEST_CASE("memory utils empty owners can retain a bound plan", "[memory utils]") {
    const auto &plan = MemoryUtils::plan_for<uint32_t>();

    auto handle = MemoryUtils::ErasedOwner<>::empty(plan);
    REQUIRE_FALSE(handle.has_value());
    REQUIRE(handle.plan() == &plan);
    REQUIRE(handle.data() == nullptr);

    auto copied = handle;
    REQUIRE_FALSE(copied.has_value());
    REQUIRE(copied.plan() == &plan);

    auto cloned = handle.clone();
    REQUIRE_FALSE(cloned.has_value());
    REQUIRE(cloned.plan() == &plan);

    handle.reset_payload();
    REQUIRE_FALSE(handle.has_value());
    REQUIRE(handle.plan() == &plan);
}

TEST_CASE("memory utils separates allocation through allocator ops", "[memory utils]") {
    const auto                     &plan = MemoryUtils::plan_for<TrackedValue>();
    const MemoryUtils::AllocatorOps allocator{
        .allocate   = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };

    TrackedValue::reset();
    AllocationProbe::reset();

    {
        MemoryUtils::ErasedOwner<> handle(plan, allocator);
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

TEST_CASE("memory utils heap-backed owners deep-copy on copy and transfer on move", "[memory utils]") {
    TrackedValue::reset();

    const auto &plan = MemoryUtils::plan_for<TrackedValue>();

    REQUIRE(plan.valid());
    REQUIRE_FALSE(plan.template stores_inline<>());
    REQUIRE(plan.requires_destroy());
    REQUIRE(plan.template requires_deallocate<>());

    {
        MemoryUtils::ErasedOwner<> source(plan);
        REQUIRE(source.is_owning());
        REQUIRE(source.stores_heap());
        source.as<TrackedValue>()->value = 17;
        REQUIRE(TrackedValue::default_constructed == 1);

        MemoryUtils::ErasedOwner<> copied = source;
        REQUIRE(copied.is_owning());
        REQUIRE(copied.stores_heap());
        REQUIRE(copied.as<TrackedValue>()->value == 17);
        REQUIRE(TrackedValue::copy_constructed == 1);

        source.as<TrackedValue>()->value = 23;
        REQUIRE(copied.as<TrackedValue>()->value == 17);

        MemoryUtils::ErasedOwner<> moved = std::move(source);
        REQUIRE_FALSE(source);
        REQUIRE(moved.is_owning());
        REQUIRE(moved.as<TrackedValue>()->value == 23);
    }

    REQUIRE(TrackedValue::destroyed == 2);
}

TEST_CASE("memory utils storage plans expose copy and move assignment hooks", "[memory utils]") {
    TrackedValue::reset();

    const auto &plan = MemoryUtils::plan_for<TrackedValue>();
    REQUIRE(plan.can_copy_assign());
    REQUIRE(plan.can_move_assign());

    MemoryUtils::ErasedOwner<> dst(plan);
    MemoryUtils::ErasedOwner<> src(plan);
    MemoryUtils::ErasedOwner<> moved(plan);

    dst.as<TrackedValue>()->value   = 1;
    src.as<TrackedValue>()->value   = 7;
    moved.as<TrackedValue>()->value = 11;

    plan.copy_assign(dst.data(), src.data());
    REQUIRE(dst.as<TrackedValue>()->value == 7);
    REQUIRE(TrackedValue::copy_assigned == 1);

    plan.move_assign(dst.data(), moved.data());
    REQUIRE(dst.as<TrackedValue>()->value == 11);
    REQUIRE(TrackedValue::move_assigned == 1);
}

TEST_CASE("memory utils composite and array plans forward assignment support from child plans", "[memory utils]") {
    TrackedValue::reset();

    auto        tuple_builder = MemoryUtils::named_tuple()
                                    .add_field("value", MemoryUtils::plan_for<TrackedValue>())
                                    .add_field("count", MemoryUtils::plan_for<uint32_t>());
    const auto &tuple_plan    = tuple_builder.build();
    REQUIRE(tuple_plan.can_copy_assign());
    REQUIRE(tuple_plan.can_move_assign());

    MemoryUtils::ErasedOwner<> tuple_dst(tuple_plan);
    MemoryUtils::ErasedOwner<> tuple_src(tuple_plan);

    MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(tuple_dst.data(), tuple_plan.component("value").offset))->value = 1;
    *MemoryUtils::cast<uint32_t>(MemoryUtils::advance(tuple_dst.data(), tuple_plan.component("count").offset))           = 2u;
    MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(tuple_src.data(), tuple_plan.component("value").offset))->value = 9;
    *MemoryUtils::cast<uint32_t>(MemoryUtils::advance(tuple_src.data(), tuple_plan.component("count").offset))           = 17u;

    tuple_plan.copy_assign(tuple_dst.data(), tuple_src.data());
    REQUIRE(MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(tuple_dst.data(), tuple_plan.component("value").offset))->value ==
            9);
    REQUIRE(*MemoryUtils::cast<uint32_t>(MemoryUtils::advance(tuple_dst.data(), tuple_plan.component("count").offset)) == 17u);
    REQUIRE(TrackedValue::copy_assigned >= 1);

    const auto &array_plan = MemoryUtils::array_plan<TrackedValue>(2);
    REQUIRE(array_plan.can_copy_assign());
    REQUIRE(array_plan.can_move_assign());

    MemoryUtils::ErasedOwner<> array_dst(array_plan);
    MemoryUtils::ErasedOwner<> array_src(array_plan);
    MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(array_dst.data(), array_plan.element_offset(0)))->value = 3;
    MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(array_dst.data(), array_plan.element_offset(1)))->value = 4;
    MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(array_src.data(), array_plan.element_offset(0)))->value = 21;
    MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(array_src.data(), array_plan.element_offset(1)))->value = 22;

    array_plan.copy_assign(array_dst.data(), array_src.data());
    REQUIRE(MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(array_dst.data(), array_plan.element_offset(0)))->value == 21);
    REQUIRE(MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(array_dst.data(), array_plan.element_offset(1)))->value == 22);
}

TEST_CASE("memory utils plans report missing assignment hooks when a type is not assignable", "[memory utils]") {
    const auto &plan = MemoryUtils::plan_for<CopyConstructOnlyValue>();

    REQUIRE_FALSE(plan.can_copy_assign());
    REQUIRE_FALSE(plan.can_move_assign());

    MemoryUtils::ErasedOwner<> dst(plan);
    MemoryUtils::ErasedOwner<> src(plan);

    REQUIRE_THROWS_AS(plan.copy_assign(dst.data(), src.data()), std::logic_error);
    REQUIRE_THROWS_AS(plan.move_assign(dst.data(), src.data()), std::logic_error);
}

TEST_CASE("memory utils erased owners have no borrowed-storage factory", "[memory utils]") {
    using Owner = MemoryUtils::ErasedOwner<>;
    static_assert(!HasBorrowedFactory<Owner>);
}

TEST_CASE("memory utils reset destroys an erased owner exactly once", "[memory utils]") {
    TrackedValue::reset();
    const auto &plan = MemoryUtils::plan_for<TrackedValue>();

    MemoryUtils::ErasedOwner<> owner(plan);
    REQUIRE(owner.has_value());
    owner.reset_payload();
    REQUIRE_FALSE(owner.has_value());
    REQUIRE(owner.plan() == &plan);
    REQUIRE(TrackedValue::destroyed == 1);

    owner.reset_payload();
    REQUIRE(TrackedValue::destroyed == 1);
    owner.reset();
    REQUIRE(owner.plan() == nullptr);
    REQUIRE(TrackedValue::destroyed == 1);
}

TEST_CASE("memory utils supports custom inline policies and aligned heap ownership", "[memory utils]") {
    using WideInlinePolicy = MemoryUtils::InlineStoragePolicy<sizeof(WideInlineValue), alignof(WideInlineValue)>;

    const auto &wide_inline_plan = MemoryUtils::plan_for<WideInlineValue>();
    REQUIRE_FALSE(wide_inline_plan.template stores_inline<>());
    REQUIRE(wide_inline_plan.template stores_inline<WideInlinePolicy>());

    MemoryUtils::ErasedOwner<WideInlinePolicy> wide_inline_handle(wide_inline_plan);
    REQUIRE(wide_inline_handle.stores_inline());
    wide_inline_handle.as<WideInlineValue>()->lhs = 3;
    wide_inline_handle.as<WideInlineValue>()->rhs = 9;
    REQUIRE(wide_inline_handle.as<WideInlineValue>()->lhs + wide_inline_handle.as<WideInlineValue>()->rhs == 12);

    const auto &over_aligned_plan = MemoryUtils::plan_for<OverAlignedValue>();
    REQUIRE_FALSE(over_aligned_plan.template stores_inline<>());
    REQUIRE_FALSE(over_aligned_plan.requires_destroy());
    REQUIRE(over_aligned_plan.template requires_deallocate<>());

    MemoryUtils::ErasedOwner<> over_aligned_handle(over_aligned_plan);
    REQUIRE(over_aligned_handle.stores_heap());
    REQUIRE(reinterpret_cast<std::uintptr_t>(over_aligned_handle.data()) % alignof(OverAlignedValue) == 0u);

    using OverAlignedInlinePolicy =
        MemoryUtils::InlineStoragePolicy<sizeof(OverAlignedValue), alignof(OverAlignedValue)>;
    REQUIRE(over_aligned_plan.template stores_inline<OverAlignedInlinePolicy>());

    MemoryUtils::ErasedOwner<OverAlignedInlinePolicy> over_aligned_inline(over_aligned_plan);
    REQUIRE(over_aligned_inline.stores_inline());
    REQUIRE(reinterpret_cast<std::uintptr_t>(over_aligned_inline.data()) % alignof(OverAlignedValue) == 0u);
}

TEST_CASE("memory utils caches tuple and named tuple plans and supports nesting", "[memory utils]") {
    auto        point_builder = MemoryUtils::named_tuple().add_field<uint16_t>("x").add_field<uint16_t>("y");
    const auto &point         = point_builder.build();

    const auto &point_again =
        MemoryUtils::named_tuple_plan({{"x", &MemoryUtils::plan_for<uint16_t>()}, {"y", &MemoryUtils::plan_for<uint16_t>()}});

    auto        payload_builder = MemoryUtils::tuple()
                                      .add_type<uint8_t>()
                                      .add_plan(point)
                                      .add_plan(MemoryUtils::named_tuple_plan({{"id", &MemoryUtils::plan_for<uint32_t>()}}));
    const auto &payload         = payload_builder.build();

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

TEST_CASE("memory utils caches array plans and exposes homogeneous array metadata", "[memory utils]") {
    auto        point_builder = MemoryUtils::named_tuple().add_field<uint16_t>("x").add_field<uint16_t>("y");
    const auto &point         = point_builder.build();

    const auto &points       = MemoryUtils::array_plan(point, 3);
    const auto &points_again = MemoryUtils::array_plan(point, 3);
    const auto &empty_values = MemoryUtils::array_plan<uint32_t>(0);
    auto        payload_builder = MemoryUtils::tuple().add_type<uint8_t>().add_plan(points);
    const auto &payload         = payload_builder.build();

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

TEST_CASE("memory utils stores composite components in trailing composite-state storage", "[memory utils]") {
    auto        point_builder = MemoryUtils::named_tuple().add_field<uint16_t>("x").add_field<uint16_t>("y");
    const auto &point         = point_builder.build();

    const auto *state = point.composite_state();
    REQUIRE(state != nullptr);
    REQUIRE(state->component_count == 2);

    const auto *state_bytes     = reinterpret_cast<const std::byte *>(state);
    const auto *component_bytes = reinterpret_cast<const std::byte *>(state->components());

    REQUIRE(component_bytes == state_bytes + MemoryUtils::CompositeState::components_offset());
    REQUIRE(&point.component(0) == state->components());
}

TEST_CASE("memory utils composite builders reject invalid tuple and named tuple mixes", "[memory utils]") {
    const auto &scalar = MemoryUtils::plan_for<uint32_t>();

    auto tuple_builder = MemoryUtils::tuple();
    REQUIRE_THROWS_AS(tuple_builder.add_field("value", scalar), std::logic_error);

    auto named_builder = MemoryUtils::named_tuple();
    REQUIRE_THROWS_AS(named_builder.add_plan(scalar), std::logic_error);
    REQUIRE_NOTHROW(named_builder.add_field("value", scalar));
    REQUIRE_THROWS_AS(named_builder.add_field("value", scalar), std::logic_error);
}

TEST_CASE("memory utils nested composite owners construct and destroy in deterministic order", "[memory utils]") {
    LifecycleRecorder::reset();

    auto        inner_builder = MemoryUtils::named_tuple().add_field<OrderedValue<2>>("lhs").add_field<OrderedValue<3>>("rhs");
    const auto &inner         = inner_builder.build();

    {
        auto        outer_builder = MemoryUtils::tuple().add_type<OrderedValue<1>>().add_plan(inner);
        const auto &outer         = outer_builder.build();
        MemoryUtils::ErasedOwner<> handle(outer);
        REQUIRE(handle.is_owning());
    }

    REQUIRE(LifecycleRecorder::events == std::vector<int>{1, 2, 3, -3, -2, -1});
}

TEST_CASE("memory utils composite owners deep-copy nested child payloads", "[memory utils]") {
    TrackedValue::reset();

    auto        composite_builder = MemoryUtils::named_tuple()
                                       .add_field("value", MemoryUtils::plan_for<TrackedValue>())
                                       .add_field("count", MemoryUtils::plan_for<uint32_t>());
    const auto &composite         = composite_builder.build();

    {
        MemoryUtils::ErasedOwner<> source(composite);

        auto *source_value =
            MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(source.data(), composite.component("value").offset));
        auto *source_count = MemoryUtils::cast<uint32_t>(MemoryUtils::advance(source.data(), composite.component("count").offset));

        source_value->value = 23;
        *source_count       = 99u;

        MemoryUtils::ErasedOwner<> copied = source;
        auto                        *copied_value =
            MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(copied.data(), composite.component("value").offset));
        auto *copied_count = MemoryUtils::cast<uint32_t>(MemoryUtils::advance(copied.data(), composite.component("count").offset));

        REQUIRE(copied_value->value == 23);
        REQUIRE(*copied_count == 99u);
        REQUIRE(TrackedValue::copy_constructed == 1);

        source_value->value = 77;
        *source_count       = 5u;
        REQUIRE(copied_value->value == 23);
        REQUIRE(*copied_count == 99u);

        MemoryUtils::ErasedOwner<> moved = std::move(source);
        REQUIRE_FALSE(source);
        REQUIRE(MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(moved.data(), composite.component("value").offset))->value ==
                77);
    }

    REQUIRE(TrackedValue::destroyed == 2);
}

TEST_CASE("memory utils array owners deep-copy element payloads", "[memory utils]") {
    TrackedValue::reset();

    const auto &array = MemoryUtils::array_plan<TrackedValue>(3);

    {
        MemoryUtils::ErasedOwner<> source(array);
        REQUIRE(source.is_owning());
        REQUIRE(source.stores_heap());
        REQUIRE(TrackedValue::default_constructed == 3);

        for (size_t index = 0; index < array.array_count(); ++index) {
            auto *value  = MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(source.data(), array.element_offset(index)));
            value->value = static_cast<int>(index + 1) * 10;
        }

        MemoryUtils::ErasedOwner<> copied = source;
        REQUIRE(TrackedValue::copy_constructed == 3);

        for (size_t index = 0; index < array.array_count(); ++index) {
            auto *value = MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(copied.data(), array.element_offset(index)));
            REQUIRE(value->value == static_cast<int>(index + 1) * 10);
        }

        auto *source_first  = MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(source.data(), array.element_offset(0)));
        source_first->value = 99;
        REQUIRE(MemoryUtils::cast<TrackedValue>(MemoryUtils::advance(copied.data(), array.element_offset(0)))->value == 10);
    }

    REQUIRE(TrackedValue::destroyed == 6);
}

TEST_CASE("memory utils composite plans clean up partial owner construction", "[memory utils]") {
    PartiallyConstructedValue::reset();

    auto        composite_builder = MemoryUtils::tuple().add_type<PartiallyConstructedValue>().add_type<ThrowsOnDefault>();
    const auto &composite         = composite_builder.build();

    REQUIRE_THROWS_AS(MemoryUtils::ErasedOwner<>{composite}, std::runtime_error);
    REQUIRE(PartiallyConstructedValue::destroyed == 1);
}

TEST_CASE("memory utils array plans clean up partial owner construction", "[memory utils]") {
    ThrowsOnThirdDefault::reset();

    const auto &array = MemoryUtils::array_plan<ThrowsOnThirdDefault>(4);

    REQUIRE_THROWS_AS(MemoryUtils::ErasedOwner<>{array}, std::runtime_error);
    REQUIRE(ThrowsOnThirdDefault::constructed == 2);
    REQUIRE(ThrowsOnThirdDefault::destroyed == 2);
}
