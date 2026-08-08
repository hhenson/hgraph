#include <catch2/catch_test_macros.hpp>

#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <array>
#include <atomic>
#include <barrier>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace
{
    struct MoveAssignableOnlyScalar
    {
        std::int32_t value{0};

        MoveAssignableOnlyScalar() = default;
        explicit MoveAssignableOnlyScalar(std::int32_t v) : value(v) {}
        MoveAssignableOnlyScalar(const MoveAssignableOnlyScalar &) = default;
        MoveAssignableOnlyScalar(MoveAssignableOnlyScalar &&) noexcept = default;
        MoveAssignableOnlyScalar &operator=(const MoveAssignableOnlyScalar &) = delete;
        MoveAssignableOnlyScalar &operator=(MoveAssignableOnlyScalar &&other) noexcept
        {
            value = other.value;
            return *this;
        }

        [[nodiscard]] bool operator==(const MoveAssignableOnlyScalar &) const = default;
        [[nodiscard]] auto operator<=>(const MoveAssignableOnlyScalar &) const = default;
    };

    struct ThrowsOnDynamicChildDefault
    {
        ThrowsOnDynamicChildDefault() { throw std::runtime_error("dynamic child construction failed"); }
        [[nodiscard]] bool operator==(const ThrowsOnDynamicChildDefault &) const = default;
        [[nodiscard]] auto operator<=>(const ThrowsOnDynamicChildDefault &) const = default;
    };

    void mutate_supported_ts_child(hgraph::TSDataView child, hgraph::DateTime modified_time, std::int32_t seed)
    {
        using namespace hgraph;

        const auto *schema = child.schema();
        REQUIRE(schema != nullptr);

        switch (schema->kind)
        {
        case TSTypeKind::TS:
        {
            Value value{seed};
            auto  mutation = child.begin_mutation(modified_time);
            REQUIRE(mutation.copy_value_from(value.view()));
            break;
        }
        case TSTypeKind::SIGNAL:
        {
            Value value{true};
            auto  mutation = child.begin_mutation(modified_time);
            REQUIRE(mutation.copy_value_from(value.view()));
            break;
        }
        case TSTypeKind::TSS:
        {
            Value value{seed};
            auto  set = child.as_set();
            auto  mutation = set.begin_mutation(modified_time);
            REQUIRE(mutation.add(value.view()));
            break;
        }
        case TSTypeKind::TSD:
        {
            Value key{seed};
            auto  dict = child.as_dict();
            auto  mutation = dict.begin_mutation(modified_time);
            auto  nested = mutation.at(key.view());
            mutate_supported_ts_child(std::move(nested), modified_time, seed + 1);
            break;
        }
        case TSTypeKind::TSL:
        {
            auto list = child.as_list();
            if (schema->fixed_size() == 0 && list.size() == 0)
            {
                const auto *element_ts = schema->element_ts();
                REQUIRE(element_ts != nullptr);
                REQUIRE(element_ts->kind == TSTypeKind::TS);
                const auto element_binding =
                    ValuePlanFactory::instance().type_for(element_ts->value_schema);
                const auto source_binding =
                    ValuePlanFactory::instance().type_for(schema->value_schema);
                REQUIRE(element_binding != nullptr);
                REQUIRE(source_binding != nullptr);
                Value       value{seed};
                ListBuilder builder{element_binding};
                builder.push_back_copy(value.view().data());
                auto  source_storage = builder.build_storage();
                Value source{source_binding, &source_storage};
                auto  mutation = child.begin_mutation(modified_time);
                REQUIRE(mutation.copy_value_from(source.view()));
                break;
            }
            REQUIRE(list.size() > 0);
            auto nested = list.at(0);
            mutate_supported_ts_child(std::move(nested), modified_time, seed + 1);
            break;
        }
        case TSTypeKind::TSB:
        {
            auto bundle = child.as_bundle();
            REQUIRE(bundle.size() > 0);
            auto nested = bundle.at(0);
            mutate_supported_ts_child(std::move(nested), modified_time, seed + 1);
            break;
        }
        case TSTypeKind::TSW:
        {
            Value value{seed};
            auto  window = child.as_window();
            auto  mutation = window.begin_mutation(modified_time);
            mutation.push(value.view());
            break;
        }
        case TSTypeKind::REF:
            FAIL("REF is intentionally excluded from this nesting matrix");
            break;
        }
    }

    void require_canonical_snapshots(const hgraph::TSDataView &root, hgraph::DateTime modified_time)
    {
        using namespace hgraph;

        REQUIRE(root.schema() != nullptr);
        Value current{root.value()};
        REQUIRE(current.binding() == ValuePlanFactory::instance().type_for(root.schema()->value_schema));
        REQUIRE(current.binding().ops_ref().kind != ValueOpsKind::Invalid);

        REQUIRE(root.modified(modified_time));
        Value delta{root.delta_value(modified_time)};
        REQUIRE(delta.binding() == ValuePlanFactory::instance().type_for(root.schema()->delta_value_schema));
        REQUIRE(delta.binding().ops_ref().kind != ValueOpsKind::Invalid);
    }

    void require_no_tsdata_relocation_hooks(const hgraph::MemoryUtils::StoragePlan &plan)
    {
        CHECK_FALSE(plan.can_copy_construct());
        CHECK_FALSE(plan.can_move_construct());
        CHECK_FALSE(plan.can_copy_assign());
        CHECK_FALSE(plan.can_move_assign());
    }
}  // namespace

TEST_CASE("ValuePlanFactory: atomic round-trip via TypeRegistry")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *plan     = factory.plan_for(int_meta);

    REQUIRE(plan != nullptr);
    REQUIRE(plan == &MemoryUtils::plan_for<std::int32_t>());
    REQUIRE(plan->layout.size == sizeof(std::int32_t));
    REQUIRE(plan->layout.alignment == alignof(std::int32_t));
}

TEST_CASE("ValuePlanFactory returns one canonical record under concurrent structured lookup")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    auto &factory = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta = registry.register_scalar<std::string>("string");
    const auto *tuple_meta = registry.tuple({int_meta, str_meta});

    constexpr std::size_t thread_count = 8;
    std::array<std::thread, thread_count> threads;
    std::array<ValueTypeRef, thread_count> results{};
    std::barrier start{static_cast<std::ptrdiff_t>(thread_count)};
    std::atomic_bool stable{true};

    for (std::size_t index = 0; index < thread_count; ++index)
    {
        threads[index] = std::thread([&, index] {
            start.arrive_and_wait();
            for (std::size_t attempt = 0; attempt < 100; ++attempt)
            {
                const ValueTypeRef current = factory.type_for(tuple_meta);
                if (!current)
                {
                    stable.store(false, std::memory_order_relaxed);
                    continue;
                }
                if (!results[index]) { results[index] = current; }
                else if (results[index] != current) { stable.store(false, std::memory_order_relaxed); }
            }
        });
    }
    for (auto &thread : threads) { thread.join(); }

    REQUIRE(stable.load(std::memory_order_relaxed));
    REQUIRE(results.front());
    for (const ValueTypeRef result : results) { REQUIRE(result == results.front()); }
    REQUIRE(factory.find_type(tuple_meta) == results.front());
}

TEST_CASE("TSData plan classifiers reject malformed compact value kinds")
{
    using namespace hgraph;

    ValueTypeMetaData malformed{ValueTypeKind::Atomic, ValueTypeFlags::None, "malformed"};
    malformed.header.kind = static_cast<TypeKind>(ValueTypeKind::Any) + 1;

    TSValueTypeMetaData scalar{TSTypeKind::TS, &malformed};
    scalar.value_schema       = &malformed;
    scalar.delta_value_schema = &malformed;
    REQUIRE_FALSE(ts_data_plan_factory_detail::is_compact_atomic_ts_data(scalar));

    TSValueTypeMetaData set{TSTypeKind::TSS, &malformed};
    set.value_schema       = &malformed;
    set.delta_value_schema = &malformed;
    REQUIRE_FALSE(ts_data_plan_factory_detail::is_slot_ts_data(set));
}

TEST_CASE("ValuePlanFactory::find returns null for unregistered atomic schemas")
{
    using namespace hgraph;
    auto                 &factory = ValuePlanFactory::instance();
    ValueTypeMetaData     orphan(ValueTypeKind::Atomic, ValueTypeFlags::None, "orphan");
    REQUIRE(factory.find(&orphan) == nullptr);
}

TEST_CASE("ValuePlanFactory::plan_for throws for unregistered atomic schemas")
{
    using namespace hgraph;
    auto             &factory = ValuePlanFactory::instance();
    ValueTypeMetaData orphan(ValueTypeKind::Atomic, ValueTypeFlags::None, "orphan");
    REQUIRE_THROWS_AS(factory.plan_for(&orphan), std::logic_error);
}

TEST_CASE("ValuePlanFactory::plan_for handles null schemas")
{
    using namespace hgraph;
    auto &factory = ValuePlanFactory::instance();
    REQUIRE(factory.plan_for(nullptr) == nullptr);
    REQUIRE(factory.find(nullptr) == nullptr);
}

TEST_CASE("ValuePlanFactory: tuple synthesis matches MemoryUtils::tuple_plan")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    auto       &factory    = ValuePlanFactory::instance();
    const auto *int_meta   = registry.register_scalar<std::int32_t>("int32");
    const auto *float_meta = registry.register_scalar<float>("float32");
    const auto *tuple_meta = registry.tuple({int_meta, float_meta});
    const auto *plan       = factory.plan_for(tuple_meta);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->is_tuple());
    // Public fields + a hidden trailing validity-words component (field
    // validity, core_concepts.rst): fixed tuples now track per-slot
    // set-ness so a partial tuple (relaxed combine) reads unset slots as
    // None. The validity component is unnamed and appended last.
    REQUIRE(plan->component_count() == 3);
    REQUIRE(plan->component(0).plan == &MemoryUtils::plan_for<std::int32_t>());
    REQUIRE(plan->component(1).plan == &MemoryUtils::plan_for<float>());
    REQUIRE(plan->component(2).name == nullptr);
    REQUIRE(plan->component(2).plan->is_array());
    REQUIRE(&plan->component(2).plan->array_element_plan() == &MemoryUtils::plan_for<std::uint64_t>());
}

TEST_CASE("ValuePlanFactory projects an Owned Bundle onto structural storage")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *integer = registry.value_type("int");
    REQUIRE(integer != nullptr);
    const auto *bundle =
        registry.bundle("tests.plan", "ProjectedBundle", {{"value", integer}});
    const auto *owned = registry.owned(bundle);
    const auto integer_binding = factory.type_for(integer);

    const std::array fields{integer_binding};
    const auto projected = factory.projected_composite_type_for(owned, fields);

    REQUIRE(projected.schema() == owned);
    REQUIRE(projected.checked_plan().is_composite());
    REQUIRE(projected.checked_plan().component_count() == 2);
    const auto *indexed = checked_value_ops<IndexedValueOps>(
        projected, "projected Owned Bundle");
    REQUIRE(indexed->size(indexed->context, nullptr) == 1);
    REQUIRE(indexed->element_binding(indexed->context, nullptr, 0) ==
            integer_binding);
}

TEST_CASE("ValuePlanFactory: bundle synthesis preserves field names and shape")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    auto       &factory     = ValuePlanFactory::instance();
    const auto *int_meta    = registry.register_scalar<std::int32_t>("int32");
    const auto *float_meta  = registry.register_scalar<float>("float32");
    const auto *bundle_meta = registry.bundle("PlanFactoryBundleA", {{"x", int_meta}, {"y", float_meta}});
    const auto *plan        = factory.plan_for(bundle_meta);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->is_named_tuple());
    // Public fields + hidden trailing validity words (field validity,
    // core_concepts.rst). The validity component is unnamed so it cannot
    // collide with user bundle fields.
    REQUIRE(plan->component_count() == 3);
    REQUIRE(plan->component(2).name == nullptr);
    REQUIRE(plan->component(2).plan->is_array());
    REQUIRE(plan->component(2).plan->array_count() == 1);
    REQUIRE(&plan->component(2).plan->array_element_plan() == &MemoryUtils::plan_for<std::uint64_t>());
    REQUIRE(plan->find_component("___validity") == nullptr);

    const auto *x_comp = plan->find_component("x");
    REQUIRE(x_comp != nullptr);
    REQUIRE(x_comp->plan == &MemoryUtils::plan_for<std::int32_t>());

    const auto *y_comp = plan->find_component("y");
    REQUIRE(y_comp != nullptr);
    REQUIRE(y_comp->plan == &MemoryUtils::plan_for<float>());
}

TEST_CASE("ValuePlanFactory: fixed list synthesis matches MemoryUtils::array_plan")
{
    using namespace hgraph;
    auto       &registry  = TypeRegistry::instance();
    auto       &factory   = ValuePlanFactory::instance();
    const auto *int_meta  = registry.register_scalar<std::int32_t>("int32");
    const auto *list_meta = registry.list(int_meta, 4);
    const auto *plan      = factory.plan_for(list_meta);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->is_array());
    REQUIRE(plan->array_count() == 4);
    REQUIRE(plan == &MemoryUtils::array_plan(MemoryUtils::plan_for<std::int32_t>(), 4));
}

TEST_CASE("ValuePlanFactory: nested composites synthesise correctly")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    auto       &factory    = ValuePlanFactory::instance();
    const auto *int_meta   = registry.register_scalar<std::int32_t>("int32");
    const auto *float_meta = registry.register_scalar<float>("float32");
    const auto *inner      = registry.tuple({int_meta, float_meta});
    const auto *outer      = registry.tuple({inner, int_meta});
    const auto *plan       = factory.plan_for(outer);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->is_tuple());
    // field_count public components + the hidden validity words.
    REQUIRE(plan->component_count() == 3);

    const auto *inner_plan = factory.plan_for(inner);
    REQUIRE(plan->component(0).plan == inner_plan);
    REQUIRE(plan->component(1).plan == &MemoryUtils::plan_for<std::int32_t>());
    REQUIRE(plan->component(2).name == nullptr);
    REQUIRE(plan->component(2).plan->is_array());
}

TEST_CASE("ValuePlanFactory: caching returns the same pointer on repeat lookups")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    auto       &factory    = ValuePlanFactory::instance();
    const auto *int_meta   = registry.register_scalar<std::int32_t>("int32");
    const auto *float_meta = registry.register_scalar<float>("float32");
    const auto *tuple_meta = registry.tuple({int_meta, float_meta});

    const auto *first    = factory.plan_for(tuple_meta);
    const auto *second   = factory.plan_for(tuple_meta);
    const auto *via_find = factory.find(tuple_meta);

    REQUIRE(first != nullptr);
    REQUIRE(first == second);
    REQUIRE(first == via_find);
}

TEST_CASE("ValuePlanFactory: dynamic list uses compact value-layer storage")
{
    using namespace hgraph;
    auto       &registry  = TypeRegistry::instance();
    auto       &factory   = ValuePlanFactory::instance();
    const auto *int_meta  = registry.register_scalar<std::int32_t>("int32");
    const auto *list_meta = registry.list(int_meta, 0);
    const auto int_binding = registry.scalar_type<std::int32_t>();

    const auto *plan = factory.plan_for(list_meta);
    const auto binding = factory.type_for(list_meta);

    REQUIRE(int_binding != nullptr);
    REQUIRE(plan == &compact_list_plan(int_binding));
    REQUIRE(binding != nullptr);
    REQUIRE(binding.schema() == list_meta);
    REQUIRE(binding.plan() == plan);
    REQUIRE(binding.ops() == &compact_list_ops());
}

TEST_CASE("ValuePlanFactory: container kinds use compact value-layer storage")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto int_binding = registry.scalar_type<std::int32_t>();
    REQUIRE(int_binding != nullptr);

    const auto *set_meta = registry.set(int_meta);
    const auto *set_plan = factory.plan_for(set_meta);
    const auto set_binding = factory.type_for(set_meta);
    REQUIRE(set_plan == &compact_set_plan(int_binding));
    REQUIRE(set_binding != nullptr);
    REQUIRE(set_binding.schema() == set_meta);
    REQUIRE(set_binding.ops() == &compact_set_ops());

    const auto *map_meta = registry.map(int_meta, int_meta);
    const auto *map_plan = factory.plan_for(map_meta);
    const auto map_binding = factory.type_for(map_meta);
    REQUIRE(map_plan == &compact_map_plan(int_binding, int_binding));
    REQUIRE(map_binding != nullptr);
    REQUIRE(map_binding.schema() == map_meta);
    REQUIRE(map_binding.ops() == &compact_map_ops());

    const auto *cyclic_meta = registry.cyclic_buffer(int_meta, 4);
    const auto *cyclic_plan = factory.plan_for(cyclic_meta);
    const auto cyclic_binding = factory.type_for(cyclic_meta);
    REQUIRE(cyclic_plan == &compact_cyclic_buffer_plan(int_binding, 4));
    REQUIRE(cyclic_binding != nullptr);
    REQUIRE(cyclic_binding.schema() == cyclic_meta);
    REQUIRE(cyclic_binding.ops() == &compact_cyclic_buffer_ops());

    const auto *queue_meta = registry.queue(int_meta, 4);
    const auto *queue_plan = factory.plan_for(queue_meta);
    const auto queue_binding = factory.type_for(queue_meta);
    REQUIRE(queue_plan == &compact_queue_plan(int_binding, 4));
    REQUIRE(queue_binding != nullptr);
    REQUIRE(queue_binding.schema() == queue_meta);
    REQUIRE(queue_binding.ops() == &compact_queue_ops());
}

TEST_CASE("ValuePlanFactory: binding_for synthesises structured composite bindings")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    auto       &factory    = ValuePlanFactory::instance();
    const auto *int_meta   = registry.register_scalar<std::int32_t>("int32");
    const auto *float_meta = registry.register_scalar<float>("float32");

    const auto *tuple_meta = registry.tuple({int_meta, float_meta});
    const auto tuple_binding = factory.type_for(tuple_meta);
    REQUIRE(tuple_binding != nullptr);
    REQUIRE(tuple_binding.schema() == tuple_meta);
    REQUIRE(tuple_binding.plan() == factory.plan_for(tuple_meta));

    const auto *bundle_meta = registry.bundle("PlanFactoryBindingBundle", {{"x", int_meta}, {"y", float_meta}});
    const auto bundle_binding = factory.type_for(bundle_meta);
    REQUIRE(bundle_binding != nullptr);
    REQUIRE(bundle_binding.schema() == bundle_meta);
    REQUIRE(bundle_binding.plan() == factory.plan_for(bundle_meta));

    const auto *fixed_list_meta = registry.list(int_meta, 3);
    const auto fixed_list_binding = factory.type_for(fixed_list_meta);
    REQUIRE(fixed_list_binding != nullptr);
    REQUIRE(fixed_list_binding.schema() == fixed_list_meta);
    REQUIRE(fixed_list_binding.plan() == factory.plan_for(fixed_list_meta));
    REQUIRE(fixed_list_binding.plan()->is_array());
}

TEST_CASE("ValuePlanFactory::register_atomic is idempotent for the same plan")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    REQUIRE_NOTHROW(factory.register_atomic(int_meta, &MemoryUtils::plan_for<std::int32_t>(), &ops_for<std::int32_t>()));
    REQUIRE(factory.find(int_meta) == &MemoryUtils::plan_for<std::int32_t>());
}

TEST_CASE("ValuePlanFactory::register_atomic rejects conflicting plans")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    REQUIRE_THROWS_AS(
        factory.register_atomic(int_meta, &MemoryUtils::plan_for<float>(), &ops_for<std::int32_t>()),
        std::logic_error);
}

TEST_CASE("ValuePlanFactory::register_atomic ignores null inputs")
{
    using namespace hgraph;
    auto &factory = ValuePlanFactory::instance();
    REQUIRE_NOTHROW(factory.register_atomic(nullptr, &MemoryUtils::plan_for<std::int32_t>(), &ops_for<std::int32_t>()));
    ValueTypeMetaData orphan(ValueTypeKind::Atomic, ValueTypeFlags::None, "orphan");
    REQUIRE_NOTHROW(factory.register_atomic(&orphan, nullptr, &ops_for<std::int32_t>()));
}

TEST_CASE("TSDataPlanFactory: atomic TSData uses value storage and last-modified tracking")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    const auto *plan    = factory.plan_for(ts_int);
    const auto type = factory.data_type_for(ts_int);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->is_named_tuple());
    REQUIRE(plan->component_count() == 2);
    REQUIRE(plan->find_component("value") != nullptr);
    REQUIRE(plan->find_component("delta") == nullptr);
    REQUIRE(plan->find_component("tracking") != nullptr);
    REQUIRE(plan->component("value").plan == &MemoryUtils::plan_for<std::int32_t>());
    REQUIRE(plan->component("tracking").plan == &MemoryUtils::plan_for<TSDataTracking>());
    REQUIRE(plan->component("tracking").offset != plan->component("value").offset);

    REQUIRE(type);
    REQUIRE(type.schema() == ts_int);
    REQUIRE(type.plan() == plan);
    REQUIRE(type.ops_ref().allows_mutation);
    TSData data{type};
    REQUIRE(data.type_ref().record() == type.record());
    REQUIRE(data.view().layout().value_binding == registry.scalar_type<std::int32_t>());
    REQUIRE(data.view().layout().delta_binding == registry.scalar_type<std::int32_t>());
    REQUIRE_FALSE(data.view().parent_link().has_parent());
}

TEST_CASE("TSDataView: mutation capability is supplied by TSDataOps")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto data_type = factory.data_type_for(ts_int);
    const auto input_type = checked_ts_role_type(
        intern_ts_type(*ts_int, TypeRole::Input, data_type.checked_plan(), data_type.ops_ref()),
        std::integral_constant<TypeRole, TypeRole::Input>{});
    TSData immutable_data{input_type};
    auto   immutable_view = immutable_data.view();
    REQUIRE_THROWS_AS(immutable_view.mutable_data(), std::logic_error);
    REQUIRE_THROWS_AS(immutable_view.begin_mutation(MIN_ST), std::logic_error);
}

TEST_CASE("TSDataPlanFactory: compact atomic TSData tracks deltas by modified time")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto type = factory.data_type_for(ts_int);
    REQUIRE(type);

    TSData data{type};
    auto   view = data.view();
    REQUIRE(view.value().checked_as<std::int32_t>() == 0);
    REQUIRE(view.last_modified_time() == MIN_DT);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    REQUIRE_FALSE(view.modified(t1));
    REQUIRE_FALSE(view.delta_value(t1).has_value());

    Value source{42};
    {
        auto mutation = view.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(source.view()));
    }
    REQUIRE(view.value().checked_as<std::int32_t>() == 42);
    REQUIRE(view.delta_value(t1).checked_as<std::int32_t>() == 42);
    REQUIRE(view.delta_value(t1).data() == view.value().data());
    REQUIRE(view.last_modified_time() == t1);
    REQUIRE(view.modified(t1));
    REQUIRE_FALSE(view.modified(t2));
    REQUIRE_FALSE(view.delta_value(t2).has_value());

    Value same_tick_overwrite{99};
    {
        auto mutation = view.begin_mutation(t1);
        REQUIRE_FALSE(mutation.copy_value_from(same_tick_overwrite.view()));
    }
    REQUIRE(view.value().checked_as<std::int32_t>() == 99);
    REQUIRE(view.delta_value(t1).checked_as<std::int32_t>() == 99);
    REQUIRE(view.last_modified_time() == t1);

    {
        auto mutation = view.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(source.view()));
        REQUIRE(view.value().checked_as<std::int32_t>() == 42);
        REQUIRE(view.delta_value(t2).checked_as<std::int32_t>() == 42);
        REQUIRE(view.last_modified_time() == t2);
    }
}

TEST_CASE("TSDataView: child modifications propagate through parent view")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsl      = registry.tsl(ts_int, 2);
    const auto type = factory.data_type_for(tsl);
    REQUIRE(type);

    TSData data{type};
    auto   parent = data.view();
    auto   list   = parent.as_list();
    auto   child  = list.at(0);
    auto   sibling = list.at(1);
    REQUIRE_FALSE(parent.has_parent());
    REQUIRE(child.has_parent());
    REQUIRE(child.child_id() == 0);
    REQUIRE(child.parent_link().parent_storage_type() == parent.storage_type());
    REQUIRE(child.parent_link().parent_data() == parent.data());
    REQUIRE(child.parent_link().child_id == 0);
    REQUIRE(sibling.has_parent());
    REQUIRE(sibling.child_id() == 1);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    Value      first{1};
    Value      second{2};
    Value      third{3};

    {
        auto outer = child.begin_mutation(t1);
        REQUIRE(outer.copy_value_from(first.view()));
        REQUIRE(parent.last_modified_time() == t1);
        REQUIRE(parent.modified(t1));

        {
            auto nested = child.begin_mutation(t1);
            REQUIRE_FALSE(nested.copy_value_from(second.view()));
        }

        REQUIRE(parent.last_modified_time() == t1);
        REQUIRE(child.value().checked_as<std::int32_t>() == 2);
        REQUIRE(child.delta_value(t1).checked_as<std::int32_t>() == 2);
    }

    REQUIRE(parent.last_modified_time() == t1);

    {
        auto mutation = sibling.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(first.view()));
    }
    REQUIRE(parent.last_modified_time() == t1);
    auto        t1_delta = parent.delta_value(t1).as_map();
    Value       key_zero{std::int64_t{0}};
    Value       key_one{std::int64_t{1}};
    const auto  key_zero_view = key_zero.view();
    const auto  key_one_view  = key_one.view();
    REQUIRE(t1_delta.contains(key_zero_view));
    REQUIRE(t1_delta.contains(key_one_view));

    {
        auto same_tick = child.begin_mutation(t1);
        REQUIRE_FALSE(same_tick.copy_value_from(third.view()));
    }

    REQUIRE(parent.last_modified_time() == t1);
    REQUIRE(child.value().checked_as<std::int32_t>() == 3);
    REQUIRE(child.delta_value(t1).checked_as<std::int32_t>() == 3);

    {
        auto next_tick = child.begin_mutation(t2);
        REQUIRE(next_tick.copy_value_from(first.view()));
        REQUIRE(parent.last_modified_time() == t2);
    }

    REQUIRE(parent.last_modified_time() == t2);
    REQUIRE(parent.modified(t2));
    auto t2_delta = parent.delta_value(t2).as_map();
    REQUIRE(t2_delta.contains(key_zero_view));
    REQUIRE_FALSE(t2_delta.contains(key_one_view));

    {
        auto mutation = child.begin_mutation(t3);
        mutation.mark_modified();
    }
    REQUIRE(child.last_modified_time() == t3);
    REQUIRE(parent.last_modified_time() == t3);
    REQUIRE(parent.delta_value(t3).as_map().contains(key_zero_view));
}

TEST_CASE("TSDataView: child parent link is stored in TSData tracking")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);
    const auto type = factory.data_type_for(tsd);

    TSData data{type};
    Value  key{7};
    Value  initial{1};
    Value  updated{12};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    {
        auto view = data.view();
        auto dict = view.as_dict();
        auto mutation = dict.begin_mutation(t1);
        mutation.set(key.view(), initial.view());
    }

    TSDataView child;
    {
        auto view = data.view();
        auto dict = view.as_dict();
        child = dict.at(key.view());
        REQUIRE(child.has_parent());
        REQUIRE(child.parent_link().parent_storage_type() == dict.base().storage_type());
        REQUIRE(child.parent_link().parent_data() == dict.base().data());
    }

    REQUIRE(child.has_parent());
    REQUIRE(child.path_from_root() == std::vector<std::size_t>{child.child_id()});
    auto root = child.root_view();
    REQUIRE(root.storage_type() == data.type_ref());
    REQUIRE(root.data() == data.view().data());
    {
        auto mutation = child.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(updated.view()));
    }

    auto view = data.view();
    auto dict = view.as_dict();
    REQUIRE(dict.modified(t2));
    REQUIRE(dict.modified_keys(t2).begin() != dict.modified_keys(t2).end());
    REQUIRE(dict.at(key.view()).value().checked_as<std::int32_t>() == 12);
}

TEST_CASE("TSDataPlanFactory: REF and SIGNAL use compact atomic TSData")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    REQUIRE(factory.data_type_for(registry.signal()));
    REQUIRE(factory.data_type_for(registry.ref(ts_int)));
}

TEST_CASE("TSDataPlanFactory: fixed TSB groups current values before child tracking")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsb      = registry.tsb("PlanFactoryTSB", {{"a", ts_int}, {"b", ts_int}});

    const auto type = factory.data_type_for(tsb);
    REQUIRE(type);
    REQUIRE(type.plan()->is_named_tuple());
    const auto *value_component = type.plan()->find_component("value");
    const auto *aux_component   = type.plan()->find_component("aux");
    REQUIRE(value_component != nullptr);
    REQUIRE(aux_component != nullptr);
    REQUIRE(value_component->offset == 0);
    REQUIRE(value_component->plan == ValuePlanFactory::instance().plan_for(tsb->value_schema));
    REQUIRE(aux_component->plan->find_component("field_0") != nullptr);
    REQUIRE(aux_component->plan->find_component("field_1") != nullptr);
    REQUIRE(aux_component->plan->find_component("tracking") != nullptr);

    TSData data{type};
    auto   view = data.view();
    auto   tsb_view = view.as_bundle();
    REQUIRE(tsb_view.size() == 2);
    REQUIRE_FALSE(tsb_view.empty());
    REQUIRE(tsb_view.has_field("a"));
    REQUIRE_FALSE(tsb_view.has_field("missing"));
    auto first_child = tsb_view.at(0);
    REQUIRE(tsb_view.field("a").storage_type() == first_child.storage_type());
    REQUIRE(tsb_view["a"].storage_type() == first_child.storage_type());
    REQUIRE(first_child.schema() == ts_int);
    REQUIRE(first_child.storage_type().plan() == type.plan());
    const auto &child_ops = first_child.ops();
    REQUIRE(&first_child.layout() == child_ops.layout_impl(child_ops.context));
    REQUIRE(view.value().is_bundle());
    REQUIRE(view.value().binding() == ValuePlanFactory::instance().type_for(tsb->value_schema));
    REQUIRE(view.value().binding().ops_ref().kind == ValueOpsKind::Indexed);
    REQUIRE(tsb_view.valid_items().begin() == tsb_view.valid_items().end());

    auto current = view.value().as_bundle();
    REQUIRE_FALSE(current.at("a").has_value());
    REQUIRE_FALSE(current.at("b").has_value());

    std::size_t keyed_items = 0;
    for (const auto [name, element] : tsb_view.items())
    {
        REQUIRE(element.storage_type());
        if (keyed_items == 0) { REQUIRE(std::string{name} == "a"); }
        if (keyed_items == 1) { REQUIRE(std::string{name} == "b"); }
        ++keyed_items;
    }
    REQUIRE(keyed_items == 2);

    const auto t1 = MIN_ST;
    Value      seven{7};
    {
        auto mutation = first_child.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(seven.view()));
    }

    REQUIRE(view.modified(t1));
    REQUIRE(view.last_modified_time() == t1);
    REQUIRE(view.value().as_bundle().at("a").checked_as<std::int32_t>() == 7);
    {
        auto valid_items = tsb_view.valid_items();
        auto it = valid_items.begin();
        REQUIRE(it != valid_items.end());
        const auto [name, element] = *it;
        REQUIRE(std::string{name} == "a");
        REQUIRE(element.value().checked_as<std::int32_t>() == 7);
        ++it;
        REQUIRE(it == valid_items.end());
    }
    {
        auto modified_items = tsb_view.modified_items(t1);
        auto it = modified_items.begin();
        REQUIRE(it != modified_items.end());
        const auto [name, element] = *it;
        REQUIRE(std::string{name} == "a");
        REQUIRE(element.delta_value(t1).checked_as<std::int32_t>() == 7);
        ++it;
        REQUIRE(it == modified_items.end());
    }

    auto delta = view.delta_value(t1).as_bundle();
    REQUIRE(delta.at("a").checked_as<std::int32_t>() == 7);
    REQUIRE_FALSE(delta.at("b").has_value());

    Value materialized_value{view.value()};
    auto  materialized_current = materialized_value.as_bundle();
    REQUIRE(materialized_current.at("a").checked_as<std::int32_t>() == 7);
    REQUIRE_FALSE(materialized_current.at("b").has_value());

    Value materialized_delta{view.delta_value(t1)};
    auto  materialized_delta_bundle = materialized_delta.as_bundle();
    REQUIRE(materialized_delta_bundle.at("a").checked_as<std::int32_t>() == 7);
    REQUIRE_FALSE(materialized_delta_bundle.at("b").has_value());
}

TEST_CASE("TSDataPlanFactory: fixed TSL stores current values as a fixed value-layer list")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsl      = registry.tsl(ts_int, 3);

    const auto type = factory.data_type_for(tsl);
    REQUIRE(type);
    REQUIRE(type.plan()->is_named_tuple());
    const auto *value_component = type.plan()->find_component("value");
    const auto *aux_component   = type.plan()->find_component("aux");
    REQUIRE(value_component != nullptr);
    REQUIRE(aux_component != nullptr);
    REQUIRE(value_component->offset == 0);
    REQUIRE(value_component->plan == ValuePlanFactory::instance().plan_for(tsl->value_schema));
    REQUIRE(value_component->plan->is_array());
    REQUIRE(value_component->plan->array_count() == 3);
    REQUIRE(value_component->plan->array_stride() == sizeof(std::int32_t));
    const auto *elements_component = aux_component->plan->find_component("elements");
    REQUIRE(elements_component != nullptr);
    REQUIRE(elements_component->plan != nullptr);
    REQUIRE(elements_component->plan->is_array());
    REQUIRE(elements_component->plan->array_count() == 3);
    REQUIRE(elements_component->plan->array_element_plan().find_component("tracking") != nullptr);
    REQUIRE(aux_component->plan->find_component("tracking") != nullptr);

    TSData data{type};
    auto   view = data.view();
    auto   tsl_view = view.as_list();
    REQUIRE(tsl_view.size() == 3);
    REQUIRE_FALSE(tsl_view.empty());
    const auto &tsl_layout = static_cast<const FixedTSLDataLayout &>(view.layout());
    REQUIRE(tsl_layout.size() == 3);
    REQUIRE(tsl_layout.element_value_stride == value_component->plan->array_stride());
    REQUIRE(tsl_layout.element_value_offset(2) ==
            value_component->offset + 2 * value_component->plan->array_stride());
    REQUIRE(tsl_layout.element_auxiliary_offset == aux_component->offset + elements_component->offset);
    REQUIRE(tsl_layout.element_auxiliary_stride == elements_component->plan->array_stride());
    REQUIRE(tsl_layout.element_auxiliary_offset_at(2) ==
            aux_component->offset + elements_component->offset + 2 * elements_component->plan->array_stride());
    REQUIRE(view.value().is_list());
    REQUIRE(view.value().as_list().size() == 3);
    REQUIRE(view.value().as_list().at(2).checked_as<std::int32_t>() == 0);
    REQUIRE(tsl_view.valid_values().begin() == tsl_view.valid_values().end());
    REQUIRE(tsl_view.valid_items().begin() == tsl_view.valid_items().end());

    const auto list = view.value().as_list();
    const auto *first = static_cast<const std::byte *>(list.at(0).data());
    const auto *second = static_cast<const std::byte *>(list.at(1).data());
    REQUIRE(static_cast<std::size_t>(second - first) == value_component->plan->array_stride());

    const auto t1 = MIN_ST;
    Value      eleven{11};
    {
        auto child = tsl_view.at(2);
        REQUIRE(child.value().data() == list.at(2).data());
        auto mutation = child.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(eleven.view()));
    }

    REQUIRE(view.modified(t1));
    REQUIRE(view.value().as_list().at(2).checked_as<std::int32_t>() == 11);
    {
        auto valid_values = tsl_view.valid_values();
        auto it = valid_values.begin();
        REQUIRE(it != valid_values.end());
        REQUIRE((*it).value().checked_as<std::int32_t>() == 11);
        ++it;
        REQUIRE(it == valid_values.end());
    }
    {
        auto modified_items = tsl_view.modified_items(t1);
        auto it = modified_items.begin();
        REQUIRE(it != modified_items.end());
        const auto [index, element] = *it;
        REQUIRE(index == 2);
        REQUIRE(element.delta_value(t1).checked_as<std::int32_t>() == 11);
        ++it;
        REQUIRE(it == modified_items.end());
    }

    auto        delta = view.delta_value(t1).as_map();
    Value       key{std::int64_t{2}};
    Value       miss{std::int64_t{1}};
    const auto  key_view  = key.view();
    const auto  miss_view = miss.view();
    REQUIRE(delta.size() == 1);
    REQUIRE(delta.contains(key_view));
    REQUIRE_FALSE(delta.contains(miss_view));
    REQUIRE(delta.at(key_view).checked_as<std::int32_t>() == 11);
    REQUIRE(delta.key_set().contains(key_view));
}

TEST_CASE("TSDataPlanFactory: fixed TSL owns embedded TSS child storage")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tss_int  = registry.tss(int_meta);
    const auto *tsl      = registry.tsl(tss_int, 2);

    const auto type = factory.data_type_for(tsl);
    REQUIRE(type);
    require_no_tsdata_relocation_hooks(type.checked_plan());
    const auto *aux_component = type.plan()->find_component("aux");
    REQUIRE(aux_component != nullptr);
    const auto *elements_component = aux_component->plan->find_component("elements");
    REQUIRE(elements_component != nullptr);
    REQUIRE(elements_component->plan != nullptr);
    REQUIRE(elements_component->plan->is_array());
    REQUIRE(&elements_component->plan->array_element_plan() == factory.plan_for(tss_int));

    TSData data{type};
    auto   view = data.view();
    auto   list = view.as_list();
    REQUIRE(list.size() == 2);

    const auto t1 = MIN_ST;
    Value      one{1};
    Value      two{2};
    {
        auto child_view = list.at(1);
        auto child    = child_view.as_set();
        auto mutation = child.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
        REQUIRE(mutation.add(two.view()));
    }

    REQUIRE(view.modified(t1));
    auto child0_view = list.at(0);
    auto child1_view = list.at(1);
    REQUIRE_FALSE(child0_view.as_set().contains(one.view()));
    REQUIRE(child1_view.as_set().contains(one.view()));
    REQUIRE(child1_view.as_set().contains(two.view()));

    const auto value_list = view.value().as_list();
    REQUIRE(value_list.at(0).as_set().empty());
    REQUIRE(value_list.at(1).as_set().contains(one.view()));
    REQUIRE(value_list.at(1).as_set().contains(two.view()));

    Value child_snapshot{list.at(1).value()};
    REQUIRE(child_snapshot.binding() == ValuePlanFactory::instance().type_for(tss_int->value_schema));
    REQUIRE(child_snapshot.view().as_set().contains(one.view()));
    REQUIRE(child_snapshot.view().as_set().contains(two.view()));

    Value parent_snapshot{view.value()};
    REQUIRE(parent_snapshot.binding() == ValuePlanFactory::instance().type_for(tsl->value_schema));
    REQUIRE(parent_snapshot.view().as_list().at(0).as_set().empty());
    REQUIRE(parent_snapshot.view().as_list().at(1).as_set().contains(one.view()));
    REQUIRE(parent_snapshot.view().as_list().at(1).as_set().contains(two.view()));
}

TEST_CASE("TSDataPlanFactory: collections nest every supported non-REF TSData kind")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<bool>("bool");

    const auto *ts_int     = registry.ts(int_meta);
    const auto *signal     = registry.signal();
    const auto *tss_int    = registry.tss(int_meta);
    const auto *tsd_ts     = registry.tsd(int_meta, ts_int);
    const auto *tsl_ts     = registry.tsl(ts_int, 2);
    const auto *dynamic_tsl_ts = registry.tsl(ts_int, 0);
    const auto *tsb_ts     = registry.tsb("NestedMatrixChildBundle", {{"x", ts_int}, {"tick", signal}});
    const auto *tsw_int    = registry.tsw(int_meta, 3, 1);

    struct ChildCase
    {
        const char                      *label;
        const TSValueTypeMetaData       *schema;
    };
    const std::vector<ChildCase> children{
        {"TS", ts_int},
        {"SIGNAL", signal},
        {"TSS", tss_int},
        {"TSD", tsd_ts},
        {"TSL", tsl_ts},
        {"dynamic TSL", dynamic_tsl_ts},
        {"TSB", tsb_ts},
        {"TSW", tsw_int},
    };

    const auto t1 = MIN_ST;
    std::int32_t seed = 10;
    for (const auto &child : children)
    {
        SECTION(std::string{"fixed TSL child "} + child.label)
        {
            const auto *parent = registry.tsl(child.schema, 2);
            const auto type = factory.data_type_for(parent);
            REQUIRE(type);

            TSData data{type};
            auto   root = data.view();
            auto   list = root.as_list();
            REQUIRE(list.size() == 2);
            auto nested = list.at(0);
            REQUIRE(nested.schema() == child.schema);

            mutate_supported_ts_child(std::move(nested), t1, seed++);
            require_canonical_snapshots(root, t1);
        }

        SECTION(std::string{"TSB child "} + child.label)
        {
            const auto *parent =
                registry.tsb(std::string{"NestedMatrixParentBundle_"} + child.label, {{"child", child.schema}});
            const auto type = factory.data_type_for(parent);
            REQUIRE(type);

            TSData data{type};
            auto   root = data.view();
            auto   bundle = root.as_bundle();
            auto   nested = bundle.field("child");
            REQUIRE(nested.schema() == child.schema);

            mutate_supported_ts_child(std::move(nested), t1, seed++);
            require_canonical_snapshots(root, t1);
        }

        SECTION(std::string{"TSD child "} + child.label)
        {
            const auto *parent = registry.tsd(int_meta, child.schema);
            const auto type = factory.data_type_for(parent);

            TSData data{type};
            auto   root = data.view();
            auto   dict = root.as_dict();
            Value  key{seed++};
            {
                auto mutation = dict.begin_mutation(t1);
                auto nested = mutation.at(key.view());
                REQUIRE(nested.schema() == child.schema);
                mutate_supported_ts_child(std::move(nested), t1, seed++);
            }

            REQUIRE(dict.contains(key.view()));
            require_canonical_snapshots(root, t1);
        }
    }
}

TEST_CASE("TSDataPlanFactory: tick TSW stores a fixed cyclic current window")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tsw      = registry.tsw(int_meta, 3, 2);

    const auto type = factory.data_type_for(tsw);
    REQUIRE(type);
    REQUIRE(type.plan()->is_named_tuple());
    const auto *window_component   = type.plan()->find_component("window");
    const auto *tracking_component = type.plan()->find_component("tracking");
    REQUIRE(window_component != nullptr);
    REQUIRE(tracking_component != nullptr);
    REQUIRE(window_component->offset == 0);
    REQUIRE(tracking_component->plan == &MemoryUtils::plan_for<TSDataTracking>());

    TSData data{type};
    auto   view = data.view();
    auto   window = view.as_window();
    REQUIRE(window.size_based());
    const auto &layout = window.size_layout();
    REQUIRE(layout.period == 3);
    REQUIRE(layout.min_period == 2);
    REQUIRE(layout.element_binding == registry.scalar_type<std::int32_t>());
    REQUIRE(layout.time_binding == registry.scalar_type<DateTime>());
    REQUIRE_THROWS_AS(window.time_layout(), std::logic_error);
    REQUIRE(window.value().binding().plan() == window_component->plan);
    REQUIRE(window.value().binding().ops_ref().kind == ValueOpsKind::Indexed);
    REQUIRE(window.value().is_list());
    REQUIRE(window.value().as_list().size() == 0);
    REQUIRE(window.size() == 0);
    REQUIRE(window.empty());
    REQUIRE_FALSE(window.full());
    REQUIRE_FALSE(window.all_valid());
    REQUIRE_FALSE(window.delta_value(MIN_ST).has_value());

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    const auto t4 = t3 + TimeDelta{1};

    Value one{1};
    {
        auto mutation = window.begin_mutation(t1);
        mutation.push(one.view());
        REQUIRE(mutation.size() == 1);
        REQUIRE(mutation.back().checked_as<std::int32_t>() == 1);
    }
    REQUIRE(window.size() == 1);
    REQUIRE_FALSE(window.all_valid());
    REQUIRE(window.front().checked_as<std::int32_t>() == 1);
    REQUIRE(window.back().checked_as<std::int32_t>() == 1);
    REQUIRE(window.time_at(0) == t1);
    REQUIRE(window.time_value_at(0).checked_as<DateTime>() == t1);
    REQUIRE(window.delta_value(t1).checked_as<std::int32_t>() == 1);

    Value two{2};
    {
        auto mutation = window.begin_mutation(t2);
        mutation.push(two.view());
    }
    REQUIRE(window.size() == 2);
    REQUIRE(window.all_valid());
    REQUIRE(window.value().as_list().at(0).checked_as<std::int32_t>() == 1);
    REQUIRE(window.value().as_list().at(1).checked_as<std::int32_t>() == 2);

    Value three{3};
    {
        auto mutation = window.begin_mutation(t3);
        mutation.push(three.view());
    }
    REQUIRE(window.size() == 3);
    REQUIRE(window.full());
    REQUIRE(window.back().checked_as<std::int32_t>() == 3);

    Value four{4};
    {
        auto mutation = window.begin_mutation(t4);
        mutation.push(four.view());
    }
    REQUIRE(window.size() == 3);
    REQUIRE(window.full());
    REQUIRE(window.first_modified_time() == t2);
    REQUIRE(window.time_at(0) == t2);
    REQUIRE(window.time_at(1) == t3);
    REQUIRE(window.time_at(2) == t4);
    REQUIRE(window.time_value_at(0).binding() == layout.time_binding);
    REQUIRE(window.time_value_at(0).checked_as<DateTime>() == t2);
    REQUIRE(window.time_value_at(2).checked_as<DateTime>() == t4);
    auto times = window.value_times();
    auto time_it = times.begin();
    REQUIRE(*time_it == t2);
    ++time_it;
    REQUIRE(*time_it == t3);
    ++time_it;
    REQUIRE(*time_it == t4);
    ++time_it;
    REQUIRE(time_it == times.end());
    REQUIRE(window.at(0).checked_as<std::int32_t>() == 2);
    REQUIRE(window.at(1).checked_as<std::int32_t>() == 3);
    REQUIRE(window.at(2).checked_as<std::int32_t>() == 4);
    REQUIRE(window.value().as_list().at(0).checked_as<std::int32_t>() == 2);
    REQUIRE(window.value().as_list().at(2).checked_as<std::int32_t>() == 4);
    REQUIRE(window.delta_value(t4).checked_as<std::int32_t>() == 4);
    REQUIRE_FALSE(window.delta_value(t3).has_value());
    REQUIRE(window.has_removed_value(t4));
    REQUIRE(window.removed_value(t4).checked_as<std::int32_t>() == 1);
    REQUIRE_FALSE(window.has_removed_value(t4 + TimeDelta{1}));
    REQUIRE_THROWS_AS(window.removed_value(t4 + TimeDelta{1}), std::logic_error);

    Value duplicate{5};
    auto  duplicate_mutation = window.begin_mutation(t4);
    REQUIRE_THROWS_AS(duplicate_mutation.push(duplicate.view()), std::logic_error);

    const auto retained_capacity = window.capacity();
    const auto t5 = t4 + TimeDelta{1};
    Value five{5};
    auto  replacement = stdlib::make_list<std::int32_t>({7, 8});
    {
        auto mutation = window.begin_mutation(t5);
        mutation.clear();
        REQUIRE(mutation.empty());
        REQUIRE(mutation.modified(t5));
        REQUIRE_FALSE(mutation.delta_value(t5).has_value());
        REQUIRE_FALSE(mutation.has_removed_value(t5));
        REQUIRE_THROWS_AS(mutation.copy_value_from(replacement.view()), std::logic_error);
        REQUIRE(mutation.empty());

        // Reset and a source tick in one node evaluation are atomic: clear
        // first, then retain only the current source value.
        mutation.push(five.view());
        REQUIRE(mutation.size() == 1);
        REQUIRE(mutation.back().checked_as<std::int32_t>() == 5);
        REQUIRE_THROWS_AS(mutation.copy_value_from(replacement.view()), std::logic_error);
        REQUIRE(mutation.size() == 1);
        REQUIRE(mutation.back().checked_as<std::int32_t>() == 5);
        REQUIRE_THROWS_AS(mutation.push(five.view()), std::logic_error);
    }
    REQUIRE(window.capacity() == retained_capacity);
    REQUIRE(window.size() == 1);
    REQUIRE(window.time_at(0) == t5);

    const auto t6 = t5 + TimeDelta{1};
    {
        auto mutation = window.begin_mutation(t6);
        mutation.clear();
        REQUIRE(mutation.empty());
        REQUIRE_THROWS_AS(mutation.clear(), std::logic_error);
    }
    {
        auto replacement_mutation = window.begin_mutation(t6);
        REQUIRE_THROWS_AS(replacement_mutation.copy_value_from(replacement.view()), std::logic_error);
    }
    REQUIRE(window.capacity() == retained_capacity);
    REQUIRE(window.empty());
    REQUIRE(window.modified(t6));
    REQUIRE_FALSE(window.delta_value(t6).has_value());

    const auto t7 = t6 + TimeDelta{1};
    {
        auto mutation = window.begin_mutation(t7);
        mutation.clear();
        REQUIRE(mutation.empty());
    }
    REQUIRE(window.modified(t7));
    REQUIRE(window.capacity() == retained_capacity);

    const auto *move_assign_meta = registry.register_scalar<MoveAssignableOnlyScalar>("move_assign_only");
    const auto *move_assign_tsw  = registry.tsw(move_assign_meta, 1, 1);
    const auto move_type = factory.data_type_for(move_assign_tsw);
    REQUIRE(move_type);

    TSData move_data{move_type};
    auto   move_view   = move_data.view();
    auto   move_window = move_view.as_window();
    Value  first_custom{MoveAssignableOnlyScalar{1}};
    Value  second_custom{MoveAssignableOnlyScalar{2}};
    {
        auto mutation = move_window.begin_mutation(t1);
        mutation.push(first_custom.view());
    }
    {
        auto mutation = move_window.begin_mutation(t2);
        REQUIRE_THROWS_AS(mutation.push(second_custom.view()), std::logic_error);
    }
    REQUIRE(move_window.size() == 1);
    REQUIRE(move_window.time_at(0) == t1);
    REQUIRE(move_window.back().checked_as<MoveAssignableOnlyScalar>().value == 1);
}

TEST_CASE("TSDataPlanFactory: duration TSW stores a timestamped queue current window")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tsw      = registry.tsw_duration(int_meta, TimeDelta{10}, TimeDelta{5});

    const auto type = factory.data_type_for(tsw);
    REQUIRE(type);
    const auto *window_component = type.plan()->find_component("window");
    REQUIRE(window_component != nullptr);

    TSData data{type};
    auto   view = data.view();
    auto   window = view.as_window();
    REQUIRE(window.time_based());
    const auto &layout = window.time_layout();
    REQUIRE(layout.time_range == TimeDelta{10});
    REQUIRE(layout.min_time_range == TimeDelta{5});
    REQUIRE(layout.element_binding == registry.scalar_type<std::int32_t>());
    REQUIRE(layout.time_binding == registry.scalar_type<DateTime>());
    REQUIRE_THROWS_AS(window.size_layout(), std::logic_error);
    REQUIRE(window.value().binding().plan() == window_component->plan);
    REQUIRE(window.value().binding().ops_ref().kind == ValueOpsKind::Indexed);
    REQUIRE(window.value().is_list());
    REQUIRE(window.size() == 0);
    REQUIRE_FALSE(window.all_valid());
    REQUIRE(window.capacity() == 0);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{5};
    const auto t3 = t1 + TimeDelta{15};

    Value one{1};
    {
        auto mutation = window.begin_mutation(t1);
        mutation.push(one.view());
        REQUIRE(mutation.size() == 1);
        REQUIRE(mutation.back().checked_as<std::int32_t>() == 1);
    }
    REQUIRE(window.size() == 1);
    REQUIRE_FALSE(window.all_valid());
    REQUIRE(window.time_at(0) == t1);
    REQUIRE(window.time_value_at(0).checked_as<DateTime>() == t1);
    REQUIRE(window.delta_value(t1).checked_as<std::int32_t>() == 1);

    Value two{2};
    {
        auto mutation = window.begin_mutation(t2);
        mutation.push(two.view());
    }
    REQUIRE(window.size() == 2);
    REQUIRE(window.all_valid());
    REQUIRE(window.at(0).checked_as<std::int32_t>() == 1);
    REQUIRE(window.at(1).checked_as<std::int32_t>() == 2);

    Value three{3};
    {
        auto mutation = window.begin_mutation(t3);
        mutation.push(three.view());
    }
    REQUIRE(window.size() == 2);
    REQUIRE(window.first_modified_time() == t2);
    REQUIRE(window.time_at(0) == t2);
    REQUIRE(window.time_at(1) == t3);
    REQUIRE(window.time_value_at(0).binding() == layout.time_binding);
    REQUIRE(window.time_value_at(0).checked_as<DateTime>() == t2);
    REQUIRE(window.time_value_at(1).checked_as<DateTime>() == t3);
    REQUIRE(window.at(0).checked_as<std::int32_t>() == 2);
    REQUIRE(window.at(1).checked_as<std::int32_t>() == 3);
    REQUIRE(window.value().as_list().size() == 2);
    REQUIRE(window.value().as_list().at(0).checked_as<std::int32_t>() == 2);
    REQUIRE(window.value().as_list().at(1).checked_as<std::int32_t>() == 3);
    REQUIRE(window.delta_value(t3).checked_as<std::int32_t>() == 3);
    REQUIRE_FALSE(window.delta_value(t2).has_value());
    REQUIRE(window.has_removed_value(t3));
    REQUIRE(window.removed_value(t3).checked_as<std::int32_t>() == 1);
    REQUIRE_FALSE(window.has_removed_value(t3 + TimeDelta{1}));
    REQUIRE_THROWS_AS(window.removed_value(t3 + TimeDelta{1}), std::logic_error);

    const auto retained_capacity = window.capacity();
    const auto t4 = t3 + TimeDelta{1};
    Value four{4};
    auto  replacement = stdlib::make_list<std::int32_t>({7, 8});
    {
        auto mutation = window.begin_mutation(t4);
        mutation.clear();
        REQUIRE(mutation.empty());
        REQUIRE(mutation.modified(t4));
        REQUIRE_FALSE(mutation.delta_value(t4).has_value());
        REQUIRE_FALSE(mutation.has_removed_value(t4));
        REQUIRE_THROWS_AS(mutation.copy_value_from(replacement.view()), std::logic_error);
        REQUIRE(mutation.empty());

        mutation.push(four.view());
        REQUIRE(mutation.size() == 1);
        REQUIRE(mutation.back().checked_as<std::int32_t>() == 4);
        REQUIRE_THROWS_AS(mutation.copy_value_from(replacement.view()), std::logic_error);
        REQUIRE(mutation.size() == 1);
        REQUIRE(mutation.back().checked_as<std::int32_t>() == 4);
        REQUIRE_THROWS_AS(mutation.push(four.view()), std::logic_error);
    }
    REQUIRE(window.capacity() == retained_capacity);
    REQUIRE(window.size() == 1);
    REQUIRE(window.time_at(0) == t4);

    const auto t5 = t4 + TimeDelta{1};
    {
        auto mutation = window.begin_mutation(t5);
        mutation.clear();
        REQUIRE(mutation.empty());
        REQUIRE_THROWS_AS(mutation.clear(), std::logic_error);
    }
    {
        auto replacement_mutation = window.begin_mutation(t5);
        REQUIRE_THROWS_AS(replacement_mutation.copy_value_from(replacement.view()), std::logic_error);
    }
    REQUIRE(window.modified(t5));
    REQUIRE(window.capacity() == retained_capacity);

    const auto t6 = t5 + TimeDelta{1};
    {
        auto mutation = window.begin_mutation(t6);
        mutation.clear();
        REQUIRE(mutation.empty());
    }
    REQUIRE(window.modified(t6));
    REQUIRE(window.capacity() == retained_capacity);
}

TEST_CASE("TSDataPlanFactory: TSW dynamic storage metrics distinguish live and retained buffers", "[memory]")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    const auto slot_bytes = [](const TSWDataLayout &layout) {
        const auto aligned_size = [](const MemoryUtils::StoragePlan &plan) {
            const auto alignment = plan.layout.alignment;
            return alignment <= 1
                       ? plan.layout.size
                       : ((plan.layout.size + alignment - 1) / alignment) * alignment;
        };
        return aligned_size(layout.element_binding.checked_plan()) +
               aligned_size(layout.time_binding.checked_plan());
    };

    SECTION("fixed window reserves its configured capacity and keeps it after clear")
    {
        const auto *schema = registry.tsw(int_meta, 3, 1);
        TSData      data{factory.data_type_for(schema)};
        auto        view   = data.view();
        auto        window = view.as_window();
        const auto  stride = slot_bytes(window.layout());

        CHECK(view.dynamic_storage_metrics().live_bytes == 0);
        CHECK(view.dynamic_storage_metrics().reserved_bytes == 3 * stride);

        for (std::int32_t value = 1; value <= 4; ++value)
        {
            Value item{value};
            auto mutation = window.begin_mutation(MIN_ST + TimeDelta{value});
            mutation.push(item.view());
        }
        CHECK(view.dynamic_storage_metrics().live_bytes == 3 * stride);
        CHECK(view.dynamic_storage_metrics().reserved_bytes == 3 * stride);
        CHECK(view.value().dynamic_storage_metrics().live_bytes == 3 * stride);

        window.begin_mutation(MIN_ST + TimeDelta{5}).clear();
        CHECK(view.dynamic_storage_metrics().live_bytes == 0);
        CHECK(view.dynamic_storage_metrics().reserved_bytes == 3 * stride);
    }

    SECTION("duration window reports geometric capacity retained after eviction and clear")
    {
        const auto *schema = registry.tsw_duration(int_meta, TimeDelta{100});
        TSData      data{factory.data_type_for(schema)};
        auto        view   = data.view();
        auto        window = view.as_window();
        const auto  stride = slot_bytes(window.layout());

        CHECK(view.dynamic_storage_metrics().live_bytes == 0);
        CHECK(view.dynamic_storage_metrics().reserved_bytes == 0);

        for (std::int32_t value = 1; value <= 5; ++value)
        {
            Value item{value};
            auto mutation = window.begin_mutation(MIN_ST + TimeDelta{value});
            mutation.push(item.view());
        }
        CHECK(view.dynamic_storage_metrics().live_bytes == 5 * stride);
        CHECK(view.dynamic_storage_metrics().reserved_bytes == 8 * stride);

        Value item{6};
        window.begin_mutation(MIN_ST + TimeDelta{200}).push(item.view());
        CHECK(view.dynamic_storage_metrics().live_bytes == stride);
        CHECK(view.dynamic_storage_metrics().reserved_bytes == 8 * stride);

        window.begin_mutation(MIN_ST + TimeDelta{201}).clear();
        CHECK(view.dynamic_storage_metrics().live_bytes == 0);
        CHECK(view.dynamic_storage_metrics().reserved_bytes == 8 * stride);
    }
}

TEST_CASE("TSData storage metrics recurse through atomic, structured, and keyed values", "[memory]")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *str_meta = registry.register_scalar<std::string>("string");
    const auto *ts_str = registry.ts(str_meta);
    const std::string text(256, 'x');
    Value source{text};

    SECTION("atomic")
    {
        TSData data{factory.data_type_for(ts_str)};
        auto view = data.view();
        REQUIRE(view.begin_mutation(MIN_ST).copy_value_from(source.view()));
        const auto metrics = view.dynamic_storage_metrics();
        CHECK(metrics.live_bytes == text.size() + 1);
        CHECK(metrics.reserved_bytes >= metrics.live_bytes);
    }

    SECTION("fixed TSL")
    {
        TSData data{factory.data_type_for(registry.tsl(ts_str, 2))};
        auto view = data.view();
        auto list = view.as_list();
        REQUIRE(list.at(0).begin_mutation(MIN_ST).copy_value_from(source.view()));
        const auto metrics = view.dynamic_storage_metrics();
        CHECK(metrics.live_bytes >= text.size() + 1);
        CHECK(view.value().dynamic_storage_metrics().live_bytes == metrics.live_bytes);
    }

    SECTION("dynamic TSL")
    {
        TSData data{factory.data_type_for(registry.tsl(ts_str, 0))};
        auto view = data.view();
        auto values = stdlib::make_list<std::string>({text, text});
        REQUIRE(view.begin_mutation(MIN_ST).copy_value_from(values.view()));
        const auto metrics = view.dynamic_storage_metrics();
        CHECK(metrics.live_bytes > 2 * (text.size() + 1));
        CHECK(metrics.reserved_bytes >= metrics.live_bytes);
        CHECK(view.value().dynamic_storage_metrics().live_bytes == metrics.live_bytes);
    }

    SECTION("TSS and TSD keys plus nested values")
    {
        Value key{text};
        TSData set_data{factory.data_type_for(registry.tss(str_meta))};
        auto set_view = set_data.view();
        auto set = set_view.as_set();
        REQUIRE(set.begin_mutation(MIN_ST).add(key.view()));
        const auto set_metrics = set.base().dynamic_storage_metrics();
        CHECK(set_metrics.live_bytes >= text.size() + 1);
        CHECK(set.value().dynamic_storage_metrics().live_bytes == set_metrics.live_bytes);

        TSData dict_data{factory.data_type_for(registry.tsd(str_meta, ts_str))};
        auto dict_view = dict_data.view();
        auto dict = dict_view.as_dict();
        dict.begin_mutation(MIN_ST).set(key.view(), source.view());
        const auto full = dict.base().dynamic_storage_metrics();
        const auto keys = dict.key_set().base().dynamic_storage_metrics();
        CHECK(keys.live_bytes >= text.size() + 1);
        CHECK(full.live_bytes > keys.live_bytes);
        CHECK(full.reserved_bytes >= full.live_bytes);
        CHECK(dict.value().dynamic_storage_metrics().live_bytes == full.live_bytes);
        CHECK(dict.key_set().value().dynamic_storage_metrics().live_bytes == keys.live_bytes);
    }
}

TEST_CASE("TSDataPlanFactory: fixed structured TSData recursively embeds child layouts")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsl      = registry.tsl(ts_int, 2);
    const auto *tsb      = registry.tsb("NestedPlanFactoryTSB", {{"xs", tsl}});

    const auto type = factory.data_type_for(tsb);
    REQUIRE(type);
    const auto *root_value_component = type.plan()->find_component("value");
    REQUIRE(root_value_component != nullptr);
    REQUIRE(root_value_component->offset == 0);
    REQUIRE(root_value_component->plan->is_named_tuple());
    const auto &xs_value_component = root_value_component->plan->component(0);
    REQUIRE(xs_value_component.plan->is_array());
    REQUIRE(xs_value_component.plan->array_count() == 2);
    REQUIRE(xs_value_component.plan->array_stride() == sizeof(std::int32_t));

    TSData data{type};
    auto   root = data.view();
    auto   root_tsb = root.as_bundle();
    REQUIRE(root_tsb.size() == 1);
    auto list_child = root_tsb.field("xs");
    auto list_view = list_child.as_list();
    REQUIRE(list_view.size() == 2);
    auto item_path_probe = list_view.at(1);
    REQUIRE(item_path_probe.path_from_root() == std::vector<std::size_t>{0, 1});
    auto resolved_root = item_path_probe.root_view();
    REQUIRE(resolved_root.storage_type() == root.storage_type());
    REQUIRE(resolved_root.data() == root.data());

    const auto t1 = MIN_ST;
    Value      value{23};
    {
        auto item = list_view.at(1);
        auto mutation = item.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    REQUIRE(root.modified(t1));
    REQUIRE(list_child.modified(t1));
    REQUIRE(root.value().as_bundle().at("xs").as_list().at(1).checked_as<std::int32_t>() == 23);

    auto  nested_delta = root.delta_value(t1).as_bundle().at("xs").as_map();
    Value key{std::int64_t{1}};
    REQUIRE(nested_delta.size() == 1);
    REQUIRE(nested_delta.at(key.view()).checked_as<std::int32_t>() == 23);
}

TEST_CASE("TSDataPlanFactory: TSS uses slot storage with added and removed deltas")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tss      = registry.tss(int_meta);

    const auto type = factory.data_type_for(tss);
    require_no_tsdata_relocation_hooks(type.checked_plan());

    TSData data{type};
    auto   view = data.view();
    auto   set  = view.as_set();
    REQUIRE(set.empty());
    REQUIRE(view.value().is_set());
    REQUIRE(view.value().binding().ops_ref().kind == ValueOpsKind::Set);

    const auto t1 = MIN_ST;
    Value      one{1};
    Value      two{2};
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
        REQUIRE(mutation.add(two.view()));
        REQUIRE_FALSE(mutation.add(one.view()));
    }

    REQUIRE(view.modified(t1));
    REQUIRE(set.size() == 2);
    REQUIRE(set.contains(one.view()));
    REQUIRE(view.value().as_set().contains(two.view()));

    auto delta = view.delta_value(t1).as_bundle();
    REQUIRE(delta.at("added").as_set().contains(one.view()));
    REQUIRE(delta.at("added").as_set().contains(two.view()));
    REQUIRE(delta.at("removed").as_set().empty());
    REQUIRE(set.added_values().begin() != set.added_values().end());
    REQUIRE(set.removed_values().begin() == set.removed_values().end());

    const auto t2 = t1 + TimeDelta{1};
    {
        auto mutation = set.begin_mutation(t2);
        REQUIRE(mutation.remove(one.view()));
    }

    REQUIRE_FALSE(set.contains(one.view()));
    REQUIRE(set.contains(two.view()));
    auto next_delta = view.delta_value(t2).as_bundle();
    REQUIRE(next_delta.at("added").as_set().empty());
    REQUIRE(next_delta.at("removed").as_set().contains(one.view()));
    const auto projected_delta = view.delta_value(t2);
    Value copied_delta{projected_delta.binding().ops_ref().owning_type(projected_delta.binding())};
    REQUIRE(copied_delta.begin_mutation().try_copy_from(projected_delta));
    REQUIRE(copied_delta.view().equals(projected_delta));
    REQUIRE(set.added_values().begin() == set.added_values().end());
    REQUIRE(set.removed_values().begin() != set.removed_values().end());
    REQUIRE(view.last_modified_time() == t2);

    const auto t3 = t2 + TimeDelta{1};
    Value      three{3};
    {
        auto mutation = set.begin_mutation(t3);
        REQUIRE(mutation.add(three.view()));
        REQUIRE(mutation.remove(three.view()));
    }

    REQUIRE_FALSE(set.contains(three.view()));
    REQUIRE(view.modified(t3));
    auto netted_delta = view.delta_value(t3).as_bundle();
    REQUIRE(netted_delta.at("added").as_set().empty());
    REQUIRE(netted_delta.at("removed").as_set().empty());
    REQUIRE(view.last_modified_time() == t3);
}

TEST_CASE("TSDataPlanFactory: TSS stale mutations join the current delta window")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tss      = registry.tss(int_meta);
    const auto  type     = TSDataPlanFactory::instance().data_type_for(tss);

    TSData data{type};
    auto   view = data.view();
    auto   set  = view.as_set();
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    Value      one{1};
    Value      two{2};
    Value      three{3};

    {
        auto mutation = set.begin_mutation(t2);
        REQUIRE(mutation.add(one.view()));
        REQUIRE(mutation.add(two.view()));
    }
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(three.view()));
    }

    REQUIRE(view.last_modified_time() == t2);
    REQUIRE(view.modified(t2));
    auto delta = view.delta_value(t2).as_bundle();
    auto added = delta.at("added").as_set();
    REQUIRE(added.contains(one.view()));
    REQUIRE(added.contains(two.view()));
    REQUIRE(added.contains(three.view()));
    REQUIRE(delta.at("removed").as_set().empty());
}

TEST_CASE("TSDataPlanFactory: TSD uses slot storage with key-set and modified deltas")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);

    const auto type = factory.data_type_for(tsd);
    require_no_tsdata_relocation_hooks(type.checked_plan());

    TSData data{type};
    auto   view = data.view();
    auto   dict = view.as_dict();
    REQUIRE(dict.empty());
    REQUIRE(view.value().is_map());
    REQUIRE(view.value().binding().ops_ref().kind == ValueOpsKind::Map);

    const auto t1 = MIN_ST;
    Value      key{7};
    Value      value{42};
    MapBuilder source_builder{registry.scalar_type<std::int32_t>(), registry.scalar_type<std::int32_t>()};
    source_builder.set_item<std::int32_t, std::int32_t>(7, 42);
    auto source_map = source_builder.build();
    {
        auto generic_mutation = view.begin_mutation(t1);
        REQUIRE_THROWS_AS(generic_mutation.copy_value_from(source_map.view()), std::logic_error);
    }
    {
        auto mutation = dict.begin_mutation(t1);
        mutation.set(key.view(), value.view());
    }

    REQUIRE(view.modified(t1));
    REQUIRE(dict.size() == 1);
    REQUIRE(dict.contains(key.view()));
    REQUIRE(dict.at(key.view()).value().checked_as<std::int32_t>() == 42);
    REQUIRE(view.value().as_map().at(key.view()).checked_as<std::int32_t>() == 42);

    auto immutable_ops = static_cast<const TSDDataOps &>(type.ops_ref());
    immutable_ops.allows_mutation = false;
    const auto immutable_type = intern_ts_type(*tsd, TypeRole::Data, type.checked_plan(), immutable_ops,
                                               "test.tsd.read-only");
    auto immutable_view = TSDataView{immutable_type, view.data()};
    auto          immutable_child = immutable_view.as_dict().at(key.view());
    REQUIRE(immutable_child.value().checked_as<std::int32_t>() == 42);
    REQUIRE(immutable_child.has_parent());
    REQUIRE(immutable_child.parent_link().parent_storage_type() == TSRoleTypeRef{type.as_role()});
    REQUIRE(immutable_child.root_view().data() == view.data());

    REQUIRE(dict.key_set().contains(key.view()));
    REQUIRE(dict.key_set().added().begin() != dict.key_set().added().end());
    REQUIRE(dict.added_keys().begin() != dict.added_keys().end());
    REQUIRE(dict.added_values().begin() != dict.added_values().end());
    REQUIRE(dict.added_items().begin() != dict.added_items().end());
    REQUIRE(dict.valid_keys().begin() != dict.valid_keys().end());
    REQUIRE(dict.valid_values().begin() != dict.valid_values().end());
    REQUIRE(dict.valid_items().begin() != dict.valid_items().end());
    REQUIRE(dict.modified_keys(t1).begin() != dict.modified_keys(t1).end());
    REQUIRE(dict.modified_values(t1).begin() != dict.modified_values(t1).end());
    REQUIRE(dict.modified_items(t1).begin() != dict.modified_items(t1).end());

    auto delta = view.delta_value(t1).as_bundle();
    REQUIRE(delta.at("removed").as_set().empty());
    REQUIRE(delta.at("modified").as_map().contains(key.view()));
    REQUIRE(delta.at("modified").as_map().at(key.view()).checked_as<std::int32_t>() == 42);

    const auto t2 = t1 + TimeDelta{1};
    Value      updated{84};
    {
        auto values = dict.values();
        auto it     = values.begin();
        REQUIRE(it != values.end());
        auto child = *it;
        REQUIRE(child.has_parent());
        REQUIRE(child.parent_link().parent_storage_type() == dict.base().storage_type());
        REQUIRE(child.parent_link().parent_data() == dict.base().data());
        REQUIRE(child.child_id() == dict.find_slot(key.view()));
        auto mutation = child.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(updated.view()));
    }

    REQUIRE(view.modified(t2));
    REQUIRE(dict.at(key.view()).value().checked_as<std::int32_t>() == 84);
    REQUIRE(dict.modified_keys(t2).begin() != dict.modified_keys(t2).end());
    auto modified_delta = view.delta_value(t2).as_bundle();
    REQUIRE(modified_delta.at("removed").as_set().empty());
    REQUIRE(modified_delta.at("modified").as_map().contains(key.view()));
    REQUIRE(modified_delta.at("modified").as_map().at(key.view()).checked_as<std::int32_t>() == 84);

    const auto t3 = t2 + TimeDelta{1};
    {
        auto mutation = dict.begin_mutation(t3);
        REQUIRE(mutation.erase(key.view()));
    }

    REQUIRE_FALSE(dict.contains(key.view()));
    REQUIRE(dict.removed_keys().begin() != dict.removed_keys().end());
    REQUIRE(dict.removed_values().begin() != dict.removed_values().end());
    REQUIRE(dict.removed_items().begin() != dict.removed_items().end());
    const std::size_t removed_slot = dict.next_removed_slot();
    REQUIRE(removed_slot != TS_DATA_NO_CHILD_ID);
    REQUIRE(dict.removed_key_at_slot(removed_slot).checked_as<std::int32_t>() == 7);
    REQUIRE_THROWS_AS(dict.removed_key_at_slot(TS_DATA_NO_CHILD_ID), std::out_of_range);
    auto next_delta = view.delta_value(t3).as_bundle();
    REQUIRE(next_delta.at("removed").as_set().contains(key.view()));
    REQUIRE_FALSE(next_delta.at("modified").as_map().contains(key.view()));
    const auto projected_delta = view.delta_value(t3);
    Value copied_delta{projected_delta.binding().ops_ref().owning_type(projected_delta.binding())};
    REQUIRE(copied_delta.begin_mutation().try_copy_from(projected_delta));
    REQUIRE(copied_delta.view().equals(projected_delta));
    REQUIRE(view.last_modified_time() == t3);

    const auto t4 = t3 + TimeDelta{1};
    Value      other_key{8};
    Value      other_value{11};
    {
        auto mutation = dict.begin_mutation(t4);
        mutation.set(other_key.view(), other_value.view());
        REQUIRE(mutation.erase(other_key.view()));
    }

    REQUIRE_FALSE(dict.contains(other_key.view()));
    REQUIRE(view.modified(t4));
    auto netted_delta = view.delta_value(t4).as_bundle();
    REQUIRE(netted_delta.at("removed").as_set().empty());
    REQUIRE(netted_delta.at("modified").as_map().empty());
    REQUIRE(view.last_modified_time() == t4);
}

TEST_CASE("TSDataPlanFactory: empty collection copy still marks the collection modified")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    auto       &factory     = TSDataPlanFactory::instance();
    const auto *int_meta    = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int      = registry.ts(int_meta);
    const auto *tss         = registry.tss(int_meta);
    const auto *tsd         = registry.tsd(int_meta, ts_int);

    const auto t1 = MIN_ST;

    Value  empty_set = stdlib::make_set<std::int32_t>({});
    TSData set_data{factory.data_type_for(tss)};
    auto   set_view = set_data.view();
    auto   set      = set_view.as_set();
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(empty_set.view()));
    }

    REQUIRE(set_view.modified(t1));
    REQUIRE(set_view.all_valid());
    REQUIRE(set.empty());
    auto set_delta = set_view.delta_value(t1).as_bundle();
    REQUIRE(set_delta.at("added").as_set().empty());
    REQUIRE(set_delta.at("removed").as_set().empty());

    Value  empty_map = stdlib::make_map<std::int32_t, std::int32_t>({});
    TSData dict_data{factory.data_type_for(tsd)};
    auto   dict_view = dict_data.view();
    auto   dict      = dict_view.as_dict();
    {
        auto mutation = dict.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(empty_map.view()));
    }

    REQUIRE(dict_view.modified(t1));
    REQUIRE(dict_view.all_valid());
    REQUIRE(dict.empty());
    auto dict_delta = dict_view.delta_value(t1).as_bundle();
    REQUIRE(dict_delta.at("removed").as_set().empty());
    REQUIRE(dict_delta.at("modified").as_map().empty());
}

TEST_CASE("TSDataPlanFactory: nested TSD rejects whole-child move replacement")
{
    using namespace hgraph;
    auto       &registry     = TypeRegistry::instance();
    auto       &factory      = TSDataPlanFactory::instance();
    const auto *int_meta     = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int       = registry.ts(int_meta);
    const auto *inner_tsd    = registry.tsd(int_meta, ts_int);
    const auto *outer_tsd    = registry.tsd(int_meta, inner_tsd);
    const auto key_binding   = registry.scalar_type<std::int32_t>();
    const auto dict_type = factory.data_type_for(outer_tsd);
    REQUIRE(key_binding != nullptr);
    REQUIRE(dict_type);
    require_no_tsdata_relocation_hooks(dict_type.checked_plan());

    Value inner_map = stdlib::make_map<std::int32_t, std::int32_t>({{2, 3}});
    Value outer_key{1};
    MapBuilder outer_builder{key_binding, inner_map.binding()};
    outer_builder.set_item_copy(outer_key.view().data(), inner_map.view().data());
    Value source = outer_builder.build();

    TSData data{dict_type};
    auto   mutation = data.view().begin_mutation(MIN_ST);
    REQUIRE_THROWS_AS(mutation.move_value_from(std::move(source)), std::logic_error);
}

TEST_CASE("TSDataPlanFactory: dynamic TSL stores grow-only child TSData")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsl      = registry.tsl(ts_int, 0);

    const auto type = factory.data_type_for(tsl);
    REQUIRE(type);
    REQUIRE(factory.plan_for(tsl) == type.plan());
    require_no_tsdata_relocation_hooks(type.checked_plan());

    TSData data{type};
    auto   view = data.view();
    REQUIRE(view.as_list().empty());
    REQUIRE(view.value().binding().ops_ref().kind == ValueOpsKind::Indexed);
    REQUIRE_FALSE(view.has_current_value());

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    {
        auto source = stdlib::make_list<std::int32_t>({11, 22});
        auto mutation = view.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(source.view()));
    }

    auto list = view.as_list();
    REQUIRE(list.size() == 2);
    REQUIRE(view.has_current_value());
    REQUIRE(view.all_valid());
    REQUIRE(list.at(0).value().checked_as<std::int32_t>() == 11);
    REQUIRE(list.at(1).value().checked_as<std::int32_t>() == 22);
    const void *child0_data = list.at(0).data();
    const void *child1_data = list.at(1).data();
    REQUIRE(child0_data != nullptr);
    REQUIRE(child1_data != nullptr);

    Value parent_snapshot{view.value()};
    REQUIRE(parent_snapshot.binding() == ValuePlanFactory::instance().type_for(tsl->value_schema));
    REQUIRE(parent_snapshot.view().as_list().at(0).checked_as<std::int32_t>() == 11);
    REQUIRE(parent_snapshot.view().as_list().at(1).checked_as<std::int32_t>() == 22);

    Value key_zero{std::int64_t{0}};
    Value key_one{std::int64_t{1}};
    Value key_two{std::int64_t{2}};
    auto  t1_delta = view.delta_value(t1).as_map();
    REQUIRE(t1_delta.contains(key_zero.view()));
    REQUIRE(t1_delta.contains(key_one.view()));
    REQUIRE(t1_delta.at(key_zero.view()).checked_as<std::int32_t>() == 11);
    REQUIRE(t1_delta.at(key_one.view()).checked_as<std::int32_t>() == 22);
    REQUIRE(std::vector<std::size_t>(list.modified_indices().begin(), list.modified_indices().end()) ==
            std::vector<std::size_t>{0, 1});

    {
        auto longer = stdlib::make_list<std::int32_t>({11, 22, 44});
        auto mutation = view.begin_mutation(t1);
        REQUIRE_FALSE(mutation.copy_value_from(longer.view()));
    }
    list = view.as_list();
    REQUIRE(list.size() == 3);
    REQUIRE(list.at(0).data() == child0_data);
    REQUIRE(list.at(1).data() == child1_data);
    REQUIRE(list.at(2).value().checked_as<std::int32_t>() == 44);
    auto grown_t1_delta = view.delta_value(t1).as_map();
    REQUIRE(grown_t1_delta.contains(key_zero.view()));
    REQUIRE(grown_t1_delta.contains(key_one.view()));
    REQUIRE(grown_t1_delta.contains(key_two.view()));
    REQUIRE(grown_t1_delta.at(key_two.view()).checked_as<std::int32_t>() == 44);
    REQUIRE(std::vector<std::size_t>(list.modified_indices().begin(), list.modified_indices().end()) ==
            std::vector<std::size_t>{0, 1, 2});

    const auto *float_meta = registry.register_scalar<double>("double");
    const auto *float_tsl = registry.tsl(registry.ts(float_meta), 0);
    const auto float_type = factory.data_type_for(float_tsl);
    TSDataView mismatched{TSRoleTypeRef{float_type.as_role()}, view.mutable_data()};
    REQUIRE_THROWS_AS(mismatched.ensure_indexed_child_at(3), std::logic_error);
    REQUIRE(view.as_list().size() == 3);

    {
        Value updated{33};
        auto  child = list.at(1);
        auto  mutation = child.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(updated.view()));
    }
    REQUIRE(view.modified(t2));
    REQUIRE(list.at(1).value().checked_as<std::int32_t>() == 33);
    auto t2_delta = view.delta_value(t2).as_map();
    REQUIRE_FALSE(t2_delta.contains(key_zero.view()));
    REQUIRE(t2_delta.contains(key_one.view()));
    REQUIRE_FALSE(t2_delta.contains(key_two.view()));
    REQUIRE(t2_delta.at(key_one.view()).checked_as<std::int32_t>() == 33);
    REQUIRE(std::vector<std::size_t>(list.modified_indices().begin(), list.modified_indices().end()) ==
            std::vector<std::size_t>{1});

    {
        auto shorter = stdlib::make_list<std::int32_t>({1});
        auto mutation = view.begin_mutation(t3);
        REQUIRE_THROWS_AS(mutation.copy_value_from(shorter.view()), std::invalid_argument);
    }
    REQUIRE_FALSE(view.modified(t3));
    REQUIRE(view.as_list().size() == 3);
}

TEST_CASE("TSDataPlanFactory: dynamic TSL stale child records preserve the current modified ring")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tsl      = registry.tsl(registry.ts(int_meta), 0);
    const auto  type     = TSDataPlanFactory::instance().data_type_for(tsl);

    TSData data{type};
    auto   view = data.view();
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    {
        auto source = stdlib::make_list<std::int32_t>({11, 22});
        auto mutation = view.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(source.view()));
    }

    auto list = view.as_list();
    REQUIRE(std::vector<std::size_t>(list.modified_indices().begin(), list.modified_indices().end()) ==
            std::vector<std::size_t>{0, 1});

    const auto &table = *view.storage_type().ops();
    table.record_child_modified_impl(table.context, const_cast<void *>(view.data()), 0, t1);

    REQUIRE(view.last_modified_time() == t2);
    REQUIRE(std::vector<std::size_t>(list.modified_indices().begin(), list.modified_indices().end()) ==
            std::vector<std::size_t>{0, 1});
    Value key_zero{std::int64_t{0}};
    Value key_one{std::int64_t{1}};
    auto  delta = view.delta_value(t2).as_map();
    REQUIRE(delta.contains(key_zero.view()));
    REQUIRE(delta.contains(key_one.view()));
}

TEST_CASE("TSDataPlanFactory: failed first dynamic TSL growth restores unbound element identity")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    auto       &factory = TSDataPlanFactory::instance();
    const auto *throwing = registry.register_scalar<ThrowsOnDynamicChildDefault>(
        "throws_on_dynamic_child_default");
    const auto *throwing_list = registry.tsl(registry.ts(throwing), 0);
    const auto throwing_type = factory.data_type_for(throwing_list);
    const auto *integer = registry.register_scalar<std::int32_t>("dynamic_growth_rebind_int32");
    const auto integer_type = factory.data_type_for(registry.tsl(registry.ts(integer), 0));
    const auto &throwing_plan = throwing_type.checked_plan();
    const auto &integer_plan = integer_type.checked_plan();
    REQUIRE(throwing_plan.layout.size == integer_plan.layout.size);
    REQUIRE(throwing_plan.layout.alignment == integer_plan.layout.alignment);
    REQUIRE(throwing_plan.lifecycle.construct == integer_plan.lifecycle.construct);
    REQUIRE(throwing_plan.lifecycle.destroy == integer_plan.lifecycle.destroy);
    REQUIRE(throwing_plan.lifecycle.copy_construct == integer_plan.lifecycle.copy_construct);
    REQUIRE(throwing_plan.lifecycle.move_construct == integer_plan.lifecycle.move_construct);
    REQUIRE(throwing_plan.lifecycle.copy_assign == integer_plan.lifecycle.copy_assign);
    REQUIRE(throwing_plan.lifecycle.move_assign == integer_plan.lifecycle.move_assign);
    REQUIRE(throwing_plan.lifecycle_context == integer_plan.lifecycle_context);
    REQUIRE(throwing_plan.composite_kind_tag == integer_plan.composite_kind_tag);
    REQUIRE(throwing_plan.trivially_destructible == integer_plan.trivially_destructible);
    REQUIRE(throwing_plan.trivially_copyable == integer_plan.trivially_copyable);
    REQUIRE(throwing_plan.trivially_move_constructible == integer_plan.trivially_move_constructible);

    TSData storage{integer_type};
    auto owner_view = storage.view();
    TSDataView failing{throwing_type.as_role(), owner_view.mutable_data()};
    REQUIRE_THROWS_AS(failing.ensure_indexed_child_at(0), std::runtime_error);
    REQUIRE(owner_view.indexed_child_count() == 0);

    auto child = owner_view.ensure_indexed_child_at(0);
    REQUIRE(child.valid());
    REQUIRE(owner_view.indexed_child_count() == 1);
    REQUIRE(child.schema() == registry.ts(integer));
}

TEST_CASE("TSDataPlanFactory::find returns null and null schemas return null")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    REQUIRE(factory.find(ts_int) == nullptr);
    REQUIRE(factory.find(nullptr) == nullptr);
    REQUIRE(factory.plan_for(nullptr) == nullptr);
}

TEST_CASE("TSDataPlanFactory::instance is a stable singleton")
{
    REQUIRE(&hgraph::TSDataPlanFactory::instance() == &hgraph::TSDataPlanFactory::instance());
}
