#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/compound_scalar_storage.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

namespace polymorphic_value_builder_repro
{
    struct Event
    {
    };
}

namespace hgraph
{
    template <>
    struct scalar_descriptor<polymorphic_value_builder_repro::Event>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return true; }
        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            auto &registry = TypeRegistry::instance();
            return registry.bundle(
                "tests.value_builder", "Event",
                {{"event_id", registry.value_type("str")}}, {}, true);
        }
    };
}

namespace
{
    using namespace hgraph;
    using PolymorphicEvent = polymorphic_value_builder_repro::Event;

    struct PolymorphicBuilderSchemas
    {
        const ValueTypeMetaData *event{nullptr};
        const ValueTypeMetaData *heartbeat{nullptr};
        const ValueTypeMetaData *created{nullptr};
    };

    [[nodiscard]] PolymorphicBuilderSchemas polymorphic_builder_schemas()
    {
        auto       &registry = TypeRegistry::instance();
        const auto *text = registry.value_type("str");
        const auto *event = scalar_descriptor<PolymorphicEvent>::value_meta();
        const auto *heartbeat = registry.bundle(
            "tests.value_builder", "HeartbeatEvent", {{"event_id", text}}, {event});
        const auto *order = registry.bundle(
            "tests.value_builder", "OrderEvent",
            {{"event_id", text}, {"order_id", text}}, {event}, true);
        const auto *created = registry.bundle(
            "tests.value_builder", "CreateEvent",
            {{"event_id", text}, {"order_id", text}, {"payload", text},
             {"details_a", text}, {"details_b", text},
             {"details_c", text}, {"details_d", text}},
            {order});
        return {
            .event = event,
            .heartbeat = heartbeat,
            .created = created,
        };
    }

    [[nodiscard]] Value heartbeat_value(
        const TypeRealizationSnapshot &realization,
        const PolymorphicBuilderSchemas &schemas)
    {
        BundleBuilder builder{realization.exact_type_for(schemas.heartbeat)};
        builder.set("event_id", Value{Str{"heartbeat"}});
        return builder.build();
    }

    [[nodiscard]] Value created_value(
        const TypeRealizationSnapshot &realization,
        const PolymorphicBuilderSchemas &schemas)
    {
        BundleBuilder builder{realization.exact_type_for(schemas.created)};
        builder.set("event_id", Value{Str{"event"}});
        builder.set("order_id", Value{Str{"order"}});
        builder.set("payload", Value{Str{"created"}});
        builder.set("details_a", Value{Str{"a"}});
        builder.set("details_b", Value{Str{"b"}});
        builder.set("details_c", Value{Str{"c"}});
        builder.set("details_d", Value{Str{"d"}});
        return builder.build();
    }

    void check_event(
        const ValueView &actual, const ValueTypeMetaData *expected_schema,
        std::string_view expected_id)
    {
        const auto concrete = actual.concrete();
        REQUIRE(concrete.schema() == expected_schema);
        CHECK(concrete.as_bundle().field("event_id").checked_as<Str>() ==
              Str{expected_id});
    }
}

TEST_CASE("value builders convert concrete Bundle leaves into a declared polymorphic binding")
{
    const auto schemas = polymorphic_builder_schemas();
    for (const bool pooled : {false, true})
    {
        INFO("value builder polymorphic storage " << (pooled ? "pooled" : "inline"));
        const hgraph::TypeRealizationOptions options{
            .polymorphic_compound_storage =
                pooled ? hgraph::PolymorphicCompoundStoragePolicy::Pooled
                       : hgraph::PolymorphicCompoundStoragePolicy::Inline,
        };
        const auto realization = hgraph::TypeRealizationSnapshot::capture(
            hgraph::TypeRegistry::instance(), options);
        hgraph::TypeRealizationScope realization_scope{realization.get()};
        if (pooled)
        {
            REQUIRE(realization->inspect(schemas.event).representation ==
                    hgraph::GraphValueRepresentation::PooledUnion);
        }

        const hgraph::Value heartbeat = heartbeat_value(*realization, schemas);
        const hgraph::Value created = created_value(*realization, schemas);
        const auto event_binding = realization->type_for(schemas.event);
        const auto *tuple_schema =
            hgraph::scalar_descriptor<hgraph::HomogeneousTuple<PolymorphicEvent>>::value_meta();

        hgraph::ListBuilder list{event_binding, *tuple_schema};
        list.push_back(heartbeat.view());
        list.push_back(created.view());
        const hgraph::Value list_value = list.build();
        REQUIRE(list_value.schema() == tuple_schema);
        REQUIRE(list_value.as_list().size() == 2);
        check_event(list_value.as_list().at(0), schemas.heartbeat, "heartbeat");
        check_event(list_value.as_list().at(1), schemas.created, "event");

        hgraph::SetBuilder set{event_binding};
        CHECK(set.insert(heartbeat.view()));
        CHECK(set.insert(created.view()));
        CHECK_FALSE(set.insert(heartbeat.view()));
        const hgraph::Value set_value = set.build();
        REQUIRE(set_value.as_set().size() == 2);

        hgraph::MapBuilder map{event_binding, event_binding};
        map.set_item(heartbeat.view(), heartbeat.view());
        map.set_item(heartbeat.view(), created.view());
        map.set_item(created.view(), heartbeat.view());
        const hgraph::Value map_value = map.build();
        REQUIRE(map_value.as_map().size() == 2);
        for (const auto [key, value] : map_value.as_map())
        {
            if (key.concrete().schema() == schemas.heartbeat)
            {
                check_event(value, schemas.created, "event");
            }
            else
            {
                check_event(key, schemas.created, "event");
                check_event(value, schemas.heartbeat, "heartbeat");
            }
        }

        hgraph::QueueBuilder queue{event_binding};
        queue.push(heartbeat.view());
        queue.push(created.view());
        const hgraph::Value queue_value = queue.build();
        REQUIRE(queue_value.as_queue().size() == 2);
        check_event(queue_value.as_queue().at(0), schemas.heartbeat, "heartbeat");
        check_event(queue_value.as_queue().at(1), schemas.created, "event");

        hgraph::CyclicBufferBuilder cyclic{event_binding, 1};
        cyclic.push_back(heartbeat.view());
        cyclic.push_back(created.view());
        const hgraph::Value cyclic_value = cyclic.build();
        REQUIRE(cyclic_value.as_cyclic_buffer().size() == 1);
        check_event(cyclic_value.as_cyclic_buffer().front(), schemas.created, "event");
    }
}

TEST_CASE("value builders reject an incompatible view before changing their contents")
{
    const auto schemas = polymorphic_builder_schemas();
    const auto realization = hgraph::TypeRealizationSnapshot::capture(
        hgraph::TypeRegistry::instance());
    hgraph::TypeRealizationScope realization_scope{realization.get()};

    const auto event_binding = realization->type_for(schemas.event);
    const auto string_binding = realization->type_for(
        hgraph::scalar_descriptor<hgraph::Str>::value_meta());
    const hgraph::Value incompatible{hgraph::Int{42}};
    const hgraph::Value heartbeat = heartbeat_value(*realization, schemas);
    const hgraph::Value created = created_value(*realization, schemas);

    hgraph::ListBuilder list{event_binding};
    CHECK_THROWS_AS(list.push_back(incompatible.view()),
                    std::invalid_argument);
    CHECK(list.empty());
    list.push_back(heartbeat.view());
    const hgraph::Value list_value = list.build();
    REQUIRE(list_value.as_list().size() == 1);
    check_event(list_value.as_list().front(), schemas.heartbeat, "heartbeat");

    hgraph::SetBuilder set{event_binding};
    CHECK_THROWS_AS(set.insert(incompatible.view()), std::invalid_argument);
    CHECK(set.size() == 0);
    CHECK(set.insert(heartbeat.view()));

    hgraph::QueueBuilder queue{event_binding};
    CHECK_THROWS_AS(queue.push(incompatible.view()), std::invalid_argument);
    CHECK(queue.size() == 0);
    queue.push(heartbeat.view());

    hgraph::CyclicBufferBuilder cyclic{event_binding, 1};
    cyclic.push_back(heartbeat.view());
    CHECK_THROWS_AS(cyclic.push_back(incompatible.view()), std::invalid_argument);
    const hgraph::Value cyclic_value = cyclic.build();
    REQUIRE(cyclic_value.as_cyclic_buffer().size() == 1);
    check_event(cyclic_value.as_cyclic_buffer().front(), schemas.heartbeat, "heartbeat");

    hgraph::MapBuilder map{string_binding, event_binding};
    const hgraph::Value key{hgraph::Str{"order"}};
    CHECK_THROWS_AS(map.set_item(key.view(), incompatible.view()),
                    std::invalid_argument);
    CHECK(map.size() == 0);
    map.set_item(key.view(), heartbeat.view());
    CHECK_THROWS_AS(map.set_item(key.view(), incompatible.view()),
                    std::invalid_argument);
    CHECK(map.size() == 1);
    map.set_item(key.view(), created.view());
    const hgraph::Value map_value = map.build();
    REQUIRE(map_value.as_map().size() == 1);
    check_event(
        map_value.as_map().at(key.view()), schemas.created, "event");
}

TEST_CASE("list builders transfer through compatible erased realizations and preserve unset elements")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = scalar_descriptor<Int>::value_meta();
    const auto *list_meta = registry.list(int_meta);
    const auto  element_binding = ValuePlanFactory::instance().type_for(int_meta);
    const auto  compact_binding = compact_list_type(element_binding, *list_meta);

    // Deliberately bind the same immutable List schema to the slot-backed list
    // strategy. The shared schema makes it a compatible realization; its plan
    // and storage layout remain distinct from compact ListStorage.
    const auto slot_binding = intern_value_type(
        *list_meta, mutable_list_plan(element_binding), mutable_list_ops());
    REQUIRE(slot_binding != compact_binding);
    REQUIRE(slot_binding.ops_ref().kind == ValueOpsKind::MutableList);

    ListBuilder builder{element_binding, *list_meta};
    builder.push_back(Int{10});
    builder.push_back_unset();
    builder.push_back(Int{30});
    Value slot_value = builder.build(slot_binding);

    const auto slot_list = slot_value.as_list();
    REQUIRE(slot_list.size() == 3);
    CHECK(slot_list.element_valid(0));
    CHECK_FALSE(slot_list.element_valid(1));
    CHECK(slot_list.element_valid(2));
    CHECK(slot_list.at(0).checked_as<Int>() == Int{10});
    CHECK(slot_list.at(1).bound());
    CHECK_FALSE(slot_list.at(1).has_value());
    CHECK(slot_list.at(2).checked_as<Int>() == Int{30});

    Value compact_value{compact_binding};
    compact_binding.ops_ref().copy_assign_from(
        compact_binding, const_cast<void *>(compact_value.view().data()),
        slot_value.binding(), slot_value.view().data());

    const auto compact_list = compact_value.as_list();
    REQUIRE(compact_list.size() == 3);
    CHECK(compact_list.element_valid(0));
    CHECK_FALSE(compact_list.element_valid(1));
    CHECK(compact_list.element_valid(2));
    CHECK(compact_list.at(0).checked_as<Int>() == Int{10});
    CHECK(compact_list.at(1).bound());
    CHECK_FALSE(compact_list.at(1).has_value());
    CHECK(compact_list.at(2).checked_as<Int>() == Int{30});
}

TEST_CASE("list builders dispatch target transfer through the external owning binding")
{
    using namespace hgraph;

    const auto schemas = polymorphic_builder_schemas();
    const TypeRealizationOptions options{
        .polymorphic_compound_storage =
            PolymorphicCompoundStoragePolicy::Pooled,
    };
    const auto realization = TypeRealizationSnapshot::capture(
        TypeRegistry::instance(), options);
    TypeRealizationScope realization_scope{realization.get()};
    CompoundScalarStorage pools = CompoundScalarStorage::make_default();
    pools.bind(realization->pool_binding());

    auto       &registry = TypeRegistry::instance();
    const auto *list_schema = registry.list(schemas.event);
    const auto  external_element = realization->type_for(schemas.event);
    const auto  external_list = realization->type_for(list_schema);
    const auto  graph_list = realization->graph_type_for(list_schema);
    REQUIRE(graph_list != external_list);
    REQUIRE(value_owning_type(graph_list) == external_list);

    const Value created = created_value(*realization, schemas);
    ListBuilder builder{external_element, *list_schema};
    builder.push_back(created.view());
    const Value result = builder.build(graph_list);

    REQUIRE(result.binding() == external_list);
    const auto values = result.as_list();
    REQUIRE(values.size() == 1);
    CHECK(values.at(0).binding() == external_element);
    check_event(values.at(0), schemas.created, "event");
}

TEST_CASE("compact containers convert elements between polymorphic realizations")
{
    struct Containers
    {
        hgraph::Value list;
        hgraph::Value set;
        hgraph::Value map;
        hgraph::Value queue;
        hgraph::Value cyclic;
    };

    const auto schemas = polymorphic_builder_schemas();
    const auto inline_realization = hgraph::TypeRealizationSnapshot::capture(
        hgraph::TypeRegistry::instance());
    const hgraph::TypeRealizationOptions pooled_options{
        .polymorphic_compound_storage =
            hgraph::PolymorphicCompoundStoragePolicy::Pooled,
    };
    const auto pooled_realization = hgraph::TypeRealizationSnapshot::capture(
        hgraph::TypeRegistry::instance(), pooled_options);
    REQUIRE(pooled_realization->inspect(schemas.event).representation ==
            hgraph::GraphValueRepresentation::PooledUnion);

    const auto *tuple_schema =
        hgraph::scalar_descriptor<hgraph::HomogeneousTuple<PolymorphicEvent>>::value_meta();
    const auto *text_schema =
        hgraph::scalar_descriptor<hgraph::Str>::value_meta();
    const hgraph::Value key{hgraph::Str{"order"}};

    const auto make_containers = [&](const hgraph::TypeRealizationSnapshot &realization) {
        hgraph::TypeRealizationScope scope{&realization};
        const auto event_binding = realization.type_for(schemas.event);
        const auto text_binding = realization.type_for(text_schema);
        const hgraph::Value created = created_value(realization, schemas);

        hgraph::ListBuilder list{event_binding, *tuple_schema};
        list.push_back(created.view());
        hgraph::SetBuilder set{event_binding};
        static_cast<void>(set.insert(created.view()));
        hgraph::MapBuilder map{text_binding, event_binding};
        map.set_item(key.view(), created.view());
        hgraph::QueueBuilder queue{event_binding};
        queue.push(created.view());
        hgraph::CyclicBufferBuilder cyclic{event_binding, 2};
        cyclic.push_back(created.view());

        return Containers{
            .list = list.build(),
            .set = set.build(),
            .map = map.build(),
            .queue = queue.build(),
            .cyclic = cyclic.build(),
        };
    };

    const auto inline_values = make_containers(*inline_realization);
    const auto pooled_values = make_containers(*pooled_realization);

    const auto copy_into = [](hgraph::ValueTypeRef target,
                              const hgraph::Value &source) {
        hgraph::Value result{target};
        target.ops_ref().copy_assign_from(
            target, const_cast<void *>(result.view().data()), source.binding(),
            source.view().data());
        return result;
    };

    const auto check_conversion = [&](const Containers &source,
                                      const hgraph::TypeRealizationSnapshot &target_realization) {
        hgraph::TypeRealizationScope scope{&target_realization};
        const auto event_binding = target_realization.type_for(schemas.event);
        const auto text_binding = target_realization.type_for(text_schema);

        const hgraph::Value list = copy_into(
            hgraph::compact_list_type(event_binding, *tuple_schema), source.list);
        REQUIRE(list.as_list().size() == 1);
        check_event(list.as_list().front(), schemas.created, "event");

        const hgraph::Value set = copy_into(
            hgraph::compact_set_type(event_binding), source.set);
        REQUIRE(set.as_set().size() == 1);
        check_event(*set.as_set().values().begin(), schemas.created, "event");

        const hgraph::Value map = copy_into(
            hgraph::compact_map_type(text_binding, event_binding), source.map);
        REQUIRE(map.as_map().size() == 1);
        check_event(map.as_map().at(key.view()), schemas.created, "event");

        const hgraph::Value queue = copy_into(
            hgraph::compact_queue_type(event_binding, 0), source.queue);
        REQUIRE(queue.as_queue().size() == 1);
        check_event(queue.as_queue().front(), schemas.created, "event");

        const hgraph::Value cyclic = copy_into(
            hgraph::compact_cyclic_buffer_type(event_binding, 2), source.cyclic);
        REQUIRE(cyclic.as_cyclic_buffer().size() == 1);
        check_event(cyclic.as_cyclic_buffer().front(), schemas.created, "event");
    };

    check_conversion(inline_values, *pooled_realization);
    check_conversion(pooled_values, *inline_realization);
}
