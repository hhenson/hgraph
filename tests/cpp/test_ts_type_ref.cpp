#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_data/proxy.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace
{
    struct RecordingNotifier final : hgraph::Notifiable
    {
        void notify(hgraph::DateTime time) override { notifications.push_back(time); }
        std::vector<hgraph::DateTime> notifications{};
    };

    struct InvalidationRecorder final : hgraph::Notifiable
    {
        void notify(hgraph::DateTime) override {}
        void source_invalidated(const hgraph::TSDataTracking *source) noexcept override
        {
            ++invalidations;
            invalidated_source = source;
        }

        std::size_t invalidations{0};
        const hgraph::TSDataTracking *invalidated_source{nullptr};
    };

    struct ReentrantInvalidationRecorder final : hgraph::Notifiable
    {
        void notify(hgraph::DateTime) override {}
        void source_invalidated(const hgraph::TSDataTracking *source) noexcept override
        {
            ++invalidations;
            const_cast<hgraph::TSDataTracking *>(source)->observers.invalidate(source);
        }

        std::size_t invalidations{0};
    };

    struct ForwardingClearRecorder final : hgraph::SlotObserver
    {
        void on_capacity(std::size_t, std::size_t) override {}
        void on_insert(std::size_t) override {}
        void on_remove(std::size_t) override {}
        void on_erase(std::size_t) override {}
        void on_clear() override
        {
            ++clears;
            saw_empty_target = forwarding != nullptr &&
                               !forwarding->view(evaluation_time).forwarding_target().bound();
        }

        hgraph::TSOutput *forwarding{nullptr};
        hgraph::DateTime evaluation_time{hgraph::MIN_ST};
        std::size_t clears{0};
        bool saw_empty_target{false};
    };

    struct LifetimeSource
    {
        static constexpr auto name = "scalar_lifetime_source";
        static constexpr bool schedule_on_start = true;
        static void eval(hgraph::Out<hgraph::TS<hgraph::Int>> out) { out.set(hgraph::Int{1}); }
    };

    struct LifetimeSink
    {
        static constexpr auto name = "scalar_lifetime_sink";
        static void eval(hgraph::In<"value", hgraph::TS<hgraph::Int>>) {}
    };

    [[nodiscard]] hgraph::TSInput scalar_input(const hgraph::TSValueTypeMetaData *schema)
    {
        return hgraph::TSInput{
            hgraph::TSInputBuilderFactory::checked_builder_for(*schema, hgraph::TSEndpointSchema::peered(schema))};
    }

    void set_scalar_output(hgraph::TSOutput &output, std::int32_t value, hgraph::DateTime time)
    {
        hgraph::Value stored{value};
        REQUIRE(output.view(time).begin_mutation(time).copy_value_from(stored.view()));
    }

    void set_list_output(hgraph::TSOutput &output, std::size_t index, std::int32_t value, hgraph::DateTime time)
    {
        hgraph::Value stored{value};
        auto          view = output.view(time);
        auto          list = view.as_list();
        REQUIRE(list[index].begin_mutation(time).copy_value_from(stored.view()));
    }
}

TEST_CASE("time-series schemas expose canonical SchemaHeader prefixes and numeric kinds")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *signal = registry.signal();
    const auto *tss = registry.tss(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto *tsl = registry.tsl(ts, 2);
    const auto *tsw = registry.tsw(integer, 3, 1);
    const auto *structural = registry.un_named_tsb({{"value", ts}});
    const auto *named = registry.tsb("RoleHeaderBundle", {{"value", ts}});
    const auto *ref = registry.ref(ts);

    const std::array schemas{ts, tss, tsd, tsl, tsw, structural, ref, signal, named};
    for (const auto *schema : schemas)
    {
        REQUIRE(schema != nullptr);
        REQUIRE(reinterpret_cast<const SchemaHeader *>(schema) == &schema->header);
        REQUIRE(schema->header.valid());
        REQUIRE(schema->header.family == TypeFamily::TimeSeries);
        REQUIRE(schema->header.kind == static_cast<TypeKind>(schema->kind));
        REQUIRE_FALSE(schema->name().empty());
    }
    REQUIRE(structural->wrapped_un_named_tsb() == nullptr);
    REQUIRE(named->wrapped_un_named_tsb() == structural);
    REQUIRE(std::string{named->name()} == "RoleHeaderBundle");
}

TEST_CASE("scalar time-series role records are canonical, exact, and thread stable")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *schema = registry.ts(registry.register_scalar<std::int32_t>("int32"));

    const auto data = factory.data_type_for(schema);
    const auto output = factory.output_type_for(schema);
    const auto input_builder = TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::owned(schema));
    TSInput input{input_builder};
    const auto input_type = input.type_ref();

    for (const auto type : {data.as_role(), input_type.as_role(), output.as_role()})
    {
        REQUIRE(type.valid());
        REQUIRE(type.schema() == schema);
        REQUIRE(type.record()->schema == &schema->header);
        REQUIRE(type.record()->ops_abi_version == TS_DATA_OPS_ABI_VERSION);
        REQUIRE(type.record()->implementation_name().empty());
        REQUIRE(type.record()->debug != nullptr);
        REQUIRE(type.record()->debug->valid());
        REQUIRE(type.record()->debug->layout == DebugLayoutKind::TimeSeries);
        REQUIRE(type.record()->debug->time_series_layout != nullptr);
        REQUIRE(type.record()->debug->time_series_layout->value_type != nullptr);
        REQUIRE(type.ops_ref().kind == TSTypeKind::TS);
        REQUIRE(has_capability(type.capabilities(), TypeCapabilities::Viewable));
        REQUIRE_FALSE(has_capability(type.capabilities(), TypeCapabilities::HasChildren));
        REQUIRE_FALSE(has_capability(type.capabilities(), TypeCapabilities::Hashable));
        REQUIRE_FALSE(has_capability(type.capabilities(), TypeCapabilities::Equatable));
        REQUIRE_FALSE(has_capability(type.capabilities(), TypeCapabilities::Comparable));
    }
    REQUIRE(data.record() != output.record());
    REQUIRE(data.record() != input_type.record());
    REQUIRE(output.record() != input_type.record());
    REQUIRE(has_capability(data.capabilities(), TypeCapabilities::Mutable));
    REQUIRE(has_capability(output.capabilities(), TypeCapabilities::Mutable));
    REQUIRE_FALSE(has_capability(input_type.capabilities(), TypeCapabilities::Mutable));
    std::array<const TypeRecord *, 8> records{};
    std::array<std::thread, 8> threads{};
    for (std::size_t i = 0; i < threads.size(); ++i)
        threads[i] = std::thread([&, i] { records[i] = factory.data_type_for(schema).record(); });
    for (auto &thread : threads) thread.join();
    for (const auto *record : records) REQUIRE(record == data.record());
}

TEST_CASE("scalar role references validate AnyPtr boundaries and enforce role mutation")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *schema = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto data_type = factory.data_type_for(schema);
    const auto output_type = factory.output_type_for(schema);

    REQUIRE(TSDataTypeRef::checked(data_type.typed_null().to_any()) == data_type);
    REQUIRE_THROWS_AS(TSInputTypeRef::checked(data_type.typed_null().to_any()), std::invalid_argument);
    REQUIRE_THROWS_AS(TSDataTypeRef::checked(output_type.typed_null().to_any()), std::invalid_argument);
    const auto value_type = registry.scalar_type<std::int32_t>();
    REQUIRE_THROWS_AS(TSRoleTypeRef::checked(value_type.typed_null().to_any()), std::invalid_argument);

    const auto input_role = checked_ts_role_type(
        intern_ts_type(*schema, TypeRole::Input, data_type.checked_plan(), data_type.ops_ref()),
        std::integral_constant<TypeRole, TypeRole::Input>{});
    REQUIRE_THROWS_AS(input_role.writable(nullptr), std::logic_error);
    REQUIRE_THROWS_AS(TSInputTypeRef::checked(AnyPtr::writable(*input_role.record(), nullptr)),
                      std::invalid_argument);
    TSData data{data_type};
    Value forty_two{std::int32_t{42}};
    {
        auto mutation = data.view().begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(forty_two.view()));
    }
    REQUIRE(data.view().value().checked_as<std::int32_t>() == 42);

    TSInput owned{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::owned(schema))};
    REQUIRE(owned.type_ref().record() != nullptr);
    REQUIRE_THROWS_AS(owned.view(nullptr, MIN_ST).data_view().begin_mutation(MIN_ST), std::logic_error);
}

TEST_CASE("time-series role validation rejects common header kind mismatches")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto ts_data = factory.data_type_for(ts);

    TSValueTypeMetaData malformed_for_intern = *ts;
    malformed_for_intern.header.kind = static_cast<TypeKind>(TSTypeKind::TSS);
    REQUIRE(malformed_for_intern.header.valid());
    REQUIRE(malformed_for_intern.kind == TSTypeKind::TS);
    REQUIRE(ts_data.ops_ref().kind == TSTypeKind::TS);
    REQUIRE_THROWS_AS(
        intern_ts_type(malformed_for_intern, TypeRole::Data, ts_data.checked_plan(), ts_data.ops_ref()),
        std::invalid_argument);

    // TypeRecordRegistry validates only the common record contract, so retain
    // this deliberately malformed family schema for the registry lifetime and
    // exercise the time-series checked narrowing boundary explicitly.
    static auto *malformed_record_schema = new TSValueTypeMetaData{*ts};
    malformed_record_schema->header.kind = static_cast<TypeKind>(TSTypeKind::TSS);
    const TypeRecordDefinition definition{
        .key = TypeRecordKey{
            .schema = &malformed_record_schema->header,
            .role = TypeRole::Data,
            .plan = &ts_data.checked_plan(),
            .ops = &ts_data.ops_ref(),
            .debug = nullptr,
            .implementation_label = {},
        },
        .ops_abi_version = TS_DATA_OPS_ABI_VERSION,
        .capabilities = ts_type_capabilities(TypeRole::Data, ts_data.checked_plan(), ts_data.ops_ref()),
    };
    const TypeRecord &malformed_record = TypeRecordRegistry::instance().intern(definition);
    const AnyPtr malformed_pointer = AnyPtr::typed_null(malformed_record);
    REQUIRE(malformed_pointer.well_formed());
    REQUIRE_THROWS_AS(TSRoleTypeRef::checked(malformed_pointer), std::invalid_argument);
    REQUIRE_THROWS_AS(TSDataTypeRef::checked(malformed_pointer), std::invalid_argument);

    for (const auto *canonical : {ts, registry.signal()})
    {
        const auto type = factory.data_type_for(canonical);
        REQUIRE(type.valid());
        REQUIRE(TSRoleTypeRef::checked(type.typed_null().to_any()).valid());
        REQUIRE(TSDataTypeRef::checked(type.as_role()).valid());
    }
}

TEST_CASE("fixed structured role records preserve semantic identity and embedded roles")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *fixed_list = registry.tsl(ts, 2);
    const auto *structural = registry.un_named_tsb({{"value", ts}, {"items", fixed_list}});
    const auto *named = registry.tsb("FixedRoleBundle", {{"value", ts}, {"items", fixed_list}});
    const auto *window = registry.tsw(integer, 2, 1);
    const auto *mixed = registry.tsb("FixedRoleMixed", {{"value", ts}, {"window", window}});

    for (const auto *schema : {fixed_list, structural, named})
    {
        const auto data = factory.data_type_for(schema);
        const auto output = factory.output_type_for(schema);
        REQUIRE(data.valid());
        REQUIRE(output.valid());
        REQUIRE(data.schema() == schema);
        REQUIRE(output.schema() == schema);
        REQUIRE(data.record() != output.record());
        REQUIRE(data.record()->ops_abi_version == TS_DATA_OPS_ABI_VERSION);
        REQUIRE(output.record()->ops_abi_version == TS_DATA_OPS_ABI_VERSION);
        REQUIRE(std::string{data.record()->implementation_name()} == "ts.fixed.data.root");
        REQUIRE(std::string{output.record()->implementation_name()} == "ts.fixed.output.root");
        REQUIRE(has_capability(data.capabilities(), TypeCapabilities::Viewable));
        REQUIRE(has_capability(data.capabilities(), TypeCapabilities::HasChildren));
        REQUIRE(has_capability(data.capabilities(), TypeCapabilities::Mutable));
        REQUIRE(has_capability(output.capabilities(), TypeCapabilities::Mutable));
    }

    REQUIRE(factory.data_type_for(named).record() != factory.data_type_for(structural).record());
    REQUIRE(&factory.data_type_for(named).checked_plan() ==
            &factory.data_type_for(structural).checked_plan());

    TSData data{factory.data_type_for(named)};
    auto data_root = data.view();
    auto data_bundle = data_root.as_bundle();
    auto data_scalar = data_bundle.field("value");
    auto data_list = data_bundle.field("items");
    REQUIRE(data_scalar.type_ref().role() == TypeRole::Data);
    REQUIRE(std::string{data_scalar.type_ref().record()->implementation_name()} == "ts.fixed.data.embedded");
    REQUIRE(data_list.type_ref().role() == TypeRole::Data);
    REQUIRE(std::string{data_list.type_ref().record()->implementation_name()} == "ts.fixed.data.embedded");
    auto data_list_view = data_list.as_list();
    REQUIRE(data_list_view[0].type_ref().role() == TypeRole::Data);
    REQUIRE(data_list_view[0].type_ref().record() != data_list_view[1].type_ref().record());
    REQUIRE(data_list_view[0].layout().value_offset != data_list_view[1].layout().value_offset);

    TSOutput output{named};
    auto output_root = output.data_view();
    auto output_bundle = output_root.as_bundle();
    auto output_scalar = output_bundle.field("value");
    auto output_list = output_bundle.field("items");
    REQUIRE(output.type_ref().record() != nullptr);
    REQUIRE(output.type_ref().schema() == named);
    REQUIRE(output_scalar.type_ref().role() == TypeRole::Output);
    REQUIRE(std::string{output_scalar.type_ref().record()->implementation_name()} == "ts.fixed.output.embedded");
    REQUIRE(output_list.type_ref().role() == TypeRole::Output);
    auto output_list_view = output_list.as_list();
    REQUIRE(output_list_view[1].type_ref().role() == TypeRole::Output);
    REQUIRE(output_list_view[0].type_ref().record() != output_list_view[1].type_ref().record());
    REQUIRE(output_list_view[0].layout().value_offset != output_list_view[1].layout().value_offset);

    TSOutput mixed_output{mixed};
    auto mixed_root = mixed_output.data_view();
    auto mixed_bundle = mixed_root.as_bundle();
    REQUIRE(mixed_bundle.field("value").storage_type().record() != nullptr);
    REQUIRE(mixed_bundle.field("window").storage_type().record() != nullptr);
    REQUIRE(std::string{mixed_bundle.field("window").type_ref().record()->implementation_name()} ==
            "ts.tsw.tick.output.embedded");
}

TEST_CASE("mixed fixed output topology preserves Output roles through forwarding children")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto *inner = registry.tsb("MixedOutputInner", {{"owned", ts}, {"forwarded", ts}});
    const auto *root = registry.tsb("MixedOutputRoot", {{"nested", inner}, {"owned", ts}});
    const auto endpoint = TSEndpointSchema::non_peered(
        root,
        {TSEndpointSchema::non_peered(
             inner, {TSEndpointSchema::owned(ts), TSEndpointSchema::peered(ts)}),
         TSEndpointSchema::owned(ts)});

    TSOutput output{endpoint};
    auto root_view = output.view(MIN_ST);
    auto root_bundle = root_view.as_bundle();
    auto nested = root_bundle.field("nested");
    auto nested_bundle = nested.as_bundle();
    auto owned = nested_bundle.field("owned");
    auto forwarded = nested_bundle.field("forwarded");
    REQUIRE(detail::has_input_children(root_view.data_view()));
    REQUIRE(detail::has_input_children(nested.data_view()));
    auto forwarding_projection = detail::input_child_projection(nested.data_view(), 1);
    CAPTURE(forwarding_projection.visible.valid(), forwarding_projection.target_link.valid(),
            forwarding_projection.visible.storage_type().record(),
            forwarding_projection.target_link.storage_type().record());
    REQUIRE(forwarding_projection.target_link.valid());

    const std::array views{&nested, &owned, &forwarded};
    for (std::size_t index = 0; index < views.size(); ++index)
    {
        const auto *view = views[index];
        CAPTURE(index, view->storage_type().record());
        const auto type = view->type_ref();
        REQUIRE(type.valid());
        REQUIRE(type.as_role().role() == TypeRole::Output);
        REQUIRE(type.checked_plan().layout.size == output.type_ref().checked_plan().layout.size);
        REQUIRE(type.plan() == output.type_ref().plan());
        REQUIRE(type.ops() == &view->data_view().ops());
        REQUIRE(std::string{type.record()->implementation_name()} == "ts.fixed.output.embedded");

        TSOutputHandle handle = view->handle();
        REQUIRE(handle.type_ref() == type);
        REQUIRE(handle.storage_type() == view->storage_type());
    }

    REQUIRE(forwarded.forwarding());
    TSOutput source{ts};
    forwarded.bind_forwarding_target(source.view(MIN_ST));
    REQUIRE(forwarded.forwarding_bound());
    REQUIRE(forwarded.forwarding_target().type_ref().as_role().role() == TypeRole::Output);
    forwarded.clear_forwarding_target();
}

TEST_CASE("composed fixed input ownership remains local across copy move rebind and teardown")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto *list = registry.tsl(ts, 2);
    const auto *nested = registry.tsb("OwnedTraversalNested", {{"items", list}, {"owned", ts}});
    const auto *root = registry.tsb("OwnedTraversalRoot", {{"whole", list}, {"nested", nested}});
    const auto endpoint = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(list),
            TSEndpointSchema::non_peered(
                nested,
                {
                    TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts)),
                    TSEndpointSchema::owned(ts),
                }),
        });
    const auto &builder = TSInputBuilderFactory::checked_builder_for(*root, endpoint);

    TSOutput list_source{list};
    TSOutput list_replacement{list};
    TSOutput scalar_source{ts};
    set_list_output(list_source, 0, 10, MIN_ST);
    set_list_output(list_replacement, 0, 20, MIN_ST);
    set_scalar_output(scalar_source, 30, MIN_ST);

    auto producer_root = list_source.data_view();
    auto producer_list = producer_root.as_list();
    auto producer_child = producer_list[0];
    const auto producer_root_parent = producer_root.parent_link();
    const auto producer_child_parent = producer_child.parent_link();
    REQUIRE(producer_root_parent.parent_output() == &list_source);
    REQUIRE(producer_child_parent.parent_data() == producer_root.data());

    TSInput independent{TSInputBuilderFactory::checked_builder_for(*list, TSEndpointSchema::peered(list))};
    auto independent_view = independent.view(nullptr, MIN_ST);
    independent_view.bind_output(list_source.view(MIN_ST));

    RecordingNotifier scheduling;
    {
        TSInput input{builder};
        auto root_view = input.view(&scheduling, MIN_ST);
        auto root_bundle = root_view.as_bundle();
        auto whole = root_bundle.field("whole");
        auto nested_view = root_bundle.field("nested");
        auto nested_bundle = nested_view.as_bundle();
        auto items = nested_bundle.field("items");
        auto item_list = items.as_list();
        auto linked_leaf = item_list[0];
        auto owned_leaf = nested_bundle.field("owned");
        whole.bind_output(list_source.view(MIN_ST));
        linked_leaf.bind_output(scalar_source.view(MIN_ST));
        whole.make_active();
        linked_leaf.make_active();
        REQUIRE(owned_leaf.data_view().parent_link().parent_data() == nested_view.data_view().data());

        auto local_root = root_view.data_view().borrowed_ref();
        auto whole_local = detail::input_child_projection(local_root, 0).target_link;
        auto nested_local = detail::input_child_projection(local_root, 1).visible;
        auto items_local = detail::input_child_projection(nested_local, 0).visible;
        auto leaf_local = detail::input_child_projection(items_local, 0).target_link;
        REQUIRE(whole_local.path_from_root() == std::vector<std::size_t>{0});
        REQUIRE(leaf_local.path_from_root() == std::vector<std::size_t>{1, 0, 0});
        REQUIRE(whole_local.parent_link().parent_data() == local_root.data());
        REQUIRE(leaf_local.parent_link().parent_data() == items_local.data());

        const auto list_observers_before_copy = list_source.data_view().observer_count();
        const auto scalar_observers_before_copy = scalar_source.data_view().observer_count();
        TSInput copied{input};
        auto copied_root = copied.view(nullptr, MIN_ST);
        auto copied_bundle = copied_root.as_bundle();
        REQUIRE_FALSE(copied_bundle.field("whole").bound());
        auto copied_nested_view = copied_bundle.field("nested");
        auto copied_nested = copied_nested_view.as_bundle();
        auto copied_items_view = copied_nested.field("items");
        auto copied_items = copied_items_view.as_list();
        REQUIRE_FALSE(copied_items[0].bound());
        REQUIRE(list_source.data_view().observer_count() == list_observers_before_copy);
        REQUIRE(scalar_source.data_view().observer_count() == scalar_observers_before_copy);

        TSInput moved{std::move(input)};
        auto moved_root = moved.view(&scheduling, MIN_ST);
        auto moved_bundle = moved_root.as_bundle();
        auto moved_whole = moved_bundle.field("whole");
        auto moved_nested_view = moved_bundle.field("nested");
        auto moved_nested = moved_nested_view.as_bundle();
        auto moved_items_view = moved_nested.field("items");
        auto moved_items = moved_items_view.as_list();
        auto moved_leaf = moved_items[0];
        REQUIRE(moved_whole.bound());
        REQUIRE(moved_whole.active());
        REQUIRE(moved_leaf.bound());
        REQUIRE(moved_leaf.active());
        REQUIRE(detail::input_child_projection(moved_root.data_view(), 0).target_link.path_from_root() ==
                std::vector<std::size_t>{0});

        REQUIRE(list_source.data_view().parent_link().parent_output() == &list_source);
        REQUIRE(producer_child.parent_link().parent_data() == producer_child_parent.parent_data());
        REQUIRE(producer_child.parent_link().parent_storage_type() == producer_child_parent.parent_storage_type());

        scheduling.notifications.clear();
        moved_whole.bind_output(list_replacement.view(MIN_ST));
        set_list_output(list_replacement, 0, 21, MIN_ST + TimeDelta{1});
        REQUIRE_FALSE(scheduling.notifications.empty());
        auto moved_after_rebind = moved.view(&scheduling, MIN_ST + TimeDelta{1});
        auto moved_after_bundle = moved_after_rebind.as_bundle();
        auto moved_after_whole = moved_after_bundle.field("whole");
        auto moved_after_list = moved_after_whole.as_list();
        REQUIRE(moved_after_list[0].value().checked_as<std::int32_t>() == 21);

        set_list_output(list_source, 0, 11, MIN_ST + TimeDelta{2});
        auto independent_after = independent.view(nullptr, MIN_ST + TimeDelta{2});
        auto independent_list = independent_after.as_list();
        REQUIRE(independent_list[0].value().checked_as<std::int32_t>() == 11);
        REQUIRE(independent_after.bound());
    }

    REQUIRE(list_replacement.data_view().observer_count() == 0);
    REQUIRE(scalar_source.data_view().observer_count() == 0);
    REQUIRE(independent_view.bound());
    REQUIRE(list_source.data_view().observer_count() == 1);
    REQUIRE(list_source.data_view().parent_link().parent_output() == &list_source);
    REQUIRE(producer_child.parent_link().parent_data() == producer_child_parent.parent_data());

    TSInput producer_first{builder};
    std::optional<TSOutput> short_list{std::in_place, list};
    std::optional<TSOutput> short_scalar{std::in_place, ts};
    auto producer_first_view = producer_first.view(nullptr, MIN_ST);
    auto producer_first_root = producer_first_view.as_bundle();
    auto producer_first_whole = producer_first_root.field("whole");
    auto producer_first_nested_view = producer_first_root.field("nested");
    auto producer_first_nested = producer_first_nested_view.as_bundle();
    auto producer_first_items_view = producer_first_nested.field("items");
    auto producer_first_items = producer_first_items_view.as_list();
    auto producer_first_leaf = producer_first_items[0];
    producer_first_whole.bind_output(short_list->view(MIN_ST));
    producer_first_leaf.bind_output(short_scalar->view(MIN_ST));
    short_list.reset();
    REQUIRE_FALSE(producer_first_whole.bound());
    REQUIRE(producer_first_leaf.bound());
    short_scalar.reset();
    REQUIRE_FALSE(producer_first_leaf.bound());
}

TEST_CASE("composed forwarding output invalidates local consumers without touching producer consumers")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto *list = registry.tsl(ts, 2);
    const auto *nested = registry.tsb("ForwardOwnershipNested", {{"value", ts}});
    const auto *root = registry.tsb("ForwardOwnershipRoot", {{"whole", list}, {"nested", nested}});
    const auto endpoint = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(list),
            TSEndpointSchema::non_peered(nested, {TSEndpointSchema::peered(ts)}),
        });

    TSOutput list_source{list};
    TSOutput scalar_source{ts};
    set_list_output(list_source, 0, 1, MIN_ST);
    set_scalar_output(scalar_source, 2, MIN_ST);

    TSInput independent{TSInputBuilderFactory::checked_builder_for(*list, TSEndpointSchema::peered(list))};
    auto independent_view = independent.view(nullptr, MIN_ST);
    independent_view.bind_output(list_source.view(MIN_ST));

    TSInput root_consumer{TSInputBuilderFactory::checked_builder_for(*root, TSEndpointSchema::peered(root))};
    TSInput child_consumer{TSInputBuilderFactory::checked_builder_for(*ts, TSEndpointSchema::peered(ts))};
    RecordingNotifier root_scheduling;
    auto root_view = root_consumer.view(&root_scheduling, MIN_ST);
    auto child_view = child_consumer.view(nullptr, MIN_ST);

    std::optional<TSOutput> forwarding{std::in_place, endpoint};
    auto forwarding_view = forwarding->view(MIN_ST);
    auto forwarding_bundle = forwarding_view.as_bundle();
    auto forwarding_whole = forwarding_bundle.field("whole");
    auto forwarding_nested_view = forwarding_bundle.field("nested");
    auto forwarding_nested = forwarding_nested_view.as_bundle();
    auto forwarding_leaf = forwarding_nested.field("value");
    forwarding_whole.bind_forwarding_target(list_source.view(MIN_ST));
    forwarding_leaf.bind_forwarding_target(scalar_source.view(MIN_ST));
    auto forwarding_local_root = forwarding_view.data_view().borrowed_ref();
    auto forwarding_local_whole = detail::input_child_projection(forwarding_local_root, 0).target_link;
    auto forwarding_local_nested = detail::input_child_projection(forwarding_local_root, 1).visible;
    auto forwarding_local_leaf = detail::input_child_projection(forwarding_local_nested, 0).target_link;
    REQUIRE(forwarding_local_whole.path_from_root() == std::vector<std::size_t>{0});
    REQUIRE(forwarding_local_leaf.path_from_root() == std::vector<std::size_t>{1, 0});
    root_view.bind_output(forwarding_view.borrowed_ref());
    root_view.make_active();
    child_view.bind_output(forwarding_leaf.borrowed_ref());

    REQUIRE(root_view.bound());
    REQUIRE(root_view.active());
    REQUIRE(child_view.bound());
    REQUIRE_FALSE(child_view.active());
    REQUIRE(independent_view.bound());
    REQUIRE(list_source.data_view().parent_link().parent_output() == &list_source);

    root_scheduling.notifications.clear();
    set_scalar_output(scalar_source, 3, MIN_ST + TimeDelta{1});
    REQUIRE_FALSE(root_scheduling.notifications.empty());
    forwarding.reset();

    REQUIRE_FALSE(root_view.bound());
    REQUIRE(root_view.active());
    REQUIRE_FALSE(child_view.bound());
    REQUIRE(independent_view.bound());
    REQUIRE(list_source.data_view().parent_link().parent_output() == &list_source);

    set_list_output(list_source, 0, 4, MIN_ST + TimeDelta{2});
    auto independent_after = independent.view(nullptr, MIN_ST + TimeDelta{2});
    auto independent_list = independent_after.as_list();
    REQUIRE(independent_after.bound());
    REQUIRE(independent_list[0].value().checked_as<std::int32_t>() == 4);
}

TEST_CASE("fixed ownership traversal has the same local boundary for Data Input and Output roles")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *list = registry.tsl(ts, 2);
    const auto *root_schema = registry.tsb("RoleOwnershipBoundary", {{"items", list}});

    const auto verify = [](TSDataView root, TypeRole role) {
        REQUIRE(root.type_ref().role() == role);
        auto bundle = root.as_bundle();
        auto list_data = bundle.field("items");
        auto list_view = list_data.as_list();
        auto leaf = list_view[1];
        REQUIRE(list_data.type_ref().role() == role);
        REQUIRE(leaf.type_ref().role() == role);
        REQUIRE(list_data.parent_link().parent_data() == root.data());
        REQUIRE(leaf.parent_link().parent_data() == root.data());
        REQUIRE(leaf.path_from_root() == std::vector<std::size_t>{0, 1});
        REQUIRE(leaf.root_view().data() == root.data());
    };

    TSData data{TSDataPlanFactory::instance().data_type_for(root_schema)};
    verify(data.view(), TypeRole::Data);

    TSInput input{TSInputBuilderFactory::checked_builder_for(
        *root_schema, TSEndpointSchema::owned(root_schema))};
    verify(input.view().data_view().borrowed_ref(), TypeRole::Input);

    TSOutput output{root_schema};
    verify(output.data_view(), TypeRole::Output);
}

TEST_CASE("ownership traversal invalidates keyed children")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *schema = registry.tsd(integer, registry.ts(integer));
    InvalidationRecorder root_observer;
    InvalidationRecorder child_observer;

    {
        TSOutput output{schema};
        Value key{std::int32_t{1}};
        Value stored{std::int32_t{2}};
        auto root = output.data_view();
        auto dict = root.as_dict();
        auto mutation = dict.begin_mutation(MIN_ST);
        auto child = mutation.at(key.view());
        REQUIRE(child.begin_mutation(MIN_ST).copy_value_from(stored.view()));
        root.subscribe(&root_observer);
        child.subscribe(&child_observer);
    }

    REQUIRE(root_observer.invalidations == 1);
    REQUIRE(child_observer.invalidations == 1);
}

TEST_CASE("fixed structured role caches are canonical and thread stable")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto *schema = registry.tsl(ts, 3);
    const auto expected = factory.output_type_for(schema);

    std::array<const TypeRecord *, 8> records{};
    std::array<std::thread, 8> threads{};
    for (std::size_t index = 0; index < threads.size(); ++index)
        threads[index] = std::thread([&, index] { records[index] = factory.output_type_for(schema).record(); });
    for (auto &thread : threads) thread.join();
    for (const auto *record : records) REQUIRE(record == expected.record());
}

TEST_CASE("fixed input records describe owned target composite and embedded topology")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto *list = registry.tsl(ts, 2);
    const auto *root = registry.tsb("FixedInputRoles", {{"owned", ts}, {"target", ts}, {"items", list}});

    TSInput owned{TSInputBuilderFactory::checked_builder_for(*root, TSEndpointSchema::owned(root))};
    REQUIRE(owned.type_ref().schema() == root);
    REQUIRE(std::string{owned.type_ref().record()->implementation_name()} == "ts.fixed.input.owned");

    TSInput target{TSInputBuilderFactory::checked_builder_for(*root, TSEndpointSchema::peered(root))};
    REQUIRE(target.type_ref().schema() == root);
    REQUIRE(std::string{target.type_ref().record()->implementation_name()} == "ts.fixed.input.target");

    const auto composite_schema = TSEndpointSchema::non_peered(
        root,
        {TSEndpointSchema::owned(ts), TSEndpointSchema::peered(ts),
         TSEndpointSchema::non_peered_list(list, TSEndpointSchema::owned(ts))});
    TSInput composite{TSInputBuilderFactory::checked_builder_for(*root, composite_schema)};
    const auto root_type = composite.type_ref();
    REQUIRE(root_type.schema() == root);
    REQUIRE(std::string{root_type.record()->implementation_name()} == "ts.fixed.input.composite");
    REQUIRE(has_capability(root_type.capabilities(), TypeCapabilities::HasChildren));
    REQUIRE_FALSE(has_capability(root_type.capabilities(), TypeCapabilities::Mutable));

    auto composite_view = composite.view();
    auto bundle = composite_view.as_bundle();
    const auto owned_type = bundle.field("owned").type_ref();
    const auto target_type = bundle.field("target").type_ref();
    const auto list_type = bundle.field("items").type_ref();
    REQUIRE(owned_type.as_role().role() == TypeRole::Input);
    REQUIRE(std::string{owned_type.record()->implementation_name()} == "ts.fixed.input.embedded");
    REQUIRE(target_type.as_role().role() == TypeRole::Input);
    REQUIRE(std::string{target_type.record()->implementation_name()} == "ts.fixed.input.target");
    REQUIRE(list_type.as_role().role() == TypeRole::Input);
    REQUIRE(std::string{list_type.record()->implementation_name()} == "ts.fixed.input.embedded");
    auto items = bundle.field("items");
    auto item_list = items.as_list();
    REQUIRE(item_list[0].type_ref().as_role().role() == TypeRole::Input);
}

TEST_CASE("keyed and reference role records preserve root input and projection topology")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tss = registry.tss(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto *ref = registry.ref(ts);

    const auto label = [](auto type) {
        REQUIRE(type);
        return std::string{type.record()->implementation_name()};
    };

    REQUIRE(label(factory.data_type_for(tss).as_role()) == "ts.tss.data.root");
    REQUIRE(label(factory.output_type_for(tss).as_role()) == "ts.tss.output.root");
    REQUIRE(label(factory.data_type_for(tsd).as_role()) == "ts.tsd.data.root");
    REQUIRE(label(factory.output_type_for(tsd).as_role()) == "ts.tsd.output.root");
    REQUIRE(label(factory.data_type_for(ref).as_role()) == "ts.ref.data.root");
    REQUIRE(label(factory.output_type_for(ref).as_role()) == "ts.ref.output.root");

    TSData dict_data{factory.data_type_for(tsd)};
    const auto set_data_type = factory.data_type_for(tss);
    REQUIRE(set_data_type.ops_ref().layout_impl(set_data_type.ops_ref().context)->value_binding.record()->debug != nullptr);
    const auto dict_data_type = factory.data_type_for(tsd);
    REQUIRE(dict_data_type.ops_ref().layout_impl(dict_data_type.ops_ref().context)->value_binding.record()->debug != nullptr);
    auto dict_view = dict_data.view();
    auto dict = dict_view.as_dict();
    REQUIRE(label(dict.key_set().base().type_ref()) == "ts.tsd.key-set.data");
    REQUIRE(label(dict.layout().element_type) == "ts.tsd.value.data");

    TSOutput dict_output{tsd};
    auto output_view = dict_output.data_view();
    auto output_dict = output_view.as_dict();
    REQUIRE(label(output_dict.key_set().base().type_ref()) == "ts.tsd.key-set.output");
    REQUIRE(label(output_dict.layout().element_type) == "ts.tsd.value.output");

    for (const auto &[schema, owned_label, target_label] : std::array{
             std::tuple{tss, "ts.tss.input.owned", "ts.tss.input.target"},
             std::tuple{tsd, "ts.tsd.input.owned", "ts.tsd.input.target"},
             std::tuple{ref, "ts.ref.input.owned", "ts.ref.input.target"}})
    {
        TSInput owned{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::owned(schema))};
        TSInput target{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
        REQUIRE(label(owned.type_ref()) == owned_label);
        REQUIRE(label(target.type_ref()) == target_label);
    }

    const auto composite_schema = TSEndpointSchema::non_peered(
        tsd, {TSEndpointSchema::peered(ts)});
    TSInput composite{TSInputBuilderFactory::checked_builder_for(*tsd, composite_schema)};
    REQUIRE(label(composite.type_ref()) == "ts.tsd.input.composite");
    auto composite_dict = composite.view().data_view().as_dict();
    REQUIRE(label(composite_dict.key_set().base().type_ref()) == "ts.tsd.key-set.input");
    REQUIRE(label(composite_dict.layout().element_type) == "ts.tsd.value.input");

    const auto proxy_data = tsd_proxy_data_type_for(
        *tsd, TSRoleTypeRef{factory.data_type_for(ts).as_role()});
    const auto proxy_output = tsd_proxy_output_type_for(
        *tsd, TSRoleTypeRef{factory.output_type_for(ts).as_role()});
    REQUIRE(label(proxy_data.as_role()) == "ts.tsd.proxy.data");
    REQUIRE(label(proxy_output.as_role()) == "ts.tsd.proxy.output");
}

TEST_CASE("TSB strategies publish debugger fields through inspection operations")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *bundle = registry.tsb(
        "InspectionOpsBundle", {{"left", ts}, {"right", ts}});

    TSInput owned{TSInputBuilderFactory::checked_builder_for(
        *bundle, TSEndpointSchema::owned(bundle))};
    TSInput composite{TSInputBuilderFactory::checked_builder_for(
        *bundle,
        TSEndpointSchema::non_peered(
            bundle, {TSEndpointSchema::peered(ts),
                     TSEndpointSchema::peered(ts)}))};
    TSInput target{TSInputBuilderFactory::checked_builder_for(
        *bundle, TSEndpointSchema::peered(bundle))};

    const std::array<TSRoleTypeRef, 5> types{
        factory.data_type_for(bundle).as_role(),
        factory.output_type_for(bundle).as_role(),
        owned.type_ref().as_role(),
        composite.type_ref().as_role(),
        target.type_ref().as_role(),
    };
    for (const auto type : types)
    {
        const auto &ops = type.ops_ref();
        REQUIRE(ops.inspection_ops != nullptr);
        REQUIRE(ops.inspection_ops->field_count_impl != nullptr);
        REQUIRE(ops.inspection_ops->field_at_impl != nullptr);
        REQUIRE(ops.inspection_ops->field_count_impl(ops.context) == 2);

        const auto left = ops.inspection_ops->field_at_impl(ops.context, 0);
        const auto right = ops.inspection_ops->field_at_impl(ops.context, 1);
        CHECK(std::string{left.name} == "left");
        CHECK(std::string{right.name} == "right");
        CHECK(left.type.schema() == ts);
        CHECK(right.type.schema() == ts);
        const auto *debug = type.record()->debug;
        REQUIRE(debug != nullptr);
        REQUIRE(debug->field_count == 2);
        CHECK(debug->fields[0].offset == left.data_offset);
        CHECK(debug->fields[1].offset == right.data_offset);
        CHECK(debug->fields[0].type == left.type.record());
        CHECK(debug->fields[1].type == right.type.record());
    }
}

TEST_CASE("TSD value projection preserves an independently supplied opaque operations table")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *bundle = registry.tsb("OpaqueProjectionBundle", {{"value", ts}});
    const auto built_in = factory.data_type_for(bundle).as_role();

    struct ExtensionIndexedOps : IndexedTSDataOps
    {
        std::uintptr_t extension_marker{0x48524752u};
    };
    ExtensionIndexedOps extension_ops;
    static_cast<IndexedTSDataOps &>(extension_ops) =
        static_cast<const IndexedTSDataOps &>(built_in.ops_ref());
    // Projection must preserve this independently supplied, extended table
    // without copying or narrowing it to a built-in table layout.
    const auto extension = intern_ts_type(
        *bundle, TypeRole::Data, built_in.checked_plan(), extension_ops,
        "consumer.opaque.bundle.data");
    const auto projected = ts_data_plan_factory_detail::tsd_value_projection_type(
        extension, TypeRole::Output);
    const auto repeated = ts_data_plan_factory_detail::tsd_value_projection_type(
        extension, TypeRole::Output);

    REQUIRE(extension.ops() == &extension_ops);
    REQUIRE(projected.ops() == &extension_ops);
    REQUIRE(projected.record() != extension.record());
    REQUIRE(projected.record() == repeated.record());
    REQUIRE(projected.role() == TypeRole::Output);
    REQUIRE(projected.record()->implementation_name() == "ts.tsd.value.output");
}

TEST_CASE("keyed projection identities reseed every role without stale records")
{
    using namespace hgraph;

    const auto exercise_generation = [] {
        auto &registry = TypeRegistry::instance();
        auto &factory = TSDataPlanFactory::instance();
        const auto *integer = registry.register_scalar<std::int32_t>("int32");
        const auto *ts = registry.ts(integer);
        const auto *tss = registry.tss(integer);
        const auto *inner_tsd = registry.tsd(integer, ts);
        const auto *bundle = registry.tsb("ProjectionResetBundle", {{"value", ts}});
        const auto *ref = registry.ref(ts);

        std::vector<std::string> labels;
        for (const auto *element : std::array{ts, tss, inner_tsd, bundle, ref})
        {
            const auto *outer = registry.tsd(integer, element);

            TSData data{factory.data_type_for(outer)};
            auto data_view = data.view();
            auto data_dict = data_view.as_dict();
            const auto data_element_type = data_dict.layout().element_type;
            labels.emplace_back(data_element_type.record()->implementation_name());
            REQUIRE(data_element_type.role() == TypeRole::Data);
            REQUIRE(data_element_type.ops() == factory.data_type_for(element).ops());

            TSOutput output{outer};
            auto output_data = output.data_view();
            auto output_dict = output_data.as_dict();
            const auto output_element_type = output_dict.layout().element_type;
            labels.emplace_back(output_element_type.record()->implementation_name());
            REQUIRE(output_element_type.role() == TypeRole::Output);
            REQUIRE(output_element_type.ops() == factory.output_type_for(element).ops());

            const auto endpoint = TSEndpointSchema::non_peered(
                outer, {TSEndpointSchema::peered(element)});
            TSInput input{TSInputBuilderFactory::checked_builder_for(*outer, endpoint)};
            auto input_view = input.view();
            auto input_dict = input_view.data_view().as_dict();
            const auto input_element_type = input_dict.layout().element_type;
            labels.emplace_back(input_element_type.record()->implementation_name());
            REQUIRE(input_element_type.role() == TypeRole::Input);
        }
        return labels;
    };

    const auto first = exercise_generation();
    REQUIRE(first.size() == 15);
    reset_all_registries();
    const auto second = exercise_generation();
    REQUIRE(second == first);
}

TEST_CASE("dynamic TSL and TSW role records are canonical distinct and exactly labelled")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *dynamic = registry.tsl(ts, 0);
    const auto *tick = registry.tsw(integer, 3, 1);
    const auto *duration = registry.tsw_duration(integer, TimeDelta{10}, TimeDelta{2});

    struct RoleCase
    {
        const TSValueTypeMetaData *schema;
        const char *data_label;
        const char *input_label;
        const char *output_label;
        bool has_children;
    };
    const std::array cases{
        RoleCase{dynamic, "ts.tsl.dynamic.data.root", "ts.tsl.dynamic.input.owned",
                 "ts.tsl.dynamic.output.root", true},
        RoleCase{tick, "ts.tsw.tick.data.root", "ts.tsw.tick.input.owned",
                 "ts.tsw.tick.output.root", false},
        RoleCase{duration, "ts.tsw.duration.data.root", "ts.tsw.duration.input.owned",
                 "ts.tsw.duration.output.root", false},
    };

    REQUIRE_THROWS_AS(TSEndpointSchema::non_peered(dynamic, {}), std::invalid_argument);
    REQUIRE_THROWS_AS(TSEndpointSchema::non_peered(tick, {}), std::invalid_argument);

    const auto dynamic_composite = TSEndpointSchema::non_peered_list(
        dynamic, TSEndpointSchema::peered(ts));
    REQUIRE(dynamic_composite.is_non_peered());
    REQUIRE(dynamic_composite.child_count() == 1);
    REQUIRE(dynamic_composite.child(0).schema() == ts);
    TSInput dynamic_composite_input{
        TSInputBuilderFactory::checked_builder_for(
            *dynamic, dynamic_composite)};
    REQUIRE(std::string{
                dynamic_composite_input.type_ref().record()->implementation_name()} ==
            "ts.tsl.dynamic.input.composite");

    for (const auto &item : cases)
    {
        const auto data = factory.data_type_for(item.schema);
        const auto output = factory.output_type_for(item.schema);
        TSInput owned{TSInputBuilderFactory::checked_builder_for(
            *item.schema, TSEndpointSchema::owned(item.schema))};
        TSInput target{TSInputBuilderFactory::checked_builder_for(
            *item.schema, TSEndpointSchema::peered(item.schema))};

        REQUIRE(data.record() != output.record());
        REQUIRE(data.record() != owned.type_ref().record());
        REQUIRE(output.record() != owned.type_ref().record());
        REQUIRE(data.plan() == output.plan());
        REQUIRE(data.plan() == owned.type_ref().plan());
        REQUIRE(data.record()->ops_abi_version == TS_DATA_OPS_ABI_VERSION);
        REQUIRE(output.record()->ops_abi_version == TS_DATA_OPS_ABI_VERSION);
        REQUIRE(owned.type_ref().record()->ops_abi_version == TS_DATA_OPS_ABI_VERSION);
        REQUIRE(has_capability(data.capabilities(), TypeCapabilities::Mutable));
        REQUIRE(has_capability(output.capabilities(), TypeCapabilities::Mutable));
        REQUIRE_FALSE(has_capability(owned.type_ref().capabilities(), TypeCapabilities::Mutable));
        REQUIRE_FALSE(has_capability(target.type_ref().capabilities(), TypeCapabilities::Mutable));
        REQUIRE(std::string{data.record()->implementation_name()} == item.data_label);
        REQUIRE(std::string{owned.type_ref().record()->implementation_name()} == item.input_label);
        REQUIRE(std::string{output.record()->implementation_name()} == item.output_label);
        REQUIRE(std::string{target.type_ref().record()->implementation_name()} ==
                (item.schema->kind == TSTypeKind::TSL ? "ts.tsl.dynamic.input.target"
                                                      : "ts.tsw.input.target"));
        REQUIRE(has_capability(data.capabilities(), TypeCapabilities::HasChildren) == item.has_children);
        REQUIRE(has_capability(output.capabilities(), TypeCapabilities::HasChildren) == item.has_children);
        TSData input_storage{owned.type_ref()};
        REQUIRE_THROWS_AS(input_storage.view().begin_mutation(MIN_ST), std::logic_error);
        REQUIRE(factory.data_type_for(item.schema).record() == data.record());
        REQUIRE(factory.output_type_for(item.schema).record() == output.record());
    }
}

TEST_CASE("dynamic TSL and TSW records preserve roles through fixed parents")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *dynamic = registry.tsl(ts, 0);
    const auto *tick = registry.tsw(integer, 3, 1);
    const auto *duration = registry.tsw_duration(integer, TimeDelta{10}, TimeDelta{2});
    const auto *root = registry.tsb("DynamicRoleParent", {
        {"dynamic", dynamic}, {"tick", tick}, {"duration", duration}});

    TSData data{TSDataPlanFactory::instance().data_type_for(root)};
    TSOutput output{root};
    TSInput input{TSInputBuilderFactory::checked_builder_for(*root, TSEndpointSchema::owned(root))};

    struct ParentCase
    {
        TSDataView view;
        TypeRole role;
        std::array<const char *, 3> labels;
    };
    std::array parents{
        ParentCase{data.view(), TypeRole::Data,
                   {"ts.tsl.dynamic.data.embedded", "ts.tsw.tick.data.embedded",
                    "ts.tsw.duration.data.embedded"}},
        ParentCase{output.data_view(), TypeRole::Output,
                   {"ts.tsl.dynamic.output.embedded", "ts.tsw.tick.output.embedded",
                    "ts.tsw.duration.output.embedded"}},
        ParentCase{input.view().data_view().borrowed_ref(), TypeRole::Input,
                   {"ts.tsl.dynamic.input.embedded", "ts.tsw.tick.input.embedded",
                    "ts.tsw.duration.input.embedded"}},
    };

    for (auto &parent : parents)
    {
        auto bundle = parent.view.as_bundle();
        std::array children{bundle.field("dynamic"), bundle.field("tick"), bundle.field("duration")};
        for (std::size_t index = 0; index < children.size(); ++index)
        {
            REQUIRE(children[index].type_ref().role() == parent.role);
            REQUIRE(std::string{children[index].type_ref().record()->implementation_name()} == parent.labels[index]);
            REQUIRE(children[index].type_ref().plan() ==
                    TSDataPlanFactory::instance().plan_for(children[index].schema()));
        }
        auto dynamic_child = bundle.field("dynamic");
        auto dynamic_list = dynamic_child.as_list();
        const auto &dynamic_layout = static_cast<const FixedTSLDataLayout &>(dynamic_list.layout());
        REQUIRE(dynamic_layout.element_type.role() == parent.role);
    }
}

TEST_CASE("dynamic TSL and TSW output records are thread stable")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *dynamic = registry.tsl(registry.ts(integer), 0);
    const auto *window = registry.tsw(integer, 3, 1);

    for (const auto *schema : {dynamic, window})
    {
        const auto expected = factory.output_type_for(schema);
        std::array<const TypeRecord *, 8> records{};
        std::array<std::thread, 8> threads{};
        for (std::size_t index = 0; index < threads.size(); ++index)
            threads[index] = std::thread([&, index] { records[index] = factory.output_type_for(schema).record(); });
        for (auto &thread : threads) thread.join();
        for (const auto *record : records) REQUIRE(record == expected.record());
    }
}

TEST_CASE("dynamic TSL teardown invalidates nested TSData before child storage destruction")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto *bundle_schema = registry.tsb("DynamicOwnedChild", {{"value", ts}});
    const auto *dynamic = registry.tsl(bundle_schema, 0);

    InvalidationRecorder child_observer;
    InvalidationRecorder leaf_observer;
    const TSDataTracking *child_tracking = nullptr;
    const TSDataTracking *leaf_tracking = nullptr;
    {
        auto output = std::make_unique<TSOutput>(dynamic);
        auto root = output->data_view();
        auto child = root.ensure_indexed_child_at(0);
        auto bundle = child.as_bundle();
        auto leaf = bundle.field("value");
        child_tracking = &child.tracking();
        leaf_tracking = &leaf.tracking();
        child.subscribe(&child_observer);
        leaf.subscribe(&leaf_observer);
    }

    REQUIRE(child_observer.invalidations == 1);
    REQUIRE(leaf_observer.invalidations == 1);
    REQUIRE(child_observer.invalidated_source == child_tracking);
    REQUIRE(leaf_observer.invalidated_source == leaf_tracking);
}

TEST_CASE("scalar output and peered input preserve binding, timing, and subscriptions")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    TSOutput first{schema};
    TSOutput second{schema};
    TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    RecordingNotifier notifier;
    auto in = input.view(&notifier, MIN_ST);

    REQUIRE(first.type_ref().record() != nullptr);
    REQUIRE(input.type_ref().record() != nullptr);
    REQUIRE(first.schema() == schema);
    REQUIRE(first.data_view().schema() == schema);
    REQUIRE(first.view(MIN_ST).schema() == schema);
    REQUIRE(first.type_ref().record() != input.type_ref().record());
    REQUIRE(in.is_bindable());
    REQUIRE_FALSE(in.bound());
    in.bind_output(first.view(MIN_ST));
    in.make_active();
    REQUIRE(in.bound());
    REQUIRE(first.data_view().observer_count() == 1);

    Value one{std::int32_t{1}};
    {
        auto mutation = first.view(MIN_ST).begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    REQUIRE(in.value().checked_as<std::int32_t>() == 1);
    REQUIRE(in.modified());
    REQUIRE(in.delta_value().checked_as<std::int32_t>() == 1);
    REQUIRE(notifier.notifications == std::vector<DateTime>{MIN_ST});

    in.bind_output(second.view(MIN_ST + TimeDelta{1}));
    REQUIRE(first.data_view().observer_count() == 0);
    REQUIRE(second.data_view().observer_count() == 1);
    in.unbind_output();
    REQUIRE(second.data_view().observer_count() == 0);
    REQUIRE_FALSE(in.bound());
}

TEST_CASE("scalar output invalidation detaches passive inputs and supports rebind")
{
    using namespace hgraph;
    const auto *schema = TypeRegistry::instance().ts(
        TypeRegistry::instance().register_scalar<std::int32_t>("int32"));
    TSInput input = scalar_input(schema);
    auto in = input.view(nullptr, MIN_ST);

    std::optional<TSOutput> first{std::in_place, schema};
    in.bind_output(first->view(MIN_ST));
    REQUIRE(in.bound());
    REQUIRE(first->data_view().observer_count() == 1);

    first.reset();
    REQUIRE_FALSE(in.bound());
    REQUIRE_FALSE(in.valid());

    TSOutput replacement{schema};
    Value value{std::int32_t{42}};
    {
        auto mutation = replacement.view(MIN_ST + TimeDelta{1}).begin_mutation(MIN_ST + TimeDelta{1});
        REQUIRE(mutation.copy_value_from(value.view()));
    }
    in.bind_output(replacement.view(MIN_ST + TimeDelta{1}));
    REQUIRE(in.bound());
    REQUIRE(in.valid());
    REQUIRE(in.value().checked_as<std::int32_t>() == 42);
    REQUIRE(replacement.data_view().observer_count() == 1);
}

TEST_CASE("scalar output invalidation preserves active topology without scheduling")
{
    using namespace hgraph;
    const auto *schema = TypeRegistry::instance().ts(
        TypeRegistry::instance().register_scalar<std::int32_t>("int32"));
    TSInput input = scalar_input(schema);
    RecordingNotifier scheduling;
    auto in = input.view(&scheduling, MIN_ST);

    auto first = std::make_unique<TSOutput>(schema);
    in.bind_output(first->view(MIN_ST));
    in.make_active();
    REQUIRE(in.active());
    REQUIRE(first->data_view().observer_count() == 1);

    first.reset();
    REQUIRE_FALSE(in.bound());
    REQUIRE(in.active());
    REQUIRE(scheduling.notifications.empty());

    TSOutput replacement{schema};
    Value one{std::int32_t{1}};
    Value two{std::int32_t{2}};
    {
        auto mutation = replacement.view(MIN_ST + TimeDelta{1}).begin_mutation(MIN_ST + TimeDelta{1});
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    in.bind_output(replacement.view(MIN_ST + TimeDelta{1}));
    REQUIRE(in.active());
    REQUIRE(replacement.data_view().observer_count() == 1);

    scheduling.notifications.clear();
    {
        auto mutation = replacement.view(MIN_ST + TimeDelta{2}).begin_mutation(MIN_ST + TimeDelta{2});
        REQUIRE(mutation.copy_value_from(two.view()));
    }
    REQUIRE(scheduling.notifications == std::vector<DateTime>{MIN_ST + TimeDelta{2}});

    in.make_passive();
    REQUIRE_FALSE(in.active());
    REQUIRE(replacement.data_view().observer_count() == 1);
    scheduling.notifications.clear();
    {
        auto mutation = replacement.view(MIN_ST + TimeDelta{3}).begin_mutation(MIN_ST + TimeDelta{3});
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    REQUIRE(scheduling.notifications.empty());
}

TEST_CASE("root output invalidation detaches direct inputs for migrated and legacy shapes")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const std::array schemas{
        registry.signal(),
        registry.tss(integer),
        registry.tsl(ts, 2),
        registry.tsw(integer, 3, 1),
    };

    for (const auto *schema : schemas)
    {
        for (const bool active : {false, true})
        {
            CAPTURE(static_cast<int>(schema->kind), active);
            TSInput input{
                TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
            RecordingNotifier scheduling;
            auto in = input.view(&scheduling, MIN_ST);
            std::optional<TSOutput> output{std::in_place, schema};
            in.bind_output(output->view(MIN_ST));
            if (active) { in.make_active(); }

            REQUIRE(in.bound());
            REQUIRE(in.active() == active);
            REQUIRE(output->data_view().observer_count() == 1);
            scheduling.notifications.clear();

            output.reset();
            REQUIRE_FALSE(in.bound());
            REQUIRE(in.active() == active);
            REQUIRE(scheduling.notifications.empty());
        }
    }
}

TEST_CASE("SIGNAL inputs release legacy root outputs before input teardown")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *signal = registry.signal();
    const std::array legacy_roots{
        registry.tss(integer),
        registry.tsl(ts, 2),
        registry.tsw(integer, 3, 1),
    };

    for (const auto *root_schema : legacy_roots)
    {
        for (const bool active : {false, true})
        {
            CAPTURE(static_cast<int>(root_schema->kind), active);
            TSInput input{
                TSInputBuilderFactory::checked_builder_for(*signal, TSEndpointSchema::peered(signal))};
            RecordingNotifier scheduling;
            auto in = input.view(&scheduling, MIN_ST);
            std::optional<TSOutput> output{std::in_place, root_schema};
            in.bind_output(output->view(MIN_ST));
            if (active) { in.make_active(); }

            REQUIRE(in.bound());
            REQUIRE(in.bound_output().handle().bound());
            REQUIRE(in.active() == active);
            REQUIRE(output->data_view().observer_count() == 1);
            scheduling.notifications.clear();

            output.reset();
            REQUIRE_FALSE(in.bound());
            REQUIRE_FALSE(in.bound_output().handle().bound());
            REQUIRE(in.active() == active);
            REQUIRE(scheduling.notifications.empty());
        }
    }
}

TEST_CASE("root invalidation clears borrowed target state before slot callbacks")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.tss(registry.register_scalar<std::int32_t>("int32"));
    TSOutput forwarding{TSEndpointSchema::peered(schema)};
    ForwardingClearRecorder observer;
    observer.forwarding = &forwarding;
    observer.evaluation_time = MIN_ST;
    std::optional<TSOutput> producer{std::in_place, schema};
    auto forwarding_view = forwarding.view(MIN_ST);
    forwarding_view.bind_forwarding_target(producer->view(MIN_ST));
    auto forwarding_data = forwarding_view.data_view().borrowed_ref();
    auto forwarding_set = forwarding_data.as_set();
    forwarding_set.subscribe_slot_observer(&observer);

    producer.reset();
    REQUIRE(observer.clears == 1);
    REQUIRE(observer.saw_empty_target);
    REQUIRE_FALSE(forwarding_view.forwarding_target().bound());

    forwarding_set.unsubscribe_slot_observer(&observer);
}

TEST_CASE("scalar input teardown and move keep producer subscriptions coherent")
{
    using namespace hgraph;
    const auto *schema = TypeRegistry::instance().ts(
        TypeRegistry::instance().register_scalar<std::int32_t>("int32"));
    TSOutput output{schema};
    RecordingNotifier scheduling;
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    {
        TSInput input = scalar_input(schema);
        auto in = input.view(&scheduling, t1);
        in.bind_output(output.view(t1));
        in.make_active();
        REQUIRE(output.data_view().observer_count() == 1);

        TSInput moved{std::move(input)};
        auto moved_view = moved.view(&scheduling, t1);
        REQUIRE(moved_view.bound());
        REQUIRE(moved_view.active());
        REQUIRE(output.data_view().observer_count() == 1);

        Value value{std::int32_t{7}};
        auto mutation = output.view(t1).begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(value.view()));
        REQUIRE(scheduling.notifications == std::vector<DateTime>{t1});
    }

    REQUIRE(output.data_view().observer_count() == 0);
    Value value{std::int32_t{8}};
    {
        auto mutation = output.view(t2).begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(value.view()));
    }
}

TEST_CASE("scalar output copy and move replacement invalidate published bindings")
{
    using namespace hgraph;
    const auto *schema = TypeRegistry::instance().ts(
        TypeRegistry::instance().register_scalar<std::int32_t>("int32"));
    Value one{std::int32_t{1}};
    Value two{std::int32_t{2}};

    TSOutput source{schema};
    {
        auto mutation = source.view(MIN_ST).begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    TSInput source_input = scalar_input(schema);
    auto source_view = source_input.view(nullptr, MIN_ST);
    source_view.bind_output(source.view(MIN_ST));

    TSOutput moved{std::move(source)};
    REQUIRE_FALSE(source_view.bound());
    REQUIRE(moved.view(MIN_ST).valid());
    REQUIRE(moved.data_view().observer_count() == 0);

    TSOutput destination{schema};
    TSInput destination_input = scalar_input(schema);
    auto destination_view = destination_input.view(nullptr, MIN_ST);
    destination_view.bind_output(destination.view(MIN_ST));

    TSOutput replacement{schema};
    {
        auto mutation = replacement.view(MIN_ST + TimeDelta{1}).begin_mutation(MIN_ST + TimeDelta{1});
        REQUIRE(mutation.copy_value_from(two.view()));
    }
    TSInput replacement_input = scalar_input(schema);
    auto replacement_view = replacement_input.view(nullptr, MIN_ST + TimeDelta{1});
    replacement_view.bind_output(replacement.view(MIN_ST + TimeDelta{1}));

    destination = std::move(replacement);
    REQUIRE_FALSE(destination_view.bound());
    REQUIRE_FALSE(replacement_view.bound());
    REQUIRE(destination.view(MIN_ST + TimeDelta{1}).value().checked_as<std::int32_t>() == 2);
    REQUIRE(destination.data_view().observer_count() == 0);

    TSOutput copy_source{schema};
    {
        auto mutation = copy_source.view(MIN_ST + TimeDelta{2}).begin_mutation(MIN_ST + TimeDelta{2});
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    TSInput copy_source_input = scalar_input(schema);
    auto copy_source_view = copy_source_input.view(nullptr, MIN_ST + TimeDelta{2});
    copy_source_view.bind_output(copy_source.view(MIN_ST + TimeDelta{2}));
    TSInput copied_destination_input = scalar_input(schema);
    auto copied_destination_view = copied_destination_input.view(nullptr, MIN_ST + TimeDelta{2});
    copied_destination_view.bind_output(destination.view(MIN_ST + TimeDelta{2}));

    destination = copy_source;
    REQUIRE_FALSE(copied_destination_view.bound());
    REQUIRE(copy_source_view.bound());
    REQUIRE(copy_source.data_view().observer_count() == 1);
    REQUIRE(destination.data_view().observer_count() == 0);
}

TEST_CASE("scalar observer invalidation is detached and reentrant")
{
    using namespace hgraph;
    TSDataTracking tracking;
    ReentrantInvalidationRecorder first;
    InvalidationRecorder second;
    tracking.observers.subscribe(&first);
    tracking.observers.subscribe(&second);

    tracking.observers.invalidate(&tracking);
    REQUIRE(first.invalidations == 1);
    REQUIRE(second.invalidations == 1);
    REQUIRE(second.invalidated_source == &tracking);
    REQUIRE(tracking.observers.empty());
}

TEST_CASE("fixed output invalidates every published descendant and relinks copied storage")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto *list = registry.tsl(ts, 2);
    const auto *schema = registry.tsb("FixedLifetimeTree", {{"items", list}});

    InvalidationRecorder root_observer;
    InvalidationRecorder list_observer;
    InvalidationRecorder first_observer;
    InvalidationRecorder second_observer;
    const TSDataTracking *root_tracking = nullptr;
    const TSDataTracking *list_tracking = nullptr;
    const TSDataTracking *first_tracking = nullptr;
    const TSDataTracking *second_tracking = nullptr;
    {
        auto output = std::make_unique<TSOutput>(schema);
        auto root = output->data_view().borrowed_ref();
        auto bundle = root.as_bundle();
        auto list_data = bundle.field("items");
        auto items = list_data.as_list();
        auto first = items[0];
        auto second = items[1];
        root_tracking = &root.tracking();
        list_tracking = &list_data.tracking();
        first_tracking = &first.tracking();
        second_tracking = &second.tracking();
        root.subscribe(&root_observer);
        list_data.subscribe(&list_observer);
        first.subscribe(&first_observer);
        second.subscribe(&second_observer);

        TSOutput copied{*output};
        auto copied_root = copied.data_view().borrowed_ref();
        auto copied_bundle = copied_root.as_bundle();
        auto copied_list_data = copied_bundle.field("items");
        auto copied_items = copied_list_data.as_list();
        auto copied_leaf = copied_items[1];
        REQUIRE(copied_leaf.path_from_root() == std::vector<std::size_t>{0, 1});
        REQUIRE(copied_leaf.root_view().storage_type() == copied_root.storage_type());
        REQUIRE(copied_leaf.root_view().data() == copied_root.data());

        TSOutput moved{std::move(copied)};
        auto moved_root = moved.data_view().borrowed_ref();
        auto moved_bundle = moved_root.as_bundle();
        auto moved_list_data = moved_bundle.field("items");
        auto moved_items = moved_list_data.as_list();
        auto moved_leaf = moved_items[0];
        REQUIRE(moved_leaf.path_from_root() == std::vector<std::size_t>{0, 0});
        REQUIRE(moved_leaf.root_view().data() == moved_root.data());

        output.reset();
    }

    REQUIRE(root_observer.invalidations == 1);
    REQUIRE(list_observer.invalidations == 1);
    REQUIRE(first_observer.invalidations == 1);
    REQUIRE(second_observer.invalidations == 1);
    REQUIRE(root_observer.invalidated_source == root_tracking);
    REQUIRE(list_observer.invalidated_source == list_tracking);
    REQUIRE(first_observer.invalidated_source == first_tracking);
    REQUIRE(second_observer.invalidated_source == second_tracking);
}

TEST_CASE("scalar reverse member teardown and normal graph stop are safe")
{
    using namespace hgraph;
    const auto *schema = TypeRegistry::instance().ts(
        TypeRegistry::instance().register_scalar<std::int32_t>("int32"));
    struct InputBeforeOutput
    {
        explicit InputBeforeOutput(const TSValueTypeMetaData *type)
            : input{scalar_input(type)}, output{type}
        {
            input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
        }
        TSInput input;
        TSOutput output;
    };
    { InputBeforeOutput reverse{schema}; }

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<LifetimeSource>());
    builder.add_node(NodeBuilder{}.implementation<LifetimeSink>());
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}});
    testing::MockRootGraph graph{builder};
    auto view = graph.graph();
    view.start(MIN_ST);
    view.evaluate(MIN_ST);
    view.stop();
    REQUIRE_FALSE(view.started());
}

TEST_CASE("scalar SIGNAL role storage preserves typed-null and tick delta semantics")
{
    using namespace hgraph;
    const auto *signal = TypeRegistry::instance().signal();
    TSOutput output{signal};
    REQUIRE(output.type_ref().schema() == signal);
    REQUIRE_FALSE(output.view(MIN_ST).valid());
    REQUIRE_FALSE(output.view(MIN_ST).delta_value().has_value());

    Value tick{true};
    {
        auto mutation = output.view(MIN_ST).begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(tick.view()));
    }
    REQUIRE(output.view(MIN_ST).valid());
    REQUIRE(output.view(MIN_ST).delta_value().checked_as<bool>());
    REQUIRE_FALSE(output.view(MIN_ST + TimeDelta{1}).delta_value().has_value());
}

TEST_CASE("scalar role caches reset after owners are destroyed and reseed cleanly")
{
    using namespace hgraph;
    {
        auto &registry = TypeRegistry::instance();
        const auto *schema = registry.ts(registry.register_scalar<std::int32_t>("int32"));
        TSOutput output{schema};
        TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
        input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
    }

    reset_all_registries();

    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto data = TSDataPlanFactory::instance().data_type_for(schema);
    const auto output = TSDataPlanFactory::instance().output_type_for(schema);
    REQUIRE(schema->header.valid());
    REQUIRE(data.valid());
    REQUIRE(output.valid());
    REQUIRE(data.schema() == schema);
    REQUIRE(output.schema() == schema);
}

TEST_CASE("dynamic TSL and TSW role contexts reset and reseed without stale records")
{
    using namespace hgraph;
    const auto exercise_generation = [] {
        auto &registry = TypeRegistry::instance();
        auto &factory = TSDataPlanFactory::instance();
        const auto *integer = registry.register_scalar<std::int32_t>("int32");
        const auto *dynamic = registry.tsl(registry.ts(integer), 0);
        const auto *tick = registry.tsw(integer, 2, 1);
        const auto *duration = registry.tsw_duration(integer, TimeDelta{10}, TimeDelta{2});
        const auto *parent = registry.tsb("DynamicResetParent", {
            {"dynamic", dynamic}, {"tick", tick}, {"duration", duration}});

        std::vector<std::string> labels;
        for (const auto *schema : {dynamic, tick, duration})
        {
            TSData data{factory.data_type_for(schema)};
            TSOutput output{schema};
            TSInput owned{TSInputBuilderFactory::checked_builder_for(
                *schema, TSEndpointSchema::owned(schema))};
            TSInput target{TSInputBuilderFactory::checked_builder_for(
                *schema, TSEndpointSchema::peered(schema))};
            labels.emplace_back(data.type_ref().record()->implementation_name());
            labels.emplace_back(output.type_ref().record()->implementation_name());
            labels.emplace_back(owned.type_ref().record()->implementation_name());
            labels.emplace_back(target.type_ref().record()->implementation_name());
        }

        TSOutput parent_output{parent};
        auto parent_data = parent_output.data_view();
        auto parent_bundle = parent_data.as_bundle();
        for (const auto *name : {"dynamic", "tick", "duration"})
            labels.emplace_back(parent_bundle.field(name).type_ref().record()->implementation_name());
        return labels;
    };

    const auto first = exercise_generation();
    reset_all_registries();
    const auto second = exercise_generation();
    REQUIRE(second == first);
}

TEST_CASE("fixed role caches reset after owners are destroyed and reseed cleanly")
{
    using namespace hgraph;
    const TSValueTypeMetaData *old_schema = nullptr;
    const TypeRecord *old_data_record = nullptr;
    const TypeRecord *old_input_record = nullptr;
    const TypeRecord *old_output_record = nullptr;

    {
        auto &registry = TypeRegistry::instance();
        const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
        const auto *list = registry.tsl(ts, 2);
        old_schema = registry.tsb("FixedResetBundle", {{"items", list}, {"target", ts}});
        const auto endpoint = TSEndpointSchema::non_peered(
            old_schema,
            {TSEndpointSchema::non_peered_list(list, TSEndpointSchema::owned(ts)),
             TSEndpointSchema::peered(ts)});

        TSData data{TSDataPlanFactory::instance().data_type_for(old_schema)};
        TSInput input{TSInputBuilderFactory::checked_builder_for(*old_schema, endpoint)};
        TSOutput output{old_schema};
        old_data_record = data.type_ref().record();
        old_input_record = input.type_ref().record();
        old_output_record = output.type_ref().record();
        auto data_view = data.view();
        auto input_view = input.view();
        auto output_view = output.view(MIN_ST);
        auto data_bundle = data_view.as_bundle();
        auto input_bundle = input_view.as_bundle();
        auto output_bundle = output_view.as_bundle();
        REQUIRE(data_bundle.field("items").type_ref().role() == TypeRole::Data);
        REQUIRE(input_bundle.field("items").type_ref().as_role().role() == TypeRole::Input);
        REQUIRE(output_bundle.field("items").type_ref().as_role().role() == TypeRole::Output);
    }

    reset_all_registries();

    auto &registry = TypeRegistry::instance();
    const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto *list = registry.tsl(ts, 2);
    const auto *schema = registry.tsb("FixedResetBundle", {{"items", list}, {"target", ts}});
    const auto endpoint = TSEndpointSchema::non_peered(
        schema,
        {TSEndpointSchema::non_peered_list(list, TSEndpointSchema::owned(ts)),
         TSEndpointSchema::peered(ts)});
    TSData data{TSDataPlanFactory::instance().data_type_for(schema)};
    TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, endpoint)};
    TSOutput output{schema};

    REQUIRE(schema->header.valid());
    REQUIRE(data.type_ref().valid());
    REQUIRE(input.type_ref().valid());
    REQUIRE(output.type_ref().valid());
    REQUIRE(data.type_ref().role() == TypeRole::Data);
    REQUIRE(input.type_ref().as_role().role() == TypeRole::Input);
    REQUIRE(output.type_ref().as_role().role() == TypeRole::Output);
    REQUIRE(data.type_ref().ops()->context != nullptr);
    REQUIRE(input.type_ref().ops()->context != nullptr);
    REQUIRE(output.type_ref().ops()->context != nullptr);
    auto data_view = data.view();
    auto input_view = input.view();
    auto output_view = output.view(MIN_ST);
    auto data_bundle = data_view.as_bundle();
    auto input_bundle = input_view.as_bundle();
    auto output_bundle = output_view.as_bundle();
    REQUIRE(data_bundle.field("items").type_ref().valid());
    REQUIRE(input_bundle.field("items").type_ref().valid());
    REQUIRE(output_bundle.field("items").type_ref().valid());

    // Allocators may reuse schema and record addresses after reset. Either
    // outcome must reseed without stale ops/context conflicts.
    static_cast<void>(old_schema == schema);
    static_cast<void>(old_data_record == data.type_ref().record());
    static_cast<void>(old_input_record == input.type_ref().record());
    static_cast<void>(old_output_record == output.type_ref().record());
}
