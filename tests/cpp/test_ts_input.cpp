#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_input/target_link.h>
#include <hgraph/types/time_series/ts_input/target_link_structural_state.h>
#include <hgraph/types/value/compound_scalar_storage.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace
{
    struct NoDefaultTSInputSnapshot
    {
        explicit NoDefaultTSInputSnapshot(std::int32_t value_) : value{value_} {}
        NoDefaultTSInputSnapshot() = delete;
        auto operator<=>(const NoDefaultTSInputSnapshot &) const = default;

        std::int32_t value;
    };

    std::ostream &operator<<(std::ostream &out, const NoDefaultTSInputSnapshot &value)
    {
        return out << value.value;
    }

    struct RecordingNotifiable : hgraph::Notifiable
    {
        std::vector<hgraph::DateTime> notified{};

        void notify(hgraph::DateTime modified_time) override
        {
            notified.push_back(modified_time);
        }
    };

    [[nodiscard]] hgraph::TSEndpointSchema nested_input_schema(const hgraph::TSValueTypeMetaData *root,
                                                               const hgraph::TSValueTypeMetaData *nested,
                                                               const hgraph::TSValueTypeMetaData *scalar)
    {
        return hgraph::TSEndpointSchema::non_peered(
            root,
            {
                hgraph::TSEndpointSchema::peered(scalar),
                hgraph::TSEndpointSchema::non_peered(
                    nested,
                    {
                        hgraph::TSEndpointSchema::peered(scalar),
                    }),
            });
    }

    void set_output(hgraph::TSOutput &output, int value, hgraph::DateTime time)
    {
        hgraph::Value wrapped{value};
        auto mutation = output.view(time).begin_mutation(time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    void set_list_output(hgraph::TSOutput &output, std::size_t index, int value, hgraph::DateTime time)
    {
        hgraph::Value wrapped{value};
        auto          view = output.view(time);
        auto          list = view.as_list();
        auto          mutation = list[index].begin_mutation(time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    void set_bundle_output(hgraph::TSOutput &output,
                           std::string_view field,
                           int value,
                           hgraph::DateTime time)
    {
        hgraph::Value wrapped{value};
        auto          view = output.view(time);
        auto          bundle = view.as_bundle();
        auto          mutation = bundle.field(field).begin_mutation(time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    template <typename Range>
    [[nodiscard]] auto collect_range(const Range &range)
    {
        using value_type = std::decay_t<decltype(*range.begin())>;
        std::vector<value_type> result;
        for (auto value : range) { result.emplace_back(std::move(value)); }
        return result;
    }

    template <typename Range>
    [[nodiscard]] std::size_t range_size(const Range &range)
    {
        std::size_t result = 0;
        for (auto it = range.begin(); it != range.end(); ++it) { ++result; }
        return result;
    }
}

TEST_CASE("TSInput target-link plans allocate structural state only for keyed slots", "[memory]")
{
    using namespace hgraph;
    using namespace hgraph::detail;

    const auto &common_plan = target_link_storage_plan_for(TSTypeKind::TS);
    const auto &set_plan = target_link_storage_plan_for(TSTypeKind::TSS);
    const auto &dict_plan = target_link_storage_plan_for(TSTypeKind::TSD);

    REQUIRE(&common_plan == &target_link_storage_plan_for(TSTypeKind::TSL));
    REQUIRE(&common_plan == &target_link_storage_plan_for(TSTypeKind::TSW));
    REQUIRE(&common_plan == &target_link_storage_plan_for(TSTypeKind::TSB));
    REQUIRE(&common_plan == &target_link_storage_plan_for(TSTypeKind::REF));
    REQUIRE(&common_plan == &target_link_storage_plan_for(TSTypeKind::SIGNAL));
    REQUIRE(&set_plan == &dict_plan);
    REQUIRE(&common_plan != &set_plan);
    REQUIRE(common_plan.layout.size == sizeof(TSInputTargetLinkStorage));
    REQUIRE(set_plan.layout.size > common_plan.layout.size);

    MemoryUtils::ErasedOwner<> common_storage{common_plan};
    MemoryUtils::ErasedOwner<> structural_storage{set_plan};
    const auto *common_link =
        target_link_storage_access_for(TSTypeKind::TS).get_const(common_storage.data());
    const auto *structural_link =
        target_link_storage_access_for(TSTypeKind::TSS).get_const(structural_storage.data());

    REQUIRE(common_link != nullptr);
    REQUIRE(structural_link != nullptr);
    REQUIRE(static_cast<const void *>(common_link) == common_storage.data());
    REQUIRE(static_cast<const void *>(structural_link) == structural_storage.data());
    REQUIRE(static_cast<const void *>(&common_link->tracking) == common_storage.data());
    REQUIRE(static_cast<const void *>(&structural_link->tracking) == structural_storage.data());
}

TEST_CASE("TSInput storage caching excludes endpoint trees with owned payloads")
{
    using namespace hgraph;
    using detail::input_storage_type_is_realization_invariant;

    auto       &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *base = registry.bundle(
        "tests.ts_input_cache", "Base", {{"id", integer}}, {}, true);
    registry.bundle(
        "tests.ts_input_cache", "Leaf", {{"id", integer}, {"value", integer}}, {base});
    const auto *polymorphic = registry.ts(base);
    const auto *inner = registry.tsb("TSInputCacheableInner", {{"value", polymorphic}});
    const auto *root = registry.tsb("TSInputCacheableRoot", {{"scalar", ts}, {"inner", inner}});

    const auto all_peered = TSEndpointSchema::non_peered(
        root,
        {TSEndpointSchema::peered(ts),
         TSEndpointSchema::non_peered(inner, {TSEndpointSchema::peered(polymorphic)})});
    const auto owned_leaf = TSEndpointSchema::non_peered(
        root,
        {TSEndpointSchema::owned(ts),
         TSEndpointSchema::non_peered(inner, {TSEndpointSchema::peered(polymorphic)})});
    const auto owned_subtree = TSEndpointSchema::non_peered(
        root,
        {TSEndpointSchema::peered(ts), TSEndpointSchema::owned(inner)});

    REQUIRE(input_storage_type_is_realization_invariant(TSEndpointSchema::peered(root)));
    REQUIRE(input_storage_type_is_realization_invariant(all_peered));
    REQUIRE_FALSE(input_storage_type_is_realization_invariant(TSEndpointSchema::owned(root)));
    REQUIRE_FALSE(input_storage_type_is_realization_invariant(owned_leaf));
    REQUIRE_FALSE(input_storage_type_is_realization_invariant(owned_subtree));
}

TEST_CASE("erased TSData and target-link contexts rebuild after registry reset")
{
    using namespace hgraph;

    for (int pass = 0; pass < 2; ++pass)
    {
        CAPTURE(pass);
        {
            auto       &registry = TypeRegistry::instance();
            auto       &factory = TSDataPlanFactory::instance();
            const auto *integer = registry.register_scalar<std::int32_t>("int32");
            const auto *scalar = registry.ts(integer);
            const auto *set = registry.tss(integer);
            const auto *dict = registry.tsd(integer, scalar);
            const auto *fixed_list = registry.tsl(scalar, 2);
            const auto *dynamic_list = registry.tsl(scalar);
            const auto *size_window = registry.tsw(integer, 3, 1);
            const auto *time_window = registry.tsw_duration(
                integer, TimeDelta{10}, TimeDelta{5});
            const auto *bundle = registry.tsb(
                "ErasedContextResetBundle", {{"value", scalar}, {"items", fixed_list}});

            // Construct and use each concrete TSData context family before
            // reset. The second pass proves that cache teardown destroys the
            // exact erased context and that fresh contexts can be published.
            TSData set_data{factory.data_type_for(set)};
            TSData dict_data{factory.data_type_for(dict)};
            TSData dynamic_list_data{factory.data_type_for(dynamic_list)};
            TSData size_window_data{factory.data_type_for(size_window)};
            TSData time_window_data{factory.data_type_for(time_window)};
            Value  key{std::int32_t{1}};
            Value  value{std::int32_t{42}};
            auto   set_root = set_data.view();
            auto   dict_root = dict_data.view();
            auto   dynamic_list_root = dynamic_list_data.view();
            auto   size_window_root = size_window_data.view();
            auto   time_window_root = time_window_data.view();
            {
                auto mutation = set_root.as_set().begin_mutation(MIN_ST);
                REQUIRE(mutation.add(key.view()));
            }
            {
                auto mutation = dict_root.as_dict().begin_mutation(MIN_ST);
                auto child = mutation.at(key.view());
                auto child_mutation = child.begin_mutation(MIN_ST);
                REQUIRE(child_mutation.copy_value_from(value.view()));
            }
            {
                auto mutation = size_window_root.as_window().begin_mutation(MIN_ST);
                mutation.push(value.view());
            }
            {
                auto mutation = time_window_root.as_window().begin_mutation(MIN_ST);
                mutation.push(value.view());
            }
            REQUIRE(set_root.as_set().contains(key.view()));
            REQUIRE(dict_root.as_dict().contains(key.view()));
            REQUIRE(dynamic_list_root.schema() == dynamic_list);
            REQUIRE(size_window_root.as_window().back().checked_as<std::int32_t>() == 42);
            REQUIRE(time_window_root.as_window().back().checked_as<std::int32_t>() == 42);

            // Base, keyed-slot, indexed, and both window target-link
            // strategies all publish stable pointers into their erased owners.
            TSInput scalar_input{TSInputBuilderFactory::checked_builder_for(
                *scalar, TSEndpointSchema::peered(scalar))};
            TSInput set_input{TSInputBuilderFactory::checked_builder_for(
                *set, TSEndpointSchema::peered(set))};
            TSInput dict_input{TSInputBuilderFactory::checked_builder_for(
                *dict, TSEndpointSchema::peered(dict))};
            TSInput dynamic_list_input{TSInputBuilderFactory::checked_builder_for(
                *dynamic_list, TSEndpointSchema::peered(dynamic_list))};
            TSInput size_window_input{TSInputBuilderFactory::checked_builder_for(
                *size_window, TSEndpointSchema::peered(size_window))};
            TSInput time_window_input{TSInputBuilderFactory::checked_builder_for(
                *time_window, TSEndpointSchema::peered(time_window))};
            const auto list_endpoint = TSEndpointSchema::non_peered_list(
                fixed_list, TSEndpointSchema::peered(scalar));
            const auto bundle_endpoint = TSEndpointSchema::non_peered(
                bundle, {TSEndpointSchema::peered(scalar), list_endpoint});
            TSInput bundle_input{TSInputBuilderFactory::checked_builder_for(
                *bundle, bundle_endpoint)};

            REQUIRE(scalar_input.schema() == scalar);
            auto set_input_root = set_input.view();
            auto dict_input_root = dict_input.view();
            auto dynamic_list_input_root = dynamic_list_input.view();
            auto size_window_input_root = size_window_input.view();
            auto time_window_input_root = time_window_input.view();
            auto bundle_input_root = bundle_input.view();
            REQUIRE(set_input_root.as_set().is_bindable());
            REQUIRE(dict_input_root.as_dict().is_bindable());
            REQUIRE(dynamic_list_input_root.as_list().is_bindable());
            REQUIRE(size_window_input_root.as_window().is_bindable());
            REQUIRE(time_window_input_root.as_window().is_bindable());
            auto bundle_view = bundle_input_root.as_bundle();
            REQUIRE(bundle_view.field("value").is_bindable());
            auto bundle_items = bundle_view.field("items");
            auto bundle_list = bundle_items.as_list();
            REQUIRE(bundle_list[1].is_bindable());
        }

        if (pass == 0) { reset_all_registries(); }
    }
}

TEST_CASE("cached TSInput builders resolve owned polymorphic storage in each graph realization")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *base = registry.bundle(
        "tests.ts_input_cache.realization", "Base", {{"id", integer}}, {}, true);
    registry.bundle(
        "tests.ts_input_cache.realization", "First", {{"id", integer}, {"value", integer}}, {base});
    const auto *polymorphic = registry.ts(base);
    const auto endpoint = TSEndpointSchema::owned(polymorphic);

    const auto first_snapshot = TypeRealizationSnapshot::capture(registry);
    const TSInputBuilder *builder = nullptr;
    TSInput first_input;
    {
        TypeRealizationScope scope{first_snapshot.get()};
        builder = &TSInputBuilderFactory::checked_builder_for(*polymorphic, endpoint);
        first_input = builder->make_input();
    }
    const auto first_binding = first_input.view().data_view().layout().value_binding;
    REQUIRE(first_binding == first_snapshot->type_for(base));

    registry.bundle(
        "tests.ts_input_cache.realization", "Second",
        {{"id", integer}, {"value", integer}, {"other", integer}}, {base});
    const auto second_snapshot = TypeRealizationSnapshot::capture(registry);
    REQUIRE(second_snapshot != first_snapshot);

    TSInput second_input;
    {
        TypeRealizationScope scope{second_snapshot.get()};
        const auto &reused = TSInputBuilderFactory::checked_builder_for(*polymorphic, endpoint);
        REQUIRE(&reused == builder);
        second_input = reused.make_input();
    }
    const auto second_binding = second_input.view().data_view().layout().value_binding;
    REQUIRE(second_binding == second_snapshot->type_for(base));
    REQUIRE(second_binding != first_binding);
}

TEST_CASE("cached composite TSInput builders re-realize owned polymorphic leaves")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *base = registry.bundle(
        "tests.ts_input_cache.composite", "Base", {{"id", integer}}, {}, true);
    registry.bundle(
        "tests.ts_input_cache.composite", "First", {{"id", integer}, {"value", integer}}, {base});
    const auto *polymorphic = registry.ts(base);
    const auto *scalar = registry.ts(integer);
    const auto *root = registry.tsb(
        "TSInputCacheCompositeRoot", {{"owned", polymorphic}, {"peered", scalar}});
    const auto endpoint = TSEndpointSchema::non_peered(
        root, {TSEndpointSchema::owned(polymorphic), TSEndpointSchema::peered(scalar)});
    const auto owned_binding = [](TSInput &input) {
        auto root_view = input.view();
        auto bundle = root_view.as_bundle();
        return bundle.field("owned").data_view().layout().value_binding;
    };

    const auto first_snapshot = TypeRealizationSnapshot::capture(registry);
    const TSInputBuilder *builder = nullptr;
    TSInput first_input;
    {
        TypeRealizationScope scope{first_snapshot.get()};
        builder = &TSInputBuilderFactory::checked_builder_for(*root, endpoint);
        first_input = builder->make_input();
    }
    const auto first_binding = owned_binding(first_input);
    REQUIRE(first_binding == first_snapshot->type_for(base));

    registry.bundle(
        "tests.ts_input_cache.composite", "Second",
        {{"id", integer}, {"value", integer}, {"other", integer}}, {base});
    const auto second_snapshot = TypeRealizationSnapshot::capture(registry);

    TSInput second_input;
    {
        TypeRealizationScope scope{second_snapshot.get()};
        const auto &reused = TSInputBuilderFactory::checked_builder_for(*root, endpoint);
        REQUIRE(&reused == builder);
        second_input = reused.make_input();
    }
    const auto second_binding = owned_binding(second_input);
    REQUIRE(second_binding == second_snapshot->type_for(base));
    REQUIRE(second_binding != first_binding);
}

TEST_CASE("cached owned TSS inputs re-realize polymorphic keys")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *base = registry.bundle("tests.ts_input_cache.tss", "Base", {{"id", integer}}, {}, true);
    registry.bundle("tests.ts_input_cache.tss", "First", {{"id", integer}, {"value", integer}}, {base});
    const auto *schema = registry.tss(base);
    const auto endpoint = TSEndpointSchema::owned(schema);
    const auto key_binding = [](TSInput &input)
    {
        auto root = input.view();
        return root.as_set().data_view().layout().key_binding;
    };

    const auto first_snapshot = TypeRealizationSnapshot::capture(registry);
    const TSInputBuilder *builder = nullptr;
    TSInput first_input;
    {
        TypeRealizationScope scope{first_snapshot.get()};
        builder = &TSInputBuilderFactory::checked_builder_for(*schema, endpoint);
        first_input = builder->make_input();
    }
    const auto first_binding = key_binding(first_input);
    auto first_root = first_input.view();
    const auto *first_plan = first_root.data_view().storage_type().plan();
    REQUIRE(first_binding == first_snapshot->type_for(base));

    registry.bundle("tests.ts_input_cache.tss", "Second", {{"id", integer}, {"value", integer}, {"other", integer}},
                    {base});
    const auto second_snapshot = TypeRealizationSnapshot::capture(registry);

    TSInput second_input;
    {
        TypeRealizationScope scope{second_snapshot.get()};
        const auto &reused = TSInputBuilderFactory::checked_builder_for(*schema, endpoint);
        REQUIRE(&reused == builder);
        second_input = reused.make_input();
    }
    const auto second_binding = key_binding(second_input);
    auto second_root = second_input.view();
    const auto *second_plan = second_root.data_view().storage_type().plan();
    REQUIRE(second_binding == second_snapshot->type_for(base));
    REQUIRE(second_binding != first_binding);
    REQUIRE(second_plan != first_plan);
}

TEST_CASE("cached owned TSD inputs re-realize polymorphic keys and values")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *key_base = registry.bundle("tests.ts_input_cache.tsd_key", "Base", {{"id", integer}}, {}, true);
    const auto *value_base = registry.bundle("tests.ts_input_cache.tsd_value", "Base", {{"id", integer}}, {}, true);
    registry.bundle("tests.ts_input_cache.tsd_key", "First", {{"id", integer}, {"value", integer}}, {key_base});
    registry.bundle("tests.ts_input_cache.tsd_value", "First", {{"id", integer}, {"value", integer}}, {value_base});
    const auto *schema = registry.tsd(key_base, registry.ts(value_base));
    const auto endpoint = TSEndpointSchema::owned(schema);
    const auto bindings = [](TSInput &input)
    {
        auto root = input.view();
        auto dict = root.as_dict();
        auto data = dict.data_view();
        const auto &layout = data.layout();
        return std::pair{layout.key_binding, layout.element_layout->value_binding};
    };

    const auto first_snapshot = TypeRealizationSnapshot::capture(registry);
    const TSInputBuilder *builder = nullptr;
    TSInput first_input;
    {
        TypeRealizationScope scope{first_snapshot.get()};
        builder = &TSInputBuilderFactory::checked_builder_for(*schema, endpoint);
        first_input = builder->make_input();
    }
    const auto first = bindings(first_input);
    auto first_root = first_input.view();
    const auto *first_plan = first_root.data_view().storage_type().plan();
    REQUIRE(first.first == first_snapshot->type_for(key_base));
    REQUIRE(first.second == first_snapshot->type_for(value_base));

    registry.bundle("tests.ts_input_cache.tsd_key", "Second", {{"id", integer}, {"value", integer}, {"other", integer}},
                    {key_base});
    registry.bundle("tests.ts_input_cache.tsd_value", "Second",
                    {{"id", integer}, {"value", integer}, {"other", integer}}, {value_base});
    const auto second_snapshot = TypeRealizationSnapshot::capture(registry);

    TSInput second_input;
    {
        TypeRealizationScope scope{second_snapshot.get()};
        const auto &reused = TSInputBuilderFactory::checked_builder_for(*schema, endpoint);
        REQUIRE(&reused == builder);
        second_input = reused.make_input();
    }
    const auto second = bindings(second_input);
    auto second_root = second_input.view();
    const auto *second_plan = second_root.data_view().storage_type().plan();
    REQUIRE(second.first == second_snapshot->type_for(key_base));
    REQUIRE(second.second == second_snapshot->type_for(value_base));
    REQUIRE(second.first != first.first);
    REQUIRE(second.second != first.second);
    REQUIRE(second_plan != first_plan);
}

TEST_CASE("cached composite TSD inputs re-realize polymorphic keys and owned "
          "values")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *key_base =
        registry.bundle("tests.ts_input_cache.composite_tsd_key", "Base", {{"id", integer}}, {}, true);
    const auto *value_base =
        registry.bundle("tests.ts_input_cache.composite_tsd_value", "Base", {{"id", integer}}, {}, true);
    registry.bundle("tests.ts_input_cache.composite_tsd_key", "First", {{"id", integer}, {"value", integer}},
                    {key_base});
    registry.bundle("tests.ts_input_cache.composite_tsd_value", "First", {{"id", integer}, {"value", integer}},
                    {value_base});
    const auto *element = registry.ts(value_base);
    const auto *schema = registry.tsd(key_base, element);
    const auto endpoint = TSEndpointSchema::non_peered_dict(schema, TSEndpointSchema::owned(element));
    const auto bindings = [](TSInput &input)
    {
        auto root = input.view();
        auto dict = root.as_dict();
        auto data = dict.data_view();
        const auto &layout = data.layout();
        return std::pair{layout.key_binding, layout.element_layout->value_binding};
    };

    const auto first_snapshot = TypeRealizationSnapshot::capture(registry);
    const TSInputBuilder *builder = nullptr;
    TSInput first_input;
    {
        TypeRealizationScope scope{first_snapshot.get()};
        builder = &TSInputBuilderFactory::checked_builder_for(*schema, endpoint);
        first_input = builder->make_input();
    }
    const auto first = bindings(first_input);
    auto first_root = first_input.view();
    const auto *first_plan = first_root.data_view().storage_type().plan();
    REQUIRE(first.first == first_snapshot->type_for(key_base));
    REQUIRE(first.second == first_snapshot->type_for(value_base));

    registry.bundle("tests.ts_input_cache.composite_tsd_key", "Second",
                    {{"id", integer}, {"value", integer}, {"other", integer}}, {key_base});
    registry.bundle("tests.ts_input_cache.composite_tsd_value", "Second",
                    {{"id", integer}, {"value", integer}, {"other", integer}}, {value_base});
    const auto second_snapshot = TypeRealizationSnapshot::capture(registry);

    TSInput second_input;
    {
        TypeRealizationScope scope{second_snapshot.get()};
        const auto &reused = TSInputBuilderFactory::checked_builder_for(*schema, endpoint);
        REQUIRE(&reused == builder);
        second_input = reused.make_input();
    }
    const auto second = bindings(second_input);
    auto second_root = second_input.view();
    const auto *second_plan = second_root.data_view().storage_type().plan();
    REQUIRE(second.first == second_snapshot->type_for(key_base));
    REQUIRE(second.second == second_snapshot->type_for(value_base));
    REQUIRE(second.first != first.first);
    REQUIRE(second.second != first.second);
    REQUIRE(second_plan != first_plan);
}

TEST_CASE("cached owned dynamic TSL inputs re-realize polymorphic elements")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *base = registry.bundle("tests.ts_input_cache.dynamic_tsl", "Base", {{"id", integer}}, {}, true);
    registry.bundle("tests.ts_input_cache.dynamic_tsl", "First", {{"id", integer}, {"value", integer}}, {base});
    const auto *schema = registry.tsl(registry.ts(base));
    const auto endpoint = TSEndpointSchema::owned(schema);
    const auto element_binding = [](TSInput &input)
    {
        auto root = input.view();
        auto list = root.as_list();
        auto data = list.data_view();
        const auto &layout = static_cast<const FixedTSLDataLayout &>(data.layout());
        return layout.element_layout->value_binding;
    };

    const auto first_snapshot = TypeRealizationSnapshot::capture(registry);
    const TSInputBuilder *builder = nullptr;
    TSInput first_input;
    {
        TypeRealizationScope scope{first_snapshot.get()};
        builder = &TSInputBuilderFactory::checked_builder_for(*schema, endpoint);
        first_input = builder->make_input();
    }
    const auto first_binding = element_binding(first_input);
    auto first_root = first_input.view();
    const auto *first_plan = first_root.data_view().storage_type().plan();
    REQUIRE(first_binding == first_snapshot->type_for(base));

    registry.bundle("tests.ts_input_cache.dynamic_tsl", "Second",
                    {{"id", integer}, {"value", integer}, {"other", integer}}, {base});
    const auto second_snapshot = TypeRealizationSnapshot::capture(registry);

    TSInput second_input;
    {
        TypeRealizationScope scope{second_snapshot.get()};
        const auto &reused = TSInputBuilderFactory::checked_builder_for(*schema, endpoint);
        REQUIRE(&reused == builder);
        second_input = reused.make_input();
    }
    const auto second_binding = element_binding(second_input);
    auto second_root = second_input.view();
    const auto *second_plan = second_root.data_view().storage_type().plan();
    REQUIRE(second_binding == second_snapshot->type_for(base));
    REQUIRE(second_binding != first_binding);
    REQUIRE(second_plan == first_plan);
}

TEST_CASE("cached owned TSW inputs re-realize polymorphic elements")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *base = registry.bundle("tests.ts_input_cache.tsw", "Base", {{"id", integer}}, {}, true);
    registry.bundle("tests.ts_input_cache.tsw", "First", {{"id", integer}, {"value", integer}}, {base});
    const auto *schema = registry.tsw(base, 3, 1);
    const auto endpoint = TSEndpointSchema::owned(schema);
    const auto element_binding = [](TSInput &input)
    {
        auto root = input.view();
        return root.as_window().data_view().layout().element_binding;
    };

    const auto first_snapshot = TypeRealizationSnapshot::capture(registry);
    const TSInputBuilder *builder = nullptr;
    TSInput first_input;
    {
        TypeRealizationScope scope{first_snapshot.get()};
        builder = &TSInputBuilderFactory::checked_builder_for(*schema, endpoint);
        first_input = builder->make_input();
    }
    const auto first_binding = element_binding(first_input);
    auto first_root = first_input.view();
    const auto *first_plan = first_root.data_view().storage_type().plan();
    REQUIRE(first_binding == first_snapshot->type_for(base));

    registry.bundle("tests.ts_input_cache.tsw", "Second", {{"id", integer}, {"value", integer}, {"other", integer}},
                    {base});
    const auto second_snapshot = TypeRealizationSnapshot::capture(registry);

    TSInput second_input;
    {
        TypeRealizationScope scope{second_snapshot.get()};
        const auto &reused = TSInputBuilderFactory::checked_builder_for(*schema, endpoint);
        REQUIRE(&reused == builder);
        second_input = reused.make_input();
    }
    const auto second_binding = element_binding(second_input);
    auto second_root = second_input.view();
    const auto *second_plan = second_root.data_view().storage_type().plan();
    REQUIRE(second_binding == second_snapshot->type_for(base));
    REQUIRE(second_binding != first_binding);
    REQUIRE(second_plan != first_plan);
}

TEST_CASE("cached wholly owned fixed inputs re-realize nested keyed and dynamic storage")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *key_base =
        registry.bundle("tests.ts_input_cache.nested_key", "Base", {{"id", integer}}, {}, true);
    const auto *dict_base =
        registry.bundle("tests.ts_input_cache.nested_dict", "Base", {{"id", integer}}, {}, true);
    const auto *list_base =
        registry.bundle("tests.ts_input_cache.nested_list", "Base", {{"id", integer}}, {}, true);
    registry.bundle("tests.ts_input_cache.nested_key", "First", {{"id", integer}, {"value", integer}}, {key_base});
    registry.bundle("tests.ts_input_cache.nested_dict", "First", {{"id", integer}, {"value", integer}},
                    {dict_base});
    registry.bundle("tests.ts_input_cache.nested_list", "First", {{"id", integer}, {"value", integer}},
                    {list_base});

    const auto *dict = registry.tsd(key_base, registry.ts(dict_base));
    const auto *list = registry.tsl(registry.ts(list_base));
    const auto *root = registry.tsb("TSInputCacheNestedOwnedRoot", {{"dict", dict}, {"list", list}});
    const auto endpoint = TSEndpointSchema::owned(root);
    const auto bindings = [](TSInput &input)
    {
        auto root_view = input.view();
        auto bundle = root_view.data_view().as_bundle();
        auto dict_data = bundle.field("dict");
        auto list_data = bundle.field("list");
        const auto &dict_layout =
            static_cast<const TSDDataLayout &>(dict_data.layout());
        const auto &list_layout =
            static_cast<const FixedTSLDataLayout &>(list_data.layout());
        return std::tuple{
            dict_layout.key_binding,
            dict_layout.element_layout->value_binding,
            list_layout.element_layout->value_binding,
        };
    };

    const auto first_snapshot = TypeRealizationSnapshot::capture(registry);
    const TSInputBuilder *builder = nullptr;
    TSInput first_input;
    {
        TypeRealizationScope scope{first_snapshot.get()};
        builder = &TSInputBuilderFactory::checked_builder_for(*root, endpoint);
        first_input = builder->make_input();
    }
    const auto first = bindings(first_input);
    REQUIRE(std::get<0>(first) == first_snapshot->type_for(key_base));
    REQUIRE(std::get<1>(first) == first_snapshot->type_for(dict_base));
    REQUIRE(std::get<2>(first) == first_snapshot->type_for(list_base));

    registry.bundle("tests.ts_input_cache.nested_key", "Second",
                    {{"id", integer}, {"value", integer}, {"other", integer}}, {key_base});
    registry.bundle("tests.ts_input_cache.nested_dict", "Second",
                    {{"id", integer}, {"value", integer}, {"other", integer}}, {dict_base});
    registry.bundle("tests.ts_input_cache.nested_list", "Second",
                    {{"id", integer}, {"value", integer}, {"other", integer}}, {list_base});
    const auto second_snapshot = TypeRealizationSnapshot::capture(registry);

    TSInput second_input;
    {
        TypeRealizationScope scope{second_snapshot.get()};
        const auto &reused = TSInputBuilderFactory::checked_builder_for(*root, endpoint);
        REQUIRE(&reused == builder);
        second_input = reused.make_input();
    }
    const auto second = bindings(second_input);
    REQUIRE(std::get<0>(second) == second_snapshot->type_for(key_base));
    REQUIRE(std::get<1>(second) == second_snapshot->type_for(dict_base));
    REQUIRE(std::get<2>(second) == second_snapshot->type_for(list_base));
    REQUIRE(std::get<0>(second) != std::get<0>(first));
    REQUIRE(std::get<1>(second) != std::get<1>(first));
    REQUIRE(std::get<2>(second) != std::get<2>(first));
}

TEST_CASE("TSInput builds a non-peered TSB root with nested peered terminals")
{
    using namespace hgraph;

    static_assert(!std::is_copy_constructible_v<TSInputView>);
    static_assert(!std::is_copy_assignable_v<TSInputView>);
    static_assert(std::is_move_constructible_v<TSInputView>);
    static_assert(!std::is_copy_constructible_v<TSSInputView>);
    static_assert(!std::is_copy_constructible_v<TSDInputView>);
    static_assert(!std::is_copy_constructible_v<TSBInputView>);
    static_assert(!std::is_copy_constructible_v<TSLInputView>);
    static_assert(!std::is_copy_constructible_v<TSWInputView>);

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *nested   = registry.tsb("TSInputNested", {{"x", ts_int}});
    const auto *root     = registry.tsb("TSInputRoot", {{"a", ts_int}, {"nested", nested}});

    auto        schema  = nested_input_schema(root, nested, ts_int);
    const auto &builder = TSInputBuilderFactory::checked_builder_for(*root, schema);
    TSOutput output{*ts_int};
    TSOutput replacement{*ts_int};
    const auto t1 = MIN_ST;
    set_output(output, 42, t1);
    set_output(replacement, 99, t1);

    TSInput     input   = builder.make_input();

    REQUIRE(input.has_value());
    REQUIRE(input.schema() == root);

    auto input_root_view = input.view();
    REQUIRE_FALSE(input_root_view.has_parent_input());
    REQUIRE_FALSE(input_root_view.is_bindable());
    REQUIRE(input_root_view.bound());
    auto root_view = input_root_view.as_bundle();
    REQUIRE_FALSE(root_view.is_bindable());
    REQUIRE(root_view.bound());
    REQUIRE(root_view.size() == 2);
    REQUIRE(root_view.has_field("a"));
    REQUIRE(root_view.has_field("nested"));

    auto scalar = root_view.field("a");
    REQUIRE(scalar.has_parent_input());
    auto scalar_parent = scalar.parent_input();
    REQUIRE(scalar_parent.schema() == root);
    REQUIRE(scalar_parent.data_view().data() == input_root_view.data_view().data());
    REQUIRE(scalar.is_bindable());
    REQUIRE_FALSE(scalar.bound());
    REQUIRE_FALSE(scalar.valid());
    scalar.bind_output(output.view(t1));
    REQUIRE(scalar.bound());
    REQUIRE(input_root_view.bound());
    REQUIRE(root_view.bound());
    REQUIRE(scalar.valid());
    REQUIRE(scalar.value().checked_as<std::int32_t>() == 42);

    scalar.bind_output(replacement.view(t1));
    REQUIRE(scalar.bound());
    REQUIRE(scalar.valid());
    REQUIRE(scalar.value().checked_as<std::int32_t>() == 99);

    auto nested_input_view = input.view();
    auto nested_root = nested_input_view.as_bundle();
    auto nested_field = nested_root.field("nested");
    REQUIRE_FALSE(nested_field.is_bindable());
    REQUIRE(nested_field.bound());
    auto nested_bundle = nested_field.as_bundle();
    REQUIRE_FALSE(nested_bundle.is_bindable());
    REQUIRE(nested_bundle.bound());
    auto nested_leaf = nested_bundle.field("x");
    REQUIRE(nested_field.has_parent_input());
    auto nested_parent = nested_field.parent_input();
    REQUIRE(nested_parent.schema() == root);
    REQUIRE(nested_leaf.has_parent_input());
    auto leaf_parent = nested_leaf.parent_input();
    REQUIRE(leaf_parent.schema() == nested);
    REQUIRE(leaf_parent.has_parent_input());
    REQUIRE(nested_leaf.is_bindable());
    REQUIRE_FALSE(nested_leaf.bound());
    REQUIRE_FALSE(nested_leaf.valid());
    nested_leaf.bind_output(output.view(t1));
    REQUIRE(nested_leaf.bound());
    REQUIRE(nested_field.bound());
    REQUIRE(nested_bundle.bound());
    REQUIRE(nested_leaf.valid());
    REQUIRE(nested_leaf.value().checked_as<std::int32_t>() == 42);
}

TEST_CASE("TSInput dynamic storage metrics include activation and target-link tries", "[memory]")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *root = registry.tsb("TSInputMetricsRoot", {{"value", ts_int}});
    const auto endpoint = TSEndpointSchema::non_peered(
        root, {TSEndpointSchema::peered(ts_int)});

    TSInput input{TSInputBuilderFactory::checked_builder_for(*root, endpoint)};
    TSOutput output{*ts_int};
    auto binding_root = input.view();
    auto binding = binding_root.as_bundle();
    binding.field("value").bind_output(output.view());
    const auto passive = input.dynamic_storage_metrics();

    RecordingNotifiable notifier;
    auto active_root = input.view(&notifier);
    auto active = active_root.as_bundle();
    active.field("value").make_active();
    const auto active_metrics = input.dynamic_storage_metrics();
    CHECK(active_metrics.live_bytes > passive.live_bytes);
    CHECK(active_metrics.reserved_bytes > passive.reserved_bytes);
    CHECK(active_metrics.reserved_bytes >= active_metrics.live_bytes);
}

TEST_CASE("TSInput peered target children expose read-only structural parents")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *nested   = registry.tsb("TSInputPeeredNested", {{"x", ts_int}});
    const auto *root     = registry.tsb("TSInputPeeredRoot", {{"nested", nested}});

    TSOutput output{*root};
    TSInput input{TSInputBuilderFactory::checked_builder_for(
        *root, TSEndpointSchema::peered(root))};
    input.view().bind_output(output.view());

    auto input_root = input.view();
    auto root_bundle = input_root.as_bundle();
    auto nested_input = root_bundle.field("nested");
    auto nested_bundle = nested_input.as_bundle();
    auto leaf_input = nested_bundle.field("x");

    REQUIRE_FALSE(input_root.has_parent_input());
    REQUIRE(nested_input.has_parent_input());
    REQUIRE(nested_input.parent_input().schema() == root);
    REQUIRE(leaf_input.has_parent_input());
    auto leaf_parent = leaf_input.parent_input();
    REQUIRE(leaf_parent.schema() == nested);
    REQUIRE(leaf_parent.has_parent_input());
    REQUIRE(leaf_parent.parent_input().schema() == root);
}

TEST_CASE("TSInput dynamic storage metrics do not follow borrowed output storage", "[memory]")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);

    TSOutput output{*ts_int};
    RecordingNotifiable first;
    RecordingNotifiable second;
    output.subscribe(&first);
    output.subscribe(&second);
    REQUIRE(output.dynamic_storage_metrics().live_bytes > 0);

    TSInput input{TSInputBuilderFactory::checked_builder_for(
        *ts_int, TSEndpointSchema::peered(ts_int))};
    input.view().bind_output(output.view());

    CHECK(input.dynamic_storage_metrics().live_bytes == 0);
    CHECK(input.dynamic_storage_metrics().reserved_bytes == 0);

    output.unsubscribe(&second);
    output.unsubscribe(&first);
}

TEST_CASE("TSInput construction uses generic endpoint annotations")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TSInputAnnotatedRoot", {{"items", list}});

    const auto list_annotation = TSEndpointSchema::non_peered_list(
        list,
        TSEndpointSchema::peered(ts_int));
    REQUIRE(list_annotation.is_non_peered());
    REQUIRE(list_annotation.child_count() == 2);
    REQUIRE(list_annotation.child(0).is_peered());
    REQUIRE(list_annotation.child(1).schema() == ts_int);

    const auto input_annotation = TSEndpointSchema::non_peered(root, {list_annotation});
    const auto plan = TSInputPlanFactory::compile(*root, input_annotation);
    REQUIRE(plan.schema().kind == TSTypeKind::TSB);
    REQUIRE(plan.endpoint_schema().child(0).child_count() == 2);

    const auto input_with_peered_child = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(list),
        });
    REQUIRE_NOTHROW(TSInputPlanFactory::compile(*root, input_with_peered_child));

    REQUIRE_THROWS_AS(
        TSEndpointSchema::non_peered(
            root,
            {
                TSEndpointSchema::peered(ts_int),
            }),
        std::invalid_argument);
}

TEST_CASE("TSInput active non-peered prefixes schedule through peered terminal notifications")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TSInputListRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput lhs{*ts_int};
    TSOutput rhs{*ts_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    auto input_root_view = input.view();
    auto input_root = input_root_view.as_bundle();
    auto list_view = input_root.field("items");
    auto list_children = list_view.as_list();
    list_children[0].bind_output(lhs.view());
    list_children[1].bind_output(rhs.view());

    RecordingNotifiable recorder;
    auto active_root_view = input.view(&recorder);
    auto active_root = active_root_view.as_bundle();
    auto active_list = active_root.field("items");
    active_list.make_active();
    REQUIRE(active_list.active());

    const auto t1 = MIN_ST + TimeDelta{1};
    const auto t2 = t1 + TimeDelta{1};
    set_output(lhs, 1, t1);
    set_output(rhs, 2, t2);

    REQUIRE(recorder.notified == std::vector<DateTime>{t1, t2});

    active_list.make_passive();
    REQUIRE_FALSE(active_list.active());

    const auto t3 = t2 + TimeDelta{1};
    set_output(lhs, 3, t3);
    REQUIRE(recorder.notified == std::vector<DateTime>{t1, t2});
}

TEST_CASE("TSInput target binding updates non-peered bundle and list prefixes")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TSInputRecursiveBindingRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput first_output{*ts_int};
    TSOutput second_output{*ts_int};
    TSOutput root_output{*root};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    const auto t1 = MIN_ST + TimeDelta{20};
    const auto t2 = t1 + TimeDelta{1};

    set_output(first_output, 10, t1);

    auto input_root = input.view(nullptr, t1);
    REQUIRE(input_root.evaluation_time() == t1);
    REQUIRE(input_root.borrowed_ref(t2).evaluation_time() == t2);
    REQUIRE(input_root.type_ref().record() != nullptr);
    REQUIRE(input_root.type_ref());
    REQUIRE(std::string{input_root.type_ref().record()->implementation_name()} == "ts.fixed.input.composite");
    REQUIRE_FALSE(input_root.is_bindable());
    REQUIRE(input_root.bound());
    REQUIRE_FALSE(input_root.valid());
    REQUIRE_THROWS_AS(input_root.bind_output(root_output.view(t1)), std::logic_error);

    auto bundle = input_root.as_bundle();
    auto items = bundle.field("items");
    REQUIRE_FALSE(items.is_bindable());
    REQUIRE(items.bound());
    auto list_view = items.as_list();
    REQUIRE_FALSE(list_view.is_bindable());
    REQUIRE(list_view.bound());
    REQUIRE(list_view[0].is_bindable());
    REQUIRE_FALSE(list_view[0].bound());
    list_view[0].bind_output(first_output.view(t1));

    REQUIRE(input_root.valid());
    // ``all_valid`` is one level deep: the root bundle asks its only child
    // (``items``) for ``valid``, not for ``all_valid``. ``items`` is valid
    // because element 0 is bound, so the root is all_valid even though the
    // list beneath it is only partly bound. See the ``items`` assertion below.
    REQUIRE(input_root.all_valid());
    REQUIRE(input_root.modified());
    REQUIRE(input_root.last_modified_time() == t1);

    const auto keys = collect_range(bundle.keys());
    REQUIRE(keys.size() == 1);
    REQUIRE(std::string{keys[0]} == "items");
    REQUIRE(range_size(bundle.values()) == 1);
    REQUIRE(range_size(bundle.valid_items()) == 1);
    auto bundle_modified_items = collect_range(bundle.modified_items());
    REQUIRE(bundle_modified_items.size() == 1);
    REQUIRE(std::string{bundle_modified_items[0].first} == "items");
    REQUIRE(input_root.value().is_bundle());
    REQUIRE(input_root.value().binding().ops_ref().kind == ValueOpsKind::Indexed);

    REQUIRE(items.type_ref().record() != nullptr);
    REQUIRE(items.type_ref());
    REQUIRE(items.valid());
    REQUIRE_FALSE(items.all_valid());
    REQUIRE(items.last_modified_time() == t1);

    REQUIRE(list_view.size() == 2);
    REQUIRE(range_size(list_view.values()) == 2);
    auto list_valid_items = collect_range(list_view.valid_items());
    REQUIRE(list_valid_items.size() == 1);
    REQUIRE(list_valid_items[0].first == 0);
    REQUIRE(list_valid_items[0].second.value().checked_as<std::int32_t>() == 10);
    REQUIRE(list_view[0].type_ref().record() != nullptr);
    REQUIRE(list_view[0].type_ref());
    REQUIRE(list_view[0].evaluation_time() == t1);
    auto list_modified_items = collect_range(list_view.modified_items());
    REQUIRE(list_modified_items.size() == 1);
    REQUIRE(list_modified_items[0].first == 0);
    REQUIRE(list_view[0].valid());
    REQUIRE_FALSE(list_view[1].valid());

    set_output(second_output, 20, t2);

    auto t2_root = input.view(nullptr, t2);
    auto t2_bundle = t2_root.as_bundle();
    auto t2_items  = t2_bundle.field("items");
    auto t2_list   = t2_items.as_list();
    REQUIRE(t2_list[1].is_bindable());
    REQUIRE_FALSE(t2_list[1].bound());
    t2_list[1].bind_output(second_output.view(t2));

    REQUIRE(t2_root.valid());
    REQUIRE(t2_root.all_valid());
    REQUIRE(t2_root.modified());
    REQUIRE(t2_root.last_modified_time() == t2);

    REQUIRE(range_size(t2_list.valid_values()) == 2);
    REQUIRE(range_size(t2_list.modified_values()) == 1);
    auto t2_modified_items = collect_range(t2_list.modified_items());
    REQUIRE(t2_modified_items.size() == 1);
    REQUIRE(t2_modified_items[0].first == 1);
    REQUIRE(t2_modified_items[0].second.value().checked_as<std::int32_t>() == 20);

    REQUIRE_THROWS_AS(t2_root.unbind_output(), std::logic_error);
    t2_list[0].unbind_output();
    t2_list[1].unbind_output();
    REQUIRE_FALSE(t2_root.valid());
    REQUIRE_FALSE(t2_items.valid());
}

TEST_CASE("TSInput data views project non-peered prefixes")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root_schema = registry.tsb("TSInputDataViewNonPeeredRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root_schema,
        {
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput first_output{*ts_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root_schema, input_schema)};

    const auto t1 = MIN_ST + TimeDelta{40};
    set_output(first_output, 11, t1);

    auto root_view = input.view(nullptr, t1);
    auto bundle = root_view.as_bundle();
    auto items = bundle.field("items");
    auto list_view = items.as_list();
    list_view[0].bind_output(first_output.view(t1));

    auto root_data = root_view.data_view().borrowed_ref();
    REQUIRE(root_data.valid());
    REQUIRE(root_data.schema() == input.schema());
    REQUIRE(root_data.type_ref().record() == root_view.type_ref().record());
    REQUIRE(root_data.has_current_value());
    // One level deep: the root asks ``items`` for ``valid``, which is true.
    REQUIRE(root_data.all_valid());
    REQUIRE(root_data.modified(t1));

    auto bundle_data = root_data.as_bundle();
    REQUIRE(bundle_data.size() == 1);
    auto items_data = bundle_data.field("items");
    REQUIRE(items_data.valid());
    REQUIRE(items_data.schema() == list);
    REQUIRE(items_data.has_current_value());
    REQUIRE_FALSE(items_data.all_valid());
    REQUIRE(items_data.modified(t1));

    auto list_data = items_data.as_list();
    REQUIRE(list_data.size() == 2);
    REQUIRE(range_size(list_data.valid_items()) == 1);
    auto first_child = list_data[0];
    REQUIRE(first_child.valid());
    REQUIRE(first_child.schema() == ts_int);
    REQUIRE(first_child.value().checked_as<std::int32_t>() == 11);

    auto second_child = list_data[1];
    REQUIRE_FALSE(second_child.valid());
    REQUIRE(second_child.schema() == ts_int);
}

TEST_CASE("TSInput projected bundle snapshots own canonical storage and preserve holes")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *root = registry.tsb("TSInputOwnedSparseBundle", {{"present", ts_int}, {"hole", ts_int}});
    const auto annotation = TSEndpointSchema::non_peered(
        root, {TSEndpointSchema::peered(ts_int), TSEndpointSchema::peered(ts_int)});

    Value current;
    Value cloned;
    Value delta;
    Value delta_clone;
    {
        TSOutput output{*ts_int};
        TSInput input{TSInputBuilderFactory::checked_builder_for(*root, annotation)};
        const auto t1 = MIN_ST + TimeDelta{45};
        set_output(output, 41, t1);

        auto root_view = input.view(nullptr, t1);
        auto bundle = root_view.as_bundle();
        bundle.field("present").bind_output(output.view(t1));
        current = Value{root_view.value()};
        cloned = root_view.value().clone();
        delta = Value{root_view.delta_value()};
        delta_clone = root_view.delta_value().clone();
    }

    const auto current_type = ValuePlanFactory::instance().type_for(root->value_schema);
    const auto delta_type = ValuePlanFactory::instance().type_for(root->delta_value_schema);
    REQUIRE(current.binding() == current_type);
    REQUIRE(cloned.binding() == current_type);
    REQUIRE(delta.binding() == delta_type);
    REQUIRE(delta_clone.binding() == delta_type);
    REQUIRE(current.view().equals(cloned.view()));
    REQUIRE(current.view().hash() == cloned.view().hash());
    REQUIRE(delta.view().equals(delta_clone.view()));
    REQUIRE(delta.view().hash() == delta_clone.view().hash());
    for (Value *snapshot : {&current, &cloned})
    {
        auto bundle = snapshot->view().as_bundle();
        REQUIRE(bundle.at("present").checked_as<std::int32_t>() == 41);
        REQUIRE(bundle.at("hole").bound());
        REQUIRE_FALSE(bundle.at("hole").has_value());
    }
    auto delta_bundle = delta.view().as_bundle();
    REQUIRE(delta_bundle.at("present").checked_as<std::int32_t>() == 41);
    REQUIRE(delta_bundle.at("hole").bound());
    REQUIRE_FALSE(delta_bundle.at("hole").has_value());
}

TEST_CASE("TSInput projected fixed-list and delta snapshots own canonical storage")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *list = registry.tsl(ts_int, 2);
    const auto *root = registry.tsb("TSInputOwnedFixedList", {{"items", list}});
    const auto annotation = TSEndpointSchema::non_peered(
        root, {TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int))});

    Value current;
    Value cloned;
    Value delta;
    Value delta_clone;
    Value keys;
    {
        TSOutput first{*ts_int};
        TSInput input{TSInputBuilderFactory::checked_builder_for(*root, annotation)};
        const auto t1 = MIN_ST + TimeDelta{46};
        set_output(first, 10, t1);

        auto root_view = input.view(nullptr, t1);
        auto root_bundle = root_view.as_bundle();
        auto items = root_bundle.field("items");
        auto list_view = items.as_list();
        list_view[0].bind_output(first.view(t1));

        auto projected = items.value().as_list();
        REQUIRE(projected.at(0).checked_as<std::int32_t>() == 10);
        REQUIRE(projected.at(1).bound());
        REQUIRE_FALSE(projected.at(1).has_value());

        current = Value{items.value()};
        cloned = items.value().clone();
        auto delta_view = items.delta_value();
        delta = Value{delta_view};
        delta_clone = delta_view.clone();
        auto delta_map = delta_view.as_map();
        keys = delta_map.key_set().clone();
    }

    const auto current_type = ValuePlanFactory::instance().type_for(list->value_schema);
    const auto delta_type = ValuePlanFactory::instance().type_for(list->delta_value_schema);
    const auto key_set_type = ValuePlanFactory::instance().type_for(
        TypeRegistry::instance().set(list->delta_value_schema->key_type));
    REQUIRE(current.binding() == current_type);
    REQUIRE(cloned.binding() == current_type);
    REQUIRE(delta.binding() == delta_type);
    REQUIRE(delta_clone.binding() == delta_type);
    REQUIRE(keys.binding() == key_set_type);
    REQUIRE(current.view().equals(cloned.view()));
    REQUIRE(current.view().hash() == cloned.view().hash());
    REQUIRE(delta.view().equals(delta_clone.view()));
    REQUIRE(delta.view().hash() == delta_clone.view().hash());
    for (Value *snapshot : {&current, &cloned})
    {
        auto values = snapshot->view().as_list();
        REQUIRE(values.at(0).checked_as<std::int32_t>() == 10);
        REQUIRE(values.at(1).checked_as<std::int32_t>() == 0);
    }

    const auto key_type = ValuePlanFactory::instance().type_for(list->delta_value_schema->key_type);
    Value key_zero{key_type};
    Value key_one{key_type};
    key_zero.begin_mutation().set(Int{0});
    key_one.begin_mutation().set(Int{1});
    for (Value *snapshot : {&delta, &delta_clone})
    {
        auto map = snapshot->view().as_map();
        REQUIRE(map.size() == 1);
        REQUIRE(map.at(key_zero.view()).checked_as<std::int32_t>() == 10);
        REQUIRE_FALSE(map.contains(key_one.view()));
    }
    auto key_set = keys.view().as_set();
    REQUIRE(key_set.size() == 1);
    REQUIRE(key_set.contains(key_zero.view()));
    REQUIRE_FALSE(key_set.contains(key_one.view()));
}

TEST_CASE("TSInput projected fixed-list snapshot rejects unsupported default fill")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *value_meta = registry.register_scalar<NoDefaultTSInputSnapshot>("NoDefaultTSInputSnapshot");
    const auto *ts_value = registry.ts(value_meta);
    const auto *list = registry.tsl(ts_value, 1);
    const auto *root = registry.tsb("TSInputNoDefaultFixedList", {{"items", list}});
    const auto annotation = TSEndpointSchema::non_peered(
        root, {TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_value))});

    TSInput input{TSInputBuilderFactory::checked_builder_for(*root, annotation)};
    auto root_view = input.view(nullptr, MIN_ST + TimeDelta{47});
    auto root_bundle = root_view.as_bundle();
    auto items = root_bundle.field("items");
    auto projected = items.value().as_list();
    REQUIRE(projected.at(0).bound());
    REQUIRE_FALSE(projected.at(0).has_value());
    REQUIRE_THROWS_AS(Value{items.value()}, std::logic_error);
    REQUIRE_THROWS_AS(items.value().clone(), std::logic_error);
}

TEST_CASE("TSInput projected owned children use their nonzero child storage offsets")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *root = registry.tsb(
        "TSInputOwnedChildOffsets", {{"first", ts_int}, {"second", ts_int}, {"third", ts_int}});
    const auto annotation = TSEndpointSchema::non_peered(
        root,
        {TSEndpointSchema::owned(ts_int), TSEndpointSchema::owned(ts_int), TSEndpointSchema::owned(ts_int)});

    Value snapshot;
    Value clone;
    {
        TSInput input{TSInputBuilderFactory::checked_builder_for(*root, annotation)};
        const auto t1 = MIN_ST + TimeDelta{48};
        auto root_view = input.view(nullptr, t1);
        auto root_bundle = root_view.as_bundle();

        const auto set_owned = [&](std::string_view name, std::int32_t value) {
            auto child = root_bundle.field(name);
            REQUIRE_FALSE(child.is_bindable());
            auto &data = child.data_view();
            Value wrapped{value};
            REQUIRE(data.type_ref().role() == TypeRole::Input);
            REQUIRE_FALSE(has_capability(data.type_ref().capabilities(), TypeCapabilities::Mutable));
            const auto &ops = data.ops();
            REQUIRE(ops.copy_value_from_impl(ops.context, const_cast<void *>(data.data()), wrapped.view(), t1));
            auto *tracking = ops.mutable_tracking_impl(ops.context, const_cast<void *>(data.data()));
            REQUIRE(tracking != nullptr);
            if (tracking->record_modified(t1)) { data.parent_link().notify_child_modified(t1); }
        };
        set_owned("first", 101);
        set_owned("second", 202);
        set_owned("third", 303);

        REQUIRE(root_bundle.field("first").value().checked_as<std::int32_t>() == 101);
        REQUIRE(root_bundle.field("second").value().checked_as<std::int32_t>() == 202);
        REQUIRE(root_bundle.field("third").value().checked_as<std::int32_t>() == 303);

        auto current = root_view.value();
        auto indexed = current.as_indexed_view();
        REQUIRE(indexed.at(0).checked_as<std::int32_t>() == 101);
        REQUIRE(indexed.at(1).checked_as<std::int32_t>() == 202);
        REQUIRE(indexed.at(2).checked_as<std::int32_t>() == 303);
        auto range = indexed.elements();
        std::vector<std::int32_t> ranged;
        for (auto value : range) { ranged.push_back(value.checked_as<std::int32_t>()); }
        REQUIRE(ranged == std::vector<std::int32_t>{101, 202, 303});

        REQUIRE(current.hash() != 0);
        REQUIRE(current.equals(current));
        REQUIRE(std::is_eq(current.compare(current)));
        REQUIRE(current.to_string() == "{first: 101, second: 202, third: 303}");

        snapshot = Value{current};
        clone = current.clone();
        REQUIRE(current.equals(snapshot.view()));
        REQUIRE(std::is_eq(current.compare(snapshot.view())));
    }

    const auto canonical = ValuePlanFactory::instance().type_for(root->value_schema);
    REQUIRE(snapshot.binding() == canonical);
    REQUIRE(clone.binding() == canonical);
    REQUIRE(snapshot.view().equals(clone.view()));
    REQUIRE(snapshot.view().hash() == clone.view().hash());
    auto owned = snapshot.view().as_bundle();
    REQUIRE(owned.at("first").checked_as<std::int32_t>() == 101);
    REQUIRE(owned.at("second").checked_as<std::int32_t>() == 202);
    REQUIRE(owned.at("third").checked_as<std::int32_t>() == 303);
}

TEST_CASE("TSInput data views step through target links and rebinds")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root_schema = registry.tsb("TSInputDataViewTargetLinkRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root_schema,
        {
            TSEndpointSchema::peered(list),
        });

    TSOutput first_output{*list};
    TSOutput second_output{*list};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root_schema, input_schema)};

    const auto t1 = MIN_ST + TimeDelta{50};
    const auto t2 = t1 + TimeDelta{1};
    set_list_output(first_output, 0, 10, t1);
    set_list_output(first_output, 1, 20, t1);
    set_list_output(second_output, 0, 100, t2);
    set_list_output(second_output, 1, 200, t2);

    auto root_view = input.view(nullptr, t1);
    auto bundle = root_view.as_bundle();
    auto items = bundle.field("items");
    items.bind_output(first_output.view(t1));

    auto list_view = items.as_list();
    auto cached_child = list_view[1];
    REQUIRE(cached_child.schema() == ts_int);
    REQUIRE(cached_child.data_view().schema() == ts_int);
    REQUIRE(cached_child.value().checked_as<std::int32_t>() == 20);
    cached_child.make_active();
    REQUIRE(cached_child.active());

    auto root_data = root_view.data_view().borrowed_ref();
    auto root_data_bundle = root_data.as_bundle();
    auto target_data = root_data_bundle.field("items");
    auto target_data_list = target_data.as_list();
    auto target_child_data = target_data_list[1];
    REQUIRE(target_child_data.schema() == ts_int);
    REQUIRE(target_child_data.value().checked_as<std::int32_t>() == 20);

    auto rebound_root = input.view(nullptr, t2);
    auto rebound_bundle = rebound_root.as_bundle();
    auto rebound_items = rebound_bundle.field("items");
    rebound_items.bind_output(second_output.view(t2));

    REQUIRE(cached_child.schema() == ts_int);
    REQUIRE(cached_child.data_view().schema() == ts_int);
    REQUIRE(cached_child.value().checked_as<std::int32_t>() == 200);
    cached_child.make_passive();

    auto rebound_root_view = input.view(nullptr, t2);
    auto rebound_root_data = rebound_root_view.data_view().borrowed_ref();
    auto rebound_root_data_bundle = rebound_root_data.as_bundle();
    auto rebound_target_data = rebound_root_data_bundle.field("items");
    auto rebound_target_data_list = rebound_target_data.as_list();
    auto rebound_target_child_data = rebound_target_data_list[1];
    REQUIRE(rebound_target_child_data.schema() == ts_int);
    REQUIRE(rebound_target_child_data.value().checked_as<std::int32_t>() == 200);
}

TEST_CASE("TSInput output binding levels expose values and data views from root to leaves")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *nested   = registry.tsb("TSInputBindingLevelsNested", {{"x", ts_int}, {"y", ts_int}});
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb(
        "TSInputBindingLevelsRoot",
        {
            {"leaf", ts_int},
            {"bundle", nested},
            {"whole_list", list},
            {"leaf_list", list},
        });

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(ts_int),
            TSEndpointSchema::peered(nested),
            TSEndpointSchema::peered(list),
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput leaf_output{*ts_int};
    TSOutput bundle_output{*nested};
    TSOutput list_output{*list};
    TSOutput first_element_output{*ts_int};
    TSOutput second_element_output{*ts_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    const auto t1 = MIN_ST + TimeDelta{60};
    set_output(leaf_output, 7, t1);
    set_bundle_output(bundle_output, "x", 10, t1);
    set_bundle_output(bundle_output, "y", 20, t1);
    set_list_output(list_output, 0, 100, t1);
    set_list_output(list_output, 1, 200, t1);
    set_output(first_element_output, 1000, t1);
    set_output(second_element_output, 2000, t1);

    auto root_view = input.view(nullptr, t1);
    auto root_bundle = root_view.as_bundle();
    auto leaf = root_bundle.field("leaf");
    auto bundle = root_bundle.field("bundle");
    auto whole_list = root_bundle.field("whole_list");
    auto leaf_list = root_bundle.field("leaf_list");

    REQUIRE_FALSE(root_view.is_bindable());
    REQUIRE(leaf.is_bindable());
    REQUIRE(bundle.is_bindable());
    REQUIRE(whole_list.is_bindable());
    REQUIRE_FALSE(leaf_list.is_bindable());

    leaf.bind_output(leaf_output.view(t1));
    bundle.bind_output(bundle_output.view(t1));
    whole_list.bind_output(list_output.view(t1));
    auto leaf_list_view = leaf_list.as_list();
    leaf_list_view[0].bind_output(first_element_output.view(t1));
    leaf_list_view[1].bind_output(second_element_output.view(t1));

    REQUIRE(root_view.valid());
    REQUIRE(root_view.all_valid());
    REQUIRE(root_view.data_view().schema() == root);
    REQUIRE(root_view.data_view().type_ref().record() == root_view.type_ref().record());

    REQUIRE(leaf.valid());
    REQUIRE(leaf.value().checked_as<std::int32_t>() == 7);
    REQUIRE(leaf.data_view().schema() == ts_int);
    REQUIRE(leaf.data_view().data() == leaf_output.data_view().data());

    auto bundle_view = bundle.as_bundle();
    auto bundle_x = bundle_view.field("x");
    auto bundle_y = bundle_view.field("y");
    auto output_bundle_view = bundle_output.view(t1);
    auto output_bundle = output_bundle_view.as_bundle();
    REQUIRE(bundle.valid());
    REQUIRE(bundle.all_valid());
    REQUIRE(bundle.data_view().schema() == nested);
    REQUIRE(bundle.data_view().data() == bundle_output.data_view().data());
    REQUIRE(bundle_x.value().checked_as<std::int32_t>() == 10);
    REQUIRE(bundle_y.value().checked_as<std::int32_t>() == 20);
    REQUIRE(bundle_x.data_view().data() == output_bundle.field("x").data_view().data());
    REQUIRE(bundle_y.data_view().data() == output_bundle.field("y").data_view().data());

    auto whole_list_view = whole_list.as_list();
    auto output_list_view = list_output.view(t1);
    auto output_list = output_list_view.as_list();
    REQUIRE(whole_list.valid());
    REQUIRE(whole_list.all_valid());
    REQUIRE(whole_list.data_view().schema() == list);
    REQUIRE(whole_list.data_view().data() == list_output.data_view().data());
    REQUIRE(whole_list_view[0].value().checked_as<std::int32_t>() == 100);
    REQUIRE(whole_list_view[1].value().checked_as<std::int32_t>() == 200);
    REQUIRE(whole_list_view[0].data_view().data() == output_list[0].data_view().data());
    REQUIRE(whole_list_view[1].data_view().data() == output_list[1].data_view().data());

    REQUIRE(leaf_list.valid());
    REQUIRE(leaf_list.all_valid());
    REQUIRE(leaf_list.data_view().schema() == list);
    REQUIRE(leaf_list_view[0].value().checked_as<std::int32_t>() == 1000);
    REQUIRE(leaf_list_view[1].value().checked_as<std::int32_t>() == 2000);
    REQUIRE(leaf_list_view[0].data_view().data() == first_element_output.data_view().data());
    REQUIRE(leaf_list_view[1].data_view().data() == second_element_output.data_view().data());

    REQUIRE(root_view.modified());
    REQUIRE(leaf.delta_value().checked_as<std::int32_t>() == 7);
    auto bundle_delta = bundle.delta_value().as_bundle();
    REQUIRE(bundle_delta.at("x").checked_as<std::int32_t>() == 10);
    REQUIRE(bundle_delta.at("y").checked_as<std::int32_t>() == 20);

    Value key_zero{Int{0}};
    Value key_one{Int{1}};
    auto  whole_list_delta = whole_list.delta_value().as_map();
    REQUIRE(whole_list_delta.size() == 2);
    REQUIRE(whole_list_delta.at(key_zero.view()).checked_as<std::int32_t>() == 100);
    REQUIRE(whole_list_delta.at(key_one.view()).checked_as<std::int32_t>() == 200);

    auto leaf_list_delta = leaf_list.delta_value().as_map();
    REQUIRE(leaf_list_delta.size() == 2);
    REQUIRE(leaf_list_delta.at(key_zero.view()).checked_as<std::int32_t>() == 1000);
    REQUIRE(leaf_list_delta.at(key_one.view()).checked_as<std::int32_t>() == 2000);

    auto root_delta = root_view.delta_value().as_bundle();
    REQUIRE(root_delta.at("leaf").checked_as<std::int32_t>() == 7);
    REQUIRE(root_delta.at("bundle").as_bundle().at("x").checked_as<std::int32_t>() == 10);
    REQUIRE(root_delta.at("bundle").as_bundle().at("y").checked_as<std::int32_t>() == 20);
    REQUIRE(root_delta.at("whole_list").as_map().at(key_zero.view()).checked_as<std::int32_t>() == 100);
    REQUIRE(root_delta.at("whole_list").as_map().at(key_one.view()).checked_as<std::int32_t>() == 200);
    REQUIRE(root_delta.at("leaf_list").as_map().at(key_zero.view()).checked_as<std::int32_t>() == 1000);
    REQUIRE(root_delta.at("leaf_list").as_map().at(key_one.view()).checked_as<std::int32_t>() == 2000);

    auto root_data_delta = root_view.data_view().delta_value(t1).as_bundle();
    REQUIRE(root_data_delta.at("leaf").checked_as<std::int32_t>() == 7);
    REQUIRE(root_data_delta.at("leaf_list").as_map().at(key_one.view()).checked_as<std::int32_t>() == 2000);

    auto root_data = root_view.data_view().borrowed_ref();
    REQUIRE(root_data.valid());
    REQUIRE(root_data.schema() == root);
    REQUIRE(root_data.all_valid());

    auto root_data_bundle = root_data.as_bundle();
    auto leaf_data = root_data_bundle.field("leaf");
    auto bundle_data = root_data_bundle.field("bundle");
    auto whole_list_data = root_data_bundle.field("whole_list");
    auto leaf_list_data = root_data_bundle.field("leaf_list");

    REQUIRE(leaf_data.schema() == ts_int);
    REQUIRE(leaf_data.value().checked_as<std::int32_t>() == 7);
    REQUIRE(leaf_data.data() == leaf_output.data_view().data());

    auto bundle_data_view = bundle_data.as_bundle();
    REQUIRE(bundle_data.schema() == nested);
    REQUIRE(bundle_data.data() == bundle_output.data_view().data());
    REQUIRE(bundle_data_view.field("x").value().checked_as<std::int32_t>() == 10);
    REQUIRE(bundle_data_view.field("y").value().checked_as<std::int32_t>() == 20);

    auto whole_list_data_view = whole_list_data.as_list();
    REQUIRE(whole_list_data.schema() == list);
    REQUIRE(whole_list_data.data() == list_output.data_view().data());
    REQUIRE(whole_list_data_view[0].value().checked_as<std::int32_t>() == 100);
    REQUIRE(whole_list_data_view[1].value().checked_as<std::int32_t>() == 200);

    auto leaf_list_data_view = leaf_list_data.as_list();
    REQUIRE(leaf_list_data.schema() == list);
    REQUIRE(leaf_list_data_view[0].data() == first_element_output.data_view().data());
    REQUIRE(leaf_list_data_view[1].data() == second_element_output.data_view().data());
    REQUIRE(leaf_list_data_view[0].value().checked_as<std::int32_t>() == 1000);
    REQUIRE(leaf_list_data_view[1].value().checked_as<std::int32_t>() == 2000);
}

TEST_CASE("TSInput binding rejects non-peered views and incompatible output schemas")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *dbl_meta = registry.register_scalar<double>("double");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ts_dbl   = registry.ts(dbl_meta);
    const auto *root     = registry.tsb("TSInputBindingValidationRoot", {{"value", ts_int}});
    const auto *bad_root = registry.tsb("TSInputBindingValidationBadRoot", {{"value", ts_dbl}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(ts_int),
        });

    TSOutput wrong_root{*bad_root};
    TSOutput wrong_leaf{*ts_dbl};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    auto root_view = input.view();
    REQUIRE_FALSE(root_view.is_bindable());
    REQUIRE_THROWS_AS(root_view.bind_output(wrong_root.view()), std::logic_error);

    auto root_bundle = root_view.as_bundle();
    auto leaf        = root_bundle.field("value");
    REQUIRE_THROWS_AS(leaf.bind_output(wrong_leaf.view()), std::invalid_argument);
}

TEST_CASE("TSInput active root bubbles output modifications through non-peered prefixes")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TSInputRootBubblingRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput first_output{*ts_int};
    TSOutput second_output{*ts_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    auto root_binding_view = input.view();
    auto binding_bundle = root_binding_view.as_bundle();
    auto binding_items = binding_bundle.field("items");
    auto binding_list = binding_items.as_list();
    binding_list[0].bind_output(first_output.view());
    binding_list[1].bind_output(second_output.view());

    RecordingNotifiable recorder;
    auto active_root = input.view(&recorder);
    active_root.make_active();
    REQUIRE(active_root.active());

    const auto t1 = MIN_ST + TimeDelta{30};
    const auto t2 = t1 + TimeDelta{1};
    set_output(first_output, 1, t1);
    set_output(second_output, 2, t2);

    REQUIRE(recorder.notified == std::vector<DateTime>{t1, t2});

    auto t2_root = input.view(nullptr, t2);
    REQUIRE(t2_root.modified());
    REQUIRE(t2_root.last_modified_time() == t2);

    auto bundle = t2_root.as_bundle();
    auto modified_bundle_items = collect_range(bundle.modified_items());
    REQUIRE(modified_bundle_items.size() == 1);
    REQUIRE(std::string{modified_bundle_items[0].first} == "items");

    auto modified_list = modified_bundle_items[0].second.as_list();
    auto modified_list_items = collect_range(modified_list.modified_items());
    REQUIRE(modified_list_items.size() == 1);
    REQUIRE(modified_list_items[0].first == 1);

    active_root.make_passive();
    REQUIRE_FALSE(active_root.active());

    const auto t3 = t2 + TimeDelta{1};
    set_output(first_output, 3, t3);
    REQUIRE(recorder.notified == std::vector<DateTime>{t1, t2});
}

TEST_CASE("TSInput activation subscribes without notifying for an already-valid target")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    TSOutput output{*ts_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(
        *ts_int,
        TSEndpointSchema::peered(ts_int))};

    const auto t1 = MIN_ST + TimeDelta{30};
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    set_output(output, 1, t1);

    RecordingNotifiable recorder;
    auto                view = input.view(&recorder, t2);
    view.bind_output(output.view(t2));
    REQUIRE(view.valid());
    REQUIRE_FALSE(view.modified());

    view.make_active();
    CHECK(view.active());
    CHECK_FALSE(view.modified());
    CHECK(recorder.notified.empty());

    set_output(output, 2, t3);
    CHECK(recorder.notified == std::vector<DateTime>{t3});
}

TEST_CASE("TSInput peered collection descendants can be activated independently")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TSInputBoundListRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(list),
        });

    TSOutput output{*list};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    auto output_view = output.view();
    auto output_list = output_view.as_list();
    {
        auto first = output_list[0].begin_mutation(MIN_ST);
        hgraph::Value one{1};
        REQUIRE(first.copy_value_from(one.view()));
    }
    {
        auto second = output_list[1].begin_mutation(MIN_ST);
        hgraph::Value two{2};
        REQUIRE(second.copy_value_from(two.view()));
    }

    auto input_root_view = input.view();
    auto input_root = input_root_view.as_bundle();
    auto input_items = input_root.field("items");
    REQUIRE(input_items.is_bindable());
    REQUIRE_FALSE(input_items.bound());
    input_items.bind_output(output.view());
    REQUIRE(input_items.bound());

    RecordingNotifiable recorder;
    auto active_root_view = input.view(&recorder);
    auto active_root = active_root_view.as_bundle();
    auto active_items = active_root.field("items");
    auto active_list = active_items.as_list();
    REQUIRE(active_items.is_bindable());
    REQUIRE(active_items.bound());
    REQUIRE(active_list.is_bindable());
    REQUIRE(active_list.bound());
    auto second = active_list[1];
    REQUIRE(second.bound());
    REQUIRE(second.is_bindable());
    REQUIRE_NOTHROW(second.bind_output(output.view()));
    second.make_active();
    REQUIRE(second.active());
    REQUIRE(output.data_view().observer_count() == 1);
    REQUIRE(output_list[0].data_view().observer_count() == 0);
    REQUIRE(output_list[1].data_view().observer_count() == 1);

    const auto t1 = MIN_ST + TimeDelta{10};
    const auto t2 = t1 + TimeDelta{1};
    {
        auto view = output.view();
        auto list_view = view.as_list();
        auto first_mutation = list_view[0].begin_mutation(t1);
        hgraph::Value value{10};
        REQUIRE(first_mutation.copy_value_from(value.view()));
    }
    REQUIRE(recorder.notified.empty());

    {
        auto view = output.view();
        auto list_view = view.as_list();
        auto second_mutation = list_view[1].begin_mutation(t2);
        hgraph::Value value{20};
        REQUIRE(second_mutation.copy_value_from(value.view()));
    }
    REQUIRE(recorder.notified == std::vector<DateTime>{t2});

    second.make_passive();
    REQUIRE(output.data_view().observer_count() == 1);
    REQUIRE(output_list[1].data_view().observer_count() == 0);
}

TEST_CASE("TSInput shape casts return endpoint views for slot collections")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tss      = registry.tss(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);
    const auto *root     = registry.tsb("TSInputSlotRoot", {{"set", tss}, {"dict", tsd}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(tss),
            TSEndpointSchema::peered(tsd),
        });

    TSOutput set_output{*tss};
    TSOutput dict_output{*tsd};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    Value      one{1};
    Value      two{2};
    Value      key{7};
    Value      value{42};
    Value      replacement{84};

    {
        auto set_view = set_output.view(t1);
        auto set = set_view.as_set();
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }
    {
        auto dict_view = dict_output.view(t1);
        auto dict = dict_view.as_dict();
        auto mutation = dict.begin_mutation(t1);
        auto child = mutation.at(key.view());
        auto child_mutation = child.begin_mutation(t1);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto root_view = input.view(nullptr, t1);
    auto bundle = root_view.as_bundle();
    auto set_field = bundle.field("set");
    auto dict_field = bundle.field("dict");
    auto set_input = set_field.as_set();
    auto dict_input = dict_field.as_dict();

    set_input.bind_output(set_output.view(t1));
    dict_input.bind_output(dict_output.view(t1));

    REQUIRE(set_input.valid());
    REQUIRE(set_input.contains(one.view()));
    REQUIRE(dict_input.valid());
    REQUIRE(dict_input.contains(key.view()));
    REQUIRE(range_size(dict_input.values()) == 1);
    auto dict_child = dict_input.at(key.view());
    REQUIRE(dict_child.valid());
    REQUIRE(dict_child.value().checked_as<std::int32_t>() == 42);

    RecordingNotifiable recorder;
    auto active_root_view = input.view(&recorder, t2);
    auto active_bundle = active_root_view.as_bundle();
    auto active_set_field = active_bundle.field("set");
    auto active_dict_field = active_bundle.field("dict");
    auto active_set = active_set_field.as_set();
    auto active_dict = active_dict_field.as_dict();
    auto active_dict_child = active_dict.at(key.view());
    active_set.make_active();
    active_dict_child.make_active();
    REQUIRE(active_set.active());
    REQUIRE(active_dict_child.active());
    REQUIRE(recorder.notified.empty());

    {
        auto set_view = set_output.view(t2);
        auto set = set_view.as_set();
        auto mutation = set.begin_mutation(t2);
        REQUIRE(mutation.add(two.view()));
    }
    {
        auto dict_view = dict_output.view(t2);
        auto dict = dict_view.as_dict();
        auto child = dict.at(key.view());
        auto mutation = child.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(replacement.view()));
    }

    REQUIRE(recorder.notified == std::vector<DateTime>{t2, t2});

    active_set.make_passive();
    active_dict_child.make_passive();
}

TEST_CASE("TSInput endpoint operations own structural projections")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    auto       &factory = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *tss = registry.tss(int_meta);
    const auto *tsd = registry.tsd(int_meta, ts_int);

    TSData set_data{factory.data_type_for(tss)};
    auto set_source = set_data.view();
    auto set_structural = detail::structural_observation_for(set_source);
    REQUIRE(set_structural.data() == set_source.data());
    REQUIRE(set_structural.schema() == tss);

    TSData dict_data{factory.data_type_for(tsd)};
    auto dict_source = dict_data.view();
    auto expected_key_set = dict_source.as_dict().key_set().base();
    auto dict_structural = detail::structural_observation_for(dict_source);
    REQUIRE(dict_structural.data() == expected_key_set.data());
    REQUIRE(dict_structural.schema() == expected_key_set.schema());
    REQUIRE(dict_structural.schema()->kind == TSTypeKind::TSS);

    TSData scalar_data{factory.data_type_for(ts_int)};
    REQUIRE_THROWS_AS(detail::structural_observation_for(scalar_data.view()),
                      std::invalid_argument);
    REQUIRE_THROWS_AS(detail::structural_observation_for(TSDataView{}),
                      std::logic_error);
}

TEST_CASE("TSInput structural activation survives an unbound target link")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *tss = registry.tss(int_meta);
    const auto *tsd = registry.tsd(int_meta, ts_int);
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    TSInput set_input{TSInputBuilderFactory::checked_builder_for(
        *tss, TSEndpointSchema::peered(tss))};
    TSInput dict_input{TSInputBuilderFactory::checked_builder_for(
        *tsd, TSEndpointSchema::peered(tsd))};
    TSInput scalar_input{TSInputBuilderFactory::checked_builder_for(
        *ts_int, TSEndpointSchema::peered(ts_int))};
    TSOutput set_output{*tss};
    TSOutput dict_output{*tsd};
    RecordingNotifiable set_notifications;
    RecordingNotifiable dict_notifications;
    RecordingNotifiable scalar_notifications;

    auto active_set = set_input.view(&set_notifications, t1);
    auto active_dict = dict_input.view(&dict_notifications, t1);
    active_set.make_structural_active();
    active_dict.make_structural_active();
    REQUIRE(active_set.active());
    REQUIRE(active_dict.active());

    auto unsupported = scalar_input.view(&scalar_notifications, t1);
    REQUIRE_THROWS_AS(unsupported.make_structural_active(), std::invalid_argument);

    auto set_binding = set_input.view(nullptr, t1);
    auto dict_binding = dict_input.view(nullptr, t1);
    set_binding.bind_output(set_output.view(t1));
    dict_binding.bind_output(dict_output.view(t1));
    // TSS structural observation uses the root storage itself. It must retain
    // a dedicated scheduling subscription rather than taking the value-root
    // coalescing path.
    REQUIRE(set_output.data_view().observer_count() == 2);
    REQUIRE(dict_output.data_view().observer_count() == 1);

    Value key{std::int32_t{7}};
    Value value{std::int32_t{42}};
    {
        auto data = set_output.data_view();
        auto mutation = data.as_set().begin_mutation(t2);
        REQUIRE(mutation.add(key.view()));
    }
    {
        auto data = dict_output.data_view();
        auto mutation = data.as_dict().begin_mutation(t2);
        auto child = mutation.at(key.view());
        auto child_mutation = child.begin_mutation(t2);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    REQUIRE(set_notifications.notified == std::vector<DateTime>{t2});
    REQUIRE(dict_notifications.notified == std::vector<DateTime>{t2});

    active_set.make_passive();
    active_dict.make_passive();
}

TEST_CASE("TSInput endpoint operations preserve published structural state semantics")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    auto       &factory = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *tss = registry.tss(int_meta);
    const auto *tsd = registry.tsd(int_meta, ts_int);
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    const auto t4 = t3 + TimeDelta{1};
    const auto t5 = t4 + TimeDelta{1};
    Value key{std::int32_t{7}};
    Value other_key{std::int32_t{8}};
    Value value{std::int32_t{42}};

    TSData set_data{factory.data_type_for(tss)};
    auto set_source = set_data.view();
    auto set = set_source.as_set();
    REQUIRE_FALSE(detail::has_published_structural_state(set_source, t1));
    {
        auto empty = stdlib::make_set<std::int32_t>({});
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(empty.view()));
    }
    REQUIRE_FALSE(detail::has_published_structural_state(set_source, t1));
    REQUIRE(detail::has_published_structural_state(set_source, t2));
    {
        auto mutation = set.begin_mutation(t2);
        REQUIRE(mutation.add(key.view()));
    }
    REQUIRE_FALSE(detail::has_published_structural_state(set_source, t2));
    {
        auto mutation = set.begin_mutation(t3);
        REQUIRE(mutation.remove(key.view()));
    }
    REQUIRE(detail::has_published_structural_state(set_source, t3));

    TSData empty_dict_data{factory.data_type_for(tsd)};
    auto empty_dict_source = empty_dict_data.view();
    {
        auto empty = stdlib::make_map<std::int32_t, std::int32_t>({});
        auto mutation = empty_dict_source.as_dict().begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(empty.view()));
    }
    REQUIRE_FALSE(detail::has_published_structural_state(empty_dict_source, t1));
    REQUIRE(detail::has_published_structural_state(empty_dict_source, t2));

    TSData dict_data{factory.data_type_for(tsd)};
    auto dict_source = dict_data.view();
    auto dict = dict_source.as_dict();
    {
        auto mutation = dict.begin_mutation(t1);
        static_cast<void>(mutation.at(key.view()));
    }
    REQUIRE_FALSE(detail::has_published_structural_state(dict_source, t1));
    REQUIRE(detail::has_published_structural_state(dict_source, t2));
    {
        auto mutation = dict.begin_mutation(t2);
        static_cast<void>(mutation.at(other_key.view()));
    }
    REQUIRE_FALSE(detail::has_published_structural_state(dict_source, t2));
    {
        auto mutation = dict.begin_mutation(t3);
        auto child = mutation.at(key.view());
        auto child_mutation = child.begin_mutation(t3);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }
    REQUIRE_FALSE(detail::has_published_structural_state(dict_source, t3));
    REQUIRE(detail::has_published_structural_state(dict_source, t4));
    {
        auto child = dict.at(key.view());
        auto mutation = child.begin_mutation(t4);
        REQUIRE(mutation.invalidate());
    }
    REQUIRE_FALSE(detail::has_published_structural_state(dict_source, t4));
    {
        auto mutation = dict.begin_mutation(t5);
        REQUIRE(mutation.erase(key.view()));
    }
    REQUIRE(detail::has_published_structural_state(dict_source, t5));

    TSData scalar_data{factory.data_type_for(ts_int)};
    REQUIRE_THROWS_AS(
        detail::has_published_structural_state(scalar_data.view(), t1),
        std::invalid_argument);
    REQUIRE_FALSE(detail::has_published_structural_state(TSDataView{}, t1));
}

TEST_CASE("TSD input structural ranges do not repeat a prior removal on a forwarding retick")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_integer = registry.ts(integer);
    const auto *dict_schema = registry.tsd(integer, ts_integer);

    TSOutput first{*dict_schema};
    TSOutput second{*dict_schema};
    TSOutput forwarding{TSEndpointSchema::peered(dict_schema)};
    TSInput input{TSInputBuilderFactory::checked_builder_for(
        *dict_schema, TSEndpointSchema::peered(dict_schema))};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    Value removed_key{std::int32_t{1}};
    Value surviving_key{std::int32_t{2}};
    Value initial{std::int32_t{10}};

    for (TSOutput *source : {&first, &second})
    {
        auto output_view = source->view(t1);
        auto dict = output_view.as_dict();
        auto mutation = dict.begin_mutation(t1);
        mutation.set(removed_key.view(), initial.view());
        mutation.set(surviving_key.view(), initial.view());
    }
    for (TSOutput *source : {&first, &second})
    {
        auto output_view = source->view(t2);
        auto dict = output_view.as_dict();
        auto mutation = dict.begin_mutation(t2);
        REQUIRE(mutation.erase(removed_key.view()));
    }

    forwarding.view(t1).bind_forwarding_target(first.view(t1));
    input.view(nullptr, t1).bind_output(forwarding.view(t1));
    forwarding.view(t3).bind_forwarding_target(second.view(t3));

    auto input_view = input.view(nullptr, t3);
    auto current = input_view.as_dict();
    REQUIRE(current.modified());
    REQUIRE_FALSE(current.structure_modified());
    auto added_keys = current.added_keys();
    auto added_values = current.added_values();
    auto added_items = current.added_items();
    auto removed_keys = current.removed_keys();
    auto removed_values = current.removed_values();
    auto removed_items = current.removed_items();
    CHECK(added_keys.begin() == added_keys.end());
    CHECK(added_values.begin() == added_values.end());
    CHECK(added_items.begin() == added_items.end());
    CHECK(removed_keys.begin() == removed_keys.end());
    CHECK(removed_values.begin() == removed_values.end());
    CHECK(removed_items.begin() == removed_items.end());
}

TEST_CASE("TSW input removed value is limited to the current evaluation cycle")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *window_schema = registry.tsw(integer, 1, 1);
    TSOutput output{window_schema};
    TSInput input{TSInputBuilderFactory::checked_builder_for(
        *window_schema, TSEndpointSchema::peered(window_schema))};

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    auto binding_view = input.view(nullptr, t1);
    binding_view.bind_output(output.view(t1));

    Value one{std::int32_t{1}};
    Value two{std::int32_t{2}};
    {
        auto output_view = output.data_view();
        auto window = output_view.as_window();
        auto mutation = window.begin_mutation(t1);
        mutation.push(one.view());
    }
    auto first_input = input.view(nullptr, t1);
    auto first_window = first_input.as_window();
    REQUIRE_FALSE(first_window.has_removed_value());
    {
        auto output_view = output.data_view();
        auto window = output_view.as_window();
        auto mutation = window.begin_mutation(t2);
        mutation.push(two.view());
    }

    auto current_input = input.view(nullptr, t2);
    auto current = current_input.as_window();
    REQUIRE(current.has_removed_value());
    REQUIRE(current.removed_value().checked_as<std::int32_t>() == 1);

    auto no_tick_input = input.view(nullptr, t3);
    auto no_tick = no_tick_input.as_window();
    REQUIRE_FALSE(no_tick.has_removed_value());
    REQUIRE_THROWS_AS(no_tick.removed_value(), std::logic_error);
    REQUIRE(no_tick.back().checked_as<std::int32_t>() == 2);
}

TEST_CASE("forwarding TSData projections preserve target bindings across realizations")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("projection_binding_int32");
    const auto *text = registry.value_type("str");
    const auto *base = registry.bundle(
        "tests.projection_binding", "Base", {{"id", integer}}, {}, true);
    const auto *leaf = registry.bundle(
        "tests.projection_binding", "Leaf",
        {{"id", integer}, {"a", text}, {"b", text}, {"c", text}, {"d", text}}, {base});
    const auto *scalar_schema = registry.ts(base);
    const auto *dict_schema = registry.tsd(integer, scalar_schema);
    const auto *bundle_schema = registry.tsb(
        "ProjectionBindingBundle", {{"value", scalar_schema}});
    const auto *window_schema = registry.tsw(base, 1, 1);

    const auto inline_realization = TypeRealizationSnapshot::capture(registry);
    const TypeRealizationOptions pooled_options{
        .polymorphic_compound_storage = PolymorphicCompoundStoragePolicy::Pooled,
    };
    const auto pooled_realization = TypeRealizationSnapshot::capture(registry, pooled_options);
    REQUIRE(inline_realization->type_for(base) != pooled_realization->graph_type_for(base));

    CompoundScalarStorage pools = CompoundScalarStorage::make_default();
    TypeRealizationScope pooled_scope{pooled_realization.get()};
    CompoundScalarStorageScope pool_scope{pools.view()};

    Value concrete{ValuePlanFactory::instance().type_for(leaf)};
    auto concrete_fields = concrete.as_bundle().begin_mutation();
    concrete_fields["id"].set(std::int32_t{7});
    concrete_fields["a"].set(Str{"a"});
    concrete_fields["b"].set(Str{"b"});
    concrete_fields["c"].set(Str{"c"});
    concrete_fields["d"].set(Str{"d"});
    const auto pooled_base_binding = pooled_realization->type_for(base);
    const auto pooled_graph_binding = pooled_realization->graph_type_for(base);
    REQUIRE(pooled_graph_binding != pooled_base_binding);
    Value polymorphic{pooled_base_binding};
    pooled_base_binding.ops_ref().copy_assign_from(
        pooled_base_binding, polymorphic.begin_mutation().mutable_data(),
        concrete.binding(), concrete.view().data());

    TSOutput scalar_output{*scalar_schema};
    TSOutput dict_output{*dict_schema};
    TSOutput bundle_output{*bundle_schema};
    const auto *window_plan = ts_data_plan_factory_detail::synthesise_window_plan(
        *window_schema, pooled_graph_binding);
    REQUIRE(window_plan != nullptr);
    const auto *window_component = window_plan->find_component("window");
    const auto *tracking_component = window_plan->find_component("tracking");
    REQUIRE(window_component != nullptr);
    REQUIRE(tracking_component != nullptr);
    const auto &window_ops = ts_data_plan_factory_detail::window_ts_data_ops(
        *window_schema, *window_plan, window_component->offset, tracking_component->offset,
        pooled_graph_binding, TypeRole::Output);
    TSOutput window_output{TSOutputTypeRef::checked(intern_ts_type(
        *window_schema, TypeRole::Output, *window_plan, window_ops,
        "tests.projection-binding.window-output"))};
    Value key{std::int32_t{1}};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    REQUIRE(scalar_output.view(t1).begin_mutation(t1).copy_value_from(polymorphic.view()));
    {
        auto dict_root = dict_output.data_view();
        auto mutation = dict_root.as_dict().begin_mutation(t1);
        mutation.set(key.view(), polymorphic.view());
    }
    {
        auto bundle_root = bundle_output.data_view();
        auto bundle = bundle_root.as_bundle();
        auto child = bundle.field("value");
        REQUIRE(child.begin_mutation(t1).copy_value_from(polymorphic.view()));
    }
    const auto push_window_value = [&](DateTime time) {
        auto window_root = window_output.data_view();
        auto window = window_root.as_window();
        REQUIRE(window.layout().element_binding == pooled_graph_binding);
        Value::storage_type graph_value{*pooled_graph_binding.record()};
        pooled_graph_binding.ops_ref().copy_assign_from(
            pooled_graph_binding, graph_value.data(), polymorphic.binding(), polymorphic.view().data());
        window.begin_mutation(time).push(ValueView{pooled_graph_binding, graph_value.data()});
    };
    push_window_value(t1);
    push_window_value(t2);

    TypeRealizationScope inline_scope{inline_realization.get()};
    TSInput scalar_input{TSInputBuilderFactory::checked_builder_for(
        *scalar_schema, TSEndpointSchema::peered(scalar_schema))};
    TSInput dict_input{TSInputBuilderFactory::checked_builder_for(
        *dict_schema, TSEndpointSchema::peered(dict_schema))};
    TSInput bundle_input{TSInputBuilderFactory::checked_builder_for(
        *bundle_schema, TSEndpointSchema::peered(bundle_schema))};
    TSOutput window_forwarding{TSEndpointSchema::peered(window_schema)};
    scalar_input.view(nullptr, t2).bind_output(scalar_output.view(t2));
    dict_input.view(nullptr, t2).bind_output(dict_output.view(t2));
    bundle_input.view(nullptr, t2).bind_output(bundle_output.view(t2));
    window_forwarding.view(t2).bind_forwarding_target(window_output.view(t2));

    const auto scalar_target = scalar_output.data_view().value();
    const auto scalar_link = scalar_input.view(nullptr, t2).data_view().value();
    REQUIRE(scalar_link.binding() == scalar_target.binding());

    auto dict_target_root = dict_output.data_view();
    auto dict_target = dict_target_root.as_dict().at(key.view());
    auto dict_link_root = dict_input.view(nullptr, t2);
    auto dict_link = dict_link_root.as_dict().data_view().at(key.view());
    REQUIRE(dict_link.storage_type() == dict_target.storage_type());
    REQUIRE(dict_link.value().binding() == dict_target.value().binding());

    auto bundle_target_root = bundle_output.data_view();
    auto bundle_target_view = bundle_target_root.as_bundle();
    auto bundle_target = bundle_target_view.field("value");
    auto bundle_link_root = bundle_input.view(nullptr, t2);
    auto bundle_link_view = bundle_link_root.as_bundle();
    auto bundle_link_data = bundle_link_view.data_view();
    auto bundle_link = bundle_link_data.field("value");
    REQUIRE(bundle_link.storage_type() == bundle_target.storage_type());
    REQUIRE(bundle_link.value().binding() == bundle_target.value().binding());

    auto window_target_root = window_output.data_view();
    auto window_target = window_target_root.as_window();
    auto window_link_root = window_forwarding.data_view();
    auto window_link = window_link_root.as_window();
    REQUIRE(window_link.layout().element_binding != window_target.layout().element_binding);
    REQUIRE(window_link.at(0).binding() == window_target.at(0).binding());
    REQUIRE(window_link.front().binding() == window_target.front().binding());
    REQUIRE(window_link.back().binding() == window_target.back().binding());
    REQUIRE(window_link.time_value_at(0).binding() == window_target.time_value_at(0).binding());
    REQUIRE(window_link.has_removed_value(t2));
    REQUIRE(window_link.removed_value(t2).binding() == window_target.removed_value(t2).binding());
    REQUIRE(window_link.at(0).concrete().schema() == leaf);
    REQUIRE(window_link.removed_value(t2).concrete().schema() == leaf);
    auto linked_values = window_link.values();
    auto target_values = window_target.values();
    const auto linked_first = *linked_values.begin();
    const auto target_first = *target_values.begin();
    REQUIRE(linked_first.binding() == target_first.binding());
    REQUIRE(linked_first.concrete().schema() == leaf);
    auto linked_time_values = window_link.time_values();
    auto target_time_values = window_target.time_values();
    const auto linked_first_time = *linked_time_values.begin();
    const auto target_first_time = *target_time_values.begin();
    REQUIRE(linked_first_time.binding() == target_first_time.binding());

    const auto t3 = t2 + TimeDelta{1};
    window_target.begin_mutation(t3).clear();
    REQUIRE(window_link.cleared(t3));
}

TEST_CASE("TSW ranges use stable ops contexts across data and endpoint roles")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    auto       &factory = TSDataPlanFactory::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("tsw_range_context_int32");
    const auto *schema = registry.tsw(integer, 3, 1);
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    const auto t4 = t3 + TimeDelta{1};

    const auto populate = [&](TSWDataView window) {
        for (const auto &[time, raw] : std::array{
                 std::pair{t1, std::int32_t{11}},
                 std::pair{t2, std::int32_t{22}},
                 std::pair{t3, std::int32_t{33}},
             })
        {
            Value value{raw};
            auto mutation = window.begin_mutation(time);
            mutation.push(value.view());
        }
    };

    const auto require_ranges = [&](TSWDataView window) {
        const auto *ops = &static_cast<const TSWDataOps &>(window.base().ops());
        const auto values = window.values();
        const auto time_values = window.time_values();
        const auto times = window.value_times();
        REQUIRE(values.context == ops);
        REQUIRE(time_values.context == ops);
        REQUIRE(times.context == ops);

        std::vector<std::int32_t> observed_values;
        for (const auto value : values) observed_values.push_back(value.checked_as<std::int32_t>());
        REQUIRE(observed_values == std::vector<std::int32_t>{11, 22, 33});

        std::vector<DateTime> observed_times;
        for (const auto time : times) observed_times.push_back(time);
        REQUIRE(observed_times == std::vector<DateTime>{t1, t2, t3});

        std::vector<DateTime> observed_time_values;
        for (const auto value : time_values) observed_time_values.push_back(value.checked_as<DateTime>());
        REQUIRE(observed_time_values == observed_times);
    };

    TSData data{factory.data_type_for(schema)};
    auto data_view = data.view();
    populate(data_view.as_window());
    require_ranges(data_view.as_window());

    TSOutput output{schema};
    auto output_data = output.data_view();
    populate(output_data.as_window());
    auto output_root = output.view(t3);
    auto output_window = output_root.as_window();
    require_ranges(output_window.data_view());
    REQUIRE(range_size(output_window.values()) == 3);
    REQUIRE(range_size(output_window.time_values()) == 3);
    REQUIRE(range_size(output_window.value_times()) == 3);

    TSInput owned{TSInputBuilderFactory::checked_builder_for(
        *schema, TSEndpointSchema::owned(schema))};
    auto owned_root = owned.view(nullptr, t3);
    auto owned_window = owned_root.as_window();
    auto owned_data = owned_window.data_view();
    TSDataView owned_writable{factory.output_type_for(schema).as_role(),
                              const_cast<void *>(owned_data.base().data())};
    populate(owned_writable.as_window());
    auto populated_owned_root = owned.view(nullptr, t3);
    auto populated_owned_window = populated_owned_root.as_window();
    require_ranges(populated_owned_window.data_view());
    REQUIRE(range_size(populated_owned_window.values()) == 3);
    REQUIRE_THROWS_AS(populated_owned_window.data_view().base().begin_mutation(t4), std::logic_error);

    TSInput peered{TSInputBuilderFactory::checked_builder_for(
        *schema, TSEndpointSchema::peered(schema))};
    auto peered_binding = peered.view(nullptr, t3);
    peered_binding.bind_output(output.view(t3));
    auto peered_root = peered.view(nullptr, t3);
    auto peered_window = peered_root.as_window();
    require_ranges(peered_window.data_view());
    REQUIRE(range_size(peered_window.values()) == 3);

    auto canonical_view = data.view();
    require_ranges(canonical_view.as_window());
}

TEST_CASE("prepared routes re-check the observation kind before the fast path")
{
    using namespace hgraph;

    // RFC 0008 stage 5 regression (review catch): a runtime
    // make_structural_active() replaces the SAME trie node's observed handle
    // with a bound STRUCTURAL view; a bound-only fast path would then expose
    // structural storage as the value route. The prepared read must mirror
    // resolved_target_at_path's full trust condition and fall back.
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    // TSD: its structural observation is the KEY SET — genuinely different
    // storage from the value target (a TSS's structural view is the set
    // itself, which could not distinguish the routes).
    const auto *tsd      = registry.tsd(int_meta, ts_int);
    const auto *root     = registry.tsb("PreparedRouteKindRoot", {{"s", tsd}});

    const auto input_schema =
        TSEndpointSchema::non_peered(root, {TSEndpointSchema::peered(tsd)});

    TSOutput output{*tsd};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    auto binding_root   = input.view();
    auto binding_bundle = binding_root.as_bundle();
    binding_bundle.field("s").bind_output(output.view());

    RecordingNotifiable recorder;
    auto active_root   = input.view(&recorder);
    auto active_bundle = active_root.as_bundle();
    active_bundle.field("s").make_active();

    const auto t = MIN_ST + TimeDelta{5};
    auto root_view = input.view(nullptr, t);
    auto route     = root_view.prepare_child_route(0);
    REQUIRE(route.ready());
    REQUIRE(route.target);

    const auto require_matches_slow_path = [&] {
        auto fast = root_view.child_from_prepared(route);
        auto slow = root_view.indexed_child_at(0);
        REQUIRE(fast.data_view().data() == slow.data_view().data());
        REQUIRE(fast.data_view().schema() == slow.data_view().schema());
    };

    // Value-kind active: the fast path serves the value target.
    require_matches_slow_path();

    // Switch the SAME slot to structural observation: the trie node's
    // handle is now a bound structural view — the prepared read must
    // reject it and resolve the value route exactly like the slow path.
    active_bundle.field("s").make_structural_active();
    require_matches_slow_path();

    // Passive: observed reset in place — fall back again.
    active_bundle.field("s").make_passive();
    require_matches_slow_path();

    // Re-activated value observation: the fast path resumes on the same
    // stable trie node.
    active_bundle.field("s").make_active();
    require_matches_slow_path();
}
