// Tests for the TimeSeriesReference struct — the C++ value type that
// backs the canonical ``TimeSeriesReference`` atomic schema in the type
// registry.

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>

#include <algorithm>
#include <array>
#include <initializer_list>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

namespace
{
    template <std::size_t Count>
    [[nodiscard]] std::array<hgraph::DateTime, Count> sequential_times(
        hgraph::DateTime start = hgraph::MIN_ST,
        hgraph::TimeDelta step = hgraph::TimeDelta{1})
    {
        std::array<hgraph::DateTime, Count> result{};
        auto                                     next = start;
        for (auto &time : result)
        {
            time = next;
            next += step;
        }
        return result;
    }

    template <typename Range>
    std::size_t range_count(const Range &range)
    {
        std::size_t count = 0;
        for (auto it = range.begin(); it != range.end(); ++it) { ++count; }
        return count;
    }

    template <typename Range>
    [[nodiscard]] auto collect_range(const Range &range)
    {
        using value_type = std::decay_t<decltype(*range.begin())>;
        std::vector<value_type> result;
        for (auto value : range) { result.emplace_back(std::move(value)); }
        return result;
    }

    void set_output_value(hgraph::TSOutput &output, int value, hgraph::DateTime time)
    {
        hgraph::Value stored{value};
        auto          mutation = output.begin_mutation(time);
        REQUIRE(mutation.copy_value_from(stored.view()));
    }

    void set_output_reference(hgraph::TSOutput &output,
                              hgraph::TimeSeriesReference reference,
                              hgraph::DateTime time)
    {
        hgraph::Value stored{std::move(reference)};
        auto          mutation = output.begin_mutation(time);
        REQUIRE(mutation.copy_value_from(stored.view()));
    }

    void add_set_values(hgraph::TSOutput &output,
                        std::initializer_list<std::int32_t> values,
                        hgraph::DateTime time)
    {
        auto data     = output.data_view();
        auto set      = data.as_set();
        auto mutation = set.begin_mutation(time);
        for (const auto value : values)
        {
            hgraph::Value key{value};
            REQUIRE(mutation.add(key.view()));
        }
    }

    void set_dict_value(hgraph::TSOutput &output,
                        std::int32_t key,
                        std::int32_t value,
                        hgraph::DateTime time)
    {
        hgraph::Value key_value{key};
        hgraph::Value stored{value};
        auto          data = output.data_view();
        auto          dict = data.as_dict();
        auto          mutation = dict.begin_mutation(time);
        auto          child = mutation.at(key_value.view());
        REQUIRE(child.begin_mutation(time).copy_value_from(stored.view()));
    }

    struct ReferenceSlotObserver final : hgraph::SlotObserver
    {
        std::vector<std::string> events;

        void on_capacity(std::size_t, std::size_t) override {}
        void on_insert(std::size_t slot) override { events.emplace_back("insert:" + std::to_string(slot)); }
        void on_remove(std::size_t slot) override { events.emplace_back("remove:" + std::to_string(slot)); }
        void on_erase(std::size_t slot) override { events.emplace_back("erase:" + std::to_string(slot)); }
        void on_clear() override { events.emplace_back("clear"); }
    };

    struct ReferenceInvalidationObserver final : hgraph::Notifiable
    {
        std::size_t invalidations{0};
        const hgraph::TSDataTracking *source{nullptr};

        void notify(hgraph::DateTime) override {}
        void source_invalidated(const hgraph::TSDataTracking *invalidated) noexcept override
        {
            ++invalidations;
            source = invalidated;
        }
    };
}

TEST_CASE("TimeSeriesReference: alternative identities reseed after registry reset")
{
    using namespace hgraph;

    const auto exercise_generation = [] {
        auto &registry = TypeRegistry::instance();
        const auto *integer = registry.register_scalar<std::int32_t>("int32");
        const auto *ts = registry.ts(integer);
        const auto *ref = registry.ref(ts);
        const auto *dict = registry.tsd(integer, ts);
        const auto *ref_dict = registry.ref(dict);
        const auto *dict_of_ref = registry.tsd(integer, ref);
        const auto *source_bundle = registry.tsb("AlternativeResetSource", {{"value", ts}});
        const auto *requested_bundle = registry.tsb("AlternativeResetRequested", {{"value", ref}});

        TSOutput scalar_source{ts};
        TSOutput bundle_source{source_bundle};
        TSOutput scalar_ref_source{ref};
        TSOutput dict_ref_source{ref_dict};
        TSOutput interior_source{dict_of_ref};

        std::vector<std::string> labels;
        std::unordered_set<const void *> ops;
        const auto capture = [&](const TSOutputHandle &handle) {
            const auto type = handle.type_ref();
            REQUIRE(type.valid());
            labels.emplace_back(type.record()->implementation_name());
            ops.insert(type.record()->ops);
        };

        const auto scalar_to_ref = scalar_source.view(MIN_ST).binding_for(*ref);
        capture(scalar_to_ref);
        REQUIRE(scalar_to_ref.type_ref().ops() ==
                TSDataPlanFactory::instance().output_type_for(ref).ops());
        const auto bundle_to_ref = bundle_source.view(MIN_ST).binding_for(*requested_bundle);
        capture(bundle_to_ref);
        REQUIRE(bundle_to_ref.type_ref().ops() ==
                TSDataPlanFactory::instance().output_type_for(requested_bundle).ops());
        capture(scalar_ref_source.view(MIN_ST).binding_for(*ts));
        capture(dict_ref_source.view(MIN_ST).binding_for(*dict));
        capture(interior_source.view(MIN_ST).binding_for(*dict));
        REQUIRE(ops.size() == labels.size());
        return labels;
    };

    const auto first = exercise_generation();
    REQUIRE(first == std::vector<std::string>{
                         "ts.alternative.to-ref.output",
                         "ts.alternative.to-ref.output",
                         "ts.alternative.from-ref.output",
                         "ts.alternative.from-ref.output",
                         "ts.alternative.interior-ref.output"});

    reset_all_registries();
    const auto second = exercise_generation();
    REQUIRE(second == first);
}

TEST_CASE("TimeSeriesReference: default-constructed is empty with no target schema")
{
    using namespace hgraph;
    TimeSeriesReference ref;
    REQUIRE(ref.kind() == TimeSeriesReference::Kind::EMPTY);
    REQUIRE(ref.is_empty());
    REQUIRE_FALSE(ref.is_peered());
    REQUIRE_FALSE(ref.is_non_peered());
    REQUIRE(ref.target_schema() == nullptr);
}

TEST_CASE("TimeSeriesReference: empty reference can record an expected target schema")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    TimeSeriesReference ref{ts_int};
    REQUIRE(ref.is_empty());
    REQUIRE(ref.target_schema() == ts_int);
}

TEST_CASE("TimeSeriesReference: peered reference carries kind and target schema")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    TSOutput output{*ts_int};
    auto     ref = TimeSeriesReference::peered(output.view());
    REQUIRE(ref.is_peered());
    REQUIRE(ref.has_output());
    REQUIRE(ref.target_schema() == ts_int);
    REQUIRE(ref == TimeSeriesReference{output.view()});
}

TEST_CASE("TimeSeriesReference: non-peered reference holds sub-references")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsl      = registry.tsl(ts_int, /*fixed_size=*/2);

    TSOutput lhs{*ts_int};
    TSOutput rhs{*ts_int};

    auto composite = TimeSeriesReference::non_peered(
        tsl,
        {
            TimeSeriesReference{lhs.view()},
            TimeSeriesReference{rhs.view()},
        });

    REQUIRE(composite.is_non_peered());
    REQUIRE(composite.target_schema() == tsl);
    REQUIRE(composite.items().size() == 2);
    REQUIRE(composite[0].target_schema() == ts_int);
    REQUIRE(composite[1].is_peered());
    REQUIRE(composite[1] == TimeSeriesReference{rhs.view()});
}

TEST_CASE("TimeSeriesReference: items() and operator[] throw for non-NON_PEERED references")
{
    using namespace hgraph;
    TimeSeriesReference empty;
    REQUIRE_THROWS_AS(empty.items(), std::logic_error);
    REQUIRE_THROWS_AS(empty[0], std::logic_error);
}

TEST_CASE("TimeSeriesReference: operator[] bounds-checks NON_PEERED indexes")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    TSOutput output{*ts_int};
    auto     composite = TimeSeriesReference::non_peered(ts_int, {TimeSeriesReference{output.view()}});

    REQUIRE(composite[0].is_peered());
    REQUIRE_THROWS_AS(composite[1], std::out_of_range);
}

TEST_CASE("TimeSeriesReference: equality and hash respect kind, target_schema, and sub-items")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    const TimeSeriesReference empty_a;
    const TimeSeriesReference empty_b;
    REQUIRE(empty_a == empty_b);
    REQUIRE(empty_a.hash() == empty_b.hash());

    const TimeSeriesReference empty_with_schema{ts_int};
    REQUIRE_FALSE(empty_a == empty_with_schema);

    TSOutput first{*ts_int};
    TSOutput second{*ts_int};

    const auto peered_a = TimeSeriesReference::peered(first.view());
    const auto peered_b = TimeSeriesReference::peered(first.view());
    const auto peered_other = TimeSeriesReference::peered(second.view());
    REQUIRE(peered_a == peered_b);
    REQUIRE(peered_a.hash() == peered_b.hash());
    REQUIRE_FALSE(peered_a == peered_other);

    const auto composite_a = TimeSeriesReference::non_peered(ts_int, {TimeSeriesReference{first.view()}});
    const auto composite_b = TimeSeriesReference::non_peered(ts_int, {TimeSeriesReference{first.view()}});
    const TimeSeriesReference composite_diff{ts_int, {empty_a}};
    REQUIRE(composite_a == composite_b);
    REQUIRE_FALSE(composite_a == composite_diff);

    // hash should distinguish between kinds.
    REQUIRE(empty_a.hash() != peered_a.hash());

    // unordered_set should accept TimeSeriesReference via std::hash specialisation.
    std::unordered_set<TimeSeriesReference> seen;
    seen.insert(empty_a);
    seen.insert(peered_a);
    REQUIRE(seen.size() == 2);
}

TEST_CASE("TimeSeriesReference: input view construction handles peered terminals and non-peered prefixes")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TimeSeriesReferenceInputRoot", {{"leaf", ts_int}, {"items", list}});

    const auto endpoint_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(ts_int),
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput leaf_output{*ts_int};
    TSOutput item_output{*ts_int};
    TSOutput second_item_output{*ts_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, endpoint_schema)};

    auto root_view = input.view();
    auto bundle    = root_view.as_bundle();
    auto leaf      = bundle.field("leaf");
    leaf.bind_output(leaf_output.view());

    auto items      = bundle.field("items");
    auto list_view  = items.as_list();
    auto first_item = list_view[0];
    first_item.bind_output(item_output.view());

    const TimeSeriesReference leaf_ref = leaf.reference();
    REQUIRE(leaf_ref.is_peered());
    REQUIRE(leaf_ref == TimeSeriesReference{leaf_output.view()});

    const TimeSeriesReference missing_ref = list_view[1].reference();
    REQUIRE(missing_ref.is_empty());
    REQUIRE(missing_ref.target_schema() == ts_int);

    const TimeSeriesReference partial_root_ref{root_view};
    REQUIRE(partial_root_ref.is_non_peered());
    REQUIRE(partial_root_ref[1].is_non_peered());
    REQUIRE(partial_root_ref[1][1].is_empty());
    REQUIRE(partial_root_ref[1][1].target_schema() == ts_int);

    list_view[1].bind_output(second_item_output.view());

    const TimeSeriesReference root_ref{root_view};
    REQUIRE(root_ref.is_non_peered());
    REQUIRE(root_ref.target_schema() == root);
    REQUIRE(root_ref.items().size() == 2);
    REQUIRE(root_ref[0] == TimeSeriesReference{leaf_output.view()});
    REQUIRE(root_ref[1].is_non_peered());
    REQUIRE(root_ref[1].items().size() == 2);
    REQUIRE(root_ref[1][0] == TimeSeriesReference{item_output.view()});
    REQUIRE(root_ref[1][1] == TimeSeriesReference{second_item_output.view()});
}

TEST_CASE("TimeSeriesReference: target link negotiates output as REF alternative")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *root     = registry.tsb("TimeSeriesReferenceAlternativeInputRoot", {{"ref", ref_int}});

    const auto endpoint_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(ref_int),
        });

    TSOutput target{*ts_int};
    Value    value{17};
    const auto t1 = MIN_ST;
    {
        auto mutation = target.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    TSInput input{TSInputBuilderFactory::checked_builder_for(*root, endpoint_schema)};
    auto    root_view = input.view(nullptr, t1);
    auto    root_bundle = root_view.as_bundle();
    auto    ref_input = root_bundle.field("ref");

    ref_input.bind_output(target.view(t1));

    REQUIRE(ref_input.bound());
    REQUIRE(ref_input.valid());
    REQUIRE(ref_input.modified());

    const auto  reference_value = ref_input.value();
    const auto &reference       = reference_value.checked_as<TimeSeriesReference>();
    REQUIRE(reference.is_peered());
    REQUIRE(reference.target_schema() == ts_int);
    REQUIRE(reference == TimeSeriesReference{target.view(t1)});

    Value next_value{18};
    const auto t2 = t1 + TimeDelta{1};
    {
        auto mutation = target.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(next_value.view()));
    }

    auto t2_root_view = input.view(nullptr, t2);
    auto t2_bundle = t2_root_view.as_bundle();
    auto t2_ref_input = t2_bundle.field("ref");
    REQUIRE(t2_ref_input.valid());
    REQUIRE_FALSE(t2_ref_input.modified());
    const auto t2_reference_value = t2_ref_input.value().clone();
    REQUIRE(t2_reference_value.as<TimeSeriesReference>() == TimeSeriesReference{target.view(t2)});
}

TEST_CASE("TimeSeriesReference: to-REF alternative samples at bind time and ignores source value ticks")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *root     = registry.tsb("TimeSeriesReferenceProjectionTimingRoot", {{"ref", ref_int}});

    const auto endpoint_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(ref_int),
        });

    TSOutput target{*ts_int};
    {
        Value value{17};
        auto  mutation = target.begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    TSInput input{TSInputBuilderFactory::checked_builder_for(*root, endpoint_schema)};
    const auto bind_time = MIN_ST + TimeDelta{1};
    auto       root_view = input.view(nullptr, bind_time);
    auto       root_bundle = root_view.as_bundle();
    auto       ref_input = root_bundle.field("ref");
    ref_input.bind_output(target.view(bind_time));

    REQUIRE(ref_input.valid());
    REQUIRE(ref_input.modified());
    REQUIRE(ref_input.last_modified_time() == bind_time);

    auto reference_value = ref_input.value().clone();
    REQUIRE(reference_value.as<TimeSeriesReference>() == TimeSeriesReference{target.view(bind_time)});

    const auto source_tick_time = bind_time + TimeDelta{1};
    {
        Value value{18};
        auto  mutation = target.begin_mutation(source_tick_time);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    auto after_tick_root = input.view(nullptr, source_tick_time);
    auto after_tick_bundle = after_tick_root.as_bundle();
    auto after_tick_ref  = after_tick_bundle.field("ref");
    REQUIRE(after_tick_ref.valid());
    REQUIRE_FALSE(after_tick_ref.modified());
    REQUIRE(after_tick_ref.last_modified_time() == bind_time);

    auto after_tick_reference = after_tick_ref.value().clone();
    REQUIRE(after_tick_reference.as<TimeSeriesReference>() == TimeSeriesReference{target.view(source_tick_time)});
}

TEST_CASE("TimeSeriesReference: output alternatives are keyed by starting view and requested schema")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *named_bundle      = registry.tsb("TimeSeriesReferenceAlternativeKeyNamed", {{"value", ts_int}});
    const auto *structural_bundle = registry.un_named_tsb({{"value", ts_int}});
    const auto *ref_named         = registry.ref(named_bundle);
    const auto *ref_structural    = registry.ref(structural_bundle);

    TSOutput target{*named_bundle};
    {
        Value value{17};
        auto  target_data = target.data_view();
        auto  bundle = target_data.as_bundle();
        auto  child = bundle.field("value");
        auto  mutation = child.begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    auto source = target.view(MIN_ST);
    const auto before_alternatives = target.dynamic_storage_metrics();
    auto named_ref_handle = source.binding_for(*ref_named);
    auto structural_ref_handle = source.binding_for(*ref_structural);
    const auto after_alternatives = target.dynamic_storage_metrics();

    CHECK(after_alternatives.live_bytes > before_alternatives.live_bytes);
    CHECK(after_alternatives.reserved_bytes > before_alternatives.reserved_bytes);

    REQUIRE(named_ref_handle.schema() == ref_named);
    REQUIRE(structural_ref_handle.schema() == ref_structural);
    REQUIRE(named_ref_handle.data_view().data() != structural_ref_handle.data_view().data());

    const auto named_ref_value = named_ref_handle.view(MIN_ST).value().clone();
    const auto structural_ref_value = structural_ref_handle.view(MIN_ST).value().clone();

    REQUIRE(named_ref_value.as<TimeSeriesReference>().target_schema() == named_bundle);
    REQUIRE(structural_ref_value.as<TimeSeriesReference>().target_schema() == structural_bundle);
}

TEST_CASE("TimeSeriesReference: to-REF alternative stores fixed list children through TSData")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *source_schema = registry.tsl(ts_int, 2);
    const auto *requested_schema = registry.tsl(ref_int, 2);

    TSOutput target{*source_schema};
    auto     target_data = target.data_view();
    auto     source_list = target_data.as_list();
    {
        Value one{1};
        auto  mutation = source_list.at(0).begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    {
        Value two{2};
        auto  mutation = source_list.at(1).begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(two.view()));
    }

    auto source_output = target.view(MIN_ST);
    auto source_output_list = source_output.as_list();
    auto first_ref_handle = source_output_list.at(0).binding_for(*ref_int);
    auto second_ref_handle = source_output_list.at(1).binding_for(*ref_int);
    REQUIRE(first_ref_handle.data_view().data() != second_ref_handle.data_view().data());
    REQUIRE(first_ref_handle.view(MIN_ST).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_output_list.at(0)});
    REQUIRE(second_ref_handle.view(MIN_ST).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_output_list.at(1)});

    auto handle = target.view(MIN_ST).binding_for(*requested_schema);
    REQUIRE(handle.schema() == requested_schema);
    REQUIRE(handle.storage_type().record() != nullptr);
    REQUIRE(handle.type_ref().as_role().role() == TypeRole::Output);
    const auto requested_data_type = TSDataPlanFactory::instance().data_type_for(requested_schema);
    REQUIRE(handle.type_ref().plan() == requested_data_type.plan());

    TSData data_owner{requested_data_type};
    TSOutputHandle wrong_role{&target, data_owner.view()};
    REQUIRE_THROWS_AS(wrong_role.type_ref(), std::invalid_argument);

    auto projected_view = handle.view(MIN_ST);
    REQUIRE(projected_view.valid());
    REQUIRE(projected_view.modified());
    auto projected_list = projected_view.as_list();
    REQUIRE(projected_list.size() == 2);

    const auto  first_value  = projected_list.at(0).value();
    const auto  second_value = projected_list.at(1).value();
    const auto &first        = first_value.checked_as<TimeSeriesReference>();
    const auto &second       = second_value.checked_as<TimeSeriesReference>();
    REQUIRE(first.target_schema() == ts_int);
    REQUIRE(second.target_schema() == ts_int);
    REQUIRE(first == TimeSeriesReference{source_output_list.at(0)});
    REQUIRE(second == TimeSeriesReference{source_output_list.at(1)});
}

TEST_CASE("TimeSeriesReference: fixed to-REF owns Data storage behind an Output role facade")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *ref = registry.ref(ts);
    const auto *source_nested = registry.tsb("ToRefRoleSourceNested", {{"value", ts}});
    const auto *requested_nested = registry.tsb("ToRefRoleRequestedNested", {{"value", ref}});
    const auto *source_root = registry.tsb("ToRefRoleSourceRoot", {{"nested", source_nested}});
    const auto *requested_root = registry.tsb("ToRefRoleRequestedRoot", {{"nested", requested_nested}});

    TSOutput target{source_root};
    auto target_view = target.view(MIN_ST);
    auto target_bundle = target_view.as_bundle();
    auto target_nested_view = target_bundle.field("nested");
    auto target_nested = target_nested_view.as_bundle();
    Value stored{std::int32_t{42}};
    REQUIRE(target_nested.field("value").begin_mutation(MIN_ST).copy_value_from(stored.view()));

    auto handle = target.view(MIN_ST).binding_for(*requested_root);
    const auto data_type = TSDataPlanFactory::instance().data_type_for(requested_root);
    REQUIRE(handle.storage_type().record() != nullptr);
    REQUIRE(handle.type_ref().as_role().role() == TypeRole::Output);
    REQUIRE(handle.type_ref().plan() == data_type.plan());
    REQUIRE(std::string{handle.type_ref().record()->implementation_name()} ==
            "ts.alternative.to-ref.output");

    auto projected = handle.view(MIN_ST);
    REQUIRE(projected.type_ref() == handle.type_ref());
    auto projected_bundle = projected.as_bundle();
    auto projected_nested_view = projected_bundle.field("nested");
    REQUIRE(projected_nested_view.type_ref().as_role().role() == TypeRole::Output);
    REQUIRE(std::string{projected_nested_view.type_ref().record()->implementation_name()} ==
            "ts.fixed.output.embedded");
    auto projected_nested = projected_nested_view.as_bundle();
    const auto reference_value = projected_nested.field("value").value();
    REQUIRE(reference_value.checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{target_nested.field("value")});

    TSData owner{data_type};
    REQUIRE(owner.type_ref().role() == TypeRole::Data);
    TSOutputHandle wrong_role{&target, owner.view()};
    REQUIRE_THROWS_AS(wrong_role.type_ref(), std::invalid_argument);
}

TEST_CASE("TimeSeriesReference: to-REF TSD alternative keeps keys synchronized")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *source_schema = registry.tsd(int_meta, ts_int);
    const auto *requested_schema = registry.tsd(int_meta, ref_int);

    TSOutput target{*source_schema};
    Value    key_one{1};
    Value    key_two{2};
    {
        Value value{11};
        auto  data = target.data_view();
        auto  dict = data.as_dict();
        auto  mutation = dict.begin_mutation(MIN_ST);
        auto  child = mutation.at(key_one.view());
        auto  child_mutation = child.begin_mutation(MIN_ST);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto handle       = target.view(MIN_ST).binding_for(*requested_schema);
    auto initial_view = handle.view(MIN_ST);
    auto initial_dict = initial_view.as_dict();
    REQUIRE(initial_dict.contains(key_one.view()));
    REQUIRE(range_count(initial_dict.items()) == 1);

    const auto t2 = MIN_ST + TimeDelta{1};
    {
        Value value{22};
        auto  data = target.data_view();
        auto  dict = data.as_dict();
        auto  mutation = dict.begin_mutation(t2);
        auto  child = mutation.at(key_two.view());
        auto  child_mutation = child.begin_mutation(t2);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto add_view  = handle.view(t2);
    auto after_add = add_view.as_dict();
    REQUIRE(after_add.modified());
    REQUIRE(after_add.contains(key_one.view()));
    REQUIRE(after_add.contains(key_two.view()));
    REQUIRE(range_count(after_add.items()) == 2);
    auto source_after_add = target.view(t2);
    auto source_after_add_dict = source_after_add.as_dict();
    REQUIRE(after_add.at(key_one.view()).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_after_add_dict.at(key_one.view())});
    REQUIRE(after_add.at(key_two.view()).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_after_add_dict.at(key_two.view())});

    auto value_snapshot = add_view.value().clone();
    auto value_map = value_snapshot.as_map();
    REQUIRE(value_map.contains(key_one.view()));
    REQUIRE(value_map.contains(key_two.view()));
    REQUIRE(value_map.at(key_two.view()).checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_after_add_dict.at(key_two.view())});

    const auto t3 = t2 + TimeDelta{1};
    {
        auto data = target.data_view();
        auto dict = data.as_dict();
        auto mutation = dict.begin_mutation(t3);
        REQUIRE(mutation.erase(key_one.view()));
    }

    auto remove_view  = handle.view(t3);
    auto after_remove = remove_view.as_dict();
    REQUIRE(after_remove.modified());
    REQUIRE_FALSE(after_remove.contains(key_one.view()));
    REQUIRE(after_remove.contains(key_two.view()));
    REQUIRE(range_count(after_remove.items()) == 1);
    auto delta_snapshot = remove_view.delta_value().clone();
    auto delta_bundle = delta_snapshot.as_bundle();
    auto removed_keys = delta_bundle.field("removed").as_set();
    REQUIRE(removed_keys.contains(key_one.view()));
}

TEST_CASE("TimeSeriesReference: to-REF TSD path constructs normal child structures before REF leaves")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *source_list = registry.tsl(ts_int, 2);
    const auto *requested_list = registry.tsl(ref_int, 2);
    const auto *source_bundle = registry.tsb("TimeSeriesReferenceTSDPathSourceBundle", {{"items", source_list}});
    const auto *requested_bundle =
        registry.tsb("TimeSeriesReferenceTSDPathRequestedBundle", {{"items", requested_list}});
    const auto *source_schema = registry.tsd(int_meta, source_bundle);
    const auto *requested_schema = registry.tsd(int_meta, requested_bundle);

    TSOutput target{*source_schema};
    Value    key_one{1};
    Value    key_two{2};

    {
        auto data = target.data_view();
        auto dict = data.as_dict();
        auto dict_mutation = dict.begin_mutation(MIN_ST);
        auto child = dict_mutation.at(key_one.view());
        auto bundle = child.as_bundle();
        auto items_child = bundle.field("items");
        auto items = items_child.as_list();
        Value first{11};
        Value second{12};
        REQUIRE(items.at(0).begin_mutation(MIN_ST).copy_value_from(first.view()));
        REQUIRE(items.at(1).begin_mutation(MIN_ST).copy_value_from(second.view()));
    }

    auto target_view = target.view(MIN_ST);
    auto handle = target_view.binding_for(*requested_schema);
    auto handle_view = handle.view(MIN_ST);
    auto projected = handle_view.as_dict();
    REQUIRE(projected.contains(key_one.view()));
    REQUIRE(projected.at(key_one.view()).schema() == requested_bundle);

    auto projected_child = projected.at(key_one.view());
    auto projected_bundle = projected_child.as_bundle();
    auto projected_items_child = projected_bundle.field("items");
    auto projected_items = projected_items_child.as_list();
    REQUIRE(projected_items.at(0).schema() == ref_int);
    REQUIRE(projected_items.at(1).schema() == ref_int);

    auto source_view = target.view(MIN_ST);
    auto source = source_view.as_dict();
    auto source_child = source.at(key_one.view());
    auto source_bundle_view = source_child.as_bundle();
    auto source_items_child = source_bundle_view.field("items");
    auto source_items = source_items_child.as_list();
    REQUIRE(projected_items.at(0).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_items.at(0)});
    REQUIRE(projected_items.at(1).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_items.at(1)});

    const auto t2 = MIN_ST + TimeDelta{1};
    {
        auto data = target.data_view();
        auto dict = data.as_dict();
        auto dict_mutation = dict.begin_mutation(t2);
        auto child = dict_mutation.at(key_two.view());
        auto bundle = child.as_bundle();
        auto items_child = bundle.field("items");
        auto items = items_child.as_list();
        Value first{21};
        Value second{22};
        REQUIRE(items.at(0).begin_mutation(t2).copy_value_from(first.view()));
        REQUIRE(items.at(1).begin_mutation(t2).copy_value_from(second.view()));
    }

    auto after_add_view = handle.view(t2);
    auto after_add = after_add_view.as_dict();
    REQUIRE(after_add.modified());
    REQUIRE(after_add.contains(key_one.view()));
    REQUIRE(after_add.contains(key_two.view()));
    REQUIRE(range_count(after_add.items()) == 2);
    REQUIRE(after_add.at(key_two.view()).schema() == requested_bundle);

    auto added_child = after_add.at(key_two.view());
    auto added_bundle = added_child.as_bundle();
    auto added_items_child = added_bundle.field("items");
    auto added_items = added_items_child.as_list();
    auto source_after_add_view = target.view(t2);
    auto source_after_add = source_after_add_view.as_dict();
    auto source_added_child = source_after_add.at(key_two.view());
    auto source_added_bundle = source_added_child.as_bundle();
    auto source_added_items_child = source_added_bundle.field("items");
    auto source_added_items = source_added_items_child.as_list();
    REQUIRE(added_items.at(0).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_added_items.at(0)});
    REQUIRE(added_items.at(1).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_added_items.at(1)});
}

TEST_CASE("TimeSeriesReference: to-REF nested TSD alternative keeps each proxy subscribed")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *source_inner_schema = registry.tsd(int_meta, ts_int);
    const auto *requested_inner_schema = registry.tsd(int_meta, ref_int);
    const auto *source_schema = registry.tsd(int_meta, source_inner_schema);
    const auto *requested_schema = registry.tsd(int_meta, requested_inner_schema);

    TSOutput target{*source_schema};
    Value    outer_key{1};
    Value    inner_key_one{11};
    Value    inner_key_two{22};

    {
        Value value{11};
        auto  target_data = target.data_view();
        auto  source_dict = target_data.as_dict();
        auto  outer_mutation = source_dict.begin_mutation(MIN_ST);
        auto  inner = outer_mutation.at(outer_key.view());
        auto  inner_dict = inner.as_dict();
        auto  inner_mutation = inner_dict.begin_mutation(MIN_ST);
        auto  child = inner_mutation.at(inner_key_one.view());
        auto  child_mutation = child.begin_mutation(MIN_ST);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto handle = target.view(MIN_ST).binding_for(*requested_schema);
    auto projected_view = handle.view(MIN_ST);
    auto projected_dict = projected_view.as_dict();
    REQUIRE(projected_dict.contains(outer_key.view()));
    auto projected_inner_child = projected_dict.at(outer_key.view());
    auto projected_inner = projected_inner_child.as_dict();
    REQUIRE(projected_inner.contains(inner_key_one.view()));
    REQUIRE(range_count(projected_inner.items()) == 1);
    const auto t2 = MIN_ST + TimeDelta{1};
    {
        Value value{22};
        auto  target_data = target.data_view();
        auto  source_dict = target_data.as_dict();
        auto  outer_child = source_dict.at(outer_key.view());
        auto  inner_dict = outer_child.as_dict();
        auto  inner_mutation = inner_dict.begin_mutation(t2);
        auto  child = inner_mutation.at(inner_key_two.view());
        auto  child_mutation = child.begin_mutation(t2);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto after_add_view = handle.view(t2);
    auto after_add_dict = after_add_view.as_dict();
    auto after_add_inner_child = after_add_dict.at(outer_key.view());
    auto after_add_inner = after_add_inner_child.as_dict();
    REQUIRE(after_add_inner.modified());
    REQUIRE(after_add_inner.contains(inner_key_one.view()));
    REQUIRE(after_add_inner.contains(inner_key_two.view()));
    REQUIRE(range_count(after_add_inner.items()) == 2);

    auto source_after_add = target.view(t2);
    auto source_after_add_dict = source_after_add.as_dict();
    auto source_after_add_inner_child = source_after_add_dict.at(outer_key.view());
    auto source_after_add_inner = source_after_add_inner_child.as_dict();
    REQUIRE(after_add_inner.at(inner_key_two.view()).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_after_add_inner.at(inner_key_two.view())});
}

TEST_CASE("TimeSeriesReference: from-REF alternative binds peered target and follows target ticks")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);

    TSOutput first_target{*ts_int};
    TSOutput second_target{*ts_int};
    TSOutput ref_output{*ref_int};

    const auto [t1, t2, t3, t4, t5] = sequential_times<5>();

    set_output_value(first_target, 11, t1);
    set_output_reference(ref_output, TimeSeriesReference{first_target.view(t1)}, t2);

    auto handle = ref_output.view(t2).binding_for(*ts_int);
    auto dereferenced = handle.view(t2);
    REQUIRE(dereferenced.valid());
    REQUIRE(dereferenced.modified());
    REQUIRE(dereferenced.value().checked_as<std::int32_t>() == 11);
    REQUIRE(dereferenced.last_modified_time() == t2);

    set_output_value(first_target, 12, t3);
    auto after_target_tick = handle.view(t3);
    REQUIRE(after_target_tick.valid());
    REQUIRE(after_target_tick.modified());
    REQUIRE(after_target_tick.value().checked_as<std::int32_t>() == 12);

    set_output_value(second_target, 21, t4);
    set_output_reference(ref_output, TimeSeriesReference{second_target.view(t4)}, t4);
    auto after_rebind = handle.view(t4);
    REQUIRE(after_rebind.valid());
    REQUIRE(after_rebind.modified());
    REQUIRE(after_rebind.value().checked_as<std::int32_t>() == 21);
    REQUIRE(after_rebind.last_modified_time() == t4);

    set_output_reference(ref_output, TimeSeriesReference::empty(ts_int), t5);
    auto after_unbind = handle.view(t5);
    // UNBIND IS SILENT (linking_strategies.rst): the input reads not-valid
    // but is NOT marked modified - consumers are not notified.
    REQUIRE_FALSE(after_unbind.valid());
    REQUIRE_FALSE(after_unbind.modified());
    REQUIRE(after_unbind.last_modified_time() == t4);
}

TEST_CASE("TimeSeriesReference: from-REF set alternative rebinds target links")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *set_schema  = registry.tss(int_meta);
    const auto *ref_set     = registry.ref(set_schema);

    TSOutput first_target{*set_schema};
    TSOutput second_target{*set_schema};
    TSOutput ref_output{*ref_set};

    const auto [t1, t2, t3, t4, t5] = sequential_times<5>();

    add_set_values(first_target, {std::int32_t{1}, std::int32_t{2}}, t1);
    set_output_reference(ref_output, TimeSeriesReference{first_target.view(t1)}, t2);

    auto handle       = ref_output.view(t2).binding_for(*set_schema);
    auto dereferenced = handle.view(t2);
    auto set          = dereferenced.as_set();
    Value one{std::int32_t{1}};
    Value two{std::int32_t{2}};
    Value three{std::int32_t{3}};
    Value four{std::int32_t{4}};
    REQUIRE(dereferenced.valid());
    REQUIRE(dereferenced.modified());
    REQUIRE(set.size() == 2);
    REQUIRE(set.contains(one.view()));
    REQUIRE(set.contains(two.view()));

    add_set_values(second_target, {std::int32_t{3}}, t3);
    set_output_reference(ref_output, TimeSeriesReference{second_target.view(t3)}, t3);

    auto after_rebind = handle.view(t3);
    auto rebound_set  = after_rebind.as_set();
    REQUIRE(after_rebind.valid());
    REQUIRE(after_rebind.modified());
    REQUIRE(rebound_set.size() == 1);
    REQUIRE_FALSE(rebound_set.contains(one.view()));
    REQUIRE(rebound_set.contains(three.view()));
    REQUIRE(std::ranges::equal(rebound_set.added(), std::array{three.view()}));
    REQUIRE(std::ranges::is_permutation(rebound_set.removed(), std::array{one.view(), two.view()}));

    add_set_values(second_target, {std::int32_t{4}}, t4);
    auto after_target_tick = handle.view(t4);
    auto ticked_set        = after_target_tick.as_set();
    REQUIRE(after_target_tick.valid());
    REQUIRE(after_target_tick.modified());
    REQUIRE(ticked_set.size() == 2);
    REQUIRE(ticked_set.contains(three.view()));
    REQUIRE(ticked_set.contains(four.view()));

    set_output_reference(ref_output, TimeSeriesReference::empty(set_schema), t5);
    auto after_unbind = handle.view(t5);
    auto unbound_set  = after_unbind.as_set();
    REQUIRE_FALSE(after_unbind.valid());
    REQUIRE(after_unbind.modified());
    REQUIRE(unbound_set.size() == 0);
    REQUIRE_FALSE(unbound_set.contains(three.view()));
    REQUIRE(std::ranges::is_permutation(unbound_set.removed(), std::array{three.view(), four.view()}));
}

TEST_CASE("TimeSeriesReference: elementwise from-REF dict alternative follows per-key references")
{
    using namespace hgraph;
    auto       &registry  = TypeRegistry::instance();
    const auto *int_meta  = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int    = registry.ts(int_meta);
    const auto *ref_ts    = registry.ref(ts_int);
    const auto *source_schema    = registry.tsd(int_meta, ref_ts);   // TSD[int, REF[TS[int]]]
    const auto *requested_schema = registry.tsd(int_meta, ts_int);   // TSD[int, TS[int]]

    TSOutput target_a{*ts_int};
    TSOutput target_b{*ts_int};
    TSOutput source{*source_schema};

    const auto [t1, t2, t3, t4, t5] = sequential_times<5>();
    Value key_one{std::int32_t{1}};
    Value key_two{std::int32_t{2}};

    set_output_value(target_a, 11, t1);

    // key 1 -> ref(target_a)
    {
        Value ref_value{TimeSeriesReference{target_a.view(t2)}};
        auto  data     = source.data_view();
        auto  dict     = data.as_dict();
        auto  mutation = dict.begin_mutation(t2);
        auto  child    = mutation.at(key_one.view());
        REQUIRE(child.begin_mutation(t2).copy_value_from(ref_value.view()));
    }

    auto handle       = source.view(t2).binding_for(*requested_schema);
    REQUIRE(std::string{handle.type_ref().record()->implementation_name()} ==
            "ts.alternative.interior-ref.output");
    auto dereferenced = handle.view(t2);
    auto dict         = dereferenced.as_dict();
    REQUIRE(dereferenced.valid());
    REQUIRE(dereferenced.modified());
    REQUIRE(dict.size() == 1);
    REQUIRE(dict.contains(key_one.view()));
    REQUIRE(dict.at(key_one.view()).value().checked_as<std::int32_t>() == 11);

    // The referenced target ticks: write-through, no proxy involvement.
    set_output_value(target_a, 12, t3);
    {
        auto view = handle.view(t3);
        auto d    = view.as_dict();
        REQUIRE(d.at(key_one.view()).value().checked_as<std::int32_t>() == 12);
        REQUIRE(d.at(key_one.view()).last_modified_time() == t3);
    }

    // RETARGET key 1 to target_b: the proxy refreshes the LIVE slot.
    set_output_value(target_b, 22, t3);
    {
        Value ref_value{TimeSeriesReference{target_b.view(t4)}};
        auto  data        = source.data_view();
        auto  source_dict = data.as_dict();
        auto  mutation    = source_dict.begin_mutation(t4);
        auto  child       = mutation.at(key_one.view());
        REQUIRE(child.begin_mutation(t4).copy_value_from(ref_value.view()));
    }
    auto retargeted = handle.view(t4);
    auto retargeted_dict = retargeted.as_dict();
    REQUIRE(retargeted.modified());
    REQUIRE(retargeted_dict.at(key_one.view()).value().checked_as<std::int32_t>() == 22);

    // A NEW key materialises its own link.
    {
        Value ref_value{TimeSeriesReference{target_a.view(t5)}};
        auto  data        = source.data_view();
        auto  source_dict = data.as_dict();
        auto  mutation    = source_dict.begin_mutation(t5);
        auto  child       = mutation.at(key_two.view());
        REQUIRE(child.begin_mutation(t5).copy_value_from(ref_value.view()));
    }
    auto grown = handle.view(t5);
    auto grown_dict = grown.as_dict();
    REQUIRE(grown_dict.size() == 2);
    REQUIRE(grown_dict.at(key_two.view()).value().checked_as<std::int32_t>() == 12);
}

TEST_CASE("TimeSeriesReference: same-cycle interior-REF resurrection eagerly restores retained links")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *ref = registry.ref(ts);
    const auto *source_schema = registry.tsd(integer, ref);
    const auto *requested_schema = registry.tsd(integer, ts);

    TSOutput target_a{ts};
    TSOutput target_b{ts};
    TSOutput source{source_schema};
    const auto [t1, t2, t3, t4, t5] = sequential_times<5>();
    Value key{1};
    set_output_value(target_a, 11, t1);
    set_output_value(target_b, 21, t1);
    {
        auto source_data = source.data_view();
        auto mutation = source_data.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        Value reference{TimeSeriesReference{target_a.view(t1)}};
        REQUIRE(child.begin_mutation(t1).copy_value_from(reference.view()));
    }

    auto handle = source.view(t1).binding_for(*requested_schema);
    auto initial = handle.view(t1);
    auto initial_dict = initial.as_dict();
    const auto slot = initial_dict.find_slot(key.view());
    const auto child_address = initial_dict.at_slot(slot).data_view().data();
    const auto capacity = initial_dict.slot_capacity();
    REQUIRE(initial_dict.at(key.view()).value().checked_as<std::int32_t>() == 11);

    ReferenceSlotObserver observer;
    initial_dict.key_set().data_view().as_set().subscribe_slot_observer(&observer);
    {
        auto source_data = source.data_view();
        auto mutation = source_data.as_dict().begin_mutation(t2);
        REQUIRE(mutation.erase(key.view()));
        auto retained = mutation.at(key.view());
        REQUIRE(retained.has_current_value());
    }

    auto resurrected_view = handle.view(t2);
    auto resurrected = resurrected_view.as_dict();
    REQUIRE(resurrected.contains(key.view()));
    REQUIRE(resurrected.at_slot(slot).data_view().data() == child_address);
    REQUIRE(resurrected.slot_capacity() == capacity);
    REQUIRE_FALSE(resurrected.slot_added(slot));
    REQUIRE_FALSE(resurrected.slot_removed(slot));
    REQUIRE(resurrected.at(key.view()).value().checked_as<std::int32_t>() == 11);
    REQUIRE(observer.events == std::vector<std::string>{
                                   "remove:" + std::to_string(slot),
                                   "insert:" + std::to_string(slot)});

    set_output_value(target_a, 12, t3);
    auto after_old_target_tick = handle.view(t3);
    REQUIRE(after_old_target_tick.as_dict().at(key.view()).value().checked_as<std::int32_t>() == 12);

    {
        auto source_data = source.data_view();
        auto child = source_data.as_dict().at(key.view());
        Value reference{TimeSeriesReference{target_b.view(t4)}};
        REQUIRE(child.begin_mutation(t4).copy_value_from(reference.view()));
    }
    auto after_retarget = handle.view(t4);
    REQUIRE(after_retarget.as_dict().at(key.view()).value().checked_as<std::int32_t>() == 21);
    set_output_value(target_b, 22, t5);
    auto after_new_target_tick = handle.view(t5);
    REQUIRE(after_new_target_tick.as_dict().at(key.view()).value().checked_as<std::int32_t>() == 22);
    REQUIRE(std::ranges::count(observer.events, "insert:" + std::to_string(slot)) == 1);
    after_new_target_tick.as_dict().key_set().data_view().as_set().unsubscribe_slot_observer(&observer);
}

TEST_CASE("TimeSeriesReference: same-time resurrected REF identity reconciles before target forwarding")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *ref = registry.ref(ts);
    const auto *source_schema = registry.tsd(integer, ref);
    const auto *requested_schema = registry.tsd(integer, ts);
    TSOutput target_a{ts};
    TSOutput target_b{ts};
    TSOutput source{source_schema};
    const auto [t1, t2, t3, t4, t5, t6] = sequential_times<6>();
    Value key{1};
    Value rollover_key{2};
    set_output_value(target_a, 11, t1);
    set_output_value(target_b, 21, t1);
    {
        auto source_data = source.data_view();
        auto mutation = source_data.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        Value reference{TimeSeriesReference{target_a.view(t1)}};
        REQUIRE(child.begin_mutation(t1).copy_value_from(reference.view()));
    }

    auto handle = source.view(t1).binding_for(*requested_schema);
    REQUIRE(target_a.data_view().observer_count() == 1);
    REQUIRE(target_b.data_view().observer_count() == 0);
    {
        auto source_data = source.data_view();
        auto mutation = source_data.as_dict().begin_mutation(t2);
        REQUIRE(mutation.erase(key.view()));
        auto child = mutation.at(key.view());
        Value reference{TimeSeriesReference{target_b.view(t2)}};
        REQUIRE(child.begin_mutation(t2).copy_value_from(reference.view()));
    }

    auto reconciled_view = handle.view(t2);
    auto reconciled = reconciled_view.as_dict();
    REQUIRE(reconciled.at(key.view()).value().checked_as<std::int32_t>() == 21);
    REQUIRE(target_a.data_view().observer_count() == 0);
    REQUIRE(target_b.data_view().observer_count() == 1);
    const auto reconciled_time = reconciled.at(key.view()).last_modified_time();
    REQUIRE(reconciled.at(key.view()).value().checked_as<std::int32_t>() == 21);
    REQUIRE(reconciled.at(key.view()).last_modified_time() == reconciled_time);

    set_output_value(target_a, 12, t3);
    auto after_old_tick_view = handle.view(t3);
    REQUIRE(after_old_tick_view.as_dict().at(key.view()).value().checked_as<std::int32_t>() == 21);
    set_output_value(target_b, 22, t4);
    auto after_new_tick_view = handle.view(t4);
    REQUIRE(after_new_tick_view.as_dict().at(key.view()).value().checked_as<std::int32_t>() == 22);

    // Repeat the same-time retarget, but advance the source delta before the
    // first projected read. Reconciliation must not depend on slot_modified.
    {
        auto source_data = source.data_view();
        auto mutation = source_data.as_dict().begin_mutation(t5);
        REQUIRE(mutation.erase(key.view()));
        auto child = mutation.at(key.view());
        Value reference{TimeSeriesReference{target_a.view(t5)}};
        REQUIRE(child.begin_mutation(t5).copy_value_from(reference.view()));
    }
    {
        auto source_data = source.data_view();
        auto mutation = source_data.as_dict().begin_mutation(t6);
        auto child = mutation.at(rollover_key.view());
        Value reference{TimeSeriesReference{target_a.view(t6)}};
        REQUIRE(child.begin_mutation(t6).copy_value_from(reference.view()));
    }
    REQUIRE(target_a.data_view().observer_count() == 1);
    REQUIRE(target_b.data_view().observer_count() == 0);
    auto after_rollover_view = handle.view(t6);
    REQUIRE(after_rollover_view.as_dict().at(key.view()).value().checked_as<std::int32_t>() == 12);
}

TEST_CASE("TimeSeriesReference: cached to-REF sync does not repeat inserts for same-time new keys")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *ref = registry.ref(ts);
    const auto *source_schema = registry.tsd(integer, ts);
    const auto *requested_schema = registry.tsd(integer, ref);

    TSOutput source{source_schema};
    const auto [t1, t2, t3] = sequential_times<3>();
    Value key_one{1};
    Value key_two{2};
    auto handle = source.view(t1).binding_for(*requested_schema);
    auto empty_view = handle.view(t1);
    auto empty = empty_view.as_dict();
    ReferenceSlotObserver lifecycle;
    empty.key_set().data_view().as_set().subscribe_slot_observer(&lifecycle);

    {
        auto source_data = source.data_view();
        auto mutation = source_data.as_dict().begin_mutation(t2);
        auto one = mutation.at(key_one.view());
        Value first{11};
        REQUIRE(one.begin_mutation(t2).copy_value_from(first.view()));
        auto two = mutation.at(key_two.view());
        Value second{21};
        REQUIRE(two.begin_mutation(t2).copy_value_from(second.view()));
    }

    auto source_after_insert = source.view(t2);
    auto source_dict = source_after_insert.as_dict();
    const auto slot_one = source_dict.find_slot(key_one.view());
    const auto slot_two = source_dict.find_slot(key_two.view());
    REQUIRE(lifecycle.events == std::vector<std::string>{
                                    "insert:" + std::to_string(slot_one),
                                    "insert:" + std::to_string(slot_two)});

    // The second child write shared the root's evaluation time, so its root
    // notification was coalesced. The cache hit completes any deferred build
    // but both structural inserts have already been announced.
    auto cache_hit = source.view(t2).binding_for(*requested_schema);
    REQUIRE(cache_hit.data_view().data() == handle.data_view().data());
    auto cached_view = cache_hit.view(t2);
    auto cached = cached_view.as_dict();
    REQUIRE(lifecycle.events.size() == 2);
    REQUIRE(cached.at(key_one.view()).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_dict.at(key_one.view())});
    REQUIRE(cached.at(key_two.view()).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_dict.at(key_two.view())});
    auto reference = cached.at(key_two.view()).value().checked_as<TimeSeriesReference>();
    const auto second_built_time = cached.at(key_two.view()).last_modified_time();

    auto repeat_hit = source.view(t2).binding_for(*requested_schema);
    REQUIRE(repeat_hit.data_view().data() == handle.data_view().data());
    auto repeated_view = repeat_hit.view(t2);
    auto repeated = repeated_view.as_dict();
    REQUIRE(lifecycle.events.size() == 2);
    REQUIRE(repeated.at(key_two.view()).last_modified_time() == second_built_time);

    {
        auto source_data = source.data_view();
        auto child = source_data.as_dict().at(key_two.view());
        Value next{22};
        REQUIRE(child.begin_mutation(t3).copy_value_from(next.view()));
    }
    REQUIRE(reference.target_output().view(t3).value().checked_as<std::int32_t>() == 22);
    REQUIRE(lifecycle.events.size() == 2);
    repeated.key_set().data_view().as_set().unsubscribe_slot_observer(&lifecycle);
}

TEST_CASE("TimeSeriesReference: cached nested to-REF bind retains pending children until resurrection or erase")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *ref = registry.ref(ts);
    const auto *source_inner = registry.tsd(integer, ts);
    const auto *requested_inner = registry.tsd(integer, ref);
    const auto *source_schema = registry.tsd(integer, source_inner);
    const auto *requested_schema = registry.tsd(integer, requested_inner);

    TSOutput source{source_schema};
    const auto [t1, t2, t3] = sequential_times<3>();
    Value erased_key{1};
    Value resurrected_key{2};
    Value inner_key{10};
    {
        auto source_data = source.data_view();
        auto outer = source_data.as_dict().begin_mutation(t1);
        const auto add_outer_child = [&](const Value &key) {
            auto inner = outer.at(key.view());
            auto inner_mutation = inner.as_dict().begin_mutation(t1);
            auto leaf = inner_mutation.at(inner_key.view());
            Value value{11};
            REQUIRE(leaf.begin_mutation(t1).copy_value_from(value.view()));
        };
        add_outer_child(erased_key);
        add_outer_child(resurrected_key);
    }

    auto handle = source.view(t1).binding_for(*requested_schema);
    auto initial_view = handle.view(t1);
    auto initial = initial_view.as_dict();
    const auto erased_slot = initial.find_slot(erased_key.view());
    const auto resurrected_slot = initial.find_slot(resurrected_key.view());
    const auto capacity = initial.slot_capacity();
    auto erased_child = initial.at_slot(erased_slot);
    auto resurrected_child = initial.at_slot(resurrected_slot);
    const auto erased_address = erased_child.data_view().data();
    const auto resurrected_address = resurrected_child.data_view().data();

    ReferenceSlotObserver outer_lifecycle;
    ReferenceInvalidationObserver erased_invalidation;
    initial.key_set().data_view().as_set().subscribe_slot_observer(&outer_lifecycle);
    erased_child.data_view().subscribe(&erased_invalidation);
    const auto *erased_tracking = &erased_child.data_view().tracking();

    {
        auto source_data = source.data_view();
        auto mutation = source_data.as_dict().begin_mutation(t2);
        REQUIRE(mutation.erase(erased_key.view()));
        REQUIRE(mutation.erase(resurrected_key.view()));
    }

    auto pending_view = handle.view(t2);
    auto pending = pending_view.as_dict();
    REQUIRE_FALSE(pending.slot_live(erased_slot));
    REQUIRE(pending.slot_removed(erased_slot));
    REQUIRE(pending.slot_occupied(erased_slot));
    REQUIRE_FALSE(pending.slot_live(resurrected_slot));
    REQUIRE(pending.slot_removed(resurrected_slot));
    REQUIRE(pending.slot_occupied(resurrected_slot));
    auto pending_erased_child = pending.at_slot(erased_slot);
    auto pending_resurrected_child = pending.at_slot(resurrected_slot);
    REQUIRE(pending_erased_child.data_view().data() == erased_address);
    REQUIRE(pending_resurrected_child.data_view().data() == resurrected_address);
    REQUIRE(pending.slot_capacity() == capacity);
    REQUIRE(outer_lifecycle.events == std::vector<std::string>{
                                          "remove:" + std::to_string(erased_slot),
                                          "remove:" + std::to_string(resurrected_slot)});
    REQUIRE(erased_invalidation.invalidations == 0);

    // This is a cache hit and re-enters populate_to_ref_dict/bind_tsd_proxy.
    // Pending slots must remain stopped and must not emit insert.
    auto cache_hit = source.view(t2).binding_for(*requested_schema);
    REQUIRE(cache_hit.data_view().data() == handle.data_view().data());
    auto after_cache_hit_view = cache_hit.view(t2);
    auto after_cache_hit = after_cache_hit_view.as_dict();
    auto cached_erased_child = after_cache_hit.at_slot(erased_slot);
    auto cached_resurrected_child = after_cache_hit.at_slot(resurrected_slot);
    REQUIRE(cached_erased_child.data_view().data() == erased_address);
    REQUIRE(cached_resurrected_child.data_view().data() == resurrected_address);
    REQUIRE(after_cache_hit.slot_capacity() == capacity);
    REQUIRE_FALSE(after_cache_hit.slot_live(erased_slot));
    REQUIRE(after_cache_hit.slot_removed(erased_slot));
    REQUIRE(after_cache_hit.slot_occupied(erased_slot));
    REQUIRE(outer_lifecycle.events.size() == 2);
    REQUIRE(erased_invalidation.invalidations == 0);

    {
        auto source_data = source.data_view();
        auto mutation = source_data.as_dict().begin_mutation(t2);
        auto retained = mutation.at(resurrected_key.view());
        REQUIRE(retained.has_current_value());
    }

    auto resurrected_view = cache_hit.view(t2);
    auto resurrected = resurrected_view.as_dict();
    REQUIRE(resurrected.slot_live(resurrected_slot));
    REQUIRE_FALSE(resurrected.slot_removed(resurrected_slot));
    REQUIRE(resurrected.at_slot(resurrected_slot).data_view().data() == resurrected_address);
    REQUIRE(resurrected.slot_capacity() == capacity);
    REQUIRE(std::ranges::count(outer_lifecycle.events,
                               "insert:" + std::to_string(resurrected_slot)) == 1);
    auto resurrected_child_view = resurrected.at_slot(resurrected_slot);
    auto resurrected_inner = resurrected_child_view.as_dict();
    auto reference = resurrected_inner.at(inner_key.view()).value().checked_as<TimeSeriesReference>();

    {
        auto source_data = source.data_view();
        auto outer = source_data.as_dict().begin_mutation(t3);
        auto inner = outer.at(resurrected_key.view());
        auto inner_mutation = inner.as_dict().begin_mutation(t3);
        auto leaf = inner_mutation.at(inner_key.view());
        Value value{12};
        REQUIRE(leaf.begin_mutation(t3).copy_value_from(value.view()));
    }

    auto after_erase_view = cache_hit.view(t3);
    auto after_erase = after_erase_view.as_dict();
    REQUIRE_FALSE(after_erase.slot_occupied(erased_slot));
    REQUIRE(std::ranges::count(outer_lifecycle.events,
                               "erase:" + std::to_string(erased_slot)) == 1);
    REQUIRE(erased_invalidation.invalidations == 1);
    REQUIRE(erased_invalidation.source == erased_tracking);
    REQUIRE(reference.target_output().view(t3).value().checked_as<std::int32_t>() == 12);

    after_erase.key_set().data_view().as_set().unsubscribe_slot_observer(&outer_lifecycle);
}

TEST_CASE("TimeSeriesReference: nested interior-REF resurrection rebinds retained proxy storage")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *ref = registry.ref(ts);
    const auto *source_inner = registry.tsd(integer, ref);
    const auto *requested_inner = registry.tsd(integer, ts);
    const auto *source_schema = registry.tsd(integer, source_inner);
    const auto *requested_schema = registry.tsd(integer, requested_inner);

    TSOutput target{ts};
    TSOutput source{source_schema};
    const auto [t1, t2, t3] = sequential_times<3>();
    Value outer_key{1};
    Value inner_key{2};
    set_output_value(target, 11, t1);
    {
        auto source_data = source.data_view();
        auto outer_mutation = source_data.as_dict().begin_mutation(t1);
        auto inner = outer_mutation.at(outer_key.view());
        auto inner_mutation = inner.as_dict().begin_mutation(t1);
        auto leaf = inner_mutation.at(inner_key.view());
        Value reference{TimeSeriesReference{target.view(t1)}};
        REQUIRE(leaf.begin_mutation(t1).copy_value_from(reference.view()));
    }

    auto handle = source.view(t1).binding_for(*requested_schema);
    auto initial = handle.view(t1);
    auto outer = initial.as_dict();
    const auto outer_slot = outer.find_slot(outer_key.view());
    const auto outer_address = outer.at_slot(outer_slot).data_view().data();
    const auto outer_capacity = outer.slot_capacity();
    auto inner_output = outer.at(outer_key.view());
    auto inner_dict = inner_output.as_dict();
    const auto inner_slot = inner_dict.find_slot(inner_key.view());
    const auto inner_address = inner_dict.at_slot(inner_slot).data_view().data();
    const auto inner_capacity = inner_dict.slot_capacity();
    REQUIRE(inner_dict.at(inner_key.view()).value().checked_as<std::int32_t>() == 11);

    {
        auto source_data = source.data_view();
        auto mutation = source_data.as_dict().begin_mutation(t2);
        REQUIRE(mutation.erase(outer_key.view()));
        auto retained = mutation.at(outer_key.view());
        REQUIRE(retained.has_current_value());
    }

    auto resurrected_view = handle.view(t2);
    auto resurrected_outer = resurrected_view.as_dict();
    auto resurrected_inner_output = resurrected_outer.at(outer_key.view());
    auto resurrected_inner = resurrected_inner_output.as_dict();
    REQUIRE(resurrected_outer.at_slot(outer_slot).data_view().data() == outer_address);
    REQUIRE(resurrected_outer.slot_capacity() == outer_capacity);
    REQUIRE(resurrected_inner.at_slot(inner_slot).data_view().data() == inner_address);
    REQUIRE(resurrected_inner.slot_capacity() == inner_capacity);
    REQUIRE(resurrected_inner.at(inner_key.view()).value().checked_as<std::int32_t>() == 11);

    set_output_value(target, 12, t3);
    auto after_tick = handle.view(t3);
    auto after_tick_outer = after_tick.as_dict();
    auto after_tick_inner_output = after_tick_outer.at(outer_key.view());
    REQUIRE(after_tick_inner_output.as_dict().at(inner_key.view()).value().checked_as<std::int32_t>() == 12);
}

TEST_CASE("TimeSeriesReference: from-REF dict alternative rebinds target links")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int      = registry.ts(int_meta);
    const auto *dict_schema = registry.tsd(int_meta, ts_int);
    const auto *ref_dict    = registry.ref(dict_schema);

    TSOutput first_target{*dict_schema};
    TSOutput second_target{*dict_schema};
    TSOutput ref_output{*ref_dict};

    const auto [t1, t2, t3, t4, t5] = sequential_times<5>();
    Value      key_one{std::int32_t{1}};
    Value      key_two{std::int32_t{2}};

    set_dict_value(first_target, std::int32_t{1}, std::int32_t{11}, t1);
    set_output_reference(ref_output, TimeSeriesReference{first_target.view(t1)}, t2);

    auto handle       = ref_output.view(t2).binding_for(*dict_schema);
    REQUIRE(std::string{handle.type_ref().record()->implementation_name()} ==
            "ts.alternative.from-ref.output");
    auto dereferenced = handle.view(t2);
    auto dict         = dereferenced.as_dict();
    REQUIRE(dereferenced.valid());
    REQUIRE(dereferenced.modified());
    REQUIRE(dict.size() == 1);
    REQUIRE(dict.contains(key_one.view()));
    REQUIRE(dict.at(key_one.view()).value().checked_as<std::int32_t>() == 11);

    set_dict_value(second_target, std::int32_t{2}, std::int32_t{22}, t3);
    set_output_reference(ref_output, TimeSeriesReference{second_target.view(t3)}, t3);

    auto after_rebind = handle.view(t3);
    auto rebound_dict = after_rebind.as_dict();
    REQUIRE(after_rebind.valid());
    REQUIRE(after_rebind.modified());
    REQUIRE(rebound_dict.size() == 1);
    REQUIRE_FALSE(rebound_dict.contains(key_one.view()));
    REQUIRE(rebound_dict.contains(key_two.view()));
    REQUIRE(rebound_dict.at(key_two.view()).value().checked_as<std::int32_t>() == 22);
    REQUIRE(std::ranges::equal(rebound_dict.removed_keys(), std::array{key_one.view()}));
    REQUIRE(std::ranges::equal(rebound_dict.modified_keys(), std::array{key_two.view()}));

    set_dict_value(second_target, std::int32_t{2}, std::int32_t{23}, t4);
    auto after_target_tick = handle.view(t4);
    auto ticked_dict       = after_target_tick.as_dict();
    REQUIRE(after_target_tick.valid());
    REQUIRE(after_target_tick.modified());
    REQUIRE(ticked_dict.size() == 1);
    REQUIRE(ticked_dict.contains(key_two.view()));
    REQUIRE(ticked_dict.at(key_two.view()).value().checked_as<std::int32_t>() == 23);

    set_output_reference(ref_output, TimeSeriesReference::empty(dict_schema), t5);
    auto after_unbind = handle.view(t5);
    auto unbound_dict = after_unbind.as_dict();
    REQUIRE_FALSE(after_unbind.valid());
    REQUIRE(after_unbind.modified());
    REQUIRE(unbound_dict.size() == 0);
    REQUIRE_FALSE(unbound_dict.contains(key_two.view()));
    REQUIRE(std::ranges::equal(unbound_dict.removed_keys(), std::array{key_two.view()}));
}

TEST_CASE("TimeSeriesReference: from-REF alternative expands non-peered bundle references")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *bundle_schema =
        registry.tsb("TimeSeriesReferenceFromRefNonPeeredBundle", {{"lhs", ts_int}, {"rhs", ts_int}});
    const auto *ref_bundle = registry.ref(bundle_schema);

    TSOutput lhs_target{*ts_int};
    TSOutput rhs_target{*ts_int};
    TSOutput ref_output{*ref_bundle};

    const auto [t1, t2, t3] = sequential_times<3>();

    set_output_value(lhs_target, 1, t1);
    set_output_value(rhs_target, 2, t1);
    set_output_reference(ref_output,
                         TimeSeriesReference::non_peered(
                             bundle_schema,
                             {
                                 TimeSeriesReference{lhs_target.view(t1)},
                                 TimeSeriesReference{rhs_target.view(t1)},
                             }),
                         t2);

    auto handle = ref_output.view(t2).binding_for(*bundle_schema);
    auto dereferenced_view = handle.view(t2);
    auto bundle = dereferenced_view.as_bundle();
    auto lhs = bundle.field("lhs");
    auto rhs = bundle.field("rhs");
    REQUIRE(dereferenced_view.valid());
    REQUIRE(dereferenced_view.modified());
    REQUIRE(lhs.value().checked_as<std::int32_t>() == 1);
    REQUIRE(rhs.value().checked_as<std::int32_t>() == 2);

    set_output_value(rhs_target, 3, t3);
    auto after_rhs_tick_view = handle.view(t3);
    auto after_rhs_tick = after_rhs_tick_view.as_bundle();
    REQUIRE(after_rhs_tick_view.modified());
    auto modified_items = collect_range(after_rhs_tick.modified_items());
    REQUIRE(modified_items.size() == 1);
    REQUIRE(std::string{modified_items[0].first} == "rhs");
    REQUIRE(modified_items[0].second.value().checked_as<std::int32_t>() == 3);
}

TEST_CASE("TimeSeriesReference: from-REF alternative expands non-peered fixed-list references")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list_schema = registry.tsl(ts_int, 2);
    const auto *ref_list = registry.ref(list_schema);

    TSOutput first_target{*ts_int};
    TSOutput second_target{*ts_int};
    TSOutput ref_output{*ref_list};

    const auto [t1, t2, t3] = sequential_times<3>();

    set_output_value(first_target, 4, t1);
    set_output_value(second_target, 5, t1);
    set_output_reference(ref_output,
                         TimeSeriesReference::non_peered(
                             list_schema,
                             {
                                 TimeSeriesReference{first_target.view(t1)},
                                 TimeSeriesReference{second_target.view(t1)},
                             }),
                         t2);

    auto handle = ref_output.view(t2).binding_for(*list_schema);
    auto dereferenced_view = handle.view(t2);
    auto list = dereferenced_view.as_list();
    REQUIRE(dereferenced_view.valid());
    REQUIRE(dereferenced_view.modified());
    REQUIRE(list[0].value().checked_as<std::int32_t>() == 4);
    REQUIRE(list[1].value().checked_as<std::int32_t>() == 5);

    set_output_value(first_target, 6, t3);
    auto after_first_tick_view = handle.view(t3);
    auto after_first_tick = after_first_tick_view.as_list();
    REQUIRE(after_first_tick_view.modified());
    auto modified_items = collect_range(after_first_tick.modified_items());
    REQUIRE(modified_items.size() == 1);
    REQUIRE(modified_items[0].first == 0);
    REQUIRE(modified_items[0].second.value().checked_as<std::int32_t>() == 6);
}

TEST_CASE("TimeSeriesReference: from-REF alternative splits peered bundle references into child links")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *bundle_schema =
        registry.tsb("TimeSeriesReferenceFromRefPeeredBundle", {{"lhs", ts_int}, {"rhs", ts_int}});
    const auto *ref_bundle = registry.ref(bundle_schema);

    TSOutput target{*bundle_schema};
    TSOutput ref_output{*ref_bundle};

    const auto [t1, t2, t3] = sequential_times<3>();

    {
        auto data = target.data_view();
        auto bundle = data.as_bundle();
        auto lhs = bundle.field("lhs");
        auto rhs = bundle.field("rhs");
        Value lhs_value{5};
        Value rhs_value{6};
        REQUIRE(lhs.begin_mutation(t1).copy_value_from(lhs_value.view()));
        REQUIRE(rhs.begin_mutation(t1).copy_value_from(rhs_value.view()));
    }

    set_output_reference(ref_output, TimeSeriesReference{target.view(t1)}, t2);

    auto handle = ref_output.view(t2).binding_for(*bundle_schema);
    auto dereferenced_view = handle.view(t2);
    auto bundle = dereferenced_view.as_bundle();
    REQUIRE(bundle.field("lhs").value().checked_as<std::int32_t>() == 5);
    REQUIRE(bundle.field("rhs").value().checked_as<std::int32_t>() == 6);

    {
        auto target_view = target.view(t3);
        auto target_bundle = target_view.as_bundle();
        auto rhs = target_bundle.field("rhs");
        Value rhs_value{7};
        REQUIRE(rhs.begin_mutation(t3).copy_value_from(rhs_value.view()));
    }

    auto after_rhs_tick_view = handle.view(t3);
    auto after_rhs_tick = after_rhs_tick_view.as_bundle();
    REQUIRE(after_rhs_tick_view.modified());
    auto modified_items = collect_range(after_rhs_tick.modified_items());
    REQUIRE(modified_items.size() == 1);
    REQUIRE(std::string{modified_items[0].first} == "rhs");
    REQUIRE(modified_items[0].second.value().checked_as<std::int32_t>() == 7);
}

TEST_CASE("TimeSeriesReference: empty_reference() returns a stable singleton")
{
    using namespace hgraph;
    const auto &a = TimeSeriesReference::empty_reference();
    const auto &b = TimeSeriesReference::empty_reference();
    REQUIRE(&a == &b);
    REQUIRE(a.is_empty());
    REQUIRE(a.target_schema() == nullptr);
}

TEST_CASE("TimeSeriesReference: registry pairs the type with a real plan via ValuePlanFactory")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();

    // Trigger registration by asking for any REF schema.
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    (void)registry.ref(ts_int);

    const auto *ref_atom = registry.value_type("TimeSeriesReference");
    REQUIRE(ref_atom != nullptr);
    REQUIRE(ref_atom->value_kind() == ValueTypeKind::Atomic);

    // Unlike the previous synthetic atomic, the registered type now has a
    // canonical plan paired with it.
    const auto *plan = factory.find(ref_atom);
    REQUIRE(plan != nullptr);
    REQUIRE(plan == &MemoryUtils::plan_for<TimeSeriesReference>());
}
