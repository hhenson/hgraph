#include <hgl/ordinary_values.h>

#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/time_series/ts_output.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <type_traits>

TEST_CASE("ordinary delta identities retain fixed extent and nominal origin", "[ordinary][delta]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<std::int64_t>::value_meta();
    const auto *scalar = registry.ts(integer);
    const auto *two = registry.tsl(scalar, 2);
    const auto *three = registry.tsl(scalar, 3);
    REQUIRE(two->delta_value_schema == three->delta_value_schema);
    CHECK(delta_schema(two) != delta_schema(three));
    CHECK(delta_schema(two) == delta_schema(two));
    CHECK(delta_schema(scalar) == integer);
    const auto *first = registry.tsb("ordinary.First", {{"value", scalar}});
    const auto *second = registry.tsb("ordinary.Second", {{"value", scalar}});
    CHECK(delta_schema(first) != delta_schema(second));
    const auto *module_a = registry.bundle("ordinary.one", "Record", {{"value", integer}});
    const auto *module_b = registry.bundle("ordinary.two", "Record", {{"value", integer}});
    CHECK(delta_schema(registry.tsb(module_a)) != delta_schema(registry.tsb(module_b)));

    using Temporal = TSL<TS<std::int64_t>, 2>;
    static_assert(!std::is_same_v<Delta<Temporal>, Held<Temporal>>);
    CHECK(scalar_descriptor<Held<Temporal>>::value_meta() == two->value_schema);
    CHECK(scalar_descriptor<Delta<Temporal>>::value_meta() == delta_schema(two));
    CHECK((scalar_descriptor<hgl::ordinary::List<std::int64_t, 2>>::value_meta() == two->value_schema));
}

TEST_CASE("required payload guards separate absent errors from present scalar reads", "[ordinary][unset-read]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    const Value zero{Int{0}}, falsity{Bool{false}};
    const PreparedValuePlan integer{zero.binding()}, boolean{falsity.binding()};
    const auto absent = integer.retain(Value::typed_null(integer.binding()).view());
    CHECK_FALSE(absent.has_value());
    CHECK(absent.schema() == zero.schema());
    try { (void)required_scalar<Int>(absent.view()); FAIL("absent required payload unexpectedly read"); }
    catch (const hgl::ExecutionError &error) { CHECK(error.code() == "value.unset_read"); }
    const auto before = type_system_lock_count();
    Int sum = 0;
    bool observed_true = false;
    for (std::size_t index = 0; index < 128; ++index) {
        sum += required_scalar<Int>(zero.view());
        observed_true = observed_true || required_scalar<Bool>(falsity.view());
    }
    CHECK(sum == 0);
    CHECK_FALSE(observed_true);
    CHECK(type_system_lock_count() == before);
    const PreparedValuePlan tuple{scalar_descriptor<Tuple<Int, Bool>>::value_meta()};
    const auto absent_tuple = Value::typed_null(tuple.binding());
    const auto projection_locks = type_system_lock_count();
    for (std::size_t index = 0; index < 128; ++index) {
        const auto child = tuple.field_observation(absent_tuple.view(), 1);
        CHECK_FALSE(child.has_value());
        CHECK(child.binding() == tuple.field_binding(1));
    }
    CHECK(type_system_lock_count() == projection_locks);
    const auto expect_uncoded = [](auto invoke) {
        try { invoke(); FAIL("existing collection error unexpectedly succeeded"); }
        catch (const std::exception &error) { CHECK(hgl::execution_error_code(error).empty()); }
    };
    const PreparedValuePlan list{scalar_descriptor<List<Int, 2>>::value_meta()};
    const auto present_list = list.empty_list();
    expect_uncoded([&] { (void)list.index(present_list.view(), 2); });
    // Root access requires the retained parent's payload; selecting an unset
    // child of a present parent remains an observation, not a payload read.
    try { (void)list.index(Value::typed_null(list.binding()).view(), 0); FAIL("absent indexed root unexpectedly read"); }
    catch (const hgl::ExecutionError &error) { CHECK(error.code() == "value.unset_read"); }
    expect_uncoded([&] { (void)list.index(Value::typed_null(list.binding()).view(), 0, false); });
    expect_uncoded([&] { (void)list.len(Value::typed_null(list.binding()).view(), false); });
    const PreparedValuePlan map{scalar_descriptor<Map<Int, Int>>::value_meta()};
    const Value empty_map{map.binding()};
    expect_uncoded([&] { (void)map.map_index(empty_map.view(), zero.view()); });
    try { (void)map.map_index(Value::typed_null(map.binding()).view(), zero.view()); FAIL("absent mapped root unexpectedly read"); }
    catch (const hgl::ExecutionError &error) { CHECK(error.code() == "value.unset_read"); }
    expect_uncoded([&] { (void)map.map_index(Value::typed_null(map.binding()).view(), zero.view(), false); });
}

TEST_CASE("prepared generic publications reconcile recursive values without registry access", "[ordinary][publication]") {
    using namespace hgraph;
    using hgl::ordinary::PreparedValuePlan;
    using hgl::ordinary::PreparedPublicationPlan;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *boolean = scalar_descriptor<Bool>::value_meta();
    const auto *pair_source = registry.tuple({integer, boolean});
    const auto *pair_target = registry.un_named_tsb({{"0", registry.ts(integer)}, {"1", registry.ts(boolean)}});
    const PreparedValuePlan pair_value{pair_source};
    const PreparedPublicationPlan pair_publish{pair_target, pair_source};
    const Value one{Int{1}}, two{Int{2}}, falsity{Bool{false}};
    const std::array full_fields{std::pair<std::size_t, ValueView>{0, one.view()}, std::pair<std::size_t, ValueView>{1, falsity.view()}};
    const std::array partial_fields{std::pair<std::size_t, ValueView>{0, two.view()}};
    const auto full_pair = pair_value.bundle(full_fields);
    const auto partial_pair = pair_value.bundle(partial_fields);
    const auto *map_source = registry.map(integer, pair_source);
    const auto *map_target = registry.tsd(integer, pair_target);
    const PreparedValuePlan map_value{map_source};
    const PreparedPublicationPlan map_publish{map_target, map_source};
    MapBuilder full_builder{map_value.key_binding(), map_value.element_binding()};
    full_builder.set_item(one.view(), full_pair.view());
    full_builder.set_item(two.view(), full_pair.view());
    auto full_storage = full_builder.build_storage();
    const Value full_map{map_value.binding(), &full_storage, Value::AdoptStorage{}};
    MapBuilder partial_builder{map_value.key_binding(), map_value.element_binding()};
    partial_builder.set_item(one.view(), partial_pair.view());
    auto partial_storage = partial_builder.build_storage();
    const Value partial_map{map_value.binding(), &partial_storage, Value::AdoptStorage{}};
    TSOutput pair_output{*pair_target};
    TSOutput map_output{*map_target};
    const auto before = type_system_lock_count();
    for (std::size_t cycle = 0; cycle < 64; ++cycle) {
        const auto time = MIN_ST + MIN_TD * static_cast<std::int64_t>(cycle);
        const bool full = cycle % 2 == 0;
        pair_publish.apply(pair_output.view(time), full ? full_pair.view() : partial_pair.view());
        map_publish.apply(map_output.view(time), full ? full_map.view() : partial_map.view());
        CHECK(pair_output.view(time).all_valid() == full);
        // TS-9 checks immediate children: the partial Pair is still valid.
        CHECK(map_output.view(time).all_valid());
        auto map_endpoint = map_output.view(time);
        auto map_view = map_endpoint.as_dict();
        CHECK(map_view.contains(two.view()) == full);
        auto member = map_view.at(one.view());
        CHECK(member.all_valid() == full);
        CHECK(pair_output.view(time).value().as_bundle().at(0).checked_as<Int>() == (full ? 1 : 2));
        if (full) { CHECK_FALSE(pair_output.view(time).value().as_bundle().at(1).checked_as<Bool>()); }
    }
    CHECK(type_system_lock_count() == before);
    CHECK_THROWS_AS((PreparedPublicationPlan{registry.tsl(registry.ts(integer), 3), registry.fixed_list(integer, 2)}), std::invalid_argument);
    CHECK_THROWS_AS((PreparedPublicationPlan{registry.tsd(boolean, pair_target), map_source}), std::invalid_argument);
}

TEST_CASE("generic observations retain ordinary origins and recursive holes without registry access", "[ordinary][observation]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    using Pair = FixedTuple<Int, Bool>;
    using Record = NominalBundle<"ordinary", "GenericObserved", false, BundleParents<>, BundleArguments<Pair>, Field<"payload", Pair>>;
    using Lifted = Temporal<Record>;
    const auto *ordinary = scalar_descriptor<Record>::value_meta();
    const auto *shape = schema_descriptor<Lifted>::ts_meta();
    CHECK(shape->value_schema != ordinary);
    CHECK(ordinary_nominal_origin(shape->value_schema) == ordinary);
    CHECK(ordinary->bundle_generic_arguments().front() == scalar_descriptor<Pair>::value_meta());
    CHECK(shape->fields()[0].type == schema_descriptor<Temporal<Pair>>::ts_meta());
    CHECK((schema_descriptor<TS<Pair>>::ts_meta()->kind == TSTypeKind::TS));
    CHECK((schema_descriptor<Temporal<List<Pair, 2>>>::ts_meta()->element_ts() == schema_descriptor<Temporal<Pair>>::ts_meta()));
    CHECK((schema_descriptor<Temporal<Map<Int, Pair>>>::ts_meta()->element_ts() == schema_descriptor<Temporal<Pair>>::ts_meta()));
    const PreparedValuePlan ordinary_plan{ordinary};
    const PreparedValuePlan pair_plan{scalar_descriptor<Pair>::value_meta()};
    const Value seven{Int{7}}, falsity{Bool{false}};
    const auto partial = pair_plan.bundle(std::array{std::pair<std::size_t, ValueView>{0, seven.view()}});
    const auto full = pair_plan.bundle(std::array{std::pair<std::size_t, ValueView>{0, seven.view()},
        std::pair<std::size_t, ValueView>{1, falsity.view()}});
    const auto partial_record = ordinary_plan.bundle(std::array{std::pair<std::size_t, ValueView>{0, partial.view()}});
    const auto full_record = ordinary_plan.bundle(std::array{std::pair<std::size_t, ValueView>{0, full.view()}});
    const PreparedObservationPlan to_held{shape, ordinary, false};
    const PreparedObservationPlan to_ordinary{shape, ordinary};
    const auto before = type_system_lock_count();
    for (std::size_t cycle = 0; cycle < 32; ++cycle) {
        const auto held = to_held.retain(cycle % 2 == 0 ? partial_record.view() : full_record.view());
        CHECK(held.schema() == shape->value_schema);
        const auto copy = to_ordinary.retain(held.view());
        CHECK(copy.schema() == ordinary);
        auto payload = ordinary_plan.index(copy.view(), 0);
        CHECK(pair_plan.index(payload, 0).checked_as<Int>() == 7);
        CHECK(pair_plan.index(payload, 1).has_value() == (cycle % 2 != 0));
        if (cycle % 2 != 0) { CHECK_FALSE(pair_plan.index(payload, 1).checked_as<Bool>()); }
    }
    CHECK(type_system_lock_count() == before);
    using PlainOrigin = NominalBundle<"ordinary", "PlainObserved", false, BundleParents<>, BundleArguments<>, Field<"number", Int>, Field<"flag", Bool>>;
    using PlainCanonical = NominalTSB<PlainOrigin, Field<"number", TS<Int>>, Field<"flag", TS<Bool>>>;
    const auto *plain_ordinary = scalar_descriptor<PlainOrigin>::value_meta();
    const auto *plain_held = schema_descriptor<Temporal<PlainOrigin>>::ts_meta();
    const auto *plain_canonical = schema_descriptor<PlainCanonical>::ts_meta();
    TSOutput provider{*plain_canonical};
    TSInput declared{TSInputBuilderFactory::checked_builder_for(*plain_held, TSEndpointSchema::peered(plain_held))};
    const PreparedValuePlan plain_value{plain_ordinary};
    const auto plain_payload = plain_value.bundle(std::array{std::pair<std::size_t, ValueView>{0, seven.view()},
        std::pair<std::size_t, ValueView>{1, falsity.view()}});
    const PreparedPublicationPlan plain_publish{plain_canonical, plain_ordinary};
    plain_publish.apply(provider.view(MIN_ST), plain_payload.view());
    declared.view(nullptr, MIN_ST).bind_output(provider.view(MIN_ST));
    const PreparedObservationPlan endpoint_plan{plain_held, plain_ordinary};
    CHECK(declared.view(nullptr, MIN_ST).value().schema() == plain_ordinary);
    CHECK_THROWS_AS((PreparedPublicationPlan{plain_held, TypeRegistry::instance().bundle("ordinary", "WrongOrigin",
        {{"number", scalar_descriptor<Int>::value_meta()}, {"flag", scalar_descriptor<Bool>::value_meta()}})}), std::invalid_argument);
    CHECK_THROWS_AS((PreparedPublicationPlan{plain_held, TypeRegistry::instance().un_named_bundle({
        {"missing", scalar_descriptor<Int>::value_meta()}, {"flag", scalar_descriptor<Bool>::value_meta()}})}), std::invalid_argument);
    CHECK_THROWS_AS((TypeRegistry::instance().projected_bundle(plain_ordinary, {
        {"flag", scalar_descriptor<Bool>::value_meta()}, {"number", scalar_descriptor<Int>::value_meta()}})), std::invalid_argument);
    // Private conversion maps names once even when an independently prepared
    // structural destination has a different order. Such a destination is not
    // temporally equivalent to the nominal input, so wiring still rejects it.
    const auto *reordered = TypeRegistry::instance().un_named_tsb({
        {"flag", TypeRegistry::instance().ts(scalar_descriptor<Bool>::value_meta())},
        {"number", TypeRegistry::instance().ts(scalar_descriptor<Int>::value_meta())}});
    CHECK_FALSE(time_series_schema_equivalent(plain_held, reordered));
    const PreparedPublicationPlan reordered_publish{reordered, plain_ordinary};
    TSOutput reordered_output{*reordered};
    reordered_publish.apply(reordered_output.view(MIN_ST), plain_payload.view());
    auto reordered_endpoint = reordered_output.view(MIN_ST);
    CHECK_FALSE(reordered_endpoint.value().as_bundle().at(0).checked_as<Bool>());
    CHECK(reordered_endpoint.value().as_bundle().at(1).checked_as<Int>() == 7);
    const auto endpoint_before = type_system_lock_count();
    for (std::size_t copy_index = 0; copy_index < 32; ++copy_index) {
        const auto copy = endpoint_plan.retain_endpoint(declared.view(nullptr, MIN_ST));
        CHECK(copy.schema() == plain_ordinary);
        CHECK(plain_value.index(copy.view(), 0).checked_as<Int>() == 7);
        CHECK_FALSE(plain_value.index(copy.view(), 1).checked_as<Bool>());
    }
    CHECK(type_system_lock_count() == endpoint_before);
    const auto nil = to_ordinary.retain(Value::typed_null(storage_binding(shape->value_schema)).view());
    CHECK_FALSE(nil.has_value());
    CHECK(nil.schema() == ordinary);
    auto &registry = TypeRegistry::instance();
    const auto *wrong_origin = registry.bundle("ordinary", "OtherObserved", {{"payload", scalar_descriptor<Pair>::value_meta()}},
        {}, false, "__type__", {scalar_descriptor<Pair>::value_meta()});
    CHECK_THROWS_AS((PreparedObservationPlan{shape, wrong_origin}), std::invalid_argument);
    CHECK_THROWS_AS((PreparedObservationPlan{schema_descriptor<TS<Pair>>::ts_meta(), shape->value_schema}), std::invalid_argument);
}

TEST_CASE("cold Bundle field indices preserve reordered publication and exact named observations", "[ordinary][field-mapping]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *origin = registry.bundle("ordinary", "IndexedFields", {{"alpha", integer}, {"beta", integer}, {"gamma", integer}});
    const auto *named = registry.tsb(origin);
    const auto *reordered = registry.un_named_tsb({{"gamma", registry.ts(integer)}, {"alpha", registry.ts(integer)}, {"beta", registry.ts(integer)}});
    const PreparedValuePlan values{origin};
    const Value seven{Int{7}}, eight{Int{8}}, nine{Int{9}};
    const auto payload = values.bundle(std::array{std::pair<std::size_t, ValueView>{0, seven.view()},
        std::pair<std::size_t, ValueView>{1, eight.view()}, std::pair<std::size_t, ValueView>{2, nine.view()}});
    const PreparedObservationPlan forward{named, origin}, inverse{named, origin, false};
    const PreparedPublicationPlan publication{reordered, origin};
    TSOutput output{*reordered};
    const auto before = type_system_lock_count();
    for (std::size_t iteration = 0; iteration < 8; ++iteration) {
        const auto held = inverse.retain(payload.view());
        const auto copy = forward.retain(held.view());
        CHECK(values.index(copy.view(), 0).checked_as<Int>() == 7);
        CHECK(values.index(copy.view(), 1).checked_as<Int>() == 8);
        CHECK(values.index(copy.view(), 2).checked_as<Int>() == 9);
        publication.apply(output.view(MIN_ST), copy.view());
        const auto result = output.view(MIN_ST).value().as_bundle();
        CHECK(result.at(0).checked_as<Int>() == 9);
        CHECK(result.at(1).checked_as<Int>() == 7);
        CHECK(result.at(2).checked_as<Int>() == 8);
    }
    CHECK(type_system_lock_count() == before);
    // Malformed cold metadata must fail before any child can be mapped twice.
    // Its value binding remains the exact registered origin, so these controls
    // exercise name validation rather than introducing a schema alias.
    std::array<TSFieldMetaData, 3> duplicate{named->fields()[0], named->fields()[1], named->fields()[2]};
    const std::string duplicate_name{"alpha"};
    duplicate[1].name = duplicate_name.c_str();
    auto malformed = *named;
    malformed.set_tsb(duplicate.data(), duplicate.size(), named->bundle_name());
    CHECK_THROWS_AS((PreparedObservationPlan{&malformed, origin}), std::invalid_argument);
    CHECK_THROWS_AS((PreparedObservationPlan{&malformed, origin, false}), std::invalid_argument);
    CHECK_THROWS_AS((PreparedPublicationPlan{&malformed, origin}), std::invalid_argument);
    duplicate[1].name = "missing";
    CHECK_THROWS_AS((PreparedObservationPlan{&malformed, origin}), std::invalid_argument);
    CHECK_THROWS_AS((PreparedObservationPlan{&malformed, origin, false}), std::invalid_argument);
    CHECK_THROWS_AS((PreparedPublicationPlan{&malformed, origin}), std::invalid_argument);
}

TEST_CASE("cold Bundle preparation reports doubling per-field costs", "[.][ordinary-field-preparation-benchmark]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *scalar = registry.ts(integer);
    constexpr std::size_t repetitions = 32;
    for (const auto count : {std::size_t{512}, std::size_t{1024}, std::size_t{2048}, std::size_t{4096}}) {
        std::vector<std::pair<std::string, const ValueTypeMetaData *>> value_fields;
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> temporal_fields;
        value_fields.reserve(count);
        temporal_fields.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
            value_fields.emplace_back("field_" + std::to_string(index), integer);
            temporal_fields.emplace_back("field_" + std::to_string(count - index - 1), scalar);
        }
        // Registry/schema creation and initial plan realization are outside
        // every timed region. Only cold conversion/publication preparation is
        // measured; runtime name lookups are covered by the functional test.
        const auto *origin = registry.bundle("ordinary", "ScalingFields" + std::to_string(count), value_fields);
        const auto *named = registry.tsb(origin);
        const auto *reordered = registry.un_named_tsb(temporal_fields);
        const PreparedObservationPlan warm_forward{named, origin}, warm_inverse{named, origin, false};
        const PreparedPublicationPlan warm_publication{reordered, origin};
        CHECK(warm_forward.binding().schema() == origin);
        CHECK(warm_inverse.binding().schema() == named->value_schema);
        const auto measure = [&](auto prepare) {
            const auto start = std::chrono::steady_clock::now();
            for (std::size_t iteration = 0; iteration < repetitions; ++iteration) { prepare(); }
            return std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - start).count()
                / static_cast<double>(count * repetitions);
        };
        const auto forward_ns = measure([&] { const PreparedObservationPlan plan{named, origin}; });
        const auto inverse_ns = measure([&] { const PreparedObservationPlan plan{named, origin, false}; });
        const auto publication_ns = measure([&] { const PreparedPublicationPlan plan{reordered, origin}; });
        std::cout << "cold Bundle preparation fields=" << count << " forward_ns/field=" << forward_ns
                  << " inverse_ns/field=" << inverse_ns << " publication_ns/field=" << publication_ns << '\n';
    }
}

TEST_CASE("ordinary lists keep canonical schemas with mutable planned storage", "[ordinary][list]") {
    using namespace hgraph;
    using hgl::ordinary::PreparedValuePlan;
    const auto *schema = scalar_descriptor<hgl::ordinary::List<std::int64_t>>::value_meta();
    const PreparedValuePlan plan{schema};
    REQUIRE(plan.binding().schema() == schema);
    REQUIRE(plan.binding().ops()->kind == ValueOpsKind::MutableList);
    const Value one{std::int64_t{1}};
    const Value two{std::int64_t{2}};
    const auto before = type_system_lock_count();
    Value source = plan.empty_list();
    plan.push(source.view(), one.view());
    Value retained = plan.retain(source.view());
    plan.push(source.view(), two.view());
    CHECK(plan.len(source.view()) == 2);
    REQUIRE(plan.len(retained.view()) == 1);
    CHECK(plan.index(retained.view(), 0).checked_as<std::int64_t>() == 1);
    CHECK_THROWS_AS(plan.index(retained.view(), -1), std::out_of_range);
    CHECK_THROWS_AS(plan.index(retained.view(), 1), std::out_of_range);
    CHECK(type_system_lock_count() == before);
}

TEST_CASE("prepared fixed List retention preserves observations and dense defaults separately", "[ordinary][list][observation]") {
    using namespace hgraph;
    using hgl::ordinary::PreparedValuePlan;
    const auto *schema = TypeRegistry::instance().fixed_list(scalar_descriptor<Int>::value_meta(), 2);
    const PreparedValuePlan plan{schema};
    const Value seven{Int{7}};
    ListBuilder builder{plan.element_binding(), *schema};
    builder.push_back(seven.view());
    builder.push_back_unset();
    auto storage = builder.build_storage();
    auto observed = plan.list(storage);
    const auto before = type_system_lock_count();
    for (std::size_t iteration = 0; iteration < 32; ++iteration) {
        const auto retained = plan.retain(observed.view());
        CHECK(retained.binding().schema() == schema);
        CHECK(plan.index(retained.view(), 0).checked_as<Int>() == 7);
        CHECK_FALSE(plan.index(retained.view(), 1).has_value());
    }
    CHECK(type_system_lock_count() == before);
    auto dense = plan.empty_list();
    CHECK(plan.index(dense.view(), 0).checked_as<Int>() == 0);
    CHECK(plan.index(dense.view(), 1).checked_as<Int>() == 0);
    plan.replace_index(dense.view(), 1, seven.view());
    CHECK(plan.index(dense.view(), 1).checked_as<Int>() == 7);
    CHECK_FALSE(plan.index(observed.view(), 1).has_value());
    const auto absent = Value::typed_null(plan.binding());
    CHECK_THROWS_WITH(plan.index(absent.view(), 0), "ordinary scalar value is absent");
    CHECK_THROWS_WITH(plan.len(absent.view()), "ordinary scalar value is absent");
    const PreparedValuePlan nested_list{TypeRegistry::instance().fixed_list(schema, 2)};
    ListBuilder nested_builder{nested_list.element_binding(), *nested_list.binding().schema()};
    nested_builder.push_back(observed.view());
    nested_builder.push_back_unset();
    auto nested_storage = nested_builder.build_storage();
    const auto nested = nested_list.list(nested_storage);
    const auto nested_copy = nested_list.retain(nested.view());
    CHECK_FALSE(nested_list.index(nested_copy.view(), 1).has_value());
    CHECK_FALSE(plan.index(nested_list.index(nested_copy.view(), 0), 1).has_value());
    const PreparedValuePlan map{TypeRegistry::instance().map(seven.binding().schema(), seven.binding().schema())};
    const auto absent_map = Value::typed_null(map.binding());
    CHECK_THROWS_WITH(map.items(absent_map.view()), "ordinary scalar value is absent");
    MapBuilder map_builder{map.key_binding(), map.element_binding()};
    map_builder.set_item(seven.view(), seven.view());
    auto map_storage = map_builder.build_storage();
    const Value map_value{map.binding(), &map_storage, Value::AdoptStorage{}};
    const PreparedValuePlan nested_map{TypeRegistry::instance().fixed_list(map.binding().schema(), 2)};
    ListBuilder maps{nested_map.element_binding(), *nested_map.binding().schema()};
    maps.push_back(map_value.view());
    maps.push_back_unset();
    auto maps_storage = maps.build_storage();
    const auto maps_value = nested_map.list(maps_storage);
    const auto maps_copy = nested_map.retain(maps_value.view());
    CHECK_FALSE(nested_map.index(maps_copy.view(), 1).has_value());
    CHECK(nested_map.index(maps_copy.view(), 1).binding().schema() == map.binding().schema());
    CHECK(nested_map.index(maps_copy.view(), 0).as_map().at(seven.view()).checked_as<Int>() == 7);
}

TEST_CASE("ordinary bundle construction retains nested lists without registry access", "[ordinary][bundle]") {
    using namespace hgraph;
    using hgl::ordinary::PreparedValuePlan;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<std::int64_t>::value_meta();
    const auto *list_schema = registry.list(integer);
    const auto *bundle_schema = registry.bundle("ordinary", "Record", {{"id", integer}, {"values", list_schema}});
    const PreparedValuePlan list_plan{list_schema};
    const PreparedValuePlan record_plan{bundle_schema};
    const Value id{std::int64_t{7}};
    const Value other{std::int64_t{9}};
    Value list = list_plan.empty_list();
    list_plan.push(list.view(), id.view());
    std::array<std::pair<std::size_t, ValueView>, 2> fields{
        std::pair<std::size_t, ValueView>{1U, list.view()},
        std::pair<std::size_t, ValueView>{0U, id.view()}};
    const auto before = type_system_lock_count();
    Value record = record_plan.bundle(fields);
    Value retained = record_plan.retain(record.view());
    auto record_list = record_plan.index_mutable(record.view(), 1);
    list_plan.push(record_list, other.view());
    CHECK(list_plan.len(record_plan.index(record.view(), 1)) == 2);
    CHECK(list_plan.len(record_plan.index(retained.view(), 1)) == 1);
    CHECK(list_plan.len(list.view()) == 1);
    CHECK(record_plan.index(record.view(), 0).checked_as<std::int64_t>() == 7);
    CHECK(type_system_lock_count() == before);
}

TEST_CASE("ordinary nested list mutation preserves independent retained values", "[ordinary][list]") {
    using namespace hgraph;
    using hgl::ordinary::PreparedValuePlan;
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *inner_schema = TypeRegistry::instance().list(integer);
    const PreparedValuePlan inner_plan{inner_schema};
    const PreparedValuePlan outer_plan{TypeRegistry::instance().list(inner_schema)};
    const Value one{Int{1}}, two{Int{2}};
    auto inner = inner_plan.empty_list();
    inner_plan.push(inner.view(), one.view());
    auto outer = outer_plan.empty_list();
    outer_plan.push(outer.view(), inner.view());
    auto retained = outer_plan.retain(outer.view());
    const auto before = type_system_lock_count();
    inner_plan.push(outer_plan.index_mutable(outer.view(), 0), two.view());
    CHECK(inner_plan.len(outer_plan.index(outer.view(), 0)) == 2);
    CHECK(inner_plan.len(outer_plan.index(retained.view(), 0)) == 1);
    CHECK(inner_plan.len(inner.view()) == 1);
    CHECK(type_system_lock_count() == before);
}

TEST_CASE("prepared delta capture retains mutable publications independently", "[ordinary][delta]") {
    using namespace hgraph;
    using hgl::ordinary::PreparedDeltaPlan;
    auto &registry = TypeRegistry::instance();
    auto &factory = ValuePlanFactory::instance();
    const auto *integer = scalar_descriptor<std::int64_t>::value_meta();
    const auto *shape = registry.tsl(registry.ts(integer), 2);
    const PreparedDeltaPlan delta_plan{shape};
    const auto *payload_schema = shape->delta_value_schema;
    const auto key_binding = factory.type_for(payload_schema->key_type);
    const auto element_binding = factory.type_for(integer);
    const auto mutable_payload = intern_value_type(*payload_schema,
        mutable_map_plan(key_binding, element_binding), mutable_map_ops());
    Value payload{mutable_payload};
    // TSL's native sparse-index key schema owns its exact integer width.
    Value index{key_binding};
    const Value first{std::int64_t{12}};
    const Value later{std::int64_t{20}};
    const auto *ops = checked_value_ops<MutableMapValueOps>(mutable_payload, "test mutable delta payload");
    ops->insert(ops->context, payload.view().begin_mutation().mutable_data(), index.view().data(), first.view().data());
    const auto before = type_system_lock_count();
    Value saved = delta_plan.capture(payload.view());
    ops->insert(ops->context, payload.view().begin_mutation().mutable_data(), index.view().data(), later.view().data());
    CHECK(delta_plan.payload(saved.view()).as_map().at(index.view()).checked_as<std::int64_t>() == 12);
    CHECK(payload.as_map().at(index.view()).checked_as<std::int64_t>() == 20);
    CHECK(type_system_lock_count() == before);
}

TEST_CASE("ordinary push retains an element before growing its owner", "[ordinary][list]") {
    using namespace hgraph;
    const hgl::ordinary::PreparedValuePlan plan{scalar_descriptor<hgl::ordinary::List<Str>>::value_meta()};
    auto list = plan.empty_list();
    const Value seed{std::string(128, 'x')};
    plan.push(list.view(), seed.view());
    for (int count = 0; count < 128; ++count) { plan.push(list.view(), plan.index(list.view(), 0)); }
    REQUIRE(plan.len(list.view()) == 129);
    for (std::int64_t index = 0; index < plan.len(list.view()); ++index) {
        CHECK(plan.index(list.view(), index).checked_as<Str>() == std::string(128, 'x'));
    }
}

#include "wiring/delta_trace.h"

TEST_CASE("eval trace admission keeps membership across sparse nested updates", "[ordinary][trace]") {
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = ValuePlanFactory::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *shape = registry.tsd(integer, registry.tss(integer));
    const auto binding = factory.type_for(integer);
    const auto set_delta = [&](std::initializer_list<Int> added, std::initializer_list<Int> removed) {
        SetBuilder additions{binding}, removals{binding};
        for (const auto value : added) { additions.insert(Value{value}.view()); }
        for (const auto value : removed) { removals.insert(Value{value}.view()); }
        BundleBuilder result{factory.type_for(shape->element_ts()->delta_value_schema)};
        result.set(0, additions.build().view()).set(1, removals.build().view());
        return result.build();
    };
    const auto map_delta = [&](const Value *child, bool remove) {
        SetBuilder removed{binding};
        MapBuilder modified{binding, factory.type_for(shape->element_ts()->delta_value_schema)};
        const Value key{Int{7}};
        if (remove) { removed.insert(key.view()); }
        if (child) { modified.set_item(key.view(), child->view()); }
        BundleBuilder result{factory.type_for(shape->delta_value_schema)};
        result.set(0, removed.build().view()).set(1, modified.build().view());
        return result.build();
    };
    hgl::wiring::DeltaTrace trace;
    auto first = set_delta({1, 2}, {});
    auto update = map_delta(&first, false);
    CHECK_NOTHROW(trace.accept(shape, update.view()));
    CHECK_THROWS_WITH(trace.accept(shape, update.view()), "addition of a present set member");
    auto second = set_delta({}, {1});
    update = map_delta(&second, false);
    CHECK_NOTHROW(trace.accept(shape, update.view()));
    auto empty = set_delta({}, {});
    update = map_delta(&empty, false);
    CHECK_NOTHROW(trace.accept(shape, update.view()));
    auto remove = map_delta(nullptr, true);
    CHECK_NOTHROW(trace.accept(shape, remove.view()));
    CHECK_THROWS_WITH(trace.accept(shape, remove.view()), "removal of an absent map key");
    update = map_delta(&second, false);
    CHECK_THROWS_WITH(trace.accept(shape, update.view()), "removal of an absent set member");
}

TEST_CASE("atomic publication schemas and values require complete finite payloads", "[ordinary][atomic]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *empty_schema = registry.bundle("ordinary", "AtomicEmpty", {});
    const PreparedValuePlan empty_plan{empty_schema};
    const auto empty = empty_plan.bundle({});
    CHECK_NOTHROW(validate_complete_value(empty.view()));
    CHECK(delta_schema(registry.ts(empty_schema)) == empty_schema);
    const auto *record_schema = registry.bundle("ordinary", "AtomicRequired", {{"value", integer}});
    const PreparedValuePlan record_plan{record_schema};
    const auto missing = record_plan.bundle({});
    CHECK_THROWS_AS(validate_complete_value(missing.view()), std::invalid_argument);
    CHECK(delta_schema(registry.ts(registry.set(integer))) == registry.set(integer));
    CHECK(delta_schema(registry.ts(registry.map(integer, integer))) == registry.map(integer, integer));
    CHECK_THROWS_AS(delta_schema(registry.ts(registry.set(registry.list(integer)))), std::invalid_argument);
    CHECK_THROWS_AS(delta_schema(registry.ts(registry.list(integer, 0, true))), std::invalid_argument);
}

TEST_CASE("rolling arrival payloads preserve exact cold origin metadata", "[ordinary][rolling]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const std::array shapes{registry.tsw(integer, 2, 2), registry.tsw(integer, 2, 0),
        registry.tsw(integer, 3, 2), registry.tsw_duration(integer, TimeDelta{5}, TimeDelta{1}),
        registry.tsw_duration(integer, TimeDelta{5}, TimeDelta{0})};
    for (const auto *shape : shapes) {
        CHECK(delta_schema(shape) == integer);
        CHECK(origin_source(origin_schema(shape)) == shape);
        const PreparedDeltaPlan plan{shape};
        const Value arrival{Int{10}};
        const auto retained = plan.capture(arrival.view());
        CHECK(retained.schema() == integer);
        CHECK(plan.payload(retained.view()).checked_as<Int>() == 10);
        for (const auto *other : shapes) {
            CHECK((origin_schema(shape) == origin_schema(other)) == (shape == other));
        }
    }
    const auto *list = registry.list(integer);
    const auto *shape = registry.tsw(list, 2, 1);
    const PreparedValuePlan list_plan{list};
    const PreparedDeltaPlan arrival_plan{shape};
    auto source = list_plan.empty_list();
    list_plan.push(source.view(), Value{Int{1}}.view());
    const auto retained = arrival_plan.capture(source.view());
    list_plan.push(source.view(), Value{Int{2}}.view());
    CHECK(list_plan.len(arrival_plan.payload(retained.view())) == 1);
    CHECK(origin_source(origin_schema(shape)) == shape);
}

TEST_CASE("recursive atomic plans retain finite owned trees and validate descendants", "[ordinary][recursive]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *list = registry.list(integer);
    const auto *node = registry.recursive_bundle("ordinary.recursive", "Node",
        {{"value", integer}, {"items", list}, {"next", nullptr}});
    const PreparedValuePlan plan{node}, list_plan{list};
    const OptionalField optional = [node](const ValueTypeMetaData *schema, std::size_t index) {
        return schema == node && index == 2;
    };
    CHECK_NOTHROW(validate_delta_shape(registry.ts(node)));
    auto items = list_plan.empty_list();
    list_plan.push(items.view(), Value{Int{1}}.view());
    const Value one{Int{1}}, two{Int{2}};
    std::array leaf_fields{std::pair<std::size_t, ValueView>{0, one.view()},
        std::pair<std::size_t, ValueView>{1, items.view()}};
    auto leaf = plan.bundle(leaf_fields);
    std::array root_fields{std::pair<std::size_t, ValueView>{0, two.view()},
        std::pair<std::size_t, ValueView>{1, items.view()},
        std::pair<std::size_t, ValueView>{2, leaf.view()}};
    auto root = plan.bundle(root_fields);
    auto retained = plan.retain(root.view());
    CHECK_NOTHROW(validate_complete_value(root.view(), optional));
    CHECK_THROWS_AS(validate_complete_value(root.view()), std::invalid_argument);
    list_plan.push(items.view(), two.view());
    plan.replace_index(leaf.view(), 0, two.view());
    CHECK(root.equals(retained));
    const auto child = root.as_bundle().field("next").concrete();
    CHECK(child.as_bundle().field("value").checked_as<Int>() == 1);
    CHECK(child.as_bundle().field("items").as_list().size() == 1);
    Value incomplete{storage_binding(node)};
    std::array bad_fields{std::pair<std::size_t, ValueView>{0, one.view()},
        std::pair<std::size_t, ValueView>{1, items.view()},
        std::pair<std::size_t, ValueView>{2, incomplete.view()}};
    auto bad = plan.bundle(bad_fields);
    CHECK_THROWS_AS(validate_complete_value(bad.view(), optional), std::invalid_argument);
    auto edge = plan.index_mutable(root.view(), 2);
    const PreparedValuePlan edge_plan{edge.binding()};
    const auto absent_edge = Value::typed_null(edge_plan.binding());
    const PreparedValuePlan absent_next_plan{edge_plan.field_binding(2)};
    const auto projection_locks = type_system_lock_count();
    for (std::size_t index = 0; index < 128; ++index) {
        const auto next = edge_plan.field_observation(absent_edge.view(), 2);
        const auto number = absent_next_plan.field_observation(next, 0);
        CHECK_FALSE(next.has_value());
        CHECK_FALSE(number.has_value());
        CHECK(number.schema() == integer);
    }
    CHECK(type_system_lock_count() == projection_locks);
    CHECK(edge_plan.len(edge) == 3);
    edge_plan.replace_index(edge, 0, two.view());
    CHECK(edge_plan.index(edge, 0).checked_as<Int>() == 2);
    CHECK(retained.as_bundle().field("next").concrete().as_bundle().field("value").checked_as<Int>() == 1);
    auto edge_items = edge_plan.index_mutable(edge, 1);
    const PreparedValuePlan edge_items_plan{edge_items.binding()};
    CHECK_THROWS_WITH(edge_items_plan.push(edge_items, two.view()), "ordinary list storage does not support growth");
}

TEST_CASE("family preflight rejects unrelated native payloads before publication", "[ordinary][family]") {
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *family = registry.bundle("ordinary.family", "Family", {{"value", integer}}, {}, true);
    const auto *member = registry.bundle("ordinary.family", "Member", {{"value", integer}}, {family});
    const auto *other = registry.bundle("ordinary.family", "Other", {{"value", integer}});
    Value accepted{hgl::ordinary::storage_binding(member)};
    accepted.as_bundle().begin_mutation().at(0).set(Int{1});
    Value rejected{hgl::ordinary::storage_binding(other)};
    rejected.as_bundle().begin_mutation().at(0).set(Int{1});
    hgl::wiring::DeltaTrace trace;
    CHECK_NOTHROW(trace.accept(registry.ts(family), accepted.view()));
    CHECK_THROWS_WITH(trace.accept(registry.ts(family), rejected.view()),
        "publication is not a concrete member of its declared family");
    const hgl::ordinary::PreparedValuePlan plan{family};
    auto retained = plan.retain(accepted.view());
    CHECK(retained.view().concrete().schema() == member);
    CHECK(plan.index(retained.view(), 0).checked_as<Int>() == 1);
}

TEST_CASE("composite keys reject deep NaN and compare full retained values", "[ordinary][composite-keys]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    auto &registry = TypeRegistry::instance();
    const auto *floating = scalar_descriptor<Float>::value_meta();
    const auto *pair = registry.tuple({scalar_descriptor<Int>::value_meta(), floating});
    const auto *key = registry.bundle("ordinary.keys", "Key", {{"pair", pair}});
    const PreparedValuePlan pair_plan{pair}, key_plan{key};
    const auto make_key = [&](Float value) {
        const Value one{Int{1}}, number{value};
        std::array parts{std::pair<std::size_t, ValueView>{0, one.view()}, std::pair<std::size_t, ValueView>{1, number.view()}};
        auto tuple = pair_plan.bundle(parts);
        std::array fields{std::pair<std::size_t, ValueView>{0, tuple.view()}};
        return key_plan.bundle(fields);
    };
    auto first = make_key(0.0), duplicate = make_key(-0.0), different = make_key(1.0);
    ScalarKeySet keys{key_plan.binding()};
    CHECK_NOTHROW(keys.insert(first.view()));
    CHECK_THROWS_WITH(keys.insert(duplicate.view()), "duplicate or overlapping delta member, key or index");
    CHECK_NOTHROW(keys.insert(different.view()));
    auto nan = make_key(std::numeric_limits<Float>::quiet_NaN());
    CHECK_THROWS_WITH(keys.insert(nan.view()), "NaN collection keys are outside the publication profile");
    CHECK_THROWS_AS(validate_key_schema(registry.list(floating)), std::invalid_argument);
    const auto *shape = registry.tss(key);
    SetBuilder added{key_plan.binding()}, removed{key_plan.binding()};
    added.insert(first.view());
    BundleBuilder payload{ValuePlanFactory::instance().type_for(shape->delta_value_schema)};
    payload.set(0, added.build().view()).set(1, removed.build().view());
    auto delta = payload.build();
    hgl::wiring::DeltaTrace trace;
    CHECK_NOTHROW(trace.accept(shape, delta.view()));
    CHECK_THROWS_WITH(trace.accept(shape, delta.view()), "addition of a present set member");
}

TEST_CASE("publication predicates preserve unrelated failures", "[ordinary][delta][errors]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    const auto *schema = TypeRegistry::instance().bundle("ordinary", "IncompleteProfile",
        {{"value", scalar_descriptor<Int>::value_meta()}});
    const Value incomplete{ValuePlanFactory::instance().type_for(schema)};
    CHECK_THROWS_AS(validate_complete_value(incomplete.view()), PublicationProfileError);
    CHECK_THROWS_AS(validate_complete_value(incomplete.view(),
        [](const ValueTypeMetaData *, std::size_t) -> bool { throw std::bad_alloc{}; }), std::bad_alloc);
    CHECK_THROWS_AS(validate_complete_value(incomplete.view(),
        [](const ValueTypeMetaData *, std::size_t) -> bool { throw std::runtime_error{"storage failure"}; }), std::runtime_error);
}

TEST_CASE("bytes construction owns octets and native operations preserve unsigned identity", "[ordinary][bytes]") {
    using namespace hgraph;
    using namespace hgl::ordinary;
    const PreparedValuePlan list{scalar_descriptor<List<Int>>::value_meta()};
    Value                   source = list.empty_list();
    const Value             zero{Int{0}}, last{Int{255}}, invalid{Int{-1}};
    list.push(source.view(), zero.view());
    const Bytes captured = bytes_from_octets(source.view());
    list.push(source.view(), last.view());
    CHECK(bytes_length(captured) == 1);
    CHECK(captured == Bytes{std::string{"\0", 1}});
    const Bytes complete = bytes_from_octets(source.view());
    CHECK(bytes_length(complete) == 2);
    CHECK(captured < complete);
    CHECK(Bytes{std::string{"\x7f", 1}} < Bytes{std::string{"\x80", 1}});
    const Value a{complete}, b{bytes_from_octets(source.view())};
    CHECK(a.schema() == scalar_descriptor<Bytes>::value_meta());
    CHECK(a.equals(b));
    CHECK(a.hash() == b.hash());
    CHECK(bytes_length(bytes_from_octets(list.empty_list().view())) == 0);
    list.push(source.view(), invalid.view());
    try {
        (void)bytes_from_octets(source.view());
        FAIL("invalid octet unexpectedly constructed bytes");
    } catch (const hgl::ExecutionError &error) { CHECK(error.code() == "value.byte_range"); }
    CHECK(bytes_length(complete) == 2);
    validate_key_schema(scalar_descriptor<Bytes>::value_meta());
    validate_delta_shape(schema_descriptor<TS<Bytes>>::ts_meta());
}
