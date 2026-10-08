#include <hgl/ordinary_values.h>

#include <hgraph/types/utils/counted_mutex.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

#include <array>
#include <cstdint>
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
    CHECK_THROWS_WITH(trace.accept(shape, update.view()), "empty set publication");
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
