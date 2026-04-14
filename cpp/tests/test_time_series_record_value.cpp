#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

#include <string_view>
#include <type_traits>
#include <vector>

static_assert(!std::is_copy_constructible_v<hgraph::TupleMutationView>);
static_assert(!std::is_copy_assignable_v<hgraph::TupleMutationView>);
static_assert(std::is_move_constructible_v<hgraph::TupleMutationView>);
static_assert(!std::is_move_assignable_v<hgraph::TupleMutationView>);

static_assert(!std::is_copy_constructible_v<hgraph::BundleMutationView>);
static_assert(!std::is_copy_assignable_v<hgraph::BundleMutationView>);
static_assert(std::is_move_constructible_v<hgraph::BundleMutationView>);
static_assert(!std::is_move_assignable_v<hgraph::BundleMutationView>);

TEST_CASE("Tuple values support positional heterogeneous access")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.tuple()
                             .add_element(hgraph::value::scalar_type_meta<int32_t>())
                             .add_element(hgraph::value::scalar_type_meta<std::string>())
                             .build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto tuple = value.view().as_tuple();

    {
        auto mutation = tuple.begin_mutation();
        mutation.set(0, hgraph::value_for(int32_t{7}).view());
        mutation.set(1, hgraph::value_for(std::string{"alpha"}).view());
    }

    CHECK(tuple[0].as_atomic().as<int32_t>() == 7);
    CHECK(tuple[1].as_atomic().as<std::string>() == "alpha");

    tuple.begin_mutation().set(1, hgraph::View::invalid_for(hgraph::value::scalar_type_meta<std::string>()));
    CHECK_FALSE(tuple[1].has_value());
}

TEST_CASE("Bundle values support named and positional access")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.bundle()
                             .add_field("count", hgraph::value::scalar_type_meta<int32_t>())
                             .add_field("label", hgraph::value::scalar_type_meta<std::string>())
                             .build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto bundle = value.view().as_bundle();

    {
        auto mutation = bundle.begin_mutation();
        mutation.set_field("count", hgraph::value_for(int32_t{11}).view());
        mutation.set(1, hgraph::value_for(std::string{"beta"}).view());
    }

    CHECK(bundle.field("count").as_atomic().as<int32_t>() == 11);
    CHECK(bundle.field("label").as_atomic().as<std::string>() == "beta");
    CHECK(bundle.has_field("count"));
    CHECK_FALSE(bundle.has_field("missing"));
}

TEST_CASE("Tuple and bundle mutation views support fluent command-style chaining")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *tuple_schema = registry.tuple()
                                   .add_element(hgraph::value::scalar_type_meta<int32_t>())
                                   .add_element(hgraph::value::scalar_type_meta<std::string>())
                                   .build();
    const auto *bundle_schema = registry.bundle()
                                    .add_field("lhs", hgraph::value::scalar_type_meta<int32_t>())
                                    .add_field("rhs", hgraph::value::scalar_type_meta<std::string>())
                                    .build();

    hgraph::Value tuple_value{*tuple_schema};
    auto tuple = tuple_value.tuple_view();
    tuple.begin_mutation().setting(0, int32_t{7}).setting(1, std::string{"seven"});

    CHECK(tuple[0].as_atomic().as<int32_t>() == 7);
    CHECK(tuple[1].as_atomic().as<std::string>() == "seven");

    hgraph::Value bundle_value{*bundle_schema};
    auto bundle = bundle_value.bundle_view();
    bundle.begin_mutation().setting_field("lhs", int32_t{9}).setting_field("rhs", std::string{"nine"});

    CHECK(bundle.field("lhs").as_atomic().as<int32_t>() == 9);
    CHECK(bundle.field("rhs").as_atomic().as<std::string>() == "nine");
}

TEST_CASE("Delta-tracked tuple fields retain nested buffer deltas")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.tuple()
                             .add_element(registry.cyclic_buffer(hgraph::value::scalar_type_meta<int32_t>(), 2).build())
                             .add_element(hgraph::value::scalar_type_meta<int32_t>())
                             .build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto tuple = value.tuple_view();
    auto buffer = tuple[0].as_cyclic_buffer();

    buffer.begin_mutation().pushing(int32_t{1}).pushing(int32_t{2}).pushing(int32_t{3});

    CHECK(buffer.size() == 2);
    CHECK(buffer.front().as_atomic().as<int32_t>() == 2);
    CHECK(buffer.back().as_atomic().as<int32_t>() == 3);
    CHECK(buffer.has_removed());
    CHECK(buffer.removed().as_atomic().as<int32_t>() == 1);
}

TEST_CASE("Tuple values expose updated field indices and values for the current mutation epoch")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.tuple()
                             .add_element(hgraph::value::scalar_type_meta<int32_t>())
                             .add_element(hgraph::value::scalar_type_meta<std::string>())
                             .build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto tuple = value.tuple_view();

    tuple.begin_mutation().setting(0, int32_t{3}).setting(1, std::string{"three"});
    tuple.begin_mutation().setting(1, std::string{"updated"});

    auto delta = tuple.delta();

    std::vector<size_t> updated_indices;
    for (size_t index : delta.updated_indices()) {
        updated_indices.push_back(index);
    }

    std::vector<std::string> updated_values;
    for (hgraph::View field : delta.updated_values()) {
        updated_values.push_back(field.to_string());
    }

    CHECK(updated_indices == std::vector<size_t>{1});
    CHECK(updated_values == std::vector<std::string>{"updated"});
}

TEST_CASE("Bundle values expose updated keys and values for the current mutation epoch")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.bundle()
                             .add_field("count", hgraph::value::scalar_type_meta<int32_t>())
                             .add_field("label", hgraph::value::scalar_type_meta<std::string>())
                             .build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Delta};
    auto bundle = value.bundle_view();

    bundle.begin_mutation().setting_field("count", int32_t{1}).setting_field("label", std::string{"one"});
    bundle.begin_mutation().setting_field("label", std::string{"updated"});

    auto delta = bundle.delta();

    std::vector<std::string_view> updated_keys;
    for (std::string_view key : delta.updated_keys()) {
        updated_keys.push_back(key);
    }

    std::vector<std::string> updated_values;
    for (hgraph::View field : delta.updated_values()) {
        updated_values.push_back(field.to_string());
    }

    CHECK(updated_keys == std::vector<std::string_view>{"label"});
    CHECK(updated_values == std::vector<std::string>{"updated"});
}

TEST_CASE("Plain records do not retain delta markers")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.bundle()
                             .add_field("count", hgraph::value::scalar_type_meta<int32_t>())
                             .add_field("label", hgraph::value::scalar_type_meta<std::string>())
                             .build();

    hgraph::Value value{*schema, hgraph::MutationTracking::Plain};
    auto bundle = value.bundle_view();

    bundle.begin_mutation().setting_field("count", int32_t{1}).setting_field("label", std::string{"one"});

    auto delta = bundle.delta();
    std::vector<std::string_view> updated_keys;
    for (std::string_view key : delta.updated_keys()) {
        updated_keys.push_back(key);
    }
    std::vector<std::string> updated_values;
    for (hgraph::View field : delta.updated_values()) {
        updated_values.push_back(field.to_string());
    }
    CHECK(updated_keys.empty());
    CHECK(updated_values.empty());
}

TEST_CASE("Record view copy_from supports matching schemas across tracking layouts")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.bundle()
                             .add_field("count", hgraph::value::scalar_type_meta<int32_t>())
                             .add_field("items", registry.list(hgraph::value::scalar_type_meta<int32_t>()).build())
                             .build();

    hgraph::Value delta_value{*schema, hgraph::MutationTracking::Delta};
    auto delta_bundle = delta_value.bundle_view();
    delta_bundle.begin_mutation().setting_field("count", int32_t{3});
    delta_bundle.field("items").as_list().begin_mutation().pushing_back(int32_t{7}).pushing_back(int32_t{11});

    hgraph::Value plain_value{*schema, hgraph::MutationTracking::Plain};
    plain_value.view().copy_from(delta_value.view());

    auto plain_bundle = plain_value.bundle_view();
    CHECK(plain_bundle.field("count").as_atomic().as<int32_t>() == 3);
    auto plain_items = plain_bundle.field("items").as_list();
    REQUIRE(plain_items.size() == 2);
    CHECK(plain_items[0].as_atomic().as<int32_t>() == 7);
    CHECK(plain_items[1].as_atomic().as<int32_t>() == 11);

    hgraph::Value roundtrip{*schema, hgraph::MutationTracking::Delta};
    roundtrip.view().copy_from(plain_value.view());

    auto roundtrip_bundle = roundtrip.bundle_view();
    CHECK(roundtrip_bundle.field("count").as_atomic().as<int32_t>() == 3);
    auto roundtrip_items = roundtrip_bundle.field("items").as_list();
    REQUIRE(roundtrip_items.size() == 2);
    CHECK(roundtrip_items[0].as_atomic().as<int32_t>() == 7);
    CHECK(roundtrip_items[1].as_atomic().as<int32_t>() == 11);
}
