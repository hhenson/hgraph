#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

#include <type_traits>

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

    hgraph::Value value{*schema};
    auto tuple = value.view().as_tuple();

    {
        auto mutation = tuple.begin_mutation();
        mutation.set(0, hgraph::value_for(int32_t{7}).view());
        mutation.set(1, hgraph::value_for(std::string{"alpha"}).view());
    }

    CHECK(tuple[0].as_atomic().as<int32_t>() == 7);
    CHECK(tuple[1].as_atomic().as<std::string>() == "alpha");

    tuple.begin_mutation().set(1, hgraph::View::invalid_for(hgraph::value::scalar_type_meta<std::string>()));
    CHECK_FALSE(tuple[1].valid());
}

TEST_CASE("Bundle values support named and positional access")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.bundle()
                             .add_field("count", hgraph::value::scalar_type_meta<int32_t>())
                             .add_field("label", hgraph::value::scalar_type_meta<std::string>())
                             .build();

    hgraph::Value value{*schema};
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
