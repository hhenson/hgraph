#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

TEST_CASE("Tuple values support positional heterogeneous access")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.tuple()
                             .add_element(hgraph::value::scalar_type_meta<int32_t>())
                             .add_element(hgraph::value::scalar_type_meta<std::string>())
                             .build();

    hgraph::Value value{*schema};
    auto tuple = value.view().as_tuple();

    tuple.set(0, hgraph::value_for(int32_t{7}).view());
    tuple.set(1, hgraph::value_for(std::string{"alpha"}).view());

    CHECK(tuple[0].as_atomic().as<int32_t>() == 7);
    CHECK(tuple[1].as_atomic().as<std::string>() == "alpha");

    tuple.set(1, hgraph::View::invalid_for(hgraph::value::scalar_type_meta<std::string>()));
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

    bundle.set_field("count", hgraph::value_for(int32_t{11}).view());
    bundle.set(1, hgraph::value_for(std::string{"beta"}).view());

    CHECK(bundle.field("count").as_atomic().as<int32_t>() == 11);
    CHECK(bundle.field("label").as_atomic().as<std::string>() == "beta");
    CHECK(bundle.has_field("count"));
    CHECK_FALSE(bundle.has_field("missing"));
}
