#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

TEST_CASE("Set values support add contains and remove")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.set(hgraph::value::scalar_type_meta<int32_t>()).build();

    hgraph::Value value{*schema};
    auto set = value.view().as_set();

    auto one = hgraph::value_for(int32_t{1});
    auto two = hgraph::value_for(int32_t{2});

    CHECK(set.add(one.view()));
    CHECK(set.add(two.view()));
    CHECK_FALSE(set.add(one.view()));
    CHECK(set.contains(one.view()));
    CHECK(set.contains(two.view()));
    CHECK(set.remove(one.view()));
    CHECK_FALSE(set.contains(one.view()));
    CHECK(set.size() == 1);
}

TEST_CASE("Map values support lookup and invalid values")
{
    auto &registry = hgraph::value::TypeRegistry::instance();
    const auto *schema = registry.map(hgraph::value::scalar_type_meta<std::string>(),
                                      hgraph::value::scalar_type_meta<int32_t>())
                             .build();

    hgraph::Value value{*schema};
    auto map = value.view().as_map();

    auto key = hgraph::value_for(std::string{"alpha"});
    auto val = hgraph::value_for(int32_t{42});

    map.set(key.view(), val.view());
    CHECK(map.contains(key.view()));
    CHECK(map.at(key.view()).as_atomic().as<int32_t>() == 42);

    map.set(key.view(), hgraph::View::invalid_for(hgraph::value::scalar_type_meta<int32_t>()));
    CHECK(map.contains(key.view()));
    CHECK_FALSE(map.at(key.view()).valid());
}
