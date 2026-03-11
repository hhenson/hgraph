#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/state.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

#include <compare>
#include <cstdint>

namespace {

using hgraph::Value;
using hgraph::ValueBuilderFactory;
namespace value = hgraph::value;

TEST_CASE("Fixed lists default construct inline atomic elements", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().fixed_list(value::scalar_type_meta<int32_t>(), 3).build();

    Value value{*schema};
    auto  list = value.view().as_list();

    REQUIRE(list.valid());
    CHECK(list.is_fixed());
    REQUIRE(list.size() == 3);
    CHECK(list[0].as_atomic().as<int32_t>() == 0);
    CHECK(list[1].as_atomic().as<int32_t>() == 0);
    CHECK(list[2].as_atomic().as<int32_t>() == 0);
}

TEST_CASE("Dynamic lists support resize and push_back", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().list(value::scalar_type_meta<int32_t>()).build();

    Value value{*schema};
    auto  list = value.view().as_list();

    CHECK_FALSE(list.is_fixed());
    CHECK(list.empty());

    list.push_back(int32_t{4});
    list.push_back(int32_t{7});

    REQUIRE(list.size() == 2);
    CHECK(list.front().as_atomic().as<int32_t>() == 4);
    CHECK(list.back().as_atomic().as<int32_t>() == 7);

    list.resize(4);
    REQUIRE(list.size() == 4);
    CHECK(list[2].as_atomic().as<int32_t>() == 0);
    CHECK(list[3].as_atomic().as<int32_t>() == 0);

    list.clear();
    CHECK(list.empty());
}

TEST_CASE("Lists preserve invalid element slots", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().fixed_list(value::scalar_type_meta<int32_t>(), 3).build();

    Value value{*schema};
    auto  list = value.view().as_list();

    list.set(1, hgraph::View{nullptr, value::scalar_type_meta<int32_t>()});

    CHECK(list[0].valid());
    CHECK_FALSE(list[1].valid());
    CHECK(list[1].schema() == value::scalar_type_meta<int32_t>());
    CHECK(list[2].valid());
    CHECK(value.view().to_string() == "[0, None, 0]");
}

TEST_CASE("List comparison is lexicographic within a schema", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().fixed_list(value::scalar_type_meta<int32_t>(), 2).build();

    Value lower{*schema};
    Value upper{*schema};

    auto lower_list = lower.view().as_list();
    auto upper_list = upper.view().as_list();

    lower_list.set(0, int32_t{1});
    lower_list.set(1, int32_t{2});
    upper_list.set(0, int32_t{1});
    upper_list.set(1, int32_t{3});

    CHECK(std::is_lt(lower.view() <=> upper.view()));
    CHECK(std::is_gt(upper.view() <=> lower.view()));
    CHECK_FALSE(lower.view().eq(upper.view()));
}

TEST_CASE("Builder lookup is singleton per list schema", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().list(value::scalar_type_meta<int32_t>()).build();

    const auto &first  = ValueBuilderFactory::checked_builder_for(schema);
    const auto &second = ValueBuilderFactory::checked_builder_for(schema);

    CHECK(&first == &second);
}

}  // namespace
