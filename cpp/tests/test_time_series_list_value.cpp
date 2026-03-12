#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/state.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

#include <compare>
#include <cstdint>
#include <string>

namespace {

using hgraph::Value;
using hgraph::ValueBuilderFactory;
namespace value = hgraph::value;

TEST_CASE("Fixed lists default construct inline atomic elements", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().fixed_list(value::scalar_type_meta<int32_t>(), 3).build();

    Value value{*schema};
    auto  list = value.list_view();

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
    auto  list = value.list_view();

    CHECK_FALSE(list.is_fixed());
    CHECK(list.empty());

    {
        auto mutation = list.begin_mutation();
        mutation.push_back(int32_t{4});
        mutation.push_back(int32_t{7});
    }

    REQUIRE(list.size() == 2);
    CHECK(list.front().as_atomic().as<int32_t>() == 4);
    CHECK(list.back().as_atomic().as<int32_t>() == 7);

    list.begin_mutation().resize(4);
    REQUIRE(list.size() == 4);
    CHECK(list[2].as_atomic().as<int32_t>() == 0);
    CHECK(list[3].as_atomic().as<int32_t>() == 0);

    list.begin_mutation().clear();
    CHECK(list.empty());
}

TEST_CASE("Lists preserve invalid element slots", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().fixed_list(value::scalar_type_meta<int32_t>(), 3).build();

    Value value{*schema};
    auto  list = value.view().as_list();

    list.begin_mutation().set(1, hgraph::View::invalid_for(value::scalar_type_meta<int32_t>()));

    CHECK(list[0].valid());
    CHECK_FALSE(list[1].valid());
    CHECK(list[1].schema() == value::scalar_type_meta<int32_t>());
    CHECK(list[2].valid());
    CHECK(value.view().to_string() == "[0, None, 0]");
}

TEST_CASE("Fixed lists clear by invalidating each slot", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().fixed_list(value::scalar_type_meta<int32_t>(), 3).build();

    Value value{*schema};
    auto  list = value.view().as_list();

    {
        auto mutation = list.begin_mutation();
        mutation.set(0, int32_t{1});
        mutation.set(1, int32_t{2});
        mutation.set(2, int32_t{3});
    }

    list.begin_mutation().clear();

    REQUIRE(list.size() == 3);
    CHECK_FALSE(list[0].valid());
    CHECK_FALSE(list[1].valid());
    CHECK_FALSE(list[2].valid());
    CHECK(value.view().to_string() == "[None, None, None]");
}

TEST_CASE("Fixed lists clear releases non-trivial payloads and keeps slots writable", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().fixed_list(value::scalar_type_meta<std::string>(), 2).build();

    Value value{*schema};
    auto  list = value.view().as_list();

    {
        auto mutation = list.begin_mutation();
        mutation.set(0, std::string{"alpha"});
        mutation.set(1, std::string{"beta"});
    }

    list.begin_mutation().clear();

    REQUIRE(list.size() == 2);
    CHECK_FALSE(list[0].valid());
    CHECK_FALSE(list[1].valid());

    {
        auto mutation = list.begin_mutation();
        mutation.set(0, std::string{"gamma"});
        mutation.set(1, std::string{"delta"});
    }

    CHECK(list[0].as_atomic().as<std::string>() == "gamma");
    CHECK(list[1].as_atomic().as<std::string>() == "delta");
    CHECK(value.view().to_string() == "[gamma, delta]");
}

TEST_CASE("List comparison is lexicographic within a schema", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().fixed_list(value::scalar_type_meta<int32_t>(), 2).build();

    Value lower{*schema};
    Value upper{*schema};

    auto lower_list = lower.view().as_list();
    auto upper_list = upper.view().as_list();

    {
        auto mutation = lower_list.begin_mutation();
        mutation.set(0, int32_t{1});
        mutation.set(1, int32_t{2});
    }
    {
        auto mutation = upper_list.begin_mutation();
        mutation.set(0, int32_t{1});
        mutation.set(1, int32_t{3});
    }

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

TEST_CASE("List mutation views support fluent command-style chaining", "[time_series][value][list]")
{
    const value::TypeMeta *schema = value::TypeRegistry::instance().list(value::scalar_type_meta<int32_t>()).build();

    Value value{*schema};
    auto  list = value.list_view();

    list.begin_mutation().pushing_back(int32_t{1}).pushing_back(int32_t{2}).setting(0, int32_t{3});

    REQUIRE(list.size() == 2);
    CHECK(list[0].as_atomic().as<int32_t>() == 3);
    CHECK(list[1].as_atomic().as<int32_t>() == 2);
}

}  // namespace
