#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/value/state.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/value/type_registry.h>

#include <compare>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace {

using hgraph::Value;
using hgraph::ValueBuilderFactory;
using hgraph::value_for;
namespace value = hgraph::value;

TEST_CASE("Atomic values default construct through the bound schema", "[time_series][value][atomic]")
{
    const value::TypeMeta &schema = *value::scalar_type_meta<int32_t>();
    Value                  value{schema};

    REQUIRE(value.valid());
    REQUIRE(value.schema() == &schema);

    auto view = value.view().as_atomic();
    REQUIRE(view.valid());
    CHECK(view.as<int32_t>() == 0);
    CHECK(value.view().to_string() == "0");
}

TEST_CASE("Atomic values support inline and heap-backed payloads", "[time_series][value][atomic]")
{
    auto integer_value = value_for(int32_t{42});
    auto string_value  = value_for(std::string{"alpha"});

    CHECK(integer_value.view().as_atomic().as<int32_t>() == 42);
    CHECK(string_value.view().as_atomic().as<std::string>() == "alpha");

    string_value.view().as_atomic().set(std::string{"beta"});
    CHECK(string_value.view().as_atomic().as<std::string>() == "beta");
}

TEST_CASE("Atomic views support typed and view-based assignment", "[time_series][value][atomic]")
{
    Value lhs{*value::scalar_type_meta<int32_t>()};
    Value rhs = value_for(int32_t{99});

    auto lhs_view = lhs.view().as_atomic();
    auto rhs_view = rhs.view().as_atomic();

    lhs_view = int32_t{7};
    CHECK(lhs_view.as<int32_t>() == 7);

    lhs_view = rhs_view;
    CHECK(lhs_view.as<int32_t>() == 99);
}

TEST_CASE("Generic view set forwards into the atomic dispatch path", "[time_series][value][atomic]")
{
    Value value{*value::scalar_type_meta<int32_t>()};

    value.view().set(int32_t{55});
    CHECK(value.view().as_atomic().as<int32_t>() == 55);

    Value explicit_schema_value = value_for(*value::scalar_type_meta<int32_t>(), int32_t{11});
    CHECK(explicit_schema_value.view().as_atomic().as<int32_t>() == 11);
}

TEST_CASE("Atomic value copy and move preserve payload semantics", "[time_series][value][atomic]")
{
    Value source = value_for(int32_t{42});
    Value copy{source};

    CHECK(copy.view().as_atomic().as<int32_t>() == 42);

    Value moved{std::move(source)};
    CHECK(moved.view().as_atomic().as<int32_t>() == 42);

    CHECK_FALSE(source.valid());
    CHECK(source.view().schema() == value::scalar_type_meta<int32_t>());
    CHECK_FALSE(source.view().valid());
    CHECK(source.view().eq(source.view()));
    CHECK((source.view() <=> source.view()) == std::partial_ordering::equivalent);
}

TEST_CASE("Atomic value assignment requires matching builder identity", "[time_series][value][atomic]")
{
    Value integers{*value::scalar_type_meta<int32_t>()};
    Value doubles{*value::scalar_type_meta<double>()};

    CHECK_THROWS_AS(integers = doubles, std::invalid_argument);
    CHECK_THROWS_AS(integers = std::move(doubles), std::invalid_argument);
}

TEST_CASE("Builder lookup is singleton per atomic schema", "[time_series][value][atomic]")
{
    const auto &first  = ValueBuilderFactory::checked_builder_for(value::scalar_type_meta<int32_t>());
    const auto &second = ValueBuilderFactory::checked_builder_for(value::scalar_type_meta<int32_t>());

    CHECK(&first == &second);
}

TEST_CASE("Atomic builders cache lifecycle traits for the resolved state", "[time_series][value][atomic]")
{
    const auto &integer_builder = ValueBuilderFactory::checked_builder_for(value::scalar_type_meta<int32_t>());
    const auto &string_builder  = ValueBuilderFactory::checked_builder_for(value::scalar_type_meta<std::string>());

    // Under the current erased-state design, even trivially destructible atomic
    // payloads sit inside a full state object with virtual dispatch.
    CHECK(integer_builder.requires_destroy());
    CHECK(string_builder.requires_destroy());

    // The current `Value` handle model still stores all supported atomic
    // states in separately allocated storage, even when the payload itself is
    // inline within the atomic state.
    CHECK(integer_builder.requires_deallocate());
    CHECK(string_builder.requires_deallocate());
}

TEST_CASE("Atomic view type checks remain persistent", "[time_series][value][atomic]")
{
    Value value = value_for(int32_t{5});
    auto  view  = value.view().as_atomic();

    CHECK(view.try_as<int32_t>() != nullptr);
    CHECK(view.try_as<double>() == nullptr);
    CHECK_THROWS_AS((void)view.checked_as<double>(), std::runtime_error);
}

TEST_CASE("Atomic view comparison follows same-schema ordering rules", "[time_series][value][atomic]")
{
    Value lower = value_for(int32_t{1});
    Value upper = value_for(int32_t{2});

    CHECK(std::is_lt(lower.view() <=> upper.view()));
    CHECK(std::is_gt(upper.view() <=> lower.view()));
    CHECK_FALSE(lower.view().eq(upper.view()));
}

}  // namespace
