// Tests for the CHECK_OUTPUT / REQUIRE_OUTPUT comparison utility and its delta
// message. The matching cases are exercised through the macro; the mismatch
// messages are checked directly (a failing macro would fail the test).

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;  // brings `none` into scope for expected lists

    struct AddOne
    {
        static constexpr auto name = "add_one";
        static void           eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
    };
}  // namespace

TEST_CASE("CHECK_OUTPUT passes when an eval_node result matches the expected sequence")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    CHECK_OUTPUT(testing::eval_node<AddOne>({1, none, 3}), {2, none, 4});
}

TEST_CASE("CHECK_OUTPUT also compares against an existing vector")
{
    const std::vector<std::optional<Int>> actual{1, std::nullopt, 2};
    CHECK_OUTPUT(actual, {1, none, 2});
    REQUIRE_OUTPUT(actual, {1, none, 2});
}

TEST_CASE("testing::values builds typed optional sequences")
{
    const auto ints = testing::values<Int>(1, none, 3);
    REQUIRE(ints.size() == 3);
    CHECK(ints[0] == std::optional<Int>{Int{1}});
    CHECK_FALSE(ints[1].has_value());
    CHECK(ints[2] == std::optional<Int>{Int{3}});
}

TEST_CASE("output delta message pinpoints a value difference")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");  // display renders via the value layer
    const std::vector<std::optional<Int>> actual{1, 2, std::nullopt, 3};
    const std::vector<std::optional<Int>> expected{1, 5, std::nullopt, 3};

    const std::string msg = testing::detail::output_delta_message(actual, expected);
    CHECK(msg.find("[1, 2, none, 3]") != std::string::npos);    // actual rendered with none
    CHECK(msg.find("[1, 5, none, 3]") != std::string::npos);    // expected rendered
    CHECK(msg.find("index 1") != std::string::npos);
    CHECK(msg.find("actual = 2") != std::string::npos);
    CHECK(msg.find("expected = 5") != std::string::npos);
}

TEST_CASE("output delta message reports a size difference")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");  // display renders via the value layer
    const std::vector<std::optional<Int>> actual{1, 2};
    const std::vector<std::optional<Int>> expected{1, 2, 3};

    const std::string msg = testing::detail::output_delta_message(actual, expected);
    CHECK(msg.find("sizes differ") != std::string::npos);
    CHECK(msg.find("index 2") != std::string::npos);
    CHECK(msg.find("(missing)") != std::string::npos);
}
