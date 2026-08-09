// Scalar value-type coverage for the testing toolkit: the toolkit is generic over
// the value type (it plumbs Value/Views), so it should round-trip every scalar
// type end-to-end. Also pins the type-erased to_string rendering (1-byte integers
// show numerically, not as characters).

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    template <typename T>
    struct Identity
    {
        static constexpr auto name = "identity";
        static void           eval(In<"in", TS<T>> in, Out<TS<T>> out) { out.set(in.value()); }
    };

}  // namespace

TEMPLATE_TEST_CASE("eval_node round-trips every scalar value type", "[value_types]", bool, std::int8_t, std::int16_t,
                   std::int32_t, std::int64_t, std::uint8_t, std::uint16_t, std::uint32_t, std::uint64_t, float, double)
{
    using T = TestType;
    // Arithmetic values that are representable in every type above (incl. bool).
    CHECK_OUTPUT(testing::eval_node<Identity<T>>({T(1), none, T(0)}), {T(1), none, T(0)});
}

TEST_CASE("eval_node round-trips string values")
{
    using namespace std::string_literals;
    CHECK_OUTPUT(testing::eval_node<Identity<std::string>>({"a"s, none, "cee"s}), {"a"s, none, "cee"s});
}

TEST_CASE("eval_node round-trips floating-point values exactly when representable")
{
    CHECK_OUTPUT(testing::eval_node<Identity<double>>({1.5, none, -2.25}), {1.5, none, -2.25});
    CHECK_OUTPUT(testing::eval_node<Identity<float>>({0.5f, none, 4.0f}), {0.5f, none, 4.0f});
}

TEST_CASE("value to_string renders 1-byte integers numerically, not as characters")
{
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int8_t>("int8");
    (void)registry.register_scalar<std::uint8_t>("uint8");

    CHECK(Value{std::int8_t(65)}.to_string() == "65");    // not "A"
    CHECK(Value{std::int8_t(-3)}.to_string() == "-3");
    CHECK(Value{std::uint8_t(200)}.to_string() == "200");
}

TEST_CASE("stdlib::const_ produces the configured value for non-int scalar types")
{
    using namespace std::string_literals;
    hgraph::stdlib::register_standard_operators();   // const_ is an operator

    CHECK_OUTPUT(testing::eval_node<stdlib::const_>(3.5), {Value{Float{3.5}}});
    CHECK_OUTPUT(testing::eval_node<stdlib::const_>(std::string{"hi"}), {Value{Str{"hi"}}});
}
