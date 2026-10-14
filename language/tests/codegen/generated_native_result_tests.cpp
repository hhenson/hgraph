#include "native-result/provider.h"
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <catch2/catch_test_macros.hpp>
using namespace hgraph;
using namespace hgraph::testing;
TEST_CASE("native result Err stays data until checked propagation", "[native][result]") {
    const auto provider = checks::native_result::register_operators();
    REQUIRE(provider.active());
    const auto native = checks::native_result_provider::native;
    const auto error = native.checked(-1);
    CHECK_FALSE(error.is_ok());
    CHECK(error.error().code == "value.missing_key");
    CHECK_FALSE(error.error().context);
    CHECK(native.checked(7).value() == 7);
    CHECK_OUTPUT(eval_node<checks::native_result::tags>(values<Int>(-1, -1, 7)), values<Bool>(true, true, false));
    CHECK_OUTPUT(eval_node<checks::native_result::captured>(values<Int>(7, -1, -1, 8)),
                 values<Str>(none, "native missing price", "native missing price", none));
}

TEST_CASE("result generator publishes complete selected outcomes", "[result][generator]") {
    const auto provider = checks::native_result::register_operators();
    REQUIRE(provider.active());
    CHECK_OUTPUT(eval_node<checks::native_result::generated_tags>(), values<Bool>(false, true));
}


TEST_CASE("canonical error constructors retain nullable codes and evaluate arguments in written order", "[error][constructor]") {
    const auto provider = checks::native_result::register_operators();
    REQUIRE(provider.active());
    CHECK_OUTPUT(eval_node<checks::native_result::constructors>(values<Int>(0, none, 1)), values<Bool>(true, none, true));
    checks::native_result_provider::diagnostic_arguments.clear();
    CHECK_OUTPUT(eval_node<checks::native_result::diagnostic_order>(values<Int>(0, 1)),
                 values<CanonicalError>(CanonicalError{"text", "text", {}}, CanonicalError{"text", "text", {}}));
    CHECK(checks::native_result_provider::diagnostic_arguments == std::vector<Int>{1, 2, 2, 1});
}
