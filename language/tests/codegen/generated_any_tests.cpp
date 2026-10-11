#include "wiring/backend.h"
#include <any-values.h>
#include <catch2/catch_test_macros.hpp>
#include <hgl/execution_error.h>
#include <hgl/ordinary_values.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

using namespace hgraph;
using namespace hgraph::testing;
namespace any_values = checks::any_values;

TEST_CASE("generated any boxes preserve leaf publications and capability errors", "[codegen][any]") {
    hgl::wiring::ensure_session();
    any_values::register_operators();
    const hgl::ordinary::PreparedValuePlan boxes{scalar_descriptor<hgl::ordinary::Any>::value_meta()};
    const Value                            zero{Int{0}}, one{Int{1}}, falsity{Bool{false}};
    const auto empty = boxes.empty_box(), integer = boxes.box(zero.view()), boolean = boxes.box(falsity.view());
    CHECK(any_values::hgl_values::defaults_valid_hgl_value());
    CHECK_OUTPUT(
        (eval_node<any_values::operators::forward, TS<hgl::ordinary::Any>>(values<Value>(empty, integer, boolean, none, boolean))),
        values<Value>(empty, integer, boolean, none, boolean));
    CHECK_OUTPUT(eval_node<any_values::operators::box_input>(values<Int>(0, none, 0, 1)),
                 values<Value>(integer, none, integer, boxes.box(one.view())));
    try {
        (void)eval_node<any_values::missing_order>(values<Int>(0));
        FAIL("missing box capability unexpectedly succeeded");
    } catch (const std::exception &error) { CHECK(hgl::execution_error_code(error) == "value.capability"); }
}
