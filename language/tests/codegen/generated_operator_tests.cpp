#include <operator-properties.h>
#include <operators.h>
#include <system-operators.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;

TEST_CASE("generated symbol and node arithmetic share native semantics", "[codegen][generated][operators]") {
    hgl::wiring::ensure_session();
    checks::system_operators::register_operators();
    namespace ops = checks::system_operators;
    CHECK_OUTPUT(eval_node<ops::plus>(values<Int>(7, -7), values<Int>(3, 3)), values<Int>(10, -4));
    CHECK_OUTPUT(eval_node<ops::ratio>(values<Int>(7, -7), values<Int>(2, 2)), values<Float>(3.5, -3.5));
    CHECK_OUTPUT(eval_node<ops::ratio_node>(values<Int>(7, -7), values<Int>(2, 2)), values<Float>(3.5, -3.5));
    CHECK_OUTPUT(eval_node<ops::remainder>(values<Int>(7, -7), values<Int>(-3, 3)), values<Int>(-2, 2));
    CHECK_OUTPUT(eval_node<ops::remainder_node>(values<Int>(7, -7), values<Int>(-3, 3)), values<Int>(-2, 2));
    CHECK_OUTPUT(eval_node<ops::remainder_float>(values<Float>(7.5, -7.5), values<Float>(-2.0, 2.0)), values<Float>(-0.5, 0.5));
    CHECK_OUTPUT(eval_node<ops::remainder_float_node>(values<Float>(7.5, -7.5), values<Float>(-2.0, 2.0)),
                 values<Float>(-0.5, 0.5));
}

TEST_CASE("generated domain contracts register working implementations", "[codegen][generated][operators]") {
    hgl::wiring::ensure_session();
    examples::operator_properties::register_operators();
    namespace properties = examples::operator_properties;
    CHECK_OUTPUT(eval_node<properties::concatenate_text>(values<Str>("a", "b"), values<Str>("c", "d")), values<Str>("ac", "bd"));
    CHECK_OUTPUT(eval_node<properties::multiply_ints>(values<Int>(2, 3), values<Int>(4, 5)), values<Int>(8, 15));
    CHECK_OUTPUT(eval_node<properties::divide_ints>(values<Int>(7, -3), values<Int>(2, 2)), values<Float>(3.5, -1.5));
}

TEST_CASE("HGL standard operator candidates delegate to the native library", "[codegen][generated][operators]") {
    hgl::wiring::ensure_session();
    hgraph_::operators_::register_operators();
    namespace ops = hgraph_::operators_::operators;
    CHECK_OUTPUT(eval_node<ops::add_>(values<Int>(7, -7), values<Int>(3, 3)), values<Int>(10, -4));
    CHECK_OUTPUT(eval_node<ops::div_>(values<Int>(7, -7), values<Int>(2, 2)), values<Float>(3.5, -3.5));
    CHECK_OUTPUT(eval_node<ops::floordiv_>(values<Int>(7, -7), values<Int>(3, 3)), values<Int>(2, -3));
    CHECK_OUTPUT(eval_node<ops::mod_>(values<Int>(7, -7), values<Int>(-3, 3)), values<Int>(-2, 2));
    CHECK_OUTPUT(eval_node<ops::sub_>(values<Int>(7), values<Int>(3)), values<Int>(4));
    CHECK_OUTPUT(eval_node<ops::mul_>(values<Int>(7), values<Int>(3)), values<Int>(21));
    CHECK_OUTPUT(eval_node<ops::eq_>(values<Int>(7, 3), values<Int>(3, 3)), values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<ops::ne_>(values<Int>(7, 3), values<Int>(3, 3)), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<ops::lt_>(values<Int>(7, 2), values<Int>(3, 3)), values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<ops::le_>(values<Int>(7, 3), values<Int>(3, 3)), values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<ops::gt_>(values<Int>(7, 2), values<Int>(3, 3)), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<ops::ge_>(values<Int>(7, 3), values<Int>(3, 3)), values<Bool>(true, true));
    CHECK_OUTPUT(eval_node<ops::and_>(values<Bool>(true, false), values<Bool>(true, true)), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<ops::or_>(values<Bool>(true, false), values<Bool>(false, false)), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<ops::neg_>(values<Int>(7, -7)), values<Int>(-7, 7));
    CHECK_OUTPUT(eval_node<ops::not_>(values<Bool>(true, false)), values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<ops::add_>(values<Str>("a"), values<Str>("b")), values<Str>("ab"));
    CHECK_OUTPUT(eval_node<ops::mul_>(values<Int>(3), values<Float>(2.5)), values<Float>(7.5));
}
