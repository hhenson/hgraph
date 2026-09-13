#include <value-functions.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace value_functions = tests::value_functions;

TEST_CASE("value functions lift through the public native wiring API", "[codegen][value-function]") {
    hgl::wiring::ensure_session();
    auto provider = value_functions::register_operators();
    CHECK_OUTPUT(eval_node<value_functions::operators::automatic>(values<Float>(1.0, none, 3.0)), values<Float>(2.0, none, 6.0));
    CHECK_OUTPUT(eval_node<value_functions::operators::preferred>(values<Float>(1.0, none, 3.0)), values<Float>(13.0, none, 19.0));
    CHECK_OUTPUT(eval_node<value_functions::operators::explicit_value>(values<Float>(1.0, none, 3.0)),
                 values<Float>(3.0, none, 9.0));
    CHECK_OUTPUT(eval_node<value_functions::operators::lifecycle>(values<Float>(1.0, none, 3.0), Float{3.0}),
                 values<Float>(7.0, none, 9.0));
    CHECK_OUTPUT(eval_node<value_functions::operators::with_sink>(values<Float>(1.0, none, 3.0)),
                 values<Float>(1.0, none, 3.0));
}
