#include <native-scalar-import.h>

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;

TEST_CASE("generated runtime nodes call exact native scalar functions", "[codegen][runtime][native]") {
    CHECK_OUTPUT(eval_node<checks::native_consumer::smooth>(values<Float>(1.0, 2.5)), values<Float>(4.0, 5.5));
}
