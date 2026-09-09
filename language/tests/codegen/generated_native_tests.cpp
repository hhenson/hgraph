#include <native-scalar-import.h>

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;

TEST_CASE("generated runtime nodes call exact native scalar functions", "[codegen][runtime][native]") {
    CHECK_OUTPUT(eval_node<checks::native_consumer::smooth>(values<Float>(1.0, 2.5)), values<Float>(4.0, 5.5));
}

TEST_CASE("generated runtime nodes select native collection-view overloads", "[codegen][runtime][native]") {
    CHECK_OUTPUT(eval_node<checks::native_consumer::list_size>(values<Value>(list_delta<TS<Int>>({1, 2}))), values<Int>(2));
    CHECK_OUTPUT((eval_node<checks::native_consumer::set_size>(values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({}, {1})))),
                 values<Int>(2, 1));
    CHECK_OUTPUT((eval_node<checks::native_consumer::map_size>(
                     values<Value>(dict_delta<Int, TS<Float>>({{1, 1.0}, {2, 2.0}}), dict_delta<Int, TS<Float>>({}, {1})))),
                 values<Int>(2, 1));
}
