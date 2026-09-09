#include <inline-native-consumer.h>
#include <native-functions.h>

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;

TEST_CASE("generated inline C++ native functions use values and live collection views", "[codegen][runtime][native]") {
    CHECK_OUTPUT(eval_node<examples::native_functions::incremented>(values<Float>(1.0, 2.5)), values<Float>(2.0, 3.5));
    CHECK_OUTPUT(eval_node<examples::native_functions::list_size>(values<Value>(list_delta<TS<Int>>({1, 2}))), values<Int>(2));
    CHECK_OUTPUT((eval_node<examples::native_functions::set_size>(
                     values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({}, {1})))),
                 values<Int>(2, 1));
    CHECK_OUTPUT((eval_node<examples::native_functions::map_size>(
                     values<Value>(dict_delta<Int, TS<Float>>({{1, 1.0}, {2, 2.0}}), dict_delta<Int, TS<Float>>({}, {1})))),
                 values<Int>(2, 1));
}

TEST_CASE("a generated inline native overload is importable through its module descriptor", "[codegen][runtime][native]") {
    CHECK_OUTPUT((eval_node<checks::inline_native_consumer::set_size>(
                     values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({}, {1})))),
                 values<Int>(2, 1));
}
