#include <core-native-library.h>
#include <native.h>

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace native_consumer = examples::core_native_library;

TEST_CASE("the HGL core native library handles scalar and collection values", "[codegen][runtime][native][stdlib]") {
    CHECK_OUTPUT(eval_node<native_consumer::text_length>(values<Str>("hgl", "")), values<Int>(3, 0));
    CHECK_OUTPUT(eval_node<native_consumer::text_is_empty>(values<Str>("hgl", "")), values<Bool>(false, true));

    CHECK_OUTPUT(eval_node<native_consumer::fixed_list_length>(values<Value>(list_delta<TS<Int>>({1, 2}))), values<Int>(2));
    CHECK_OUTPUT(eval_node<native_consumer::fixed_list_is_empty>(values<Value>(list_delta<TS<Int>>({1, 2}))),
                 values<Bool>(false));
    CHECK_OUTPUT((eval_node<native_consumer::dynamic_list_length>(
                     values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}}), dynamic_list_delta<TS<Int>>({{1, 2}, {2, 3}}),
                                                               dynamic_list_delta<TS<Int>>({}, {2})))),
                 values<Int>(1, 3, 2));
    CHECK_OUTPUT((eval_node<native_consumer::dynamic_list_is_empty>(
                     values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}}), dynamic_list_delta<TS<Int>>({}, {0})))),
                 values<Bool>(false, true));
    CHECK_OUTPUT((eval_node<native_consumer::set_length>(
                     values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({}, {1})))),
                 values<Int>(2, 1));
    CHECK_OUTPUT((eval_node<native_consumer::map_length>(
                     values<Value>(dict_delta<Int, TS<Float>>({{1, 1.0}, {2, 2.0}}),
                                                           dict_delta<Int, TS<Float>>({}, {1})))),
                 values<Int>(2, 1));
    CHECK_OUTPUT((eval_node<native_consumer::map_is_empty>(
                     values<Value>(dict_delta<Int, TS<Float>>({{1, 1.0}}), dict_delta<Int, TS<Float>>({}, {1})))),
                 values<Bool>(false, true));
    CHECK_OUTPUT((eval_node<native_consumer::set_is_empty>(
                     values<Value>(set_delta<Int>({1}, {}), set_delta<Int>({}, {1})))),
                 values<Bool>(false, true));
}

TEST_CASE("the HGL core native library reads the live rolling-window view", "[codegen][runtime][native][stdlib]") {
    CHECK_OUTPUT(eval_node<native_consumer::window_length>(values<Int>(1, 2, 3, 4)), values<Int>(1, 2, 3, 3));
    CHECK_OUTPUT(eval_node<native_consumer::window_is_empty>(values<Int>(1, 2)), values<Bool>(false, false));
}
