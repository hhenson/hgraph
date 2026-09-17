#include <inline-native-consumer.h>
#include <native-functions.h>

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/node_error.h>
#include <hgraph/types/subgraph_wiring.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

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

TEST_CASE("a throws native ends the evaluation with the native exception", "[codegen][runtime][native]") {
    CHECK_OUTPUT(eval_node<examples::native_functions::reciprocal>(values<Float>(2.0, 4.0)), values<Float>(0.5, 0.25));
    CHECK_THROWS_WITH(eval_node<examples::native_functions::reciprocal>(values<Float>(2.0, 0.0)),
                      Catch::Matchers::ContainsSubstring("checked_reciprocal: division by zero"));
}

TEST_CASE("a generated inline native overload is importable through its module descriptor", "[codegen][runtime][native]") {
    CHECK_OUTPUT((eval_node<checks::inline_native_consumer::set_size>(
                     values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({}, {1})))),
                 values<Int>(2, 1));
}

namespace
{
    struct NativeErrorMessage
    {
        static void eval(In<"error", TS<NodeError>> error, Out<TS<Str>> out) {
            out.set(error.base().value().as_bundle().at("error_msg").checked_as<Str>());
        }
    };

    struct CapturedReciprocalErrors
    {
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Float>> value) {
            auto result = wire<examples::native_functions::counted_reciprocal>(w, value);
            return wire<NativeErrorMessage>(w, exception_time_series(result));
        }
    };

    struct CapturedReciprocalValues
    {
        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> value) {
            auto result = wire<examples::native_functions::counted_reciprocal>(w, value);
            (void)exception_time_series(result);
            return result;
        }
    };
}

TEST_CASE("captured native errors preserve earlier writes and later evaluations", "[codegen][runtime][native]") {
    const auto input = values<Float>(2.0, 0.0, 4.0);
    CHECK_OUTPUT(eval_node<CapturedReciprocalErrors>(input),
                 values<Str>(none, "checked_reciprocal: division by zero", none));
    CHECK_OUTPUT(eval_node<CapturedReciprocalValues>(input), values<Float>(0.5, 0.0, 0.75));
}
