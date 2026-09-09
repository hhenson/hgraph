#include <core-native-library.h>
#include <native.h>

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace native_consumer = examples::core_native_library;

namespace
{
    template <typename S> struct EndpointValidGraph
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<S> value) { return wire<native_consumer::endpoint_valid>(w, value); }
    };

    template <typename S> struct EndpointAllValidGraph
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<S> value) { return wire<native_consumer::endpoint_all_valid>(w, value); }
    };

    template <typename S> struct EndpointModifiedGraph
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<S> value) { return wire<native_consumer::endpoint_modified>(w, value); }
    };

    template <typename S> struct EndpointLastModifiedGraph
    {
        static Port<TS<DateTime>> compose(Wiring &w, Port<S> value) {
            return wire<native_consumer::endpoint_last_modified>(w, value);
        }
    };

    template <typename S> struct EndpointsEqualGraph
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<S> left, Port<S> right) {
            return wire<native_consumer::endpoints_equal>(w, left, right);
        }
    };

    struct ReferencePublisher
    {
        static constexpr auto name = "reference_publisher";

        static void eval(In<"value", TS<Int>> value, Out<REF<TS<Int>>> out) { out.set(value.reference()); }
    };

    struct ReferenceEndpointValidGraph
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> value) {
            return wire<native_consumer::endpoint_valid>(w, wire<ReferencePublisher>(w, value));
        }
    };

    using ErasedOpsBundle = TSB<"ErasedOpsBundle", Field<"left", TS<Int>>, Field<"right", TS<Str>>>;
}  // namespace

TEST_CASE("the HGL core native library exposes erased endpoint metadata", "[codegen][runtime][native][stdlib][signal]") {
    CHECK_OUTPUT(eval_node<EndpointValidGraph<TS<Int>>>(values<Int>(1, 2)), values<Bool>(true, true));
    CHECK_OUTPUT(eval_node<native_consumer::float_endpoint_valid>(values<Float>(1.0, 2.0)), values<Bool>(true, true));
    CHECK_OUTPUT(eval_node<EndpointModifiedGraph<TS<Str>>>(values<Str>("one", "two")), values<Bool>(true, true));
    CHECK_OUTPUT(eval_node<EndpointLastModifiedGraph<TS<Float>>>(values<Float>(1.0, none, 2.0)),
                 values<DateTime>(MIN_ST, none, MIN_ST + 2 * MIN_TD));

    CHECK_OUTPUT(eval_node<EndpointAllValidGraph<ErasedOpsBundle>>(values<Value>(
                     tsb_delta<ErasedOpsBundle>(Int{1}, std::nullopt), tsb_delta<ErasedOpsBundle>(std::nullopt, Str{"ready"}))),
                 values<Bool>(false, true));
    CHECK_OUTPUT((eval_node<EndpointAllValidGraph<TSL<TS<Int>, 2>>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}}), list_delta<TS<Int>>({{1, 2}})))),
                 values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<EndpointValidGraph<TSL<TS<Int>>>>(values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}}))),
                 values<Bool>(true));
    CHECK_OUTPUT(eval_node<EndpointValidGraph<TSS<Int>>>(values<Value>(set_delta<Int>({1}, {}))), values<Bool>(true));
    CHECK_OUTPUT((eval_node<EndpointValidGraph<TSD<Int, TS<Float>>>>(values<Value>(dict_delta<Int, TS<Float>>({{1, 1.0}})))),
                 values<Bool>(true));
    CHECK_OUTPUT((eval_node<EndpointValidGraph<TSW<Int, 3, 1>>>(values<Int>(1, 2))), values<Bool>(true, true));
    CHECK_OUTPUT((eval_node<EndpointValidGraph<TSWDuration<Int, 10, 1>>>(values<Int>(1, 2))), values<Bool>(true, true));
    CHECK_OUTPUT(eval_node<ReferenceEndpointValidGraph>(values<Int>(1, 2)), values<Bool>(true, true));
    CHECK_OUTPUT(eval_node<EndpointValidGraph<SIGNAL>>(values<Bool>(true, true)), values<Bool>(true, true));
}

TEST_CASE("the HGL core native library compares erased current values", "[codegen][runtime][native][stdlib][signal]") {
    CHECK_OUTPUT(eval_node<EndpointsEqualGraph<TS<Int>>>(values<Int>(1, 2, 3), values<Int>(1, 4, 3)),
                 values<Bool>(true, false, true));
}

TEST_CASE("the HGL core native library handles scalar and collection values", "[codegen][runtime][native][stdlib]") {
    CHECK_OUTPUT(eval_node<native_consumer::text_length>(values<Str>("hgl", "")), values<Int>(3, 0));
    CHECK_OUTPUT(eval_node<native_consumer::text_is_empty>(values<Str>("hgl", "")), values<Bool>(false, true));

    CHECK_OUTPUT(eval_node<native_consumer::fixed_list_length>(values<Value>(list_delta<TS<Int>>({1, 2}))), values<Int>(2));
    CHECK_OUTPUT(eval_node<native_consumer::fixed_list_is_empty>(values<Value>(list_delta<TS<Int>>({1, 2}))), values<Bool>(false));
    CHECK_OUTPUT((eval_node<native_consumer::dynamic_list_length>(values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}}),
                                                                                dynamic_list_delta<TS<Int>>({{1, 2}, {2, 3}}),
                                                                                dynamic_list_delta<TS<Int>>({}, {2})))),
                 values<Int>(1, 3, 2));
    CHECK_OUTPUT((eval_node<native_consumer::dynamic_list_is_empty>(
                     values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}}), dynamic_list_delta<TS<Int>>({}, {0})))),
                 values<Bool>(false, true));
    CHECK_OUTPUT((eval_node<native_consumer::set_length>(values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({}, {1})))),
                 values<Int>(2, 1));
    CHECK_OUTPUT((eval_node<native_consumer::map_length>(
                     values<Value>(dict_delta<Int, TS<Float>>({{1, 1.0}, {2, 2.0}}), dict_delta<Int, TS<Float>>({}, {1})))),
                 values<Int>(2, 1));
    CHECK_OUTPUT((eval_node<native_consumer::map_is_empty>(
                     values<Value>(dict_delta<Int, TS<Float>>({{1, 1.0}}), dict_delta<Int, TS<Float>>({}, {1})))),
                 values<Bool>(false, true));
    CHECK_OUTPUT((eval_node<native_consumer::set_is_empty>(values<Value>(set_delta<Int>({1}, {}), set_delta<Int>({}, {1})))),
                 values<Bool>(false, true));
}

TEST_CASE("the HGL core native library reads the live rolling-window view", "[codegen][runtime][native][stdlib]") {
    CHECK_OUTPUT(eval_node<native_consumer::window_length>(values<Int>(1, 2, 3, 4)), values<Int>(1, 2, 3, 3));
    CHECK_OUTPUT(eval_node<native_consumer::window_is_empty>(values<Int>(1, 2)), values<Bool>(false, false));
}
