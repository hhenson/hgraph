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

    struct EndpointStatusGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Port<TS<Bool>> clock) {
            return wire<native_consumer::endpoint_status>(w, value, clock);
        }
    };

    // REF is intentional: the test changes source identity without copying maps.
    struct MapReferenceSwitch
    {
        static void eval(In<"left", TSD<Int, TS<Float>>> left, In<"right", TSD<Int, TS<Float>>> right,
                         In<"choose", TS<Bool>> choose, Out<REF<TSD<Int, TS<Float>>>> out) {
            if (choose.value()) {
                out.set(left.base().reference());
            } else {
                out.set(right.base().reference());
            }
        }
    };

    struct MapReferenceKeysGraph
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<TSD<Int, TS<Float>>> left, Port<TSD<Int, TS<Float>>> right,
                                      Port<TS<Bool>> choose) {
            return wire<native_consumer::map_keys_changed>(w, wire<MapReferenceSwitch>(w, left, right, choose));
        }
    };
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

TEST_CASE("native endpoint queries distinguish binding, validity and activity", "[codegen][runtime][native][stdlib][signal]") {
    CHECK_OUTPUT(eval_node<EndpointStatusGraph>(values<Int>(none, 7, none), values<Bool>(true, true, true)),
                 values<Int>(17, 29, 21));
}

TEST_CASE("runtime key_set modification means additions or removals", "[codegen][runtime][native][stdlib][key_set]") {
    const auto ticks = values<Value>(dict_delta<Int, TS<Float>>({}, {}), dict_delta<Int, TS<Float>>({{1, 1.0}}),
                                     dict_delta<Int, TS<Float>>({{1, 2.0}}), dict_delta<Int, TS<Float>>({{2, 3.0}}),
                                     dict_delta<Int, TS<Float>>({}, {1}), dict_delta<Int, TS<Float>>({}, {2}));
    CHECK_OUTPUT(eval_node<native_consumer::map_keys_changed>(ticks), values<Bool>(false, true, false, true, true, true));
    CHECK_OUTPUT(eval_node<native_consumer::map_keys_only>(ticks), values<Bool>(none, true, none, true, true, true));
    CHECK_OUTPUT(eval_node<native_consumer::map_key_delta_count>(ticks), values<Int>(0, 1, 0, 1, 1, 1));
    const auto expected = values<Value>(set_delta<Int>({}, {}), set_delta<Int>({1}, {}), set_delta<Int>({}, {}),
                                        set_delta<Int>({2}, {}), set_delta<Int>({}, {1}), set_delta<Int>({}, {2}));
    CHECK_OUTPUT(eval_node<native_consumer::map_key_values>(ticks), expected);
    CHECK_OUTPUT(eval_node<native_consumer::map_key_values_assigned>(ticks), expected);
}

TEST_CASE("structural collection reads align owned output contents", "[codegen][runtime][native][stdlib][access]") {
    const auto maps = values<Value>(dict_delta<Int, TSS<Int>>({{1, set_delta<Int>({10, 20}, {})}, {2, set_delta<Int>({30}, {})}}),
                                    none, dict_delta<Int, TSS<Int>>({{2, set_delta<Int>({40}, {30})}}));
    CHECK_OUTPUT(eval_node<native_consumer::map_child_set>(maps, values<Int>(1, 2, 2)),
                 values<Value>(set_delta<Int>({10, 20}, {}), set_delta<Int>({30}, {10, 20}), set_delta<Int>({40}, {30})));
    CHECK_OUTPUT(eval_node<native_consumer::list_child_set>(
                     values<Value>(list_delta<TSS<Int>>({{0, set_delta<Int>({10}, {})}, {1, set_delta<Int>({20}, {})}}), none),
                     values<Int>(0, 1)),
                 values<Value>(set_delta<Int>({10}, {}), set_delta<Int>({20}, {10})));
    CHECK_OUTPUT(
        eval_node<native_consumer::map_child_map>(
            values<Value>(dict_delta<Int, TSD<Int, TS<Float>>>({{1, dict_delta<Int, TS<Float>>({{2, 3.0}})}})), values<Int>(1)),
        values<Value>(dict_delta<Int, TS<Float>>({{2, 3.0}})));
    CHECK_OUTPUT(eval_node<native_consumer::map_child_list>(
                     values<Value>(dict_delta<Int, TSL<TS<Int>, 2>>({{1, list_delta<TS<Int>>({4, 5})}})), values<Int>(1)),
                 values<Value>(list_delta<TS<Int>>({4, 5})));
    using Record = native_consumer::NativeRecord::time_series;
    CHECK_OUTPUT(eval_node<native_consumer::map_child_struct>(
                     values<Value>(dict_delta<Int, Record>({{1, tsb_delta<Record>(Int{4}, Str{"five"})}})), values<Int>(1)),
                 values<Value>(tsb_delta<Record>(Int{4}, Str{"five"})));
}

TEST_CASE("collection access preserves membership and strict bounds", "[codegen][runtime][native][stdlib][access]") {
    // Growth creates index zero without making its child valid.
    CHECK_THROWS(eval_node<native_consumer::list_at>(values<Value>(dynamic_list_delta<TS<Int>>({{1, 9}})), values<Int>(0)));
    const auto maps = values<Value>(dict_delta<Int, TS<Float>>({{1, 1.0}}), dict_delta<Int, TS<Float>>({}, {1}));
    CHECK_OUTPUT(eval_node<native_consumer::map_contains>(maps, values<Int>(1, 1)), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<native_consumer::map_key_set_contains>(maps, values<Int>(1, 1)), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<native_consumer::set_contains>(values<Value>(set_delta<Int>({1}, {}), set_delta<Int>({}, {1})),
                                                          values<Int>(1, 1)),
                 values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<native_consumer::map_at>(values<Value>(dict_delta<Int, TS<Float>>({{1, 1.5}})), values<Int>(1)),
                 values<Float>(1.5));
    CHECK_OUTPUT(eval_node<native_consumer::list_at>(values<Value>(dynamic_list_delta<TS<Int>>({{0, 7}, {1, 9}})), values<Int>(1)),
                 values<Int>(9));
    CHECK_THROWS(eval_node<native_consumer::map_at>(values<Value>(dict_delta<Int, TS<Float>>({{1, 1.5}})), values<Int>(2)));
    CHECK_OUTPUT(eval_node<native_consumer::map_at_valid>(values<Value>(dict_delta<Int, TS<Float>>({{1, 1.5}})), values<Int>(1)),
                 values<Bool>(true));
    CHECK_OUTPUT(eval_node<native_consumer::map_invalid_child_valid>(values<Int>(1, 0), values<Int>(1, 1)),
                 values<Bool>(true, false));
    CHECK_THROWS(eval_node<native_consumer::map_at_valid>(values<Value>(dict_delta<Int, TS<Float>>({{1, 1.5}})), values<Int>(2)));
    CHECK_THROWS(
        eval_node<native_consumer::map_at_modified>(values<Value>(dict_delta<Int, TS<Float>>({{1, 1.5}})), values<Int>(2)));
    CHECK_THROWS(eval_node<native_consumer::list_at>(values<Value>(dynamic_list_delta<TS<Int>>({{0, 7}})), values<Int>(-1)));
    CHECK_THROWS(eval_node<native_consumer::list_at>(values<Value>(dynamic_list_delta<TS<Int>>({{0, 7}})), values<Int>(1)));
}

TEST_CASE("key_set compares memberships across reference rebinds", "[codegen][runtime][key_set]") {
    CHECK_OUTPUT(eval_node<MapReferenceKeysGraph>(
                     values<Value>(dict_delta<Int, TS<Float>>({{1, 1.0}}), none, none),
                     values<Value>(dict_delta<Int, TS<Float>>({{1, 2.0}}), none, dict_delta<Int, TS<Float>>({{2, 3.0}})),
                     values<Bool>(true, false, false)),
                 values<Bool>(true, false, true));
}

TEST_CASE("window access follows logical order through ring wraparound", "[codegen][runtime][native][stdlib][access]") {
    CHECK_OUTPUT(eval_node<native_consumer::window_at>(values<Int>(1, 2, 3, 4), values<Int>(0, 1, 2, 0)), values<Int>(1, 2, 3, 2));
    CHECK_OUTPUT(eval_node<native_consumer::window_front>(values<Int>(1, 2, 3, 4, 5)), values<Int>(1, 1, 1, 2, 3));
    CHECK_OUTPUT(eval_node<native_consumer::window_back>(values<Int>(1, 2, 3, 4, 5)), values<Int>(1, 2, 3, 4, 5));
    CHECK_OUTPUT(eval_node<native_consumer::window_time_at>(values<Int>(1, 2, 3, 4), values<Int>(0, 1, 2, 0)),
                 values<DateTime>(MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + MIN_TD));
    CHECK_OUTPUT(eval_node<native_consumer::window_removed_value>(values<Int>(1, 2, 3, 4, 5)), values<Int>(none, none, none, 1, 2));
    CHECK_THROWS(eval_node<native_consumer::window_at>(values<Int>(1), values<Int>(1)));
    CHECK_THROWS(eval_node<native_consumer::window_time_at>(values<Int>(1), values<Int>(-1)));
}

TEST_CASE("native string queries use byte sequences and accept empty needles", "[codegen][runtime][native][stdlib]") {
    CHECK_OUTPUT(eval_node<native_consumer::text_contains>(values<Str>("hgraph", "hgraph", "", "abc", "abc"),
                                                           values<Str>("graph", "Graph", "", "", "abcd")),
                 values<Bool>(true, false, true, true, false));
    CHECK_OUTPUT(eval_node<native_consumer::text_starts_with>(values<Str>("hgraph", "hgraph", "", "abc"),
                                                              values<Str>("hg", "graph", "", "abcd")),
                 values<Bool>(true, false, true, false));
    CHECK_OUTPUT(eval_node<native_consumer::text_ends_with>(values<Str>("hgraph", "hgraph", "", "abc"),
                                                            values<Str>("graph", "hg", "", "abcd")),
                 values<Bool>(true, false, true, false));
    // Embedded NUL is data, not a terminator; UTF-8 length counts bytes.
    CHECK_OUTPUT(eval_node<native_consumer::text_contains>(values<Str>(Str{"a\0b", 3}), values<Str>(Str{"\0b", 2})),
                 values<Bool>(true));
    CHECK_OUTPUT(eval_node<native_consumer::text_length>(values<Str>(Str{"\xc3\xa9", 2})), values<Int>(2));
}

TEST_CASE("native window metadata follows growth, validity and eviction", "[codegen][runtime][native][stdlib]") {
    CHECK_OUTPUT(eval_node<native_consumer::window_capacity>(values<Int>(1, 2, 3, 4)), values<Int>(3, 3, 3, 3));
    CHECK_OUTPUT(eval_node<native_consumer::window_period>(values<Int>(1, 2, 3, 4)), values<Int>(3, 3, 3, 3));
    CHECK_OUTPUT(eval_node<native_consumer::window_min_period>(values<Int>(1, 2, 3, 4)), values<Int>(2, 2, 2, 2));
    // The current C++ window is valid after its first sample; all_valid is the
    // minimum-sample readiness check. Native metadata must preserve both.
    CHECK_OUTPUT((eval_node<EndpointAllValidGraph<TSW<Int, 3, 2>>>(values<Int>(1, 2, 3, 4))),
                 values<Bool>(false, true, true, true));
    CHECK_OUTPUT(eval_node<native_consumer::window_is_full>(values<Int>(1, 2, 3, 4)), values<Bool>(false, false, true, true));
    CHECK_OUTPUT(eval_node<native_consumer::window_first_modified>(values<Int>(1, 2, 3, 4, 5)),
                 values<DateTime>(MIN_ST, MIN_ST, MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD));
    CHECK_OUTPUT(eval_node<native_consumer::window_has_removed_value>(values<Int>(1, 2, 3, 4, none, 5),
                                                                      values<Bool>(true, true, true, true, true, true)),
                 values<Bool>(false, false, false, true, false, true));
}
