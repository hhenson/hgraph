#include <standard.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace standard = hgraph_::std_;

namespace
{
    void register_standard_hgl() {
        hgl::wiring::ensure_session();
        const auto provider = standard::register_operators();
        REQUIRE(provider.active());
    }
}  // namespace

TEST_CASE("the HGL standard library registers its first operator families", "[codegen][runtime][stdlib][hgl]") {
    register_standard_hgl();

    REQUIRE(hgl::wiring::has_operator("hgraph.std.len_"));
    REQUIRE(hgl::wiring::has_operator("hgraph.std.is_empty"));
}

TEST_CASE("HGL len_ and is_empty handle scalar and collection inputs", "[codegen][runtime][stdlib][hgl]") {
    register_standard_hgl();

    CHECK_OUTPUT(eval_node<standard::operators::len_>(values<Str>("hgl", "")), values<Int>(3, 0));
    CHECK_OUTPUT(eval_node<standard::operators::is_empty>(values<Str>("hgl", "")), values<Bool>(false, true));

    CHECK_OUTPUT((eval_node<standard::operators::len_, TSL<TS<Int>, 2>>(values<Value>(list_delta<TS<Int>>({1, 2})))),
                 values<Int>(2));
    CHECK_OUTPUT((eval_node<standard::operators::is_empty, TSL<TS<Int>, 2>>(values<Value>(list_delta<TS<Int>>({1, 2})))),
                 values<Bool>(false));

    CHECK_OUTPUT((eval_node<standard::operators::len_, TSL<TS<Int>>>(values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}}),
                                                                                   dynamic_list_delta<TS<Int>>({{1, 2}, {2, 3}}),
                                                                                   dynamic_list_delta<TS<Int>>({}, {2})))),
                 values<Int>(1, 3, 2));
    CHECK_OUTPUT((eval_node<standard::operators::is_empty, TSL<TS<Int>>>(
                     values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}}), dynamic_list_delta<TS<Int>>({}, {0})))),
                 values<Bool>(false, true));

    CHECK_OUTPUT(
        (eval_node<standard::operators::len_, TSS<Int>>(values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({}, {1})))),
        values<Int>(2, 1));
    CHECK_OUTPUT(
        (eval_node<standard::operators::is_empty, TSS<Int>>(values<Value>(set_delta<Int>({1}, {}), set_delta<Int>({}, {1})))),
        values<Bool>(false, true));

    CHECK_OUTPUT((eval_node<standard::operators::len_, TSD<Int, TS<Float>>>(
                     values<Value>(dict_delta<Int, TS<Float>>({{1, 1.0}, {2, 2.0}}), dict_delta<Int, TS<Float>>({{2, 3.0}}),
                                   dict_delta<Int, TS<Float>>({}, {1})))),
                 values<Int>(2, none, 1));
    CHECK_OUTPUT(
        (eval_node<standard::operators::is_empty, TSD<Int, TS<Float>>>(values<Value>(
            dict_delta<Int, TS<Float>>({{1, 1.0}}), dict_delta<Int, TS<Float>>({{1, 2.0}}), dict_delta<Int, TS<Float>>({}, {1})))),
        values<Bool>(false, none, true));
}
