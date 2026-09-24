#include "native-provider/provider.h"
#include <catch2/catch_test_macros.hpp>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <native-provider-consumer.h>

using namespace hgraph;
using namespace hgraph::testing;

namespace
{
    struct WrongResult
    { static double bit_and(Int, Int) noexcept; };
    struct WrongInput
    { static Int bit_and(double, Int) noexcept; };
    struct MayThrow
    { static Int bit_and(Int, Int); };
    struct Missing
    {};

    static_assert(!checks::native_provider::native_interface::Implementation<WrongResult>);
    static_assert(!checks::native_provider::native_interface::Implementation<WrongInput>);
    static_assert(!checks::native_provider::native_interface::Implementation<MayThrow>);
    static_assert(!checks::native_provider::native_interface::Implementation<Missing>);
}  // namespace

TEST_CASE("source-owned native bindings preserve missing and equal ticks", "[codegen][native][interface]") {
    CHECK_OUTPUT((eval_node<checks::native_provider::bitwise>(values<Int>(6, none, -1, none, 7, 7),
                                                              values<Int>(none, 3, none, 7, none, none))),
                 values<Int>(none, 2, 3, 7, 7, 7));
    CHECK(checks::scalar_provider::native.bit_and(6, 3) == 2);
}

TEST_CASE("native const functions lift with the same local and imported ticks", "[codegen][native][interface]") {
    CHECK_OUTPUT((eval_node<checks::native_provider::lifted>(values<Int>(6, none, -1, none, 7, 7),
                                                             values<Int>(none, 3, none, 7, none, none))),
                 values<Int>(none, 2, 3, 7, 7, 7));
    CHECK_OUTPUT((eval_node<checks::native_consumer::lifted>(values<Int>(6, none, -1, none, 7, 7),
                                                             values<Int>(none, 3, none, 7, none, none))),
                 values<Int>(none, 2, 3, 7, 7, 7));
    CHECK_OUTPUT((eval_node<checks::native_provider::mixed>(values<Int>(6, none, -1, none, 7, 7))),
                 values<Int>(2, none, 3, none, 3, 3));
}
