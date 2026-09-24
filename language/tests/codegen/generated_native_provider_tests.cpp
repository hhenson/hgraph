#include "native-provider/provider.h"
#include <catch2/catch_test_macros.hpp>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <native-provider-consumer.h>
#include <spdlog/sinks/ringbuffer_sink.h>

using namespace hgraph;
using namespace hgraph::testing;

namespace
{
    struct WrongResult : checks::scalar_provider::Implementation
    { static double bit_and(Int, Int) noexcept; };
    struct WrongInput : checks::scalar_provider::Implementation
    { static Int bit_and(double, Int) noexcept; };
    struct MayThrow : checks::scalar_provider::Implementation
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

TEST_CASE("inferred services reach native and HGL helpers without changing ticks", "[codegen][capabilities]") {
    auto sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(32);
    struct RestoreLogger
    {
        ~RestoreLogger() { log::set_logger(nullptr); }
    } restore;
    log::set_logger(std::make_shared<spdlog::logger>("capability-test", sink));
    CHECK_OUTPUT(eval_node<checks::native_provider::audited>(values<Int>(2, none, 2, 3)), values<Int>(2, none, 2, 3));
    CHECK_OUTPUT(eval_node<checks::native_provider::hgl_audited>(values<Int>(2, none, 2, 3)), values<Int>(2, none, 2, 3));
    CHECK_OUTPUT(eval_node<checks::native_consumer::audited>(values<Int>(2, none, 2, 3)), values<Int>(2, none, 2, 3));
    CHECK_OUTPUT(eval_node<checks::native_provider::lifted_audited>(values<Int>(2, none, 2, 3)), values<Int>(2, none, 2, 3));
    std::size_t native = 0, hgl = 0;
    for (const auto &line : sink->last_formatted()) {
        native += line.find("native helper") != std::string::npos;
        hgl += line.find("HGL helper") != std::string::npos;
    }
    CHECK(native == 9);
    CHECK(hgl == 3);
}

TEST_CASE("an inferred clock reads the enclosing evaluation time", "[codegen][capabilities]") {
    CHECK_OUTPUT(eval_node<checks::native_provider::stamped>(values<Int>(2, none, 2, 3)),
                 values<DateTime>(MIN_ST, none, MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD));
}
