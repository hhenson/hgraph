#include <const-debug.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <catch2/catch_test_macros.hpp>
#include <iostream>
#include <sstream>

using namespace hgraph;
using namespace hgraph::testing;

namespace
{
    struct ObserveSink
    {
        static auto compose(Wiring &w, Port<TS<Int>> value) {
            wire<examples::const_debug::debug_print>(w, value);
            return value;
        }
    };

    struct Capture
    {
        std::ostringstream text;
        std::streambuf *previous{std::cout.rdbuf(text.rdbuf())};
        ~Capture() { std::cout.rdbuf(previous); }
    };
}

TEST_CASE("HGL const source publishes once", "[codegen][bootstrap]") {
    CHECK_OUTPUT(eval_node<examples::const_debug::const_>(Int{42}), values<Int>(42));
    CHECK_OUTPUT(eval_node<examples::const_debug::const_>(Int{-7}), values<Int>(-7));
}

TEST_CASE("HGL debug sink prints each valid tick including duplicates", "[codegen][bootstrap]") {
    Capture capture;
    CHECK_OUTPUT(eval_node<ObserveSink>(values<Int>(none, 42, none, 42, -7)),
                 values<Int>(none, 42, none, 42, -7));
    CHECK(capture.text.str() == "42\n42\n-7\n");
}

TEST_CASE("HGL const and debug graph prints once per execution", "[codegen][bootstrap]") {
    Capture capture;
    CHECK_OUTPUT(eval_node<examples::const_debug::main>(), values<Int>(42));
    CHECK(capture.text.str() == "42\n");
    CHECK_OUTPUT(eval_node<examples::const_debug::main>(), values<Int>(42));
    CHECK(capture.text.str() == "42\n42\n");
}
