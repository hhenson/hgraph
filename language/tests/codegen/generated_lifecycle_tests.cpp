#include <lifecycle-capabilities.h>

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/util/date_time.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;

namespace lifecycle = examples::lifecycle_capabilities;

TEST_CASE("a scheduler-driven source ticks from start until its count", "[codegen][runtime][lifecycle]") {
    // Scheduled in the starting cycle, then every `delay` until max_ticks.
    CHECK_OUTPUT(eval_node<lifecycle::ticker>(MIN_TD * 2, Int{3}), values<Int>(1, none, 2, none, 3));
    CHECK_OUTPUT(eval_node<lifecycle::ticker>(MIN_TD, Int{2}), values<Int>(1, 2));
    CHECK_OUTPUT(eval_node<lifecycle::ticker>(MIN_TD, Int{0}), values<Int>());
    CHECK_OUTPUT(eval_node<lifecycle::ticker>(MIN_TD, Int{-1}), values<Int>());
}

TEST_CASE("passivating an input stops later ticks from activating the node", "[codegen][runtime][lifecycle]") {
    CHECK_OUTPUT(eval_node<lifecycle::first_ticks>(values<Int>(10, 20, 30, 40), Int{2}), values<Int>(10, 20, none, none));
    CHECK_OUTPUT(eval_node<lifecycle::first_ticks>(values<Int>(10, none, 30), Int{5}), values<Int>(10, none, 30));
    CHECK_OUTPUT(eval_node<lifecycle::first_ticks>(values<Int>(10, 20), Int{0}), values<Int>(none, none));
    CHECK_OUTPUT(eval_node<lifecycle::first_ticks>(values<Int>(10, 20), Int{-1}), values<Int>(none, none));
}

TEST_CASE("the evaluation clock reads the engine time of the current cycle", "[codegen][runtime][lifecycle]") {
    CHECK_OUTPUT(eval_node<lifecycle::stamped>(values<Int>(1, none, 3)),
                 values<DateTime>(MIN_ST, none, MIN_ST + MIN_TD * 2));
}

TEST_CASE("a source bare handler has no modified inputs", "[codegen][runtime][lifecycle]") {
    CHECK_OUTPUT(eval_node<lifecycle::dormant>(), values<Bool>());
}

TEST_CASE("scheduled handlers sample passive inputs only when due", "[codegen][runtime][lifecycle]") {
    CHECK_OUTPUT(eval_node<lifecycle::scheduled_sample>(values<Int>(10, 20, 30, 40), MIN_TD * 2),
                 values<Int>(none, none, 30, none));
    CHECK_OUTPUT(eval_node<lifecycle::at_time>(MIN_ST + MIN_TD * 2),
                 values<DateTime>(none, none, MIN_ST + MIN_TD * 2));
}

TEST_CASE("an active control input can reactivate a passive data input", "[codegen][runtime][lifecycle]") {
    CHECK_OUTPUT(eval_node<lifecycle::controlled>(values<Int>(10, 20, 30, 40, 50),
                                                 values<Bool>(true, false, none, true, none)),
                 values<Int>(10, none, none, 40, 50));
}
