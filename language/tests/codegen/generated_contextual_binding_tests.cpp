#include <contextual-bindings.h>
#include "wiring/backend.h"
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace locals = checks::contextual_bindings;

TEST_CASE("generated locals preserve category and earlier connections", "[codegen][generated][locals]") {
    hgl::wiring::ensure_session();
    locals::register_operators();
    CHECK_OUTPUT(eval_node<locals::aliases>(values<Int>(1, 2), values<Int>(10, 20)), values<Int>(12, 23));
    CHECK_OUTPUT(eval_node<locals::static_rebind>(values<Int>(1, 2), values<Int>(10, 20), Bool{true}), values<Int>(10, 20));
    CHECK_OUTPUT(eval_node<locals::static_rebind>(values<Int>(1, 2), values<Int>(10, 20), Bool{false}), values<Int>(1, 2));
    CHECK_OUTPUT(eval_node<locals::ordinary>(values<Int>(1, 2), Bool{true}), values<Int>(3, 4));
    CHECK_OUTPUT(eval_node<locals::ordinary>(values<Int>(1, 2), Bool{false}), values<Int>(5, 6));
    CHECK_OUTPUT(eval_node<locals::branch_result>(values<Bool>(true, false), values<Int>(10, 20)), values<Int>(5, 20));
    CHECK_OUTPUT(eval_node<locals::operators::node_local>(values<Int>(-1, 2)), values<Int>(1, 5));
    const auto pair = [](Int first, Int second) {
        Value value{ValuePlanFactory::instance().type_for(scalar_descriptor<Tuple<Int, Int>>::value_meta())};
        auto fields = value.as_tuple().begin_mutation();
        fields[0].set(first);
        fields[1].set(second);
        return value;
    };
    CHECK_OUTPUT((eval_node<locals::operators::atomic_input, TS<Tuple<Int, Int>>>(values<Value>(pair(1, 2), pair(3, 4)))),
                 values<Value>(pair(1, 2), pair(3, 4)));
}
