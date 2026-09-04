// The ``switch_`` higher-order OPERATOR (lib/std/operators/higher_order.h).
//
// switch_ routes through ONE active child graph at a time, selected by its key
// input. Two fixed graph-storage slots alternate: on a key change the inactive
// slot is reused for the new branch and the old active branch is stopped, then
// retained until its slot is reused by the following switch. The output samples
// the new branch at switch time (the sampled-runtime contract; a deliberate
// divergence from Python's value=None reset). Branches are WiredFn values
// (graphs, nodes, or operators) and may take the key when their first parameter
// is named "key". See the developer guide *Nested Graphs*.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/wired_fn.h>

#include "nested_lifecycle_test_support.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct AddBoth
    {
        static constexpr auto name = "add_both";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (a + b).as<TS<Int>>();
        }
    };

    struct SubBoth
    {
        static constexpr auto name = "sub_both";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (a - b).as<TS<Int>>();
        }
    };

    // Key-consuming over two ts args: arity = ts count + 1.
    struct KeySumBoth
    {
        static constexpr auto name = "key_sum_both";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"key", TS<Int>> key, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (key + (a + b).as<TS<Int>>()).as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("switch_: variadic time-series arguments feed the branches, mixed arities")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // Two ts args; branch 1 adds them, branch 2 subtracts, branch 3 consumes
    // the key too because its first parameter is named "key".
    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Int>(1, none, 2, 3),
                     stdlib::switch_cases({{Value{Int{1}}, fn<AddBoth>()},
                                           {Value{Int{2}}, fn<SubBoth>()},
                                           {Value{Int{3}}, fn<KeySumBoth>()}}),
                     values<Int>(10, 20, none, none),
                     values<Int>(4, none, 6, none)),
                 values<Int>(14, 24, 14, 29));
}

// ---------------------------------------------------------------------------
// Keyword arguments resolve PER BRANCH against each branch's own parameter
// names (Python parity) — branches may declare the same names in different
// parameter orders.
// ---------------------------------------------------------------------------

namespace
{
    struct DiffLhsFirst
    {
        static constexpr auto name = "diff_lhs_first";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"lhs", TS<Int>> lhs, NamedPort<"rhs", TS<Int>> rhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (lhs - rhs).as<TS<Int>>();
        }
    };

    // Parameter ORDER reversed; the names still bind lhs=lhs, rhs=rhs — this
    // branch computes rhs - lhs to make the per-branch resolution observable.
    struct DiffRhsFirst
    {
        static constexpr auto name = "diff_rhs_first";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"rhs", TS<Int>> rhs, NamedPort<"lhs", TS<Int>> lhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (rhs - lhs).as<TS<Int>>();
        }
    };

}  // namespace

TEST_CASE("switch_: keyword arguments bind per branch by parameter name")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // fwd: lhs - rhs = 7; rev (params declared rhs-first): rhs - lhs = -7 —
    // the names bind per branch despite the reversed parameter order.
    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Str>(Str{"fwd"}, Str{"rev"}),
                     stdlib::switch_cases({{Value{Str{"fwd"}}, fn<DiffLhsFirst>()},
                                           {Value{Str{"rev"}}, fn<DiffRhsFirst>()}}),
                     arg<"lhs">(values<Int>(10, none)), arg<"rhs">(values<Int>(3, none))),
                 values<Int>(7, -7));
}
