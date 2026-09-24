// Native regressions for the runtime specification's operator contracts
// (docs/source/runtime_spec/operators.md, OP-1 to OP-11). Each case is the
// minimized recipe of a parity issue whose reasoned expectation matched
// released hgraph (runtime_spec/validation/parity); the Python-authored twin
// of each case is python/tests/test_operator_contracts.py.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using IntDict = TSD<Int, TS<Int>>;
}  // namespace

TEST_CASE("operator contracts: a TSD union forwards the most recent tick (OP-4, OP-5)")
{
    stdlib::register_standard_operators();

    // Parity #1069: rhs re-ticks a key lhs already holds. The most recent tick
    // wins; lhs had not ticked in that cycle, however stale its slot bit.
    CHECK_OUTPUT((eval_node<stdlib::bit_or, IntDict, IntDict>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, -17}}), none, none),
                     values<Value>(none, none, dict_delta<Int, TS<Int>>({{1, -13}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, -17}}), none,
                               dict_delta<Int, TS<Int>>({{1, -13}})));

    // Parity #982: a forwarded tick of an equal value is still published.
    CHECK_OUTPUT((eval_node<stdlib::bit_or, IntDict, IntDict>(
                     values<Value>(none, none, none, dict_delta<Int, TS<Int>>({{3, -19}})),
                     values<Value>(none, none, dict_delta<Int, TS<Int>>({{3, -19}}), none))),
                 values<Value>(none, none, dict_delta<Int, TS<Int>>({{3, -19}}),
                               dict_delta<Int, TS<Int>>({{3, -19}})));

    // A same-cycle tie goes to lhs.
    CHECK_OUTPUT((eval_node<stdlib::bit_or, IntDict, IntDict>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}), none,
                                   dict_delta<Int, TS<Int>>({{1, 3}})),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}}),
                                   dict_delta<Int, TS<Int>>({{1, 20}}),
                                   dict_delta<Int, TS<Int>>({{1, 30}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                               dict_delta<Int, TS<Int>>({{1, 20}}),
                               dict_delta<Int, TS<Int>>({{1, 3}})));
}

TEST_CASE("operator contracts: TSD symmetric difference needs both operands (OP-6)")
{
    stdlib::register_standard_operators();

    // Parity #959: a never-ticked lhs is nil, not the empty dictionary.
    CHECK_OUTPUT((eval_node<stdlib::bit_xor, IntDict, IntDict>(
                     values<Value>(none, none),
                     values<Value>(none, dict_delta<Int, TS<Int>>({{1, -19}})))),
                 values<Value>(none, none));

    // Keys that ticked before the other operand arrived backfill on admission.
    CHECK_OUTPUT((eval_node<stdlib::bit_xor, IntDict, IntDict>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}), none),
                     values<Value>(none, dict_delta<Int, TS<Int>>({{2, 2}})))),
                 values<Value>(none, dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}})));

    // Parity #1040: key 1 changes holder from lhs to rhs in the cycle rhs
    // ticks it, so the equal value is forwarded.
    CHECK_OUTPUT((eval_node<stdlib::bit_xor, IntDict, IntDict>(
                     values<Value>(dict_delta<Int, TS<Int>>({{2, -13}}), none,
                                   dict_delta<Int, TS<Int>>({{1, -19}}), none,
                                   dict_delta<Int, TS<Int>>({}, {1})),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, -19}}),
                                   dict_delta<Int, TS<Int>>({{2, -18}}),
                                   dict_delta<Int, TS<Int>>({{1, -19}}),
                                   dict_delta<Int, TS<Int>>({}, {1}),
                                   dict_delta<Int, TS<Int>>({{1, -19}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, -19}, {2, -13}}),
                               dict_delta<Int, TS<Int>>({}, {2}),
                               dict_delta<Int, TS<Int>>({}, {1}),
                               dict_delta<Int, TS<Int>>({{1, -19}}),
                               dict_delta<Int, TS<Int>>({{1, -19}})));
}

TEST_CASE("operator contracts: a TSD difference validates on admission (OP-5)")
{
    stdlib::register_standard_operators();

    // Parity #961: every lhs key is also in rhs, so the first admitted result
    // is the empty dictionary, which still validates the output.
    CHECK_OUTPUT((eval_node<stdlib::sub_, IntDict, IntDict>(
                     values<Value>(dict_delta<Int, TS<Int>>({{3, 2}})),
                     values<Value>(dict_delta<Int, TS<Int>>({{3, -17}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({})));
}

TEST_CASE("operator contracts: a nested child forwards only its own changes (OP-4)")
{
    stdlib::register_standard_operators();
    using Inner = TSD<Int, TS<Int>>;
    using Nested = TSD<Int, Inner>;

    // Codex review on #1627: forwarding the whole inner value re-ticked the
    // unchanged sibling 11; only 10 changed.
    CHECK_OUTPUT((eval_node<stdlib::bit_or, Nested, Nested>(
                     values<Value>(dict_delta<Int, Inner>({{1, dict_delta<Int, TS<Int>>({{10, 1}, {11, 2}})}}),
                                   dict_delta<Int, Inner>({{1, dict_delta<Int, TS<Int>>({{10, 5}})}})),
                     values<Value>(dict_delta<Int, Inner>({{2, dict_delta<Int, TS<Int>>({{20, 3}})}}), none))),
                 values<Value>(dict_delta<Int, Inner>({{1, dict_delta<Int, TS<Int>>({{10, 1}, {11, 2}})},
                                                       {2, dict_delta<Int, TS<Int>>({{20, 3}})}}),
                               dict_delta<Int, Inner>({{1, dict_delta<Int, TS<Int>>({{10, 5}})}})));
}
