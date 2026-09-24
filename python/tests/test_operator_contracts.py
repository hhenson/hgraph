"""Public Python wiring regressions for the runtime specification's operator
contracts (docs/source/runtime_spec/operators.md, OP-1 to OP-11).

Each test is the minimized recipe of a parity issue whose reasoned expectation
matched released hgraph 0.5.41 (runtime_spec/validation/parity), so it pins
the released trace. The native twin is tests/cpp/test_operator_contracts.cpp.
"""

import hgraph as hg
from hgraph import TS, graph
from hgraph.test import eval_node

D = hg.TSD[str, TS[int]]


def _binary(operator, a, b):
    @graph
    def g(lhs: D, rhs: D) -> D:
        return operator(lhs, rhs)

    return eval_node(g, a, b)


def test_tsd_union_forwards_the_most_recent_tick():
    # OP-4, OP-5. Parity #1069: the rhs tick of a key lhs holds wins.
    assert _binary(hg.bit_or, [{"a": -17}, None, None], [None, None, {"a": -13}]) == [
        {"a": -17},
        None,
        {"a": -13},
    ]
    # Parity #982: an equal forwarded value is still a tick, in either spelling.
    for operator in (hg.bit_or, hg.union):
        assert _binary(
            operator, [None, None, None, {"c": -19}], [None, None, {"c": -19}, None]
        ) == [None, None, {"c": -19}, {"c": -19}]
    # A same-cycle tie goes to lhs.
    assert _binary(
        hg.bit_or, [{"a": 1}, None, {"a": 3}], [{"a": 10}, {"a": 20}, {"a": 30}]
    ) == [{"a": 1}, {"a": 20}, {"a": 3}]


def test_tsd_symmetric_difference_needs_both_operands():
    # OP-6. Parity #959: a never-ticked lhs is nil, not the empty dictionary.
    assert _binary(hg.bit_xor, [None] * 6, [None] * 5 + [{"a": -19}]) is None
    # Parity #1040: a key changes holder in the cycle its new holder ticks it.
    assert _binary(
        hg.bit_xor,
        [{"b": -13}, None, {"a": -19}, None, {"a": hg.REMOVE}],
        [{"a": -19}, {"b": -18}, {"a": -19}, {"a": hg.REMOVE}, {"a": -19}],
    ) == [
        {"a": -19, "b": -13},
        {"b": hg.REMOVE},
        {"a": hg.REMOVE},
        {"a": -19},
        {"a": -19},
    ]
    # Parity #1085: three operands, two never valid, publish nothing.
    @graph
    def three(a: D, b: D, c: D) -> D:
        return hg.symmetric_difference(a, b, c)

    assert eval_node(three, [None] * 4, [None] * 4, [None, None, None, {"b": -17}]) is None


def test_tsd_difference_validates_on_admission():
    # OP-5. Parity #961: the first admitted result is empty and still ticks.
    for operator in (hg.sub_, hg.difference):
        assert _binary(operator, [{"c": 2}], [{"c": -17}]) == [{}]


def test_a_nested_child_forwards_only_its_own_changes():
    # OP-4, Codex review on #1627: only the inner key that ticked is forwarded.
    N = hg.TSD[int, hg.TSD[int, TS[int]]]

    @graph
    def joined(lhs: N, rhs: N) -> N:
        return hg.bit_or(lhs, rhs)

    assert eval_node(joined, [{1: {10: 1, 11: 2}}, {1: {10: 5}}], [{2: {20: 3}}, None]) == [
        {1: {10: 1, 11: 2}, 2: {20: 3}},
        {1: {10: 5}},
    ]
