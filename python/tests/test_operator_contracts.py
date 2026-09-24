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

def test_aggregates_publish_nothing_over_an_invalid_collection():
    # OP-1, OP-2. A never-ticked collection is nil, not empty.
    L = hg.TSL[TS[int], hg.Size[2]]

    @graph
    def sum_list(values: L) -> TS[int]:
        return hg.sum_(values)

    assert eval_node(sum_list, [None, None], resolution_dict={"values": L}) is None
    assert eval_node(sum_list, [None, {1: 5}], resolution_dict={"values": L}) == [None, 5]

    # Parity #1476 / #1538: the declared size is added to a sum that never exists.
    @graph
    def pinned(values: hg.TSL[TS[int], hg.SIZE], _sz: type[hg.SIZE] = hg.AUTO_RESOLVE) -> TS[int]:
        return hg.sum_(values) + hg.const(_sz.SIZE)

    assert eval_node(pinned, [None], resolution_dict={"values": L}) is None

    S = hg.TSS[int]
    for operator, empty in ((hg.sum_, 0), (hg.min_, None), (hg.max_, None)):
        @graph
        def over_set(values: S) -> TS[int]:
            return operator(values)

        assert eval_node(over_set, [None, None]) is None
        assert eval_node(over_set, [set(), None]) == ([empty, None] if empty is not None else None)

    # A default answers an empty set, not a missing one.
    @graph
    def minimum_or_seven(values: S) -> TS[int]:
        return hg.min_(values, default_value=7)

    assert eval_node(minimum_or_seven, [None, None]) is None
    assert eval_node(minimum_or_seven, [set(), None]) == [7, None]

    # released hgraph's dictionary mean is default(div_(sum_, len_), NaN): its
    # contract names NaN for a missing dictionary, so that stays.
    @graph
    def mean_dict(values: D) -> TS[float]:
        return hg.mean(values)

    import math

    [nan] = eval_node(mean_dict, [None])
    assert math.isnan(nan)


def test_all_and_any_publish_nothing_before_an_argument_is_valid():
    # OP-2. Parity #1181, #1246, #1355, #1494.
    B = TS[bool]

    @graph
    def all3(a: B, b: B, c: B) -> B:
        return hg.all_(a, b, c)

    @graph
    def any2(a: B, b: B) -> B:
        return hg.any_(a, b)

    assert eval_node(all3, [None], [None], [None]) is None
    assert eval_node(any2, [None], [None]) is None
    # An argument not yet valid reads as None: falsy.
    assert eval_node(all3, [True, None], [None, True], [True, None]) == [False, True]
    assert eval_node(any2, [None, False], [True, None]) == [True, True]


def test_all_and_any_evaluate_at_start_over_already_valid_arguments():
    # Codex review on #1628: a branch started after its arguments ticked sees
    # them valid at start and publishes then, as released hgraph does.
    B = TS[bool]

    @graph
    def switched(key: TS[str], x: B, y: B) -> B:
        return hg.switch_(key, {"a": lambda x, y: hg.all_(x, y), "b": lambda x, y: hg.any_(x, y)}, x, y)

    assert eval_node(switched, [None, "a"], [True, None], [True, None]) == [None, True]
    assert eval_node(switched, [None, "b"], [False, None], [True, None]) == [None, True]
