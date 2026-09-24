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

def _printed(capsys):
    # Released hgraph also logs its own [hgraph] lines to stdout.
    return [line for line in capsys.readouterr().out.splitlines() if "[hgraph]" not in line]


def test_print_waits_for_every_argument(capsys):
    # OP-8. Parity #1122, #1339, #1564, #1613: print_ formats as format_ does
    # with __strict__ true, so an argument that never ticks prints nothing.
    for tp, ticks in ((TS[int], [None]), (TS[float], [None]), (TS[bool], [None, None])):
        @graph
        def printed(ts: tp) -> tp:
            hg.print_("v={value}", value=ts)
            return ts

        eval_node(printed, ticks)
        assert _printed(capsys) == []

    @graph
    def printed_later(ts: TS[int]) -> TS[int]:
        hg.print_("v={value}", value=ts)
        return ts

    eval_node(printed_later, [None, 5])
    assert _printed(capsys) == ["v=5"]

    # With no arguments the format string itself prints when it ticks.
    @graph
    def plain(ts: TS[int]) -> TS[int]:
        hg.print_("hello")
        return ts

    eval_node(plain, [1])
    assert _printed(capsys) == ["hello"]


def test_a_formatted_assert_needs_its_arguments():
    # OP-8: released hgraph formats the message with format_ and asserts
    # through a sink that needs that message, so a failing condition whose
    # argument is not yet valid raises nothing.
    import pytest

    @graph
    def checked(condition: TS[bool], detail: TS[int]) -> TS[bool]:
        hg.assert_(condition, "failed with {}", detail)
        return condition

    assert eval_node(checked, [False], [None]) == [False]
    with pytest.raises(Exception, match="failed with 3"):
        eval_node(checked, [True, False], [None, 3])


def test_a_nested_entry_keeps_an_invalid_child_invalid():
    # Time-series spec, TSD row of the value/delta table: a delta holds only
    # valid modified children, so an inner child that never ticked is absent,
    # never a default value. Parity #963, #964, #965.
    I = TS[int]
    N = hg.TSD[str, hg.TSD[str, I]]

    @graph
    def nested(value: I, key: TS[str]) -> N:
        inner = hg.convert[hg.TSD[str, I]](key, value)
        return hg.convert[N](key, inner)

    assert eval_node(nested, [None], ["c"]) == [{"c": {}}]

    # Its membership is exact too (TS-19): the inner key exists, invalid.
    @hg.compute_node
    def inner_members(ts: N) -> TS[str]:
        return ";".join(
            f"{k}:{sorted((k2, c.valid) for k2, c in inner.items())}" for k, inner in ts.items()
        )

    @graph
    def nested_members(value: I, key: TS[str]) -> TS[str]:
        return inner_members(nested(value, key))

    assert eval_node(nested_members, [None], ["c"]) == ["c:[('c', False)]"]
    assert eval_node(nested, [None, 5], ["c", None]) == [{"c": {}}, {"c": {"c": 5}}]
    # A flat entry still forwards every tick of its value, equal or not.
    @graph
    def flat(value: I, key: TS[str]) -> hg.TSD[str, I]:
        return hg.convert[hg.TSD[str, I]](key, value)

    assert eval_node(flat, [None, 5, 5], ["c", None, None]) == [{}, {"c": 5}, {"c": 5}]


def test_a_converted_entry_mirrors_membership_through_bundles_and_withdrawals():
    # TS-19 / TS-7, Codex review on #1630.
    I = TS[int]

    class Holder(hg.TimeSeriesSchema):
        d: hg.TSD[str, I]

    @hg.compute_node
    def bundle_members(ts: hg.TSD[str, hg.TSB[Holder]]) -> TS[str]:
        return ";".join(f"{k}:{sorted((k2, c.valid) for k2, c in b.d.items())}" for k, b in ts.items())

    # A dictionary inside a bundle keeps its invalid inner key (released
    # hgraph agrees).
    @graph
    def through_bundle(value: I, key: TS[str]) -> TS[str]:
        bundle = hg.combine[hg.TSB[Holder]](d=hg.convert[hg.TSD[str, I]](key, value))
        return bundle_members(hg.convert[hg.TSD[str, hg.TSB[Holder]]](key, bundle))

    assert eval_node(through_bundle, [None], ["c"]) == ["c:[('c', False)]"]

    # A child withdrawn while its key stays is withdrawn in the entry too.
    # Accepted TS-7 behaviour (runtime spec validation DV-04): the parent reads
    # modified; released hgraph's parent does not, so it publishes nothing.
    @hg.compute_node
    def withdrawn(trigger: I, _output: hg.TSD_OUT[str, I] = None) -> hg.TSD[str, I]:
        if trigger.value == 1:
            return {"x": 1}
        _output["x"].invalidate()

    @hg.compute_node
    def nested_members(ts: hg.TSD[str, hg.TSD[str, I]]) -> TS[str]:
        return ";".join(f"{k}:{sorted((k2, c.valid) for k2, c in inner.items())}" for k, inner in ts.items())

    @graph
    def withdraw(trigger: I, key: TS[str]) -> TS[str]:
        return nested_members(hg.convert[hg.TSD[str, hg.TSD[str, I]]](key, withdrawn(trigger)))

    assert eval_node(withdraw, [1, 2], ["c", None]) == ["c:[('x', True)]", "c:[('x', False)]"]

def test_a_recording_exists_from_the_recorders_start():
    # OP-11. Parity #1315: a recorded series that never ticks leaves an EMPTY
    # recording, distinct from none; replaying it publishes nothing.
    @hg.component
    def recorded(values: TS[int]) -> TS[int]:
        return values

    with hg.GlobalState() as state:
        hg.set_record_replay_model(hg.IN_MEMORY)
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            assert eval_node(recorded, [None, None]) is None
        recording = state.get(":memory:recorded.values")
        assert recording is not None
        assert [value for _, value in recording] == []
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.REPLAY):
            assert eval_node(recorded, []) is None
