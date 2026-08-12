# Ported from release/0.5:hgraph_unit_tests/arrow/test_arrow.py
# Changes from upstream:
#  - `from hgraph.arrow._arrow import PairSchema, Pair` -> public
#    `from hgraph.arrow import ...` (private paths are internal-only here).
#  - test_assert_insufficient_args / test_assert_too_many_args: node/stop-hook
#    exceptions cross the C++ boundary as NodeException (RuntimeError) rather
#    than the original AssertionError (deviation: error-type translation).
#  - test_side_effects: converted - hg_cpp retains explicitly wired nodes, so
#    an un-flagged side-effect node evaluates even when its output is unused
#    (upstream discards nodes unreachable from a sink).
from dataclasses import dataclass
from enum import Enum

import pytest
from frozendict import frozendict as fd

import hgraph
from hgraph import TS, graph, TSB, const, NodeException, TSD, add_, eq_, compute_node, GlobalState, null_sink
from hgraph.arrow import (
    arrow,
    if_,
    if_then,
    identity,
    i,
    eval_,
    null,
    const_,
    apply_,
    assert_,
    first,
    second,
    assoc,
    fb,
    switch_,
    map_,
    reduce,
    debug_,
)
from hgraph.arrow import PairSchema, Pair
from hgraph.test import eval_node


def test_make_tuple_tsl():
    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> Pair[TS[int]]:
        return arrow(ts1, ts2).ts

    assert eval_node(g, [1, 2], [3, 4]) == [{"first": 1, "second": 3}, {"first": 2, "second": 4}]


def test_make_tuple_tsb():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> Pair[TS[int], TS[str]]:
        return arrow(ts1, ts2).ts

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"first": 1, "second": "A"}, {"first": 2, "second": "B"}]


def test_basic_arrow_wrapper():
    @graph
    def g(ts: TS[int]) -> TS[int]:
        mult_3 = arrow(lambda x: x * 3)
        add_5 = arrow(lambda x: x + 5)
        return (mult_3 >> add_5)(ts)

    assert eval_node(g, [1, 2, 3]) == [1 * 3 + 5, 2 * 3 + 5, 3 * 3 + 5]


def test_basic_arrow_wrapper_including_input_wrapper():
    @graph
    def g(ts: TS[int]) -> TS[int]:
        mult_3 = arrow(lambda x: x * 3)
        add_5 = arrow(lambda x: x + 5)
        return arrow(ts) | mult_3 >> add_5

    assert eval_node(g, [1, 2, 3]) == [1 * 3 + 5, 2 * 3 + 5, 3 * 3 + 5]


def test_nested_inputs():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> Pair[(TS[int],), (TS[str],)]:
        return arrow((ts1, ts1), (ts2, ts2)).ts

    assert eval_node(g, [1, 2], ["A", "B"]) == [
        {"first": {"first": 1, "second": 1}, "second": {"first": "A", "second": "A"}},
        {"first": {"first": 2, "second": 2}, "second": {"first": "B", "second": "B"}},
    ]


def test_first():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        return arrow(ts1, ts2) | first >> arrow(lambda x: x * 3)

    assert eval_node(g, [1, 2], ["A", "B"]) == [3, 6]


def test_second():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[str]:
        from hgraph import format_

        return arrow(ts1, ts2) | second >> arrow(lambda x: format_("{}_", x))

    assert eval_node(g, [1, 2], ["A", "B"]) == ["A_", "B_"]


def test_cross():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> Pair[TS[int], TS[str]]:
        from hgraph import format_

        return arrow(ts1, ts2) | arrow(lambda x: x * 3) // (lambda x: format_("{}_", x))

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"first": 3, "second": "A_"}, {"first": 6, "second": "B_"}]


def test_fan_out():
    @graph
    def g(ts1: TS[int]) -> Pair[TS[int]]:
        return arrow(ts1) | arrow(lambda x: x * 3) / (lambda x: x + 1)

    assert eval_node(g, [1, 2]) == [{"first": 3, "second": 2}, {"first": 6, "second": 3}]


def test_apply_():
    @graph
    def g(ts1: TS[int]) -> TS[int]:
        return arrow(const(lambda x: x * 2), ts1) | apply_(TS[int])

    assert eval_node(g, [1, 2]) == [2, 4]


def test_assoc():
    @graph
    def g(ts1: TS[int], ts2: TS[float], ts3: TS[str]) -> Pair[TS[int], (TS[float], TS[str])]:
        return arrow((ts1, ts2), ts3) | assoc

    assert eval_node(g, [1, 2], [1.0, 2.0], ["A", "B"]) == [
        {"first": 1, "second": {"first": 1.0, "second": "A"}},
        {"first": 2, "second": {"first": 2.0, "second": "B"}},
    ]


def test_identity():
    @graph
    def g(ts1: TS[int]) -> TS[int]:
        return arrow(ts1) | identity

    assert eval_node(g, [1, 2]) == [1, 2]


def test_first_():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> Pair[TS[int], TS[str]]:
        return arrow(ts1, ts2) | arrow(lambda x: x * 3) // identity

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"first": 3, "second": "A"}, {"first": 6, "second": "B"}]


def test_second_():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> Pair[TS[int], TS[str]]:
        from hgraph import format_

        return arrow(ts1, ts2) | identity // arrow(lambda x: format_("{}_", x))

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"first": 1, "second": "A_"}, {"first": 2, "second": "B_"}]


def test_arrow_const():
    @graph
    def g() -> TS[int]:
        return arrow(1) | i

    assert eval_node(g) == [1]


def test_arrow_const_2():
    @graph
    def g() -> Pair[TS[int]]:
        return arrow(1, 2) | i

    assert eval_node(g) == [{"first": 1, "second": 2}]


def test_arrow_const_3():
    @graph
    def g() -> Pair[(TS[int],), TS[int]]:
        return arrow((1, 2), 3) | i

    assert eval_node(g) == [{"first": {"first": 1, "second": 2}, "second": 3}]


def test_assert_too_many_args():
    @graph
    def g() -> TS[int]:
        return arrow(1) | assert_(1, 2)

    # deviation: the stop-hook AssertionError surfaces as NodeException
    # (RuntimeError) from the C++ runtime.
    with pytest.raises((AssertionError, NodeException)):
        assert eval_node(g) == [1]


def test_assert_insufficient_args():
    @graph
    def g(ts: TS[int]) -> TS[int]:
        return arrow(ts) | assert_(1)

    with pytest.raises(NodeException):
        assert eval_node(g, [1, 2]) == [1, 2]


def test_assert_wrong_value():
    @graph
    def g() -> TS[int]:
        return arrow(1) | assert_(2)

    with pytest.raises(NodeException):
        assert eval_node(g) == [1]


def test_assert_positive_flow():
    @graph
    def g(ts: TS[int]) -> TS[int]:
        return arrow(ts) | assert_(1, 2)

    assert eval_node(g, [1, 2]) == [1, 2]


def test_binary_op():
    @graph
    def g(ts: TS[int]) -> TS[int]:
        return arrow(ts) | i / i >> add_

    assert eval_node(g, [1, 2]) == [2, 4]


def test_eval_node():
    assert eval_(1) | i == [1]
    assert eval_([1, 2]) | i == [1, 2]
    assert eval_([1, 2], [3, 4]) | i == [(1, 3), (2, 4)]
    assert eval_(([1, 2], [3, 4]), [5, 6]) | i == [((1, 3), 5), ((2, 4), 6)]


def test_eval_and_assert():
    eval_([1, 2], [3, 4]) | add_ >> assert_(4, 6)


def test_pos():
    eval_([1, 2], [3, 4]) | +arrow(lambda x: x + 1) >> assert_((2, 3), (3, 4))


def test_neg():
    eval_([1, 2], [3, 4]) | -arrow(lambda x: x + 1) >> assert_((1, 4), (2, 5))


def test_null():
    # Send a pairs of constants
    # Then split to the identity and null operators
    # evaluate the results of the independent processing chain
    # assert will get TSL[TS[int], Size[2]] so .value is (1, None)
    eval_(1, 2) | i // null >> assert_((1, None))


def test_const_():
    eval_(1) | i / const_(2) >> assert_((1, 2))


def test_if_then_else():
    eval_([True, False], [1, 2]) | if_then(lambda x: x + 1).otherwise(lambda x: x - 1) >> assert_(2, 1)


def test_if_():
    one = const_(1)
    fn = if_(eq_ << one).then(add_ << one) >> assert_(2)
    print(fn)
    eval_([1, 2]) | fn


def test_if_then_otherwise():
    one = const_(1)
    fn = if_(eq_ << one).then(lambda x: x + 1).otherwise(lambda x: -x) >> assert_(2, -2)
    eval_([1, 2]) | fn


def test_feedback():
    eval_([1, 2, 3]) | fb["a" : TS[int], "default":0] >> add_ >> fb["a"] >> debug_("Test") >> assert_(1, 3, 6)


def test_switch():
    (eval_(["A", None, "B"], [1, 2, 3]) | switch_({"A": lambda p: p + 1, "B": lambda p: 1 - p}) >> assert_(2, 3, -2))


def test_map():
    (eval_([fd({"A": 1, "B": 2})], type_map=TSD[str, TS[int]]) | map_(lambda x: x + 1) >> assert_(fd({"A": 2, "B": 3})))


def test_reduce():
    (eval_([fd({"A": 1, "B": 2})], type_map=TSD[str, TS[int]]) | reduce(add_, 0) >> assert_(3))


def test_debug_():
    eval_([1, 2], [1, None, 2]) | debug_("Test value {}") >> assert_((1, 1), (2, 1), (2, 2))


def test_debug_is_a_direct_typed_arrow_for_enum_and_compound_scalar(capsys):
    class Side(Enum):
        BUY = "BUY"
        SELL = "SELL"

    @dataclass(frozen=True)
    class Event(hgraph.CompoundScalar):
        event_id: str

    @compute_node
    def render(side: TS[Side], event: TS[Event]) -> TS[str]:
        return f"{side.value.value}:{event.value.event_id}"

    @graph
    def app(side: TS[Side], event: TS[Event]) -> TS[str]:
        # release/0.5 exposed debug_ itself as an Arrow.  Its pass-through
        # output must retain the pair's concrete field projections so this
        # typed consumer receives TS[Side] and TS[Event], not TS[object].
        return arrow(side, event) | debug_ >> render

    buy = Event("one")
    sell = Event("two")
    assert eval_node(app, [Side.BUY, Side.SELL], [buy, sell]) == [
        "BUY:one",
        "SELL:two",
    ]
    output = capsys.readouterr().out
    assert "<Side.BUY: 'BUY'>" in output and "Event(event_id='one')" in output
    assert "<Side.SELL: 'SELL'>" in output and "Event(event_id='two')" in output

    # eval_ records the typed pair and converts it to the presentation tuple
    # only after execution; no object-valued _combine_pair_values node is
    # inserted into the graph.
    assert (
        eval_(
            [Side.BUY, Side.SELL],
            [buy, sell],
            type_map=(TS[Side], TS[Event]),
        )
        | i
    ) == [(Side.BUY, buy), (Side.SELL, sell)]


def test_side_effects():

    @compute_node
    def side_effect(ts: TS[int], global_state: GlobalState = None) -> TS[int]:
        global_state["t"] = ts.value

    @graph
    def g():
        c = const(1)
        arrow(c) | arrow(side_effect)
        null_sink(c)

    with GlobalState():
        eval_node(g)
        # hg_cpp retains every explicitly wired node.  Side-effect retention is
        # a graph-construction property rather than an opt-in Python flag.
        assert GlobalState.instance().get("t", None) == 1

    @graph
    def h():
        c = const(1)
        arrow(c) | arrow(side_effect, __has_side_effects__=True)
        null_sink(c)

    with GlobalState():
        eval_node(h)
        assert GlobalState.instance().get("t", None) == 1
