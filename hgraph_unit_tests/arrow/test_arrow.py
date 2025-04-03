import hgraph
from hgraph import TS, TSL, Size, graph, TSB, const, NodeException
from hgraph.arrow import (
    arrow,
    if_,
    if_then,
    identity,
    i,
    eval_,
    null,
    const_,
    binary_op,
    apply_,
    eq_,
    assert_,
    first,
    second,
    assoc,
    add_,
    fb,
    print_out,
    debug_print_,
)
from hgraph.arrow._arrow import _TupleSchema
from hgraph.test import eval_node
import pytest


def test_make_tuple_tsl():
    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TSL[TS[int], Size[2]]:
        return arrow(ts1, ts2).ts

    assert eval_node(g, [1, 2], [3, 4]) == [{0: 1, 1: 3}, {0: 2, 1: 4}]


def test_make_tuple_tsb():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[_TupleSchema[TS[int], TS[str]]]:
        return arrow(ts1, ts2).ts

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"ts1": 1, "ts2": "A"}, {"ts1": 2, "ts2": "B"}]


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
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[_TupleSchema[TSL[TS[int], Size[2]], TSL[TS[str], Size[2]]]]:
        return arrow((ts1, ts1), (ts2, ts2)).ts

    assert eval_node(g, [1, 2], ["A", "B"]) == [
        {"ts1": {0: 1, 1: 1}, "ts2": {0: "A", 1: "A"}},
        {"ts1": {0: 2, 1: 2}, "ts2": {0: "B", 1: "B"}},
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
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[_TupleSchema[TS[int], TS[str]]]:
        from hgraph import format_

        return arrow(ts1, ts2) | arrow(lambda x: x * 3) // (lambda x: format_("{}_", x))

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"ts1": 3, "ts2": "A_"}, {"ts1": 6, "ts2": "B_"}]


def test_fan_out():
    @graph
    def g(ts1: TS[int]) -> TSL[TS[int], Size[2]]:
        return arrow(ts1) | arrow(lambda x: x * 3) / (lambda x: x + 1)

    assert eval_node(g, [1, 2]) == [{0: 3, 1: 2}, {0: 6, 1: 3}]


def test_apply_():
    @graph
    def g(ts1: TS[int]) -> TS[int]:
        return arrow(const(lambda x: x * 2), ts1) | apply_(TS[int])

    assert eval_node(g, [1, 2]) == [2, 4]


def test_assoc():
    @graph
    def g(
        ts1: TS[int], ts2: TS[float], ts3: TS[str]
    ) -> TSB[_TupleSchema[TS[int], TSB[_TupleSchema[TS[float], TS[str]]]]]:
        return arrow((ts1, ts2), ts3) | assoc

    assert eval_node(g, [1, 2], [1.0, 2.0], ["A", "B"]) == [
        {"ts1": 1, "ts2": {"ts1": 1.0, "ts2": "A"}},
        {"ts1": 2, "ts2": {"ts1": 2.0, "ts2": "B"}},
    ]


def test_identity():
    @graph
    def g(ts1: TS[int]) -> TS[int]:
        return arrow(ts1) | identity

    assert eval_node(g, [1, 2]) == [1, 2]


def test_first_():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[_TupleSchema[TS[int], TS[str]]]:
        return arrow(ts1, ts2) | arrow(lambda x: x * 3) // identity

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"ts1": 3, "ts2": "A"}, {"ts1": 6, "ts2": "B"}]


def test_second_():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[_TupleSchema[TS[int], TS[str]]]:
        from hgraph import format_

        return arrow(ts1, ts2) | identity // arrow(lambda x: format_("{}_", x))

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"ts1": 1, "ts2": "A_"}, {"ts1": 2, "ts2": "B_"}]


def test_arrow_const():
    @graph
    def g() -> TS[int]:
        return arrow(1) | i

    assert eval_node(g) == [1]


def test_arrow_const_2():
    @graph
    def g() -> TSL[TS[int], Size[2]]:
        return arrow(1, 2) | i

    assert eval_node(g) == [{0: 1, 1: 2}]


def test_arrow_const_3():
    @graph
    def g() -> TSB[_TupleSchema[TSL[TS[int], Size[2]], TS[int]]]:
        return arrow((1, 2), 3) | i

    assert eval_node(g) == [{"ts1": {0: 1, 1: 2}, "ts2": 3}]


def test_assert_too_many_args():
    @graph
    def g() -> TS[int]:
        return arrow(1) | assert_(1, 2)

    with pytest.raises(AssertionError):
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
        return arrow(ts) | i / i >> binary_op(hgraph.add_)

    assert eval_node(g, [1, 2]) == [2, 4]


def test_eval_node():
    assert eval_(1) | i == [1]
    assert eval_([1, 2]) | i == [1, 2]
    assert eval_([1, 2], [3, 4]) | i == [{0: 1, 1: 3}, {0: 2, 1: 4}]
    assert eval_(([1, 2], [3, 4]), [5, 6]) | i == [{"ts1": {0: 1, 1: 3}, "ts2": 5}, {"ts1": {0: 2, 1: 4}, "ts2": 6}]


def test_eval_and_assert():
    eval_([1, 2], [3, 4]) | binary_op(hgraph.add_) >> assert_(4, 6)


def test_pos():
    eval_([1, 2], [3, 4]) | +arrow(lambda x: x + 1) >> assert_(2, 3)


def test_neg():
    eval_([1, 2], [3, 4]) | -arrow(lambda x: x + 1) >> assert_(4, 5)


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
    eval_([1, 2, 3]) | fb["a" : TS[int], "default":0] >> add_ >> fb["a"] >> debug_print_("Test") >> assert_(1, 3, 6)
