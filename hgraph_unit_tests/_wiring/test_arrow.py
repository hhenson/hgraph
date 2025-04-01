from typing import Callable

from hgraph import TS, TSL, Size, graph, TSB, const
from hgraph.arrow import arrow
from hgraph.arrow._arrow import _TupleSchema, first, second, apply_, assoc, identity
from hgraph.test import eval_node


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
        return arrow(ts) > mult_3 >> add_5

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
        return arrow(ts1, ts2) > first >> arrow(lambda x: x * 3)

    assert eval_node(g, [1, 2], ["A", "B"]) == [3, 6]


def test_second():

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[str]:
        from hgraph import format_

        return arrow(ts1, ts2) > second >> arrow(lambda x: format_("{}_", x))

    assert eval_node(g, [1, 2], ["A", "B"]) == ["A_", "B_"]


def test_cross():
    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[_TupleSchema[TS[int], TS[str]]]:
        from hgraph import format_

        return arrow(ts1, ts2) > arrow(lambda x: x * 3) ** (lambda x: format_("{}_", x))

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"ts1": 3, "ts2": "A_"}, {"ts1": 6, "ts2": "B_"}]


def test_fan_out():
    @graph
    def g(ts1: TS[int]) -> TSL[TS[int], Size[2]]:
        return arrow(ts1) > arrow(lambda x: x * 3) / (lambda x: x + 1)

    assert eval_node(g, [1, 2]) == [{0: 3, 1: 2}, {0: 6, 1: 3}]


def test_apply_():
    @graph
    def g(ts1: TS[int]) -> TS[int]:
        return arrow(const(lambda x: x * 2), ts1) > apply_(TS[int])

    assert eval_node(g, [1, 2]) == [2, 4]


def test_assoc():

    @graph
    def g(
        ts1: TS[int], ts2: TS[float], ts3: TS[str]
    ) -> TSB[_TupleSchema[TS[int], TSB[_TupleSchema[TS[float], TS[str]]]]]:
        return arrow((ts1, ts2), ts3) > assoc

    assert eval_node(g, [1, 2], [1.0, 2.0], ["A", "B"]) == [
        {"ts1": 1, "ts2": {"ts1": 1.0, "ts2": "A"}},
        {"ts1": 2, "ts2": {"ts1": 2.0, "ts2": "B"}},
    ]


def test_identity():
    @graph
    def g(ts1: TS[int]) -> TS[int]:
        return arrow(ts1) > identity

    assert eval_node(g, [1, 2]) == [1, 2]


def test_first_():

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[_TupleSchema[TS[int], TS[str]]]:
        return arrow(ts1, ts2) > arrow(lambda x: x * 3) ** identity

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"ts1": 3, "ts2": "A"}, {"ts1": 6, "ts2": "B"}]


def test_second_():

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[_TupleSchema[TS[int], TS[str]]]:
        from hgraph import format_

        return arrow(ts1, ts2) > identity ** arrow(lambda x: format_("{}_", x))

    assert eval_node(g, [1, 2], ["A", "B"]) == [{"ts1": 1, "ts2": "A_"}, {"ts1": 2, "ts2": "B_"}]
