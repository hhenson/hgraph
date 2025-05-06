import pytest

from hgraph import TS, graph, delayed_binding, const, Size, TSL, TSD, TSB, CompoundScalar, reduce, map_
from hgraph import pass_through_node
from hgraph.test import eval_node


def test_delayed_binding():
    @graph
    def g(v: TS[int]) -> TS[int]:
        value = delayed_binding(TS[int])  # create the delayed binding
        out = pass_through_node(value())  # Use the value
        value(const(1))  # Set the value
        return out + v

    assert eval_node(g, [1]) == [2]


def test_delayed_tsl_binding():
    @graph
    def g(v: TSL[TS[int], Size[2]]) -> TS[int]:
        value = delayed_binding(TSL[TS[int], Size[2]])  # create the delayed binding
        out = pass_through_node(value()[0] + value()[1])  # Use the value
        value(v)  # Set the value
        return out

    assert eval_node(g, [(1, 2)]) == [3]


def test_delayed_tsd_binding():
    class AB(CompoundScalar):
        a: int
        b: int

    @graph
    def g(v: TSD[str, TSB[AB]]) -> TS[int]:
        value = delayed_binding(v.output_type)  # create the delayed binding
        out = pass_through_node(map_(lambda x, y: x + y, value().a, value().b))  # Use the value
        o = reduce(lambda x, y: x + y, out, 0)
        value(v)  # Set the value
        return o

    assert eval_node(g, [{"a": {"a": 1, "b": 2}, "b": {"a": -1, "b": -2}}]) == [0]


def test_cycle():
    @graph
    def g(v: TS[int]) -> TS[int]:
        value = delayed_binding(TS[int])
        out = pass_through_node(value())
        sum = out + v
        value(sum)
        return out

    with pytest.raises(RuntimeError):
        assert eval_node(g, [1]) == [2]


def test_cycle_diamond():
    @graph
    def g(v: TS[int]) -> TS[int]:
        value = delayed_binding(TS[int])
        out = pass_through_node(value())
        sum1 = out + v
        sum2 = out + 2 * v
        value(sum1 + sum2)
        return out

    with pytest.raises(RuntimeError):
        assert eval_node(g, [1]) == [2]
