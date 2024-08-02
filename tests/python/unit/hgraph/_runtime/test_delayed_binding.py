import pytest

from hgraph import compute_node, TS, graph, feedback, delayed_binding, const
from hgraph.nodes import pass_through
from hgraph.test import eval_node


def test_delayed_binding():

    @graph
    def g(v: TS[int]) -> TS[int]:
        # Allocate a new feedback instance
        value = delayed_binding(TS[int])  # create the delayed binding
        out = pass_through(value())  # Use the value
        value(const(1))  # Set the value
        return out + v

    assert eval_node(g, [1]) == [2]


def test_cycle():
    @graph
    def g(v: TS[int]) -> TS[int]:
        value = delayed_binding(TS[int])
        out = pass_through(value())
        sum = out + v
        value(sum)
        return out

    with pytest.raises(RuntimeError):
        assert eval_node(g, [1]) == [2]


def test_cycle_diamond():
    @graph
    def g(v: TS[int]) -> TS[int]:
        value = delayed_binding(TS[int])
        out = pass_through(value())
        sum1 = out + v
        sum2 = out + 2 * v
        value(sum1 + sum2)
        return out

    with pytest.raises(RuntimeError):
        assert eval_node(g, [1]) == [2]
