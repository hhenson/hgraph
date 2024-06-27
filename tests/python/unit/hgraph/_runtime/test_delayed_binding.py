import pytest

from hgraph import compute_node, TS, graph, feedback, delayed_binding, const
from hgraph.nodes import pass_through
from hgraph.test import eval_node


def test_delayed_binding():

    @graph
    def g(v: TS[int]) -> TS[int]:
        # Allocate a new feedback instance
        value = delayed_binding(TS[int])  # create the delayed binding
        out = pass_through(value()) # Use the value
        value(const(1))  # Set the value
        return out + v

    assert eval_node(g, [1]) == [2]


def test_cycle():

    @graph
    def g(v: TS[int]) -> TS[int]:
        # Allocate a new feedback instance
        value = delayed_binding(TS[int])  # create the delayed binding
        out = pass_through(value())  # Use the value
        value(out)  # Set the value
        return out + v

    with pytest.raises(RuntimeError):
        assert eval_node(g, [1]) == [2]