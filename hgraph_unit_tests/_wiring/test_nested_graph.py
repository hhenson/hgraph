import pytest
from hgraph import graph, debug_print, const, nested_graph, sink_node, SIGNAL, TS, TSB, TimeSeriesSchema
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke

@pytest.mark.skip(reason="A node with no inputs or outputs gets dropped")
def test_nested_graph():
    side_effect_value = False

    @sink_node
    def side_effect(s: SIGNAL):
        nonlocal side_effect_value
        side_effect_value = True

    @graph
    def g():
        side_effect(const(1))

    @graph
    def h():
        nested_graph(g)

    assert eval_node(h) == None
    assert side_effect_value == True


def test_nested_graph_outputs():
    @graph
    def g() -> TS[int]:
        return const(1)

    @graph
    def h() -> TS[int]:
        return nested_graph(g)

    assert eval_node(h) == [1]


def test_nested_graph_compute():
    @graph
    def g(s: TS[int]) -> TS[int]:
        return s + 1

    @graph
    def h() -> TS[int]:
        return nested_graph(g, const(1))

    assert eval_node(h) == [2]


def test_nested_graph_sink():
    side_effect_value = False

    @sink_node
    def side_effect(s: SIGNAL):
        nonlocal side_effect_value
        side_effect_value = True

    @graph
    def g(s: TS[int]):
        side_effect(s)

    @graph
    def h():
        nested_graph(g, const(1))

    assert eval_node(h) == None
    assert side_effect_value == True


def test_nested_graph_preserves_input_subpaths():
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(ts: TSB[AB]) -> TS[int]:
        return ts.a + ts.b

    @graph
    def h(ts: TSB[AB]) -> TS[int]:
        return nested_graph(g, ts)

    assert eval_node(h, ts=[{"a": 1, "b": 2}, {"a": 3, "b": 4}]) == [3, 7]
