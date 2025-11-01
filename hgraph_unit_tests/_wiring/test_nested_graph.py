import pytest
from hgraph import graph, debug_print, const, nested_graph, sink_node, SIGNAL, TS
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
