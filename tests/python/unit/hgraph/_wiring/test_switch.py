import pytest

from hgraph import switch_, graph, TS, SCALAR
from hgraph.nodes import add_, sub_, const
from hgraph.test import eval_node


def test_switch():

    @graph
    def switch_test(key: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
        s = switch_({'add': add_, 'sub': sub_}, key, lhs, rhs)
        return s

    assert eval_node(switch_test, ['add', 'sub'], [1, 2], [3, 4]) == [4, -2]


def test_switch_with_graph():

    @graph
    def graph_1(value: SCALAR) -> TS[SCALAR]:
        return const(f"{value}_1")

    @graph
    def graph_2(value: SCALAR) -> TS[SCALAR]:
        return const(f"{value}_2")

    @graph
    def switch_test(key: TS[str], value: SCALAR) -> TS[SCALAR]:
        return switch_({'one': graph_1, 'two': graph_2}, key, value)

    assert eval_node(switch_test, ['one', 'two'], "test") == ["test_1", "test_2"]
