import pytest

from hgraph import switch_, graph, TS
from hgraph.nodes import add_, sub_
from hgraph.test import eval_node


def test_switch():

    @graph
    def switch_test(key: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
        s = switch_({'add': add_, 'sub': sub_}, key, lhs, rhs)
        return s

    assert eval_node(switch_test, ['add', 'sub'], [1, 2], [3, 4]) == [4, -2]
