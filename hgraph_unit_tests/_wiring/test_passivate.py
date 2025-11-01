from hgraph import graph, TS, passive
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke


def test_passivate():

    @graph
    def g(ts1: TS[int], ts2: TS[int]) -> TS[int]:
        return passive(ts1) + ts2

    assert eval_node(g, [None, 1, None, 2, 3], [1, None, 2, None, 4]) == [None, None, 3, None, 7]
