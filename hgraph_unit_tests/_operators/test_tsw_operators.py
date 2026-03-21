from hgraph import graph, TS, sum_, to_window, abs_, TSW, mean, min_, max_
from hgraph._runtime._constants import MIN_TD
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke

def test_tsw_sum():

    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, 3, 1)
        return sum_(window)

    assert eval_node(g, [1, 2, 3]) == [1, 3, 6]


def test_tsw_abs():

    @graph
    def g(ts: TS[int]) -> TSW[int]:
        window = to_window(ts, 3, 1)
        return abs_(window)

    assert eval_node(g, [1, -2, 3]) == [1, 2, 3]


def test_tsw_mean():
    @graph
    def g(ts: TS[int]) -> TS[float]:
        window = to_window(ts, 3, 3)
        return mean(window)

    import numpy as np
    assert eval_node(g, [1, -2, 3, 4]) == [None, None, np.mean([1, -2, 3]), np.mean([-2, 3, 4])]


def test_tsw_min():
    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, 3, 3)
        return min_(window, default_value=0)

    assert eval_node(g, [None, 1, -2, 3, 4]) == [None, None, None, -2, -2]


def test_tsw_min_time_period_with_default():
    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, MIN_TD * 2)
        return min_(window, default_value=99)

    assert eval_node(g, [1, 2, 3, 4]) == [99, 99, 1, 2]


def test_tsw_max():
    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, 3, 3)
        return max_(window)

    assert eval_node(g, [1, -2, 3, 4]) == [None, None, 3, 4]


def test_tsw_max_time_period_with_default():
    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, MIN_TD * 2)
        return max_(window, default_value=99)

    assert eval_node(g, [1, 2, 3, 4]) == [99, 99, 3, 4]
