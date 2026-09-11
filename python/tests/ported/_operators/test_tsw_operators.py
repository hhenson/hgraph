from hgraph import graph, TS, sum_, to_window, abs_, TSW, mean, min_, max_
from hgraph import MIN_TD
from hgraph.test import eval_node




def test_tsw_sum():

    @graph
    def g(ts: TS[int]) -> TS[int]:
        window = to_window(ts, 3, 1)
        return sum_(window)

    assert eval_node(g, [1, 2, 3]) == [1, 3, 6]


def test_tsw_sum_is_a_running_sum_below_the_minimum_window():
    """``sum_`` over a window does NOT wait for the minimum window period.

    Upstream's ``sum_tsw`` carries no ``all_valid`` gate and maintains a
    running total from the first tick, so it answers 1 for a window holding
    just [1] even when the minimum is 2. Its neighbours ``mean``/``min_``/
    ``max_`` DO carry ``all_valid=("ts",)`` -- an average over fewer points
    than were asked for is not the average that was asked for -- which is why
    only ``sum_`` behaves this way (parity #857).
    """

    @graph
    def g(ts: TS[int]) -> TS[int]:
        return sum_(to_window(ts, 2, min_window_period=2))

    # A single tick, below the minimum of 2: still summed.
    assert eval_node(g, [0]) == [0]
    assert eval_node(g, [5]) == [5]
    assert eval_node(g, [1, 2, 3]) == [1, 3, 5]


def test_tsw_mean_still_waits_for_the_minimum_window():
    """The guard for the above: the gate is dropped for ``sum_`` only."""

    @graph
    def g(ts: TS[int]) -> TS[float]:
        return mean(to_window(ts, 3, min_window_period=2))

    assert eval_node(g, [1, 2, 3, 4]) == [None, 1.5, 2.0, 3.0]


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
