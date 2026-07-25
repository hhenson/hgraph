from datetime import timedelta

from hgraph import SIGNAL, TS, TSW, WINDOW_SIZE, WINDOW_SIZE_MIN, compute_node, eval_node, graph, to_window


@compute_node
def _snapshot(window: TSW[int, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TS[tuple[int, ...]]:
    return tuple(window.value)


@graph
def _tick_window(ts: TS[int], reset: SIGNAL) -> TS[tuple[int, ...]]:
    return _snapshot(to_window(ts, 3, 1, reset=reset))


@graph
def _duration_window(ts: TS[int], reset: SIGNAL) -> TS[tuple[int, ...]]:
    return _snapshot(to_window(ts, timedelta(microseconds=10), timedelta(microseconds=1), reset=reset))


def test_tick_window_reset_clears_before_a_later_source_tick():
    assert eval_node(
        _tick_window,
        [1, 2, None, 3, 4],
        [None, None, True, None, None],
    ) == [(1,), (1, 2), (), (3,), (3, 4)]


def test_tick_window_reset_clears_before_a_simultaneous_source_tick():
    assert eval_node(
        _tick_window,
        [1, 2, 3],
        [None, True, None],
    ) == [(1,), (2,), (2, 3)]


def test_duration_window_supports_the_same_reset_contract():
    assert eval_node(
        _duration_window,
        [1, 2, None, 3],
        [None, None, True, None],
    ) == [(1,), (1, 2), (), (3,)]
