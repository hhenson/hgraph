from datetime import timedelta

import pytest

from hgraph import SIGNAL, TS, TSW, WINDOW_SIZE, WINDOW_SIZE_MIN, WiringError, compute_node, eval_node, graph, to_window


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


@pytest.mark.parametrize(("period", "minimum"), [(0, None), (-1, None), (3, -1), (3, 4)])
def test_tick_window_rejects_invalid_sizes(period, minimum):
    args = (period,) if minimum is None else (period, minimum)
    with pytest.raises(WiringError, match="to_window: .*period"):
        eval_node(to_window, [1, 2, 3], *args)


@pytest.mark.parametrize(
    ("period", "minimum"),
    [
        (timedelta(0), None),
        (timedelta(microseconds=-1), None),
        (timedelta(microseconds=3), timedelta(microseconds=-1)),
        (timedelta(microseconds=3), timedelta(microseconds=4)),
    ],
)
def test_duration_window_rejects_invalid_ranges(period, minimum):
    args = (period,) if minimum is None else (period, minimum)
    with pytest.raises(WiringError, match="to_window: .*period"):
        eval_node(to_window, [1, 2, 3], *args)
