from datetime import timedelta, datetime
from typing import Tuple

import numpy as np
import pytest

from hgraph import (
    TS,
    MIN_TD,
    graph,
    schedule,
    SIGNAL,
    sample,
    lag,
    resample,
    dedup,
    filter_,
    TSL,
    Size,
    throttle,
    take,
    drop,
    WindowResult,
    window,
    MIN_ST,
    TSB,
    gate,
    NodeException,
    batch,
    step,
    slice_,
    TSD,
    REMOVE,
    MIN_DT, Removed, TSS, to_window, SCALAR, compute_node, TSW, WindowSize, Array,
)
from hgraph.test import eval_node


def test_sample():
    @graph
    def g(signal: SIGNAL, ts: TS[int]) -> TS[int]:
        return sample(signal, ts)

    assert eval_node(g, [None, True, None, True], [1, 2, 3, 4, 5], resolution_dict={"signal": TS[bool]}) == [
        None,
        2,
        None,
        4,
        None,
    ]


def test_lag_tick():
    @graph
    def g(ts: TS[int], delay: int) -> TS[int]:
        return lag(ts, delay)

    assert eval_node(g, [1, 2, 3, 4, 5], 2) == [None, None, 1, 2, 3]


def test_lag_tick_tss():
    @graph
    def g(ts: TSS[int], delay: int) -> TSS[int]:
        return lag(ts, delay)

    assert eval_node(g, [{1}, {2}, {Removed(1)}, {4}, {Removed(4)}], 2) == [None, None, {1}, {2}, {Removed(1)}]


def test_lag_tick_tsd():
    @graph
    def g(ts: TSD[int, TS[int]], delay: int) -> TSD[int, TS[int]]:
        return lag(ts, delay)

    assert eval_node(g, [{1: 1}, {2: 2, 1: 2}, None, {1: REMOVE}, {4: 1}, {4: REMOVE}], 2) == [None, None, None, {1: 1}, {2: 2, 1: 2}, {1: REMOVE}]


def test_lag_timedelta():
    @graph
    def g(ts: TS[int], delay: timedelta) -> TS[int]:
        return lag(ts, delay)

    assert eval_node(g, [1, 2, 3, 4, 5], MIN_TD * 2) == [None, None, 1, 2, 3, 4, 5]


def test_schedule():
    @graph
    def g(delay: timedelta, initial_delay: bool = True, max_ticks: int = 1) -> TS[bool]:
        return schedule(delay, initial_delay, max_ticks)

    # TODO - test the times that the signals tick at
    assert eval_node(g, delay=MIN_TD, max_ticks=4, initial_delay=False) == [True, True, True, True]

    # The generator node seems to tick out None for the time every engine cycle where there is no generator value
    assert eval_node(g, delay=MIN_TD, max_ticks=4, initial_delay=True) == [None, True, True, True, True]

    assert eval_node(g, delay=MIN_TD, max_ticks=1, initial_delay=False) == [True]


def test_schedule_ts():
    @graph
    def g(delay: TS[timedelta], initial_delay: bool = True, max_ticks: int = 1) -> TS[bool]:
        return schedule(delay, initial_delay=initial_delay, max_ticks=max_ticks)

    assert eval_node(g, delay=MIN_TD * 2, max_ticks=2, initial_delay=False) == [True, None, True]

    assert eval_node(g, delay=[MIN_TD, None, None, MIN_TD * 2], max_ticks=4, initial_delay=True) == [
        None,
        True,
        True,
        None,
        None,
        True,
        None,
        True,
    ]

    assert eval_node(g, delay=MIN_TD, max_ticks=1, initial_delay=False) == [True]


def test_schedule_ts_with_start():
    @graph
    def g(delay: TS[timedelta], start: TS[datetime], initial_delay: bool = True, max_ticks: int = 1) -> TS[bool]:
        return schedule(delay, start=start, initial_delay=initial_delay, max_ticks=max_ticks)

    assert eval_node(g, delay=MIN_TD * 2, start=MIN_ST + MIN_TD*3, max_ticks=2, initial_delay=False) == [True, None, None, True]

    assert eval_node(g, delay=[MIN_TD, None, None, MIN_TD * 2], start=MIN_DT + MIN_TD*2, max_ticks=4, initial_delay=True) == [
        None,
        None,
        True,
        None,
        None,
        True,
        None,
        True,
        None,
        True,
    ]


def test_resample():
    @graph
    def g(ts: TS[int], period: timedelta) -> TS[int]:
        return resample(ts, period)

    assert eval_node(g, [1], 2 * MIN_TD, __end_time__=MIN_ST + 10 * MIN_TD) == [
        None,
        None,
        1,
        None,
        1,
        None,
        1,
        None,
        1,
    ]
    assert eval_node(g, [1, 2, 3, 4, 5, 6], 2 * MIN_TD, __end_time__=MIN_ST + 10 * MIN_TD) == [
        None,
        None,
        3,
        None,
        5,
        None,
        6,
        None,
        6,
    ]


def test_drop_dups():
    @graph
    def g_int(ts: TS[int]) -> TS[int]:
        return dedup(ts)

    @graph
    def g_float(ts: TS[float], abs_tol: float) -> TS[float]:
        return dedup(ts, abs_tol=abs_tol)

    assert eval_node(g_int, [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]) == [1, 2, None, 3, None, None, 4, None, None, None]

    assert eval_node(g_float, [1.0, 2.0, 2.0, 3.0, 3.0 + 1e-15, 3.0, 4.0, 4.0, 4.0, 4.0, 4.00001], abs_tol=1e-15) == [
        1.0,
        2.0,
        None,
        3.0,
        None,
        None,
        4.0,
        None,
        None,
        None,
        4.00001,
    ]


def test_drop_dups_tsd():
    @graph
    def g(ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return dedup(ts)

    assert eval_node(g, [{1: 1, 2: 2}, {1: 1, 2: 3}, {1: REMOVE, 2: 3}]) == [{1: 1, 2: 2}, {2: 3}, {1: REMOVE}]


def test_drop_dups_tsl():
    @graph
    def g(ts: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return dedup(ts)

    assert eval_node(g, [{0: 1}, {0: 1, 1: 2}, None, {0: 1, 1: 3}]) == [{0: 1}, {1: 2}, None, {1: 3}]


def test_filter_int():
    @graph
    def g(condition: TS[bool], ts: TS[int]) -> TS[int]:
        return filter_(condition, ts)

    assert eval_node(g, [True, False, False, True, None], [1, 2, 3, None, 4]) == [1, None, None, 3, 4]


def test_filter_tsd():
    @graph
    def g(condition: TS[bool], ts: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return filter_(condition, ts)

    assert eval_node(g, [True, False, None, True], [{1: 1}, {2: 2, 1: 2}, {1: REMOVE}, {3: 3}, None, {0: 5}]) == [
        {1: 1},
        None,
        None,
        {3: 3, 2: 2, 1: REMOVE},
        None,
        {0: 5},
    ]


def test_filter_tss():
    @graph
    def g(condition: TS[bool], ts: TSS[int]) -> TSS[int]:
        return filter_(condition, ts)

    assert eval_node(g, [True, False, None, True], [{1}, {2, Removed(1)}, None, {3}, {4}]) == [{1}, None, None, {3, 2, Removed(1)}, {4}]


def test_filter_tsl():
    @graph
    def g(condition: TS[bool], ts: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return filter_(condition, ts)

    assert eval_node(g, [True, False, None, True], [(1, 1), (2, 2), {1: 3}, None, {0: 5}]) == [
        {0: 1, 1: 1},
        None,
        None,
        {0: 2, 1: 3},
        {0: 5},
    ]


def test_throttle():
    @graph
    def g(ts: TS[int], period: timedelta) -> TS[int]:
        return throttle(ts, period)

    assert eval_node(g, [1, 1, 2, 3, 5, 2, 1], 2 * MIN_TD, __end_time__=MIN_ST + 10 * MIN_TD
                     ) == [1, None, 2, None, 5, None, 1]


def test_throttle_tsd():
    @graph
    def g(ts: TSD[int, TS[int]], period: timedelta) -> TSD[int, TS[int]]:
        return throttle(ts, period)

    assert eval_node(g, [None, {1: 1}, {2: 2}, {1: 2}, None, {2: REMOVE}, {1: REMOVE}, {1: 1}], 3 * MIN_TD, __end_time__=MIN_ST + 10 * MIN_TD
                     ) == [None, {1: 1}, None, None, {1: 2, 2: 2}, None, None, {2: REMOVE, 1: 1}]


def test_throttle_tsd_delay_first():
    @graph
    def g(ts: TSD[int, TS[int]], period: timedelta) -> TSD[int, TS[int]]:
        return throttle(ts, period, delay_first_tick=True)

    assert eval_node(g, [None, {1: 1}, {2: 2}, {1: 2}, None, {2: REMOVE}, {1: REMOVE}, {1: 1}], 3 * MIN_TD, __end_time__=MIN_ST + 10 * MIN_TD
                     ) == [None, None, None, None, {1: 2, 2: 2}, None, None, {2: REMOVE, 1: 1}]


def test_take():
    @graph
    def g(ts: TS[int], count: int) -> TS[int]:
        return take(ts, count)

    assert eval_node(g, [1, 2, 3, 4, 5], 3) == [1, 2, 3, None, None]


def test_drop():
    @graph
    def g(ts: TS[int], count: int):
        return drop(ts, count)

    assert eval_node(drop, [1, 2, 3, 4, 5], 3) == [None, None, None, 4, 5]


def test_window_cyclic_buffer():
    @graph
    def g(ts: TS[int], period: int) -> TSB[WindowResult]:
        return window(ts, period)

    expected = [
        None,
        None,
        {"buffer": (1, 2, 3), "index": (MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD)},
        {"buffer": (2, 3, 4), "index": (MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD)},
        {"buffer": (3, 4, 5), "index": (MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD, MIN_ST + 4 * MIN_TD)},
    ]

    assert eval_node(g, [1, 2, 3, 4, 5], 3) == expected


def test_window_cyclic_buffer_with_min_window_period():
    @graph
    def g(ts: TS[int], period: int, min_window_period: int) -> TSB[WindowResult]:
        return window(ts, period, min_window_period)

    expected = [
        {"buffer": (1,), "index": (MIN_ST,)},
        {
            "buffer": (
                1,
                2,
            ),
            "index": (
                MIN_ST,
                MIN_ST + MIN_TD,
            ),
        },
        {"buffer": (1, 2, 3), "index": (MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD)},
        {"buffer": (2, 3, 4), "index": (MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD)},
        {"buffer": (3, 4, 5), "index": (MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD, MIN_ST + 4 * MIN_TD)},
    ]

    assert eval_node(g, [1, 2, 3, 4, 5], 3, 1) == expected


def test_window_timedelta():
    @graph
    def g(ts: TS[int], period: timedelta, min_window_period: timedelta) -> TSB[WindowResult]:
        return window(ts, period, min_window_period)

    expected = [
        None,
        {
            "buffer": (
                1,
                2,
            ),
            "index": (
                MIN_ST,
                MIN_ST + MIN_TD,
            ),
        },
        {"buffer": (1, 2, 3), "index": (MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD)},
        {"buffer": (2, 3, 4), "index": (MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD)},
        {"buffer": (3, 4, 5), "index": (MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD, MIN_ST + 4 * MIN_TD)},
    ]

    assert eval_node(g, [1, 2, 3, 4, 5], MIN_TD * 2, MIN_TD) == expected

def test_to_window_delta():
    result = eval_node(to_window[SCALAR: int], [1, 2, 3, 4, 5], 3)
    assert result == [None, None, 3, 4, 5]

def test_to_window_delta_td():
    result = eval_node(to_window[SCALAR: int], [1, 2, 3, 4, 5], MIN_TD * 2)
    assert result == [None, None, 3, 4, 5]

def test_to_window_value():

    @compute_node
    def _as_value(ts: TSW[int, WindowSize[3]]) -> TS[Array[int, Size[3]]]:
        return ts.value

    @graph
    def g(ts: TS[int]) -> TS[Array[int, Size[3]]]:
        return _as_value(to_window(ts, 3))

    result = eval_node(g, [1, 2, 3, 4, 5])
    assert all((a == b).all() for a, b in zip(
        result,
        [None, None, np.array((1, 2, 3)), np.array((2, 3, 4)), np.array((3, 4, 5))]) if
               not (a is None and b is None))

def test_to_window_value_td():

    @compute_node
    def _as_value(ts: TSW[int, WindowSize[MIN_TD * 2]]) -> TS[Array[int, Size[-1]]]:
        return ts.value

    @graph
    def g(ts: TS[int]) -> TS[Array[int, Size[-1]]]:
        return _as_value(to_window(ts, MIN_TD * 2))

    result = eval_node(g, [1, 2, 3, 4, 5])
    assert all((a == b).all() for a, b in zip(
        result,
        [None, None, np.array((1, 2, 3)), np.array((2, 3, 4)), np.array((3, 4, 5))]) if
               not (a is None and b is None))

def test_gate():
    @graph
    def g(condition: TS[bool], ts: TS[int], delay: timedelta) -> TS[int]:
        return gate(condition, ts, delay)

    assert eval_node(g, [False, True], [1, 2, 3, 4], MIN_TD) == [None, 1, 2, 3, 4]
    assert eval_node(g, [False, True], [1, 2, 3, 4], 2 * MIN_TD) == [None, 1, None, 2, None, 3, None, 4]


def test_gate_with_buffer_overflow():
    @graph
    def g(condition: TS[bool], ts: TS[int], delay: timedelta, buffer_length: int) -> TS[int]:
        return gate(condition, ts, delay, buffer_length)

    with pytest.raises(NodeException):
        assert eval_node(g, [False], [1, 2], MIN_TD, 1) == [1, 2]


def test_batch():
    @graph
    def g(condition: TS[bool], ts: TS[int], delay: timedelta) -> TS[Tuple[int, ...]]:
        return batch(condition, ts, delay)

    assert eval_node(g, [False, True], [1, 2, 3, 4], MIN_TD) == [None, (1, 2), (3,), (4,)]
    assert eval_node(g, [False, True], [1, 2, 3, 4], 2 * MIN_TD) == [None, (1, 2), None, (3, 4)]


def test_batch_with_buffer_overflow():
    @graph
    def g(condition: TS[bool], ts: TS[int], delay: timedelta, buffer_length: int) -> TS[Tuple[int, ...]]:
        return batch(condition, ts, delay, buffer_length)

    with pytest.raises(NodeException):
        assert eval_node(g, [False], [1, 2], MIN_TD, 1) == [1, 2]


def test_step():
    @graph
    def g(ts: TS[int], step_size: int) -> TS[int]:
        return step(ts, step_size)

    assert eval_node(g, [1, 2, 3, 4, 5, 6, 7, 8], 2) == [1, None, 3, None, 5, None, 7, None]


def test_slice_():
    @graph
    def g(ts: TS[int], start: int, stop: int, step_size: int) -> TS[int]:
        return slice_(ts, start, stop, step_size)

    assert eval_node(g, [0, 1, 2, 3, 4, 5, 6, 7, 8], 0, 4, 1) == [0, 1, 2, 3, None, None, None, None, None]
    assert eval_node(g, [0, 1, 2, 3, 4, 5, 6, 7, 8], 2, 6, 2) == [None, None, 2, None, 4, None, None, None, None]
    assert eval_node(g, [0, 1, 2, 3, 4, 5, 6, 7, 8], 2, -1, 2) == [None, None, 2, None, 4, None, 6, None, 8]
    assert eval_node(g, [0, 1, 2, 3, 4, 5, 6, 7, 8], -1, -1, 2) == None
    assert eval_node(g, [0, 1, 2, 3, 4, 5, 6, 7, 8], 2, 0, 2) == None
