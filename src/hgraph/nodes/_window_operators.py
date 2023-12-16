from datetime import timedelta, datetime
from typing import TypeVar

from hgraph import TS, SCALAR, TimeSeriesSchema, compute_node, STATE, graph, TSB

__all__ = ("window", "WindowResult",)


class WindowResult(TimeSeriesSchema):
    buffer: TS[tuple[SCALAR, ...]]
    index: TS[tuple[datetime, ...]]


WINDOW_SCALAR = TypeVar("WINDOW_SCALAR", int, timedelta)


@graph
def window(ts: TS[SCALAR], period: WINDOW_SCALAR, wait_till_full: bool = True) -> TSB[WindowResult]:
    """
    Buffers the time-series. Emits a tuple of values representing the elements in the buffer.
    and a tuple of corresponding time-stamps representing the time-points at which the elements
    in the buffer correspond.

    When the window is an int, a cyclic buffer is created, if the window is a timedelta, then
    a deque is used to buffer the elements.

    Note with time-deltas the buffer will contain at most the elements that fit within the window so
    if you have 3 ticks at 1 microsecond intervals, and a window of 3 millisecond, then the buffer will
    not be full until the 4th tick.
    """
    raise NotImplementedError(f"No resolution found for buffer: ts: {ts.output_type}, window: {period}")


@compute_node(overloads=window)
def cyclic_buffer_window(ts: TS[SCALAR], period: int, wait_till_full: bool, state: STATE = None) -> TSB[WindowResult]:
    from collections import deque
    buffer: deque[SCALAR] = state.buffer
    index: deque[datetime] = state.index
    buffer.append(ts.value)
    index.append(ts.last_modified_time)
    if not wait_till_full or len(buffer) == period:
        return {'buffer': tuple(buffer), 'index': tuple(index)}


@cyclic_buffer_window.start
def cyclic_buffer_window_start(period: int, state: STATE):
    from collections import deque
    state.buffer = deque[SCALAR](maxlen=period)
    state.index = deque[datetime](maxlen=period)


@compute_node(overloads=window)
def time_delta_window(ts: TS[SCALAR], period: timedelta, wait_till_full: bool, state: STATE = None) -> TSB[WindowResult]:
    from collections import deque
    buffer: deque[SCALAR] = state.buffer
    index: deque[datetime] = state.index
    buffer.append(ts.value)
    index.append(ts.last_modified_time)
    is_full = index[-1] - index[0] >= period
    while index[-1] - index[0] > period:
        buffer.popleft()
        index.popleft()
    if not wait_till_full or is_full:
        return {'buffer': tuple(buffer), 'index': tuple(index)}


@time_delta_window.start
def time_delta_window_start(state: STATE):
    from collections import deque
    state.buffer = deque[SCALAR]()
    state.index = deque[datetime]()
