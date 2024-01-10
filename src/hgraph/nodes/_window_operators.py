from collections import deque
from datetime import timedelta, datetime
from typing import TypeVar

from hgraph import TS, SCALAR, TimeSeriesSchema, compute_node, STATE, graph, TSB, SCHEDULER, TS_OUT, SIGNAL, NUMBER, \
    AUTO_RESOLVE
from hgraph.nodes import default, const, debug_print, sample
from hgraph.nodes._conditional import if_then_else
from hgraph.nodes._operators import cast_, take, drop

__all__ = ("window", "WindowResult", "lag", "accumulate", "rolling_average", "average", "count", "diff")


class WindowResult(TimeSeriesSchema):
    buffer: TS[tuple[SCALAR, ...]]
    index: TS[tuple[datetime, ...]]


WINDOW_SCALAR = TypeVar("WINDOW_SCALAR", int, timedelta)


@graph
def window(ts: TS[SCALAR], period: WINDOW_SCALAR, min_window_period: WINDOW_SCALAR = None) -> TSB[WindowResult]:
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
    raise NotImplementedError(f"No resolution found for window: ts: {ts.output_type}, window: {period}")


@graph
def lag(ts: TS[SCALAR], period: WINDOW_SCALAR) -> TS[SCALAR]:
    """
    Delays the delivery of an input by the period specified. This period can either be a number of ticks
    or a time-delta.

    When a time-delta is specified the value will be scheduled to be delivered at the receipt time + period.
    """
    raise NotImplementedError(f"No resolution found for lag: ts: {ts.output_type}, window: {period}")


@compute_node(overloads=window)
def cyclic_buffer_window(ts: TS[SCALAR], period: int, min_window_period: int = None, _state: STATE = None) \
        -> TSB[WindowResult]:
    buffer: deque[SCALAR] = _state.buffer
    index: deque[datetime] = _state.index
    buffer.append(ts.value)
    index.append(ts.last_modified_time)
    l = len(buffer)
    if l == period or (min_window_period is not None and l >= min_window_period):
        return {'buffer': tuple(buffer), 'index': tuple(index)}


@cyclic_buffer_window.start
def cyclic_buffer_window_start(period: int, _state: STATE):
    _state.buffer = deque[SCALAR](maxlen=period)
    _state.index = deque[datetime](maxlen=period)


@compute_node(overloads=window)
def time_delta_window(ts: TS[SCALAR], period: timedelta,
                      min_window_period: timedelta = None, _state: STATE = None) -> TSB[
    WindowResult]:
    buffer: deque[SCALAR] = _state.buffer
    index: deque[datetime] = _state.index
    buffer.append(ts.value)
    index.append(ts.last_modified_time)
    delta = index[-1] - index[0]
    is_full = delta >= period
    while delta > period:
        buffer.popleft()
        index.popleft()
        delta = index[-1] - index[0]
    if is_full or (min_window_period is not None and delta >= min_window_period):
        return {'buffer': tuple(buffer), 'index': tuple(index)}


@time_delta_window.start
def time_delta_window_start(_state: STATE):
    _state.buffer = deque[SCALAR]()
    _state.index = deque[datetime]()


@compute_node(overloads=lag)
def tick_lag(ts: TS[SCALAR], period: int, _state: STATE = None) -> TS[SCALAR]:
    buffer: deque[SCALAR] = _state.buffer
    try:
        if len(buffer) == period:
            return buffer.popleft()
    finally:
        buffer.append(ts.value)


@tick_lag.start
def tick_lag_start(period: int, _state: STATE):
    from collections import deque
    _state.buffer = deque[SCALAR](maxlen=period)


@compute_node(overloads=lag)
def time_delta_lag(ts: TS[SCALAR], period: timedelta, _scheduler: SCHEDULER = None, _state: STATE = None) -> TS[SCALAR]:
    # Uses the scheduler to keep track of when to deliver the values recorded in the buffer.
    buffer: deque[SCALAR] = _state.buffer
    if ts.modified:
        buffer.append(ts.value)
        _scheduler.schedule(ts.last_modified_time + period)

    if _scheduler.is_scheduled_now:
        return buffer.popleft()


@time_delta_lag.start
def time_delta_lag_start(_state: STATE):
    _state.buffer = deque[SCALAR]()


@compute_node
def accumulate(ts: TS[NUMBER], _output: TS_OUT[NUMBER] = None) -> TS[NUMBER]:
    """
    Performs a running sum of the time-series.
    """
    return _output.value + ts.value if _output.valid else ts.value


@compute_node
def count(ts: SIGNAL, _output: TS_OUT[int] = None) -> TS[int]:
    """
    Performs a running count of the number of times the time-series has ticked (i.e. emitted a value).
    """
    return _output.value + 1 if _output.valid else 1


@graph
def average(ts: TS[NUMBER], _tp: type[NUMBER] = AUTO_RESOLVE) -> TS[float]:
    """
    Computes the average of the time-series.
    This will either average by the number of ticks or by the time-delta.
    """
    return accumulate(ts) / (count(ts) if _tp is int else cast_(float, count(ts)))


@graph
def rolling_average(ts: TS[NUMBER], period: WINDOW_SCALAR, min_window_period: WINDOW_SCALAR = None) -> TS[float]:
    """
    Computes the rolling average of the time-series.
    This will either average by the number of ticks or by the time-delta.
    """
    raise NotImplementedError(f"rolling_average is not implemented for given inputs: "
                              f"ts: {str(ts.output_type)}, period: {type(period)}, min_windowPeriod: {type(min_window_period)}")


@graph(overloads=rolling_average)
def rolling_average_p_int(ts: TS[NUMBER], period: int, min_window_period: int = None,
                          _tp: type[NUMBER] = AUTO_RESOLVE) -> TS[float]:
    lagged_ts = lag(ts, period)
    current_value = accumulate(ts)
    delayed_value = accumulate(lagged_ts)
    denom = float(period) if _tp is float else period

    if min_window_period:
        count_ = drop(count(take(ts, period)), min_window_period-1)
        period_ = if_then_else(count_ < period, count_, period)
        delayed_value = default(delayed_value, sample(count_, 0.0 if _tp is float else 0))
        denom = cast_(float, period_) if _tp is float else period_

    return (current_value - delayed_value) / denom


@graph(overloads=rolling_average)
def rolling_average_p_time_delta(ts: TS[NUMBER], period: timedelta, min_window_period: timedelta = None,
                                 _tp: type[NUMBER] = AUTO_RESOLVE) -> TS[float]:
    lagged_ts = lag(ts, period)
    current_value = accumulate(ts)
    delayed_value = accumulate(lagged_ts)

    delayed_count = count(lagged_ts)
    if min_window_period:
        delayed_count = default(delayed_count, const(0, period))

    delta_ticks = count(ts) - delayed_count
    delta_ticks = if_then_else(delta_ticks == 0, float('NaN'), cast_(float, delta_ticks))

    delta_value = current_value - delayed_value
    return (delta_value if _tp is float else cast_(float, delta_value)) / delta_ticks


@graph
def diff(ts: TS[NUMBER]) -> TS[NUMBER]:
    """
    Computes the difference between the current value and the previous value in the time-series.
    """
    return ts - lag(ts, 1)
