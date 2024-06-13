from collections import deque
from dataclasses import dataclass
from datetime import timedelta
from typing import TypeVar

from hgraph import compute_node, TS, STATE, TIME_SERIES_TYPE, graph, TSL, SIZE, NUMBER, SCALAR, SCHEDULER, \
    operator

__all__ = ("center_of_mass_to_alpha", "span_to_alpha", "lag", "INT_OR_TIME_DELTA", "pct_change")


INT_OR_TIME_DELTA = TypeVar("INT_OR_TIME_DELTA", int, timedelta)


def center_of_mass_to_alpha(com: float) -> float:
    if com <= 0:
        raise ValueError(f"Center of mass must be positive, got {com}")
    return 1.0 / (com + 1.0)


def span_to_alpha(span: float) -> float:
    if span <= 0:
        raise ValueError(f"Span must be positive, got {span}")
    return 2.0 / (span + 1.0)


@operator
def lag(ts: TIME_SERIES_TYPE, period: INT_OR_TIME_DELTA) -> TIME_SERIES_TYPE:
    """
    Delays the delivery of an input by the period specified. This period can either be a number of ticks
    or a time-delta.

    When a time-delta is specified the value will be scheduled to be delivered at the receipt time + period.
    """


@graph(overloads=lag)
def tsl_lag(ts: TSL[TIME_SERIES_TYPE, SIZE], period: INT_OR_TIME_DELTA) -> TSL[TIME_SERIES_TYPE, SIZE]:
    return TSL.from_ts(lag(ts_, period) for ts_ in ts.values())


@dataclass
class LagState:
    buffer: deque = None


@compute_node(overloads=lag)
def tick_lag(ts: TS[SCALAR], period: int, _state: STATE[LagState] = None) -> TS[SCALAR]:
    buffer: deque[SCALAR] = _state.buffer
    try:
        if len(buffer) == period:
            return buffer.popleft()
    finally:
        buffer.append(ts.value)


@tick_lag.start
def tick_lag_start(period: int, _state: STATE[LagState]):
    _state.buffer = deque[SCALAR](maxlen=period)


@compute_node(overloads=lag)
def time_delta_lag(ts: TS[SCALAR], period: timedelta, _scheduler: SCHEDULER = None,
                   _state: STATE[LagState] = None) -> TS[SCALAR]:
    # Uses the scheduler to keep track of when to deliver the values recorded in the buffer.
    buffer: deque[SCALAR] = _state.buffer
    if ts.modified:
        buffer.append(ts.value)
        _scheduler.schedule(ts.last_modified_time + period)

    if _scheduler.is_scheduled_now:
        return buffer.popleft()


@time_delta_lag.start
def time_delta_lag_start(_state: STATE[LagState]):
    _state.buffer = deque[SCALAR]()


@graph
def pct_change(ts: TS[NUMBER]) -> TS[NUMBER]:
    """
    pct_change = (ts.value - ts_1.value) / ts_1.value
    The result is in fractional percentage values
    """
    l = lag(ts, period=1)
    return (ts - l) / l


