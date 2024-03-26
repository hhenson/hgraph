from collections import deque
from dataclasses import dataclass
from datetime import timedelta
from typing import TypeVar

from hgraph import compute_node, TS, STATE, TIME_SERIES_TYPE, graph, TSL, SIZE, NUMBER, AUTO_RESOLVE, reduce, add_, TSD, \
    K, TS_OUT, SIGNAL, SCALAR, SCHEDULER, CompoundScalar
from hgraph.nodes._operators import cast_, len_

__all__ = (
    "ewma", "center_of_mass_to_alpha", "span_to_alpha", "mean", "clip", "count", "sum_", "accumulate", "lag", "diff",
    "INT_OR_TIME_DELTA", "average", "sum_collection", "pct_change")


INT_OR_TIME_DELTA = TypeVar("INT_OR_TIME_DELTA", int, timedelta)


@dataclass
class EwmaState(CompoundScalar):
    s_prev: float = None
    count: int = 0


@compute_node
def ewma(ts: TS[float], alpha: float, min_periods: int = 0, _state: STATE[EwmaState] = None) -> TS[float]:
    """Exponential Weighted Moving Average"""
    x_t = ts.value
    s_prev = _state.s_prev

    if s_prev is None:
        s = x_t
    else:
        s = s_prev + alpha * (x_t - s_prev)

    _state.s_prev = s
    _state.count += 1

    if _state.count > min_periods:
        return s


@ewma.start
def ewma_start(alpha: float):
    if not (0.0 <= alpha <= 1.0):
        raise ValueError("alpha must be between 0 and 1")


def center_of_mass_to_alpha(com: float) -> float:
    if com <= 0:
        raise ValueError(f"Center of mass must be positive, got {com}")
    return 1.0 / (com + 1.0)


def span_to_alpha(span: float) -> float:
    if span <= 0:
        raise ValueError(f"Span must be positive, got {span}")
    return 2.0 / (span + 1.0)


@graph
def mean(ts: TIME_SERIES_TYPE) -> TS[float]:
    """
    The mean of the values at point in time.
    For example:

        ``mean(TSL[TS[float], SIZE) ``

    will produce the mean of the values of the time-series list each time the list changes
    """
    raise NotImplementedError(f"No implementation found for {ts.output_type}")


@graph(overloads=mean)
def tsl_mean(ts: TSL[TS[NUMBER], SIZE], _sz: type[SIZE] = AUTO_RESOLVE,
             _num_tp: type[NUMBER] = AUTO_RESOLVE) -> TS[float]:
    numerator = reduce(add_, ts, 0.0 if _num_tp is float else 0)
    if _num_tp is int:
        numerator = cast_(float, numerator)
    return numerator / float(_sz.SIZE)


@graph(overloads=mean)
def tsd_mean(ts: TSD[K, TS[NUMBER]], _num_tp: type[NUMBER] = AUTO_RESOLVE) -> TS[float]:
    numerator = reduce(add_, ts, 0.0 if _num_tp is float else 0)
    if _num_tp is int:
        numerator = cast_(float, numerator)
    return numerator / cast_(float, len_(ts))


@compute_node
def clip(ts: TS[NUMBER], min_: NUMBER, max_: NUMBER) -> TS[NUMBER]:
    v = ts.value
    if v < min_:
        return min_
    if v > max_:
        return max_
    return v


@clip.start
def clip_start(min_: NUMBER, max_: NUMBER):
    if min_ < max_:
        return
    raise RuntimeError(f"clip given min: {min_}, max: {max_}, but min is not < max")


@graph
def sum_(ts: TIME_SERIES_TYPE) -> TS[NUMBER]:
    """
    The sum of the values in the time-series
    """
    raise NotImplementedError(f"No implementation found for {ts.output_type}")


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
def lag(ts: TIME_SERIES_TYPE, period: INT_OR_TIME_DELTA) -> TIME_SERIES_TYPE:
    """
    Delays the delivery of an input by the period specified. This period can either be a number of ticks
    or a time-delta.

    When a time-delta is specified the value will be scheduled to be delivered at the receipt time + period.
    """
    raise NotImplementedError(f"No resolution found for lag: ts: {ts.output_type}, window: {period}")


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
def diff(ts: TS[NUMBER]) -> TS[NUMBER]:
    """
    Computes the difference between the current value and the previous value in the time-series.
    """
    return ts - lag(ts, 1)


@compute_node(overloads=sum_)
def sum_collection(ts: TS[tuple[NUMBER, ...]]) -> TS[NUMBER]:
    return sum(ts.value)


@graph
def pct_change(ts: TS[NUMBER]) -> TS[NUMBER]:
    """
    pct_change = (ts.value - ts_1.value) / ts_1.value
    The result is in fractional percentage values
    """
    l = lag(ts, period=1)
    return (ts - l) / l


