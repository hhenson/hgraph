import sys
from datetime import timedelta, datetime
from typing import TypeVar, Generic, Tuple

from hgraph._types import TS, TIME_SERIES_TYPE, SIGNAL, SCALAR, TSB, TimeSeriesSchema
from hgraph._wiring._decorators import operator

__all__ = (
    "sample",
    "lag",
    "schedule",
    "resample",
    "dedup",
    "filter_",
    "throttle",
    "INT_OR_TIME_DELTA",
    "take",
    "drop",
    "window",
    "WindowResult",
    "gate",
    "batch",
    "step",
    "slice_",
    "drop_dups",
)

INT_OR_TIME_DELTA = TypeVar("INT_OR_TIME_DELTA", int, timedelta)


@operator
def sample(signal: SIGNAL, ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Snaps the value of a time series (ts) on a tick from another time series (signal).
    """


@operator
def lag(ts: TIME_SERIES_TYPE, period: INT_OR_TIME_DELTA) -> TIME_SERIES_TYPE:
    """
    Delays the delivery of an input by the period specified. This period can either be a number of ticks
    or a time-delta.

    When a time-delta is specified the value will be scheduled to be delivered at the receipt time + period.
    """


@operator
def schedule(delay: timedelta, initial_delay: bool = True, max_ticks: int = sys.maxsize) -> TS[bool]:
    """
    Generates regular ticks in the graph that tick at the specified delay. For example,
    ``schedule(timedelta(seconds=3))`` will produce a time series of type TS[bool] that will tick True every three
    seconds. The initial_delay parameter specifies whether the first tick should be delayed by the delay time or not and
    max_ticks specifies the maximum number of ticks to produce.
    """


@operator
def resample(ts: TIME_SERIES_TYPE, period: timedelta) -> TIME_SERIES_TYPE:
    """
    Resamples the time series to tick at the specified period. For example, ``resample(ts, timedelta(seconds=3))`` will
    produce a time series of ts that ticks every three seconds. This will always tick at the specified delay, even if
    the input time series does not tick.
    """


@operator
def dedup(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Drops duplicate values from a time-series.
    """


# For backwards compatibility.  Prefer dedup
drop_dups = dedup


@operator
def filter_(condition: TS[bool], ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Suppresses ticks of a time series when the condition time series' value is False
    """


@operator
def throttle(ts: TIME_SERIES_TYPE, period: timedelta) -> TIME_SERIES_TYPE:
    """
    Reduces the rate of ticks in a time series to the given period. It works like ``resample`` if the rate of ticks is
    higher than the period but unlike ``resample`` does not produce ticks when the source time series does not tick.
    """


@operator
def take(ts: TIME_SERIES_TYPE, count: INT_OR_TIME_DELTA) -> TIME_SERIES_TYPE:
    """
    filters out all ticks the input time series after ``count`` initial ticks or the given time series
    """


@operator
def drop(ts: TIME_SERIES_TYPE, count: INT_OR_TIME_DELTA) -> TIME_SERIES_TYPE:
    """
    Drops the first `count` ticks and then returns the remainder of the ticks
    """


class WindowResult(TimeSeriesSchema, Generic[SCALAR]):
    buffer: TS[tuple[SCALAR, ...]]
    index: TS[tuple[datetime, ...]]


@operator
def window(ts: TS[SCALAR], period: INT_OR_TIME_DELTA, min_window_period: INT_OR_TIME_DELTA = None) -> TSB[WindowResult]:
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


@operator
def gate(condition: TS[bool], ts: TIME_SERIES_TYPE, delay: timedelta, buffer_length: int) -> TIME_SERIES_TYPE:
    """
    Queues up ticks of a time series when the value of ``condition`` if ``False``. Once it turns `True` the queued up
    ticks are released one by one with the given delay between them. A ``RuntimeError`` is raised if the buffer exceeds
    the given buffer_length.
    """


@operator
def batch(condition: TS[bool], ts: TS[SCALAR], delay: timedelta, buffer_length: int) -> TS[Tuple[SCALAR, ...]]:
    """
    Queues up ticks of a time series when the value of ``condition`` if ``False``. Once it turns `True` the queued up
    ticks are released in batches with a given delay between each batch. A ``RuntimeError`` is raised if the buffer
    exceeds the given buffer_length.
    """


@operator
def step(ts: TIME_SERIES_TYPE, step_size: int) -> TIME_SERIES_TYPE:
    """
    Steps the time series by the specified number of ticks. This will skip ticks in the time series.
    """


@operator
def slice_(ts: TIME_SERIES_TYPE, start: INT_OR_TIME_DELTA, stop: INT_OR_TIME_DELTA, step_size: int) -> TIME_SERIES_TYPE:
    """
    Slices the time series from the start to the stop index stepped by the step size. Essentially combines ``drop``,
    ``take`` and ``step`` into one operation. It works like python's slice operator but along the ticks or time axis.
    """
