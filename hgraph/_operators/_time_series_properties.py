from datetime import datetime, date, time
from typing import TypeVar

from hgraph._operators._operators import CmpResult
from hgraph._types._time_series_types import TIME_SERIES_TYPE, SIGNAL
from hgraph._types._ts_type import TS
from hgraph._wiring._decorators import operator

__all__ = ("valid", "last_modified_date", "last_modified_time", "modified", "evaluation_time_in_range", "TIME_TYPE")


@operator
def valid(ts: TIME_SERIES_TYPE, ts_value: TIME_SERIES_TYPE = None) -> TS[bool]:
    """
    Ticks with False when the ts is not valid and True when it is.
    """


@operator
def last_modified_time(ts: SIGNAL) -> TS[datetime]:
    """
    The datetime representing the last modified time of the time series.
    """


@operator
def last_modified_date(ts: SIGNAL) -> TS[date]:
    """
    The date component of the last modified time of the time series.
    """


@operator
def modified(ts: SIGNAL) -> TS[bool]:
    """
    True if the time-series has been modified in this engine cycle, False otherwise.
    This is a live time-series as such it will actively tick False on the next engine cycle
    if the value is not modified again.
    """


TIME_TYPE = TypeVar("TIME_TYPE", datetime, date, time)


@operator
def evaluation_time_in_range(start_time: TS[TIME_TYPE], end_time: TS[TIME_TYPE]) -> TS[CmpResult]:
    """
    Indicates if the current evaluation time is less that, within, or greater than the time range provided.

    It is possible to provide a pair to datetime values, or time, or dates.

    In the case of datetime and date ranges, this is a precise comparison.
    In the case of times, the comparison will vary depending on the time ordering. A time that is within a day (when
    start_time < end_time) then it is greater than after the last time of the current date (in UTC) otherwise it is less than.
    When end < start, then the comparison will also be either before or in the range.


    :param start_time: [datetime, date, time] The start time of the range.
    :param end_time: [datetime, date, time] The end time of the range.
    :return: CmpResult indicating if the engine time is less than, within, or greater than the time range.
    """
