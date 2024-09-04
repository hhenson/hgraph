from datetime import datetime, date

from hgraph._types._time_series_types import TIME_SERIES_TYPE, SIGNAL
from hgraph._types._ts_type import TS
from hgraph._wiring._decorators import operator

__all__ = ("valid", "last_modified_date", "last_modified_time", "modified")


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
