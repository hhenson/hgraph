from abc import ABC, abstractmethod
from datetime import timedelta, datetime
from typing import Generic, Any

from hgraph._types._scalar_types import SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN
from hgraph._types._scalar_value import Array
from hgraph._types._time_series_types import TimeSeriesDeltaValue, TimeSeriesInput, TimeSeriesOutput

__all__ = ("TimeSeriesWindow", "TSW", "TSW_OUT", "TimeSeriesWindowInput",
           "TimeSeriesWindowOutput")

class TimeSeriesWindow(
    TimeSeriesDeltaValue[Array[SCALAR], SCALAR],
    Generic[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN],
):
    """
    Provides a time-series buffer over a stream of scalar values. When the size is in terms of ticks, this will provide
    an array of length at least WINDOW_SIZE_MIN and at most WINDOW_SIZE. By default, the WINDOW_MIN_SIZE is set to WINDOW_SIZE
    if the min size is not set.

    When the size is set to a timedelta, this will produce values with a maximum size of size.microseconds (the number
    of microseconds making up the time-delta set to WINDOW_SIZE) and a minimum size of 0. The WINDOW_MIN_SIZE is used to
    ensure that the graph has been up and running for at least the WINDOW_SIZE_MIN time. This ensures have captured at
    least that duration of time's ticks. This provides no guarantee as to the number of ticks available. To ensure a
    sample size, use the integer-based size and min size.
    """

    def __init__(self, __type__: SCALAR, __size__: WINDOW_SIZE, __min_size__: WINDOW_SIZE_MIN):
        self.__type__: SCALAR = __type__
        self.__size__: WINDOW_SIZE = __size__
        self.__min_size__: WINDOW_SIZE_MIN = __min_size__

    def __class_getitem__(cls, item) -> Any:
        # For now, limit to validation of item
        is_tuple = type(item) is tuple
        if is_tuple:
            if len(item) != 3:
                item = (item[0] if len(item) >= 1 else SCALAR), \
                (item[1] if len(item) == 2 else WINDOW_SIZE), \
                (item[1] if len(item) == 2 else WINDOW_SIZE_MIN)
        else:
            item = item, WINDOW_SIZE, WINDOW_SIZE_MIN
        out = super(TimeSeriesWindow, cls).__class_getitem__(item)
        if item != (SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN):
            from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

            if HgScalarTypeMetaData.parse_type(item[0]) is None:
                from hgraph import ParseError

                raise ParseError(
                    f"Type '{item[0]}' must be a scalar or a valid TypeVar (bound to a scalar value)"
                )
        return out

    def __len__(self) -> int:
        """
        Returns the length of the buffer if it is of a tick length. Since this only gets called during the wiring
        phase it can only provide the value if the size is set and it has a tick size.
        When called inside of a node this is the actual length of the buffer at the point in time.
        """
        return getattr(self, "__size__", -1)

    @property
    def min_size(self) -> int | timedelta:
        """The minimum size (either as integer when defined as int, or timedelta when defined as timedelta)"""
        return self.__min_size__.SIZE if self.__min_size__.FIXED_SIZE else self.__min_size__.TIME_RANGE

    @property
    @abstractmethod
    def size(self) -> int | timedelta:
        """The size (either as integer when defined as int, or timedelta when defined as timedelta)"""
        return self.__size__.SIZE if self.__size__.FIXED_SIZE else self.__size__.TIME_RANGE

    @property
    @abstractmethod
    def value_times(self) -> Array[datetime]:
        """
        The times associated to the value array. These are the times when the values were updated.
        """

    @property
    @abstractmethod
    def first_modified_time(self) -> datetime:
        """
        The time the first tick in the buffer was modified.
        """


class TimeSeriesWindowInput(
    TimeSeriesWindow[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN], TimeSeriesInput, ABC, Generic[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]
):
    """
    The input of a time series buffer.
    """


class TimeSeriesWindowOutput(
    TimeSeriesWindow[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN], TimeSeriesOutput, ABC, Generic[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]
):
    """
    The output of the time series list
    """

    value: TimeSeriesOutput


TSW = TimeSeriesWindowInput
TSW_OUT = TimeSeriesWindowOutput


# TSW (TimeSeriesWindow)
# add removed item

