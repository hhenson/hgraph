import functools
from abc import abstractmethod
from typing import Any, Generic, Iterable

from hg._types._scalar_types import SIZE
from hg._types._time_series_types import TimeSeriesIterable, TimeSeriesInput, TimeSeriesOutput, TIME_SERIES_TYPE,\
    TimeSeriesDeltaValue


__all__ = ("TSL", "TSL_OUT", "TimeSeriesList", "TimeSeriesListInput", "TimeSeriesListOutput")


class TimeSeriesList(TimeSeriesIterable[int, TIME_SERIES_TYPE], TimeSeriesDeltaValue[tuple, dict[int, Any]],
                     Generic[TIME_SERIES_TYPE, SIZE]):
    """
    Represents a linear collection of time-series inputs.
    Think of this as a list of time-series values.
    """

    @abstractmethod
    def __getitem__(self, item: int) -> TIME_SERIES_TYPE:
        """
        Returns the time series at this index position
        :param item:
        :return:
        """

    @abstractmethod
    def __iter__(self) -> Iterable[TIME_SERIES_TYPE]:
        """
        Iterator over the time-series values
        :return:
        """


class TimeSeriesListInput(TimeSeriesList[TIME_SERIES_TYPE, SIZE], TimeSeriesInput, Generic[TIME_SERIES_TYPE, SIZE]):
    """
    The input of a time series list.
    """

    def __class_getitem__(cls, item) -> Any:
        # For now limit to validation of item
        out = super(TimeSeriesListInput, cls).__class_getitem__(item)
        if item is not (TIME_SERIES_TYPE, SIZE):
            from hg._types._type_meta_data import HgTypeMetaData
            if HgTypeMetaData.parse(item[0]).is_scalar:
                from hg import ParseError
                raise ParseError(
                    f"Type '{item[0]}' must be a TimeSeriesSchema or a valid TypeVar (bound to to TimeSeriesSchema)")
            if hasattr(out, "from_ts"):
                fn = out.from_ts
                code = fn.__code__
                out.from_ts = functools.partial(fn, __schema__=item)
                out.from_ts.__code__ = code
        return out

    @staticmethod
    def from_ts(**kwargs) -> "TimeSeriesList[TIME_SERIES_TYPE, SIZE]":
        ...


class TimeSeriesListOutput(TimeSeriesList[TIME_SERIES_TYPE, SIZE], TimeSeriesOutput, Generic[TIME_SERIES_TYPE, SIZE]):
    """
    The output type of a time series list
    """

    value: TimeSeriesOutput


TSL = TimeSeriesListInput
TSL_OUT = TimeSeriesListOutput
