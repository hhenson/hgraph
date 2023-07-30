from abc import abstractmethod
from typing import Generic, Iterable

from frozendict import frozendict

from hg._types._time_series_types import TimeSeriesIterable, TimeSeriesInput, TimeSeriesOutput, K, V, \
    TimeSeriesDeltaValue


__all__ = ("TSD", "TSD_OUT")


class TimeSeriesDict(TimeSeriesIterable[K, V], TimeSeriesDeltaValue[frozendict, frozendict], Generic[K, V]):
    """
    A TSD is a collection of time-series values keyed off of a scalar key K.
    """

    @abstractmethod
    def __getitem__(self, item: K) -> V:
        """
        Returns the time series at this index position
        :param item:
        :return:
        """

    @abstractmethod
    def __iter__(self) -> Iterable[K]:
        """
        Iterator over the time-series values
        :return:
        """


class TimeSeriesDictInput(TimeSeriesInput, TimeSeriesDict[K, V], Generic[K, V]):
    """
    The TSD input
    """


class TimeSeriesDictOutput(TimeSeriesOutput, TimeSeriesDict[K, V], Generic[K, V]):
    """
    The TSD output
    """

    value: frozendict


TSD = TimeSeriesDictInput
TSD_OUT = TimeSeriesDictOutput
