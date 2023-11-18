from abc import abstractmethod
from typing import Generic, Optional

from hg._types._scalar_types import SCALAR
from hg._types._time_series_types import TimeSeriesOutput, TimeSeriesInput, TimeSeriesDeltaValue, TIME_SERIES_TYPE

__all__ = ("REF", "REF_OUT", "TimeSeriesReferenceOutput", "TimeSeriesReferenceInput")


class TimeSeriesReference:
    @abstractmethod
    def __init__(self, i: TimeSeriesInput):
        """Creates an instance of Reference form an input object, captures both peer and non-peer bindings"""
        pass

    @abstractmethod
    def bind_input(self, i: TimeSeriesInput):
        """Binds given input to the value of this reference"""
        pass


class TimeSeriesReferenceOutput(TimeSeriesOutput, TimeSeriesDeltaValue[TimeSeriesReference, TimeSeriesReference], Generic[TIME_SERIES_TYPE]):
    """
    The time-series output that manages and atomic value.
    """

    @property
    @abstractmethod
    def value(self) -> Optional[TimeSeriesReference]:
        """
        The current value associated to this node.
        """

    @value.setter
    @abstractmethod
    def value(self, value: TimeSeriesReference):
        """The output can set the value"""


class TimeSeriesReferenceInput(TimeSeriesInput, TimeSeriesDeltaValue[TimeSeriesReference, TimeSeriesReference], Generic[TIME_SERIES_TYPE]):
    """
    This is the wrapper class of the TimeSeriesValueOutput. It is not able to modify
    the value. It also supports the input behaviours of the TimeSeriesInput
    """


# Shorthand for a TimeSeriesValueInput
REF = TimeSeriesReferenceInput
REF_OUT = TimeSeriesReferenceOutput
