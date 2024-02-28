from abc import abstractmethod, ABC
from typing import Generic, Optional

from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import TimeSeriesOutput, TimeSeriesInput, TimeSeriesDeltaValue

__all__ = ("TS", "TS_OUT", "TimeSeriesValueOutput", "TimeSeriesValueInput")


class TimeSeriesValueOutput(TimeSeriesOutput, TimeSeriesDeltaValue[SCALAR, SCALAR], ABC, Generic[SCALAR]):
    """
    The time-series output that manages and atomic value.
    """

    @property
    @abstractmethod
    def value(self) -> Optional[SCALAR]:
        """
        The current value associated to this node.
        """

    @value.setter
    @abstractmethod
    def value(self, value: SCALAR):
        """The output can set the value"""


class TimeSeriesValueInput(TimeSeriesInput, TimeSeriesDeltaValue[SCALAR, SCALAR], ABC, Generic[SCALAR]):
    """
    This is the wrapper class of the TimeSeriesValueOutput. It is not able to modify
    the value. It also supports the input behaviours of the TimeSeriesInput
    """


# Shorthand for a TimeSeriesValueInput
TS = TimeSeriesValueInput
TS_OUT = TimeSeriesValueOutput
