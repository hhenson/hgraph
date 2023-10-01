from abc import abstractmethod
from typing import Generic

from hg._types._scalar_types import SCALAR
from hg._types._scalar_value import ScalarValue
from hg._types._time_series_types import TimeSeriesOutput, TimeSeriesInput


__all__ = ("TS", "TS_OUT")


class TimeSeriesValueOutput(TimeSeriesOutput, Generic[SCALAR]):
    """
    The time-series output that manages and atomic value.
    """

    @property
    @abstractmethod
    def value(self) -> SCALAR:
        """
        The current value associated to this node.
        """

    @value.setter
    @abstractmethod
    def value(self, value: SCALAR):
        """The output can set the value"""


class TimeSeriesValueInput(TimeSeriesInput, Generic[SCALAR]):
    """
    This is the wrapper class of the TimeSeriesValueOutput. It is not able to modify
    the value. It also support the input behaviours of the TimeSeriesInput
    """

    @property
    def bound(self) -> bool:
        """The time-series value input IS ALWAYS bound to an output"""
        return True

    @property
    @abstractmethod
    def output(self) -> TimeSeriesValueOutput[SCALAR]:
        pass

    @property
    @abstractmethod
    def value(self) -> SCALAR:
        """
        The current value associated to this node.
        :return:
        """


# Shorthand for a TimeSeriesValueInput
TS = TimeSeriesValueInput
TS_OUT = TimeSeriesValueOutput
