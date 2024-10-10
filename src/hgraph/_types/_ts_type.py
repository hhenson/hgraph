from abc import abstractmethod, ABC
from typing import Generic, Optional

from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import TimeSeriesOutput, TimeSeriesInput, TimeSeriesDeltaValue

__all__ = ("TS", "TS_OUT", "TimeSeriesValueOutput", "TimeSeriesValueInput")


class TimeSeriesValueOutput(TimeSeriesOutput, TimeSeriesDeltaValue[SCALAR, SCALAR], ABC, Generic[SCALAR]):
    """
    The time-series output that contains a scalar value. This is the most fundamental time-series output type.

    This can be represented as ``TS_OUT[SCALAR]`` when typing an ``_output`` injectable argument to a node.
    When returning the value from a node, use the ``TS[SCALAR]`` annotation to the return value.
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
    The time-series input of a SCALAR value. This peers with a ``TimeSeriesValueOutput`` instance. Use ``TS[SCALAR]``
    as the type annotation. Note, as this is an input, the ``value`` is not settable.
    """


# Shorthand for a TimeSeriesValueInput
TS = TimeSeriesValueInput
TS_OUT = TimeSeriesValueOutput
