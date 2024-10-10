from abc import abstractmethod, ABC
from typing import Generic, Optional

from hgraph._types._time_series_types import TimeSeriesOutput, TimeSeriesInput, TimeSeriesDeltaValue, TIME_SERIES_TYPE

__all__ = ("REF", "REF_OUT", "TimeSeriesReference", "TimeSeriesReferenceOutput", "TimeSeriesReferenceInput")


class TimeSeriesReference:
    """
    Contains a reference to a time-series output. This is the holder type used to tick references to outputs through the
    graph using the ``REF`` type.
    """

    @abstractmethod
    def __init__(self, input_: TimeSeriesInput = None):
        """Creates an instance of Reference form an input object, captures both peer and non-peer bindings"""
        pass

    @abstractmethod
    def bind_input(self, input_: TimeSeriesInput):
        """Binds given input to the value of this reference"""

    @property
    @abstractmethod
    def output(self) -> TimeSeriesOutput:
        """The output associated to this reference"""


class TimeSeriesReferenceOutput(
    TimeSeriesOutput, TimeSeriesDeltaValue[TimeSeriesReference, TimeSeriesReference], Generic[TIME_SERIES_TYPE]
):
    """
    The time-series output of a reference type. This is very similar to the  ``TimeSeriesValueOutput``.
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

    def observe_reference(self, input_: TimeSeriesInput):
        """Registers an input as observing the reference value"""

    def stop_observing_reference(self, input_: TimeSeriesInput):
        """Unregisters an input as observing the reference value"""


class TimeSeriesReferenceInput(
    TimeSeriesInput, TimeSeriesDeltaValue[TimeSeriesReference, TimeSeriesReference], ABC, Generic[TIME_SERIES_TYPE]
):
    """
    The reference input. This is similar to the ``TimeSeriesValueInput``.
    """

    @abstractmethod
    def clone_binding(self, other: "TimeSeriesReferenceInput"):
        """Duplicate binding of another input"""


# Shorthand for a TimeSeriesValueInput
REF = TimeSeriesReferenceInput
REF_OUT = TimeSeriesReferenceOutput
