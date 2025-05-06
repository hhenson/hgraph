from abc import abstractmethod, ABC
from typing import Generic, Optional, Iterable, TYPE_CHECKING

from hgraph._types._time_series_types import TimeSeriesOutput, TimeSeriesInput, TimeSeriesDeltaValue, TIME_SERIES_TYPE

if TYPE_CHECKING:
    ...

__all__ = ("REF", "REF_OUT", "TimeSeriesReference", "TimeSeriesReferenceOutput", "TimeSeriesReferenceInput")


class TimeSeriesReference:
    """
    Contains a reference to a time-series output. This is the holder type used to tick references to outputs through the
    graph using the ``REF`` type.
    """

    # Preparation for alternative engine implementations
    _BUILDER = None
    _INSTANCE_OF = None

    @abstractmethod
    def bind_input(self, input_: TimeSeriesInput):
        """Binds given input to the value of this reference"""

    @property
    @abstractmethod
    def is_valid(self) -> bool:
        """Indicates if the reference is valid, this is confirmed against the output or the list of items, Flase otherwise"""

    @property
    @abstractmethod
    def has_output(self) -> bool:
        """Indicates if the reference has an output"""

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        """Indicates if the reference is empty"""

    @staticmethod
    def make(
        ts: Optional[TimeSeriesInput | TimeSeriesOutput] = None,
        from_items: Iterable["TimeSeriesReference"] = None,
    ):
        if TimeSeriesReference._BUILDER is None:
            from hgraph._impl._types._ref import python_time_series_reference_builder

            TimeSeriesReference._BUILDER = python_time_series_reference_builder

        return TimeSeriesReference._BUILDER(ts=ts, from_items=from_items)

    @staticmethod
    def is_instance(obj: object) -> bool:
        if TimeSeriesReference._INSTANCE_OF is None:
            TimeSeriesReference._INSTANCE_OF = lambda obj: isinstance(obj, TimeSeriesReference)
        return TimeSeriesReference._INSTANCE_OF(obj)


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
