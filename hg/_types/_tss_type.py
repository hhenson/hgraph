from abc import abstractmethod
from typing import Protocol, Iterable, Generic, Set

from hg._types._scalar_types import SCALAR
from hg._types._time_series_types import TimeSeriesInput, TimeSeriesOutput, TimeSeriesDeltaValue


__all__ = ("SetDelta", "TSS", "TSS_OUT", "TimeSeriesSet", "TimeSeriesSetInput", "TimeSeriesSetOutput")


class SetDelta(Protocol[SCALAR]):

    @property
    @abstractmethod
    def added_elements(self) -> Iterable[SCALAR]:
        """
        The elements that were added
        """

    @property
    @abstractmethod
    def removed_elements(self) -> Iterable[SCALAR]:
        """
        The elements that were removed
        """


class TimeSeriesSet(TimeSeriesDeltaValue[SCALAR, SetDelta[SCALAR]], Generic[SCALAR]):
    """
    The representation of a set a set over time.
    """

    @abstractmethod
    def __contains__(self, item: SCALAR) -> bool:
        """
        If this time series set contain the value provided.
        """

    @abstractmethod
    def values(self) -> Iterable[SCALAR]:
        """
        Iterator over all the time-series values of this collection
        """

    @abstractmethod
    def added(self) -> Iterable[SCALAR]:
        """
        Iterator over the added values
        """

    @abstractmethod
    def was_added(self, item: SCALAR) -> bool:
        """True if the item was added in this engine cycle."""

    @abstractmethod
    def removed(self) -> Iterable[SCALAR]:
        """
        Iterator over the removed values
        """

    @abstractmethod
    def was_removed(self, item: SCALAR) -> bool:
        """True if the item was removed in this engine cycle."""


class TimeSeriesSetInput(TimeSeriesInput, TimeSeriesSet[SCALAR], Generic[SCALAR]):
    """
    The input version of the set.
    """


class TimeSeriesSetOutput(TimeSeriesOutput, TimeSeriesSet[SCALAR], Generic[SCALAR]):
    """
    The output version of the set
    """

    value: Set[SCALAR]

    delta_value: SetDelta[SCALAR]

    def add(self, element: SCALAR):
        """Add an element to the set"""

    def remove(self, element: SCALAR):
        """Removes the element from the set"""

    def clear(self):
        """Removes all elements from the set"""


TSS = TimeSeriesSetInput
TSS_OUT = TimeSeriesSetOutput
