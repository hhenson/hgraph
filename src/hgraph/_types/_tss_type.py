from abc import abstractmethod
from typing import Protocol, Iterable, Generic, Set, runtime_checkable, Callable

from hgraph._types._scalar_types import KEYABLE_SCALAR
from hgraph._types._time_series_types import TimeSeriesInput, TimeSeriesOutput, TimeSeriesDeltaValue
from hgraph._types._ts_type import TS

__all__ = (
    "SetDelta",
    "TSS",
    "TSS_OUT",
    "TimeSeriesSet",
    "TimeSeriesSetInput",
    "TimeSeriesSetOutput",
    "set_delta",
    "set_set_delta_factory",
)


@runtime_checkable
class SetDelta(Protocol[KEYABLE_SCALAR], Generic[KEYABLE_SCALAR]):
    """
    Represent the delta value of an operation performed on the TSS type.
    This contains the added and removed elements of the set (those added and removed in this engine cycle).
    This can also be used to apply the change to a TSS output.
    """

    @property
    @abstractmethod
    def added(self) -> Iterable[KEYABLE_SCALAR]:
        """
        The elements that were added
        """

    @property
    @abstractmethod
    def removed(self) -> Iterable[KEYABLE_SCALAR]:
        """
        The elements that were removed
        """


SET_DELTA_FACTORY: Callable[[Iterable[KEYABLE_SCALAR], Iterable[KEYABLE_SCALAR], type[KEYABLE_SCALAR]], None] = None


def set_set_delta_factory(
    fn: Callable[[Iterable[KEYABLE_SCALAR], Iterable[KEYABLE_SCALAR], type[KEYABLE_SCALAR]], SetDelta[KEYABLE_SCALAR]],
):
    """Set the builder function for set_delta instances, by default we will use the PythonSetDelta class"""
    global SET_DELTA_FACTORY
    SET_DELTA_FACTORY = fn


def _guess_type(added, removed) -> type[KEYABLE_SCALAR]:
    if added:
        return type(next(iter(added)))
    if removed:
        return type(next(iter(removed)))
    return object


def set_delta(
    added: Iterable[KEYABLE_SCALAR] = None, removed: Iterable[KEYABLE_SCALAR] = None, tp: type[KEYABLE_SCALAR] = None
):
    global SET_DELTA_FACTORY
    if SET_DELTA_FACTORY is None:
        from hgraph import PythonSetDelta

        SET_DELTA_FACTORY = lambda a, d, t: PythonSetDelta[t](
            added=None if a is None else frozenset(a), removed=None if d is None else frozenset(d)
        )
    if tp is None:
        tp = _guess_type(added, removed)
    if added is None:
        added = frozenset()
    if removed is None:
        removed = frozenset()
    return SET_DELTA_FACTORY(added, removed, tp)


class TimeSeriesSet(TimeSeriesDeltaValue[KEYABLE_SCALAR, SetDelta[KEYABLE_SCALAR]], Generic[KEYABLE_SCALAR]):
    """
    The core methods common to both input and output instances of the ``TSS``.
    The time-series set represents a set of SCALAR values over time. The set will tick when an item is added or removed
    from the set. If an item is added that already exists, or removed when it did not exist, the set will not tick.

    The set will tick if it has no elements added if the set is not yet valid. That is, the set will become valid
    if an empty output is set. Once valid, the set will not tick again for this condition, unless there were values
    already present and they are removed.
    """

    @abstractmethod
    def __contains__(self, item: KEYABLE_SCALAR) -> bool:
        """
        If this time series set contain the value provided.
        """

    @abstractmethod
    def __len__(self) -> int:
        """
        Number of items in the set
        """

    @abstractmethod
    def values(self) -> Iterable[KEYABLE_SCALAR]:
        """
        Iterator over all the time-series values of this collection
        """

    @abstractmethod
    def added(self) -> Iterable[KEYABLE_SCALAR]:
        """
        Iterator over the added values
        """

    @abstractmethod
    def was_added(self, item: KEYABLE_SCALAR) -> bool:
        """True if the item was added in this engine cycle."""

    @abstractmethod
    def removed(self) -> Iterable[KEYABLE_SCALAR]:
        """
        Iterator over the removed values
        """

    @abstractmethod
    def was_removed(self, item: KEYABLE_SCALAR) -> bool:
        """True if the item was removed in this engine cycle."""


class TimeSeriesSetInput(TimeSeriesInput, TimeSeriesSet[KEYABLE_SCALAR], Generic[KEYABLE_SCALAR]):
    """
    The input version of the set.
    """


class TimeSeriesSetOutput(TimeSeriesOutput, TimeSeriesSet[KEYABLE_SCALAR], Generic[KEYABLE_SCALAR]):
    """
    The output version of the set
    """

    @abstractmethod
    def get_contains_output(self, item: KEYABLE_SCALAR, requester: object) -> TS[bool]:
        """
        Returns a TS[bool] output reference that ticks True when the item value is present and False otherwise.
        """

    @abstractmethod
    def release_contains_output(self, item: KEYABLE_SCALAR, requester: object):
        """Releases the reference request"""

    @abstractmethod
    def is_empty_output(self) -> TS[bool]:
        """Returns a TS[bool] output that tracks the empty state of the set."""

    @property
    @abstractmethod
    def value(self) -> Set[KEYABLE_SCALAR]: ...

    @property
    @abstractmethod
    def delta_value(self) -> SetDelta[KEYABLE_SCALAR]: ...

    def add(self, element: KEYABLE_SCALAR):
        """Add an element to the set"""

    def remove(self, element: KEYABLE_SCALAR):
        """Removes the element from the set"""

    def clear(self):
        """Removes all elements from the set"""


TSS = TimeSeriesSetInput
TSS_OUT = TimeSeriesSetOutput
