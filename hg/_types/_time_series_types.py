from abc import abstractmethod, ABC
from datetime import datetime, timedelta
from typing import Generic, TypeVar, Any, Protocol, Iterable, Tuple, Optional, TYPE_CHECKING, Union

from hg._types._scalar_types import SCALAR
from hg._typing_utils import clone_typevar

if TYPE_CHECKING:
    from hg._runtime import Node


__all__ = ("TimeSeries", "TimeSeriesDeltaValue", "TIME_SERIES_TYPE", "K", "V")


class TimeSeriesPushQueue(Protocol):

    def __lock__(self):
        ...

    def __unlock__(self):
        ...

    def send_pyobj(self, value: object):
        ...


class TimeSeriesPullQueue(Protocol):

    def send_pyobj(self, value: object, when: Union[datetime, timedelta]):
        ...


class TimeSeries(ABC):

    @property
    @abstractmethod
    def modified(self) -> bool:
        """
        Has the value of this time-series changed in this engine cycle.
        :return: True implies this time-series has been modified in this engine cycle.
        """

    @property
    @abstractmethod
    def valid(self) -> bool:
        """
        Is there a valid value associated to this time-series input, or more generally has this property
        ever ticked.
        :return: True is there is a valid value associated to this time-series.
        """

    @property
    @abstractmethod
    def all_valid(self) -> bool:
        """
        For some time-series types, valid is not sufficient if we require all elements of the
        time-series to have ticked and not just some of them. This will return true if all time-series
        values are valid and not just some.
        """

    @property
    @abstractmethod
    def last_modified_time(self) -> datetime:
        """
        :return: The time that this property last modified.
        """


TIME_SERIES_TYPE = TypeVar("TIME_SERIES_TYPE", bound=TimeSeries)
DELTA_SCALAR: TypeVar = clone_typevar(SCALAR, "DELTA_SCALAR")


class TimeSeriesOutput(TimeSeries):
    value: Any

    @abstractmethod
    def subscribe_node(self, node: "Node"):
        """Add this node to receive notifications when this output changes
        (this is called by make_active by the bound input)"""

    @abstractmethod
    def un_subscribe_node(self, node: "Node"):
        """Remove this node from receiving notifications when this output changes
        (this is called by make_passive by the bound input)"""


class TimeSeriesInput(TimeSeries):

    @property
    @abstractmethod
    def bound(self) -> bool:
        """
        Is this time-series input bound directly to an output?
        For collections time-series values such as TSL and TSB it is possible that the input is actually not directly
        connected to a single time-series value and as such it will return False, the elements of the input, are likely
        to be bound to multiple different outputs.
        :return: True if this is bound to an output
        """

    @property
    @abstractmethod
    def output(self) -> Optional[TimeSeriesOutput]:
        """
        The output bound to this input. If no input is bound this will be None.
        """

    @property
    @abstractmethod
    def value(self) -> Any:
        """
        The value returns a point-in-time representation of this input.
        The type will depend on the actual time-series instance.
        """

    @property
    @abstractmethod
    def active(self) -> bool:
        """
        An active input will cause the node it is associated with to be scheduled when the value
        the input represents is modified.

        :return: True if this input is active.
        """

    @abstractmethod
    def make_active(self):
        """
        Marks the input as being active, if the input is already active no work is done.
        Once marked active if the value the input is bound to will cause the input's node
        to be scheduled for evaluation when the value changes.
        """

    @abstractmethod
    def make_passive(self):
        """
        Marks the input as being passive, if the input is already passive then no work is done.
        Once marked passive, the node associated to the input will not be scheduled for evaluation
        when the associated value is changed. Note that when accessing the value, the user will still
        get the most recent value. The utility to mark passive is to reduce activations in circumstances
        where the particular input is required, but the driver of a process is not this input.

        For example, a node that process a credit card transaction only needs to be woken up when the
        transaction request is received, but may depend on things such as credit history, exchange rates,
        transaction fee's etc. There is no need to evaluate the node if the transaction request has not ticked.
        Thus all other inputs can be treated as passive.
        """


class TimeSeriesSignalInput(TimeSeriesInput):
    """
    An input type that be bound to any output type. The value of the output is ignored and the signal will
    allow for usages where only the ticked state is required to be known. There is no equivalent to this on the
    output side. If only a "ticked" state is required, the convention is to use a TS[bool].
    """

    @property
    @abstractmethod
    def value(self) -> bool:
        """
        Will return the result of ticked.
        """


class TimeSeriesDeltaValue(TimeSeries, Generic[SCALAR, DELTA_SCALAR]):
    """
    A time-series that is able to express the changes between this and the last tick as a delta object.
    """

    @property
    @abstractmethod
    def value(self) -> Optional[SCALAR]:
        """
        The current value.
        """

    @property
    @abstractmethod
    def delta_value(self) -> Optional[DELTA_SCALAR]:
        """
        The delta value.
        """

K = clone_typevar(SCALAR, "K")
V = clone_typevar(TIME_SERIES_TYPE, "V")


class TimeSeriesIterable(Generic[K, V]):
    """
    All collection time-series objects should support this set of elements.
    """

    @abstractmethod
    def keys(self) -> Iterable[K]:
        """
        Iterator over all the keys of the collection
        """

    @abstractmethod
    def values(self) -> Iterable[V]:
        """
        Iterator over all the time-series values of this collection
        """

    @abstractmethod
    def items(self) -> Iterable[Tuple[K, V]]:
        """
        Iterator over all of the key value pairs in this collection.
        """

    @abstractmethod
    def modified_keys(self) -> Iterable[K]:
        """
        Iterator over the keys associted to values that have been modified in this engine cycle
        """

    @abstractmethod
    def modified_values(self) -> Iterable[V]:
        """
        Iterator over the time-series values that have been modified in this engine cycle
        """

    @abstractmethod
    def modified_items(self) -> Iterable[Tuple[K, V]]:
        """
        Iterator over the keys and values of the values that have been modified in this engine cycle.
        """

    @abstractmethod
    def valid_keys(self) -> Iterable[K]:
        """
        Iterator over the keys associated to values that have are deemed valid.
        """

    @abstractmethod
    def valid_values(self) -> Iterable[V]:
        """
        Iterator over the time-series values that have are deemed valid.
        """

    @abstractmethod
    def valid_items(self) -> Iterable[Tuple[K, V]]:
        """
        Iterator over the keys and values of the values that have been deemed valid.
        """
