from abc import abstractmethod, ABC
from datetime import datetime, timedelta
from typing import Generic, TypeVar, Protocol, Iterable, Tuple, Optional, TYPE_CHECKING, Union, Any

from hgraph._types._scalar_types import SCALAR, KEYABLE_SCALAR
from hgraph._types._typing_utils import clone_type_var

if TYPE_CHECKING:
    from hgraph._runtime._graph import Graph
    from hgraph._runtime._node import Node

__all__ = (
    "TimeSeries",
    "TimeSeriesDeltaValue",
    "TimeSeriesPushQueue",
    "TimeSeriesPullQueue",
    "TimeSeriesOutput",
    "TimeSeriesInput",
    "TimeSeriesSignalInput",
    "DELTA_SCALAR",
    "OUTPUT_TYPE",
    "TIME_SERIES_TYPE_1",
    "TIME_SERIES_TYPE",
    "K",
    "K_1",
    "K_2",
    "V",
    "TimeSeriesIterable",
    "SIGNAL",
    "TIME_SERIES_TYPE_2",
    "OUT",
    "OUT_1",
)


class TimeSeriesPushQueue(Protocol):

    def __lock__(self): ...

    def __unlock__(self): ...

    def send_pyobj(self, value: object): ...


class TimeSeriesPullQueue(Protocol):

    def send_pyobj(self, value: object, when: Union[datetime, timedelta]): ...


class TimeSeries(ABC):
    """
    The base of all time-series types in HGraph. The time-series types provide the ports to connect the output of
    a function to inputs of other functions (or nodes in the graph). The type also provides a collection of useful
    methods and properties that can be found in all time-series values.
    """

    @property
    @abstractmethod
    def owning_node(self) -> "Node":
        """
        The node that owns this time-series.
        """

    @property
    @abstractmethod
    def owning_graph(self) -> "Graph":
        """
        The graph that owns the node that owns this time-series.
        """

    @property
    @abstractmethod
    def value(self):
        """
        All time-series objects must support a value property that returns a python object representation
        of the current (point-in-time) state. For strongly typed runtime engines (for example, one implemented in C++)
        this is effectively a type erased value.
        """

    @property
    @abstractmethod
    def delta_value(self):
        """
        All time-series objects must support a ``delta_value`` property that returns a python object representation
        of the changes between the last tick and the current tick.
        """

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
        :return: True if there is a valid value associated to this time-series
        """

    @property
    @abstractmethod
    def all_valid(self) -> bool:
        """
        Is there a valid value associated to this time-series input, or loosely, "has this property
        ever ticked?". Note that it is possible for the time-series to become invalid after it has been made valid.
        The invalidation occurs mostly when working with REF values.
        :return: True if there is a valid value associated with this time-series.
        """

    @property
    @abstractmethod
    def last_modified_time(self) -> datetime:
        """
        :return: The time that this property last modified.
        """

    @abstractmethod
    def re_parent(self, parent: Union["Node", "TimeSeries"]):
        """
        FOR USE IN LIBRARY CODE.

        Change the owning node / time-series container of this time-series.
        This is used when grafting a time-series input from one node / time-series container to another.
        For example, see use in map implementation.
        """

    @abstractmethod
    def is_reference(self) -> bool:
        """
        :return: True if this time-series is a reference to another time-series.
        """


TIME_SERIES_TYPE = TypeVar("TIME_SERIES_TYPE", bound=TimeSeries)
TIME_SERIES_TYPE_1 = TypeVar("TIME_SERIES_TYPE_1", bound=TimeSeries)
TIME_SERIES_TYPE_2 = TypeVar("TIME_SERIES_TYPE_2", bound=TimeSeries)
OUT = TypeVar("OUT", bound=TimeSeries)
OUT_1 = TypeVar("OUT_1", bound=TimeSeries)
DELTA_SCALAR: TypeVar = clone_type_var(SCALAR, "DELTA_SCALAR")
OUTPUT_TYPE = TypeVar("OUTPUT_TYPE", bound="TimeSeriesOutput")


class TimeSeriesOutput(TimeSeries):
    """
    Output time-series types hold the actual value and can be set / modified.
    These types are also the observable implementations in the graph (as in the Observer pattern, with the
    exception that when the output changes it schedules subscribed input nodes instead of actually calling them).
    """

    @property
    @abstractmethod
    def parent_output(self) -> Optional["TimeSeriesOutput"]:
        """
        The output that this output is bound to. This will be None if this is the root output.
        """

    @property
    @abstractmethod
    def has_parent_output(self) -> bool:
        """True if this output is a child of another output, False otherwise"""

    @property
    @abstractmethod
    def value(self):
        """
        The time-series point-in-time value represented as a scalar value.
        """

    @value.setter
    @abstractmethod
    def value(self, value: Any):
        """
        Allows the time-series output to have it's value set using the scalar value wrapped
        value instance.
        """

    @property
    @abstractmethod
    def delta_value(self):
        """
        The scalar value wrapper of the ticked_value.
        """

    @abstractmethod
    def can_apply_result(self, result: Any) -> bool:
        """
        Return True if the result can be applies without overwriting any value modified in this engine cycle.
        """

    @abstractmethod
    def apply_result(self, result: Any):
        """
        Apply the result of calling a python method to the output.
        """

    @abstractmethod
    def subscribe(self, node: "Node"):
        """Add this node to receive notifications when this output changes
        (this is called by make_active by the bound input)"""

    @abstractmethod
    def unsubscribe(self, node: "Node"):
        """Remove this node from receiving notifications when this output changes
        (this is called by make_passive by the bound input)"""

    @abstractmethod
    def copy_from_output(self, output: "TimeSeriesOutput"):
        """
        Copy the value from the output provided to this output.
        This pattern makes it easier to implement generic behaviour relying on the copying of inputs or outputs
        to self, this code will require that the supplied output is of the same type as this output.
        """

    @abstractmethod
    def copy_from_input(self, input: "TimeSeriesInput"):
        """
        Copy the value from the input provided to this output.
        This pattern makes it easier to implement generic behaviour relying on the copying of inputs or outputs
        to self, this code will require that the supplied input is of the same/compatible type as this output.
        """

    @abstractmethod
    def clear(self):
        """
        Clear out the output, this removes all items from collection time series.
        """

    @abstractmethod
    def invalidate(self):
        """
        Invalidate the output, this removes all values and marks the output as invalid.
        """

    @abstractmethod
    def mark_invalid(self):
        """
        Marks the output as invalid, this will cause the output to be scheduled for evaluation.
        """

    @abstractmethod
    def mark_modified(self):
        """
        Marks the output as modified, this will cause the output to be scheduled for evaluation if it hasn't already.
        This will also mark the parent output as modified if it exists.
        """


class TimeSeriesInput(TimeSeries):
    """
    The time-series inputs are wrappers around an output. Inputs can either wrap a single output (when peered)
    or a collection of outputs (when non-peered). The inputs can be made active (subscribed to changes of the output)
    or passive (not subscribed to changes of the output).
    """

    @property
    @abstractmethod
    def parent_input(self) -> Optional["TimeSeriesInput"]:
        """
        The input that this input is bound to. This will be None if this is the root input.
        """

    @property
    @abstractmethod
    def has_parent_input(self) -> bool:
        """True if this input is a child of another input, False otherwise"""

    @property
    @abstractmethod
    def bound(self) -> bool:
        """
        Is this time-series input bound to an output?
        It is possible for an input to be unbound, for example, when dealing with reference types, where the node
        may not have a valid reference yet, in which case no output will be bound to the input.
        :return: True if this is bound to an output
        """

    @property
    @abstractmethod
    def has_peer(self) -> bool:
        """
        If the input is bound directly to a single output then this input is peered, however, if the input
        is bound to more then one output making up the structure of this input, then the input is not peered.
        This is generally only going to affect collection types such as TSL, TSB, and TSD where the input may be
        a collection of independent time-series outputs.

        Note: If the input is not bound, then it has no peer.

        :return: True if this input is peered.
        """

    @property
    @abstractmethod
    def output(self) -> Optional[TimeSeriesOutput]:
        """
        The output bound to this input. If the input is not bound then this will be None.
        """

    @abstractmethod
    def bind_output(self, value: Optional[TimeSeriesOutput]) -> bool:
        """
        FOR LIBRARY USE ONLY.

        Binds the output provided to this input.
        """

    @abstractmethod
    def un_bind_output(self, unbind_refs: bool = False) -> None:
        """
        FOR LIBRARY USE ONLY.

        Unbinds the output from this input.
        """

    @abstractmethod
    def do_bind_output(self, value: TimeSeriesOutput) -> bool:
        """
        Derived classes override this to implement specific behaviours
        """

    @abstractmethod
    def do_un_bind_output(self, unbind_refs: bool = False):
        """
        Derived classes override this to implement specific behaviours
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

        For example, a node that processes a credit card transaction only needs to be woken up when the
        transaction request is received, but may depend on things such as credit history, exchange rates,
        transaction fees, etc. There is no need to evaluate the node if the transaction request has not ticked.
        Thus, all other inputs can be treated as passive.
        """


class TimeSeriesSignalInput(TimeSeriesInput):
    """
    An input type that be bound to any output type. The value of the output is ignored, and the signal will
    allow for usages where only the ticked state is required to be known. There is no equivalent to this on the
    output side. If only a "ticked" state is required, the convention is to use a ``TS[bool]`` for the output type.
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
    All time-series values support this, this utility class provides the ability to describe the expected type
    of the ``value`` and ``delta_value`` properties.
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


K = clone_type_var(KEYABLE_SCALAR, "K")
K_1 = clone_type_var(KEYABLE_SCALAR, "K_1")
K_2 = clone_type_var(KEYABLE_SCALAR, "K_2")
V = clone_type_var(TIME_SERIES_TYPE, "V")


class TimeSeriesIterable(Generic[K, V]):
    """
    All collection time-series objects support this set of methods over the elements of the collection.
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
        Iterator over the key value pairs in this collection.
        """

    @abstractmethod
    def modified_keys(self) -> Iterable[K]:
        """
        Iterator over the keys associated to values that have been modified in this engine cycle
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


SIGNAL = TimeSeriesSignalInput
