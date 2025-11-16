import typing
from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic

from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._impl._types._output import PythonTimeSeriesOutput
from hgraph._runtime._constants import MIN_ST
from hgraph._types._ref_type import TimeSeriesReference, TimeSeriesReferenceOutput, TimeSeriesReferenceInput
from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import TimeSeriesInput, TIME_SERIES_TYPE, TimeSeriesOutput, TimeSeriesIterable

__all__ = (
    "python_time_series_reference_builder",
    "PythonTimeSeriesReferenceOutput",
    "PythonTimeSeriesReferenceInput",
    "PythonTimeSeriesValueReferenceInput",
    "PythonTimeSeriesValueReferenceOutput",
    "PythonTimeSeriesListReferenceInput",
    "PythonTimeSeriesListReferenceOutput",
    "PythonTimeSeriesBundleReferenceInput",
    "PythonTimeSeriesBundleReferenceOutput",
    "PythonTimeSeriesDictReferenceInput",
    "PythonTimeSeriesDictReferenceOutput",
    "PythonTimeSeriesSetReferenceInput",
    "PythonTimeSeriesSetReferenceOutput",
    "PythonTimeSeriesWindowReferenceInput",
    "PythonTimeSeriesWindowReferenceOutput",
)


def python_time_series_reference_builder(
    ts: typing.Optional[TimeSeriesInput | TimeSeriesOutput] = None, from_items: typing.Iterable[TimeSeriesOutput] = None
) -> TimeSeriesReference:
    if ts is not None:
        if isinstance(ts, TimeSeriesOutput):
            return BoundTimeSeriesReference(ts)
        if isinstance(ts, TimeSeriesReferenceInput):
            return ts.value
        if ts.has_output:
            return BoundTimeSeriesReference(ts.output)
        else:
            return UnBoundTimeSeriesReference([python_time_series_reference_builder(i) for i in ts])
    elif from_items is not None:
        return UnBoundTimeSeriesReference(from_items)
    else:
        return EmptyTimeSeriesReference()


class EmptyTimeSeriesReference(TimeSeriesReference):

    def bind_input(self, input_: TimeSeriesInput):
        # If the input is bound, unbind it since there are now no associated time-series output values.
        input_.un_bind_output()

    @property
    def is_valid(self) -> bool:
        return False

    @property
    def has_output(self) -> bool:
        return False

    @property
    def is_empty(self) -> bool:
        return True

    def __eq__(self, __value):
        return type(__value) is EmptyTimeSeriesReference

    def __str__(self):
        return "REF[<UnSet>]"

    def __repr__(self) -> str:
        # For now, we should work on a better job for formatting later.
        return self.__str__()


class BoundTimeSeriesReference(TimeSeriesReference):

    def __init__(self, output: TimeSeriesOutput):
        self.output = output

    def bind_input(self, input_: TimeSeriesInput):
        reactivate = False
        if input_.bound and not input_.has_peer:
            reactivate = input_.active
            input_.un_bind_output()

        input_.bind_output(self.output)

        if reactivate:
            input_.make_active()

    @property
    def is_valid(self) -> bool:
        return self.output.valid

    @property
    def has_output(self) -> bool:
        return True

    @property
    def is_empty(self) -> bool:
        return False

    def __eq__(self, __value):
        return type(__value) is BoundTimeSeriesReference and self.output == __value.output

    def __str__(self):
        return (
            f"REF[{self.output.owning_node.signature.name}"
            f"<{', '.join(str(i) for i in self.output.owning_node.node_id)}>.out<{hex(id(self.output))}>]"
        )

    def __repr__(self) -> str:
        # For now, we should work on a better job for formatting later.
        return self.__str__()


class UnBoundTimeSeriesReference(TimeSeriesReference):

    def __init__(self, items: typing.Iterable[TimeSeriesReference]):
        self.items = items

    def bind_input(self, input_: typing.Union[TimeSeriesInput, TimeSeriesIterable]):
        # We need to put some kind of validation here to ensure that the input is compatible with the references if
        # possible.

        reactivate = False
        if input_.bound and input_.has_peer:
            reactivate = input_.active
            input_.un_bind_output()

        for item, r in zip(input_, self.items):
            if r:
                r.bind_input(item)
            elif item.bound:
                item.un_bind_output()

        if reactivate:
            input_.make_active()

    def __getitem__(self, item):
        return self.items[item]

    @property
    def is_valid(self) -> bool:
        return any(not i.is_empty for i in self.items if i is not None)

    @property
    def has_output(self) -> bool:
        return False

    @property
    def is_empty(self) -> bool:
        return False

    def __eq__(self, other):
        return type(other) is UnBoundTimeSeriesReference and self.items == other.items

    def __str__(self) -> str:
        return f"REF[{', '.join(str(i) for i in self.items)}]"

    def __repr__(self) -> str:
        # For now, we should work on a better job for formatting later.
        return self.__str__()


@dataclass
class PythonTimeSeriesReferenceOutput(PythonTimeSeriesOutput, TimeSeriesReferenceOutput, Generic[TIME_SERIES_TYPE]):

    _tp: type | None = None
    _value: typing.Optional[TimeSeriesReference] = None
    # NOTE: Using dict to avoid requiring the inputs to be hashable
    _reference_observers: dict[int, TimeSeriesInput] = field(default_factory=dict)

    @property
    def value(self) -> TimeSeriesReference:
        return self._value

    @property
    def delta_value(self) -> TimeSeriesReference:
        return self._value

    @value.setter
    def value(self, v: TimeSeriesReference):
        if v is None:
            self.invalidate()
            return
        if not isinstance(v, TimeSeriesReference):
            raise TypeError(f"Expected TimeSeriesReference, got {type(v)}")
        self._value = v
        self.mark_modified()
        for observer in self._reference_observers.values():
            self._value.bind_input(observer)

    def can_apply_result(self, result: SCALAR) -> bool:
        return not self.modified

    def apply_result(self, result: SCALAR):
        if result is None:
            return
        self.value = result

    def observe_reference(self, input_: TimeSeriesInput):
        self._reference_observers[id(input_)] = input_

    def stop_observing_reference(
        self, input_: TimeSeriesInput
    ):  # TODO: this would only be called from nested graphs but there is no stop() on inputs. How do we clean these up?
        self._reference_observers.pop(id(input_), None)

    def clear(self):
        self.value = EmptyTimeSeriesReference()
        self.value = EmptyTimeSeriesReference()

    def invalidate(self):
        self._value = None
        self.mark_invalid()

    def copy_from_output(self, ts_output: TimeSeriesOutput):
        assert isinstance(ts_output, PythonTimeSeriesReferenceOutput)
        self.value = ts_output._value

    def copy_from_input(self, ts_input: TimeSeriesInput):
        assert isinstance(ts_input, PythonTimeSeriesReferenceInput)
        self.value = ts_input.value

    def is_reference(self) -> bool:
        return True



@dataclass
class PythonTimeSeriesReferenceInput(PythonBoundTimeSeriesInput, TimeSeriesReferenceInput, Generic[TIME_SERIES_TYPE]):
    """
    Reference input behaves just like a scalar input if bound to a reference output. If bound to a non-reference
    time series it will create a reference to it and show it as its value
    """

    _value: typing.Optional[TimeSeriesReference] = None
    _items: list[TimeSeriesReferenceInput] | None = None

    @property
    def bound(self) -> bool:
        return super().bound or self._items is not None

    def bind_output(self, output: TimeSeriesOutput) -> bool:
        peer = self.do_bind_output(output)

        if self.owning_node.is_started and self._output and self._output.valid:
            self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
            if self.active:
                self.notify(self._sample_time)

        return peer

    def do_bind_output(self, output: TimeSeriesOutput) -> bool:
        if isinstance(output, TimeSeriesReferenceOutput):
            self._value = None
            return super().do_bind_output(output)
        else:
            self._value = TimeSeriesReference.make(output)
            self._output = None
            if self.owning_node.is_started:
                self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
                self.notify(self._sample_time)
            else:
                self.owning_node.start_inputs.append(self)
            return False

    def un_bind_output(self, unbind_refs: bool = False):
        was_valid = self.valid
        self.do_un_bind_output(unbind_refs=unbind_refs)

        if self.owning_node.is_started and was_valid:
            self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
            if self.active:
                # Notify as the state of the node has changed from bound to un_bound
                self.owning_node.notify(self._sample_time)

    def do_un_bind_output(self, unbind_refs: bool = False):
        if self._output is not None:
            super().do_un_bind_output(unbind_refs=unbind_refs)
        if self._value is not None:
            self._value = None
            # TODO: Do we need to notify here? should we not only notify if the input is active?
            self._sample_time = (
                self.owning_graph.evaluation_clock.evaluation_time if self.owning_node.is_started else MIN_ST
            )
        if self._items:
            for item in self._items:
                item.un_bind_output(unbind_refs=unbind_refs)
            self._items = None

    def clone_binding(self, other: TimeSeriesReferenceInput):
        self.un_bind_output()
        if other.output:
            self.bind_output(other.output)
        elif other._items:
            for o, s in zip(other._items, self):
                s.clone_binding(o)
        elif other.value:
            self._value = other.value
            if self.owning_node.is_started:
                self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
                if self.active:
                    self.notify(self._sample_time)

    def start(self):
        # if the input was scheduled for start it means it wanted to be sampled on node start
        self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
        self.notify(self._sample_time)

    def make_active(self):
        if self._output is not None:
            super().make_active()
        else:
            self._active = True
            
        if self._items:
            for item in self._items:
                item.make_active()
                
        if self.valid:
            self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
            self.notify(self.last_modified_time)

    def make_passive(self):
        if self._output is not None:
            super().make_passive()
        else:            
            self._active = False

        if self._items:
            for item in self._items:
                item.make_passive()

    def __getitem__(self, item):
        if self._items is None:
            self._items = []
        while item > len(self._items) - 1:
            new_item = PythonTimeSeriesReferenceInput(_parent_or_node=self)
            self._items.append(new_item)
        return self._items[item]

    def notify_parent(self, child: "TimeSeriesInput", modified_time: datetime):
        self._value = None  # one of the items of a non-peer reference input has changed, clear the cached value
        self._sample_time = modified_time
        if self.active:
            super().notify_parent(self, modified_time)

    @property
    def value(self):
        if self._output is not None:
            return super().value
        elif self._value:
            return self._value
        elif self._items:
            self._value = TimeSeriesReference.make(from_items=[i.value for i in self._items])
            return self._value
        else:
            return None

    @property
    def delta_value(self):
        return self.value

    @property
    def modified(self) -> bool:
        if self._sampled:
            return True
        elif self._output is not None:
            return self.output.modified
        elif self._items:
            return any(i.modified for i in self._items)
        else:
            return False

    @property
    def valid(self) -> bool:
        return self._value is not None or (self._items and any(i.valid for i in self._items)) or (self._output is not None and self._output.valid)

    @property
    def all_valid(self) -> bool:
        return (self._items and all(i.all_valid for i in self._items)) or self._value is not None or super().all_valid

    @property
    def last_modified_time(self) -> datetime:
        times = []
        if self._items:
            times.extend(i.last_modified_time for i in self._items)
        if self._output is not None:
            times.append(self._output.last_modified_time)
        
        return max(*times, self._sample_time) if times else self._sample_time

    def is_reference(self) -> bool:
        return True


@dataclass
class PythonTimeSeriesListReferenceInput(PythonTimeSeriesReferenceInput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference input for TSL types - inherits generic wrapper, adds size-checked __getitem__.
    
    Inherits all binding behavior from PythonTimeSeriesReferenceInput (wraps entire output).
    Only customizes item access to enforce size and create all children at once.
    """

    _size: int = 0  # Set by builder - known size for validation
    _value_builder: typing.Optional["TSInputBuilder"] = None  # Set by builder - for creating children

    @property
    def max_size(self) -> int:
        """The declared maximum size of this REF[TSL]."""
        return self._size

    def __getitem__(self, item):
        # Size check before access
        if not isinstance(item, int) or item < 0 or item >= self._size:
            raise IndexError(f"Index {item} out of range for REF[TSL] with size {self._size}")
        
        # On first access, create ALL items at once (more efficient than one-by-one)
        if self._items is None:
            if self._value_builder is None:
                raise RuntimeError("REF[TSL] value_builder not set by builder")
            # Create all items up to size
            self._items = [
                self._value_builder.make_instance(owning_input=self)
                for _ in range(self._size)
            ]
        
        return self._items[item]
    
    def __len__(self):
        # Return actual number of created items, not the max size
        # This ensures consistency with __iter__
        return len(self._items) if self._items is not None else 0
    
    def __iter__(self):
        # Only iterate if items were already created (don't force creation)
        return iter(self._items) if self._items else iter([])


@dataclass
class PythonTimeSeriesBundleReferenceInput(PythonTimeSeriesReferenceInput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference input for TSB types - inherits generic wrapper.
    
    Marker class for type distinction. Supports both integer index (ordinal) and string key (field name) access.
    Uses parent's lazy creation logic - batch creation can be added later if needed.
    """

    _size: int = 0  # Set by builder - number of fields (for validation)
    _field_builders: list["TSInputBuilder"] | None = None  # Set by builder (for future batch creation)

    @property
    def max_size(self) -> int:
        """The declared maximum size (number of fields) of this REF[TSB]."""
        return self._size

    def __getitem__(self, item):
        # Check if integer index - could add size validation here
        if isinstance(item, int):
            # Use generic lazy creation for now
            return super().__getitem__(item)
        else:
            # String key (field name) - use generic lazy creation
            return super().__getitem__(item)


@dataclass
class PythonTimeSeriesListReferenceOutput(PythonTimeSeriesReferenceOutput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference output for TSL types - inherits generic wrapper.
    Simple marker class for type distinction.
    """
    _size: int = 0  # Set by builder for validation


@dataclass  
class PythonTimeSeriesBundleReferenceOutput(PythonTimeSeriesReferenceOutput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference output for TSB types - inherits generic wrapper.
    Simple marker class for type distinction.
    """
    _size: int = 0  # Set by builder for validation


@dataclass
class PythonTimeSeriesValueReferenceInput(PythonTimeSeriesReferenceInput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference input for TS (scalar/value) types - inherits generic wrapper.
    Marker class for type distinction, behaves identically to generic for now.
    """


@dataclass
class PythonTimeSeriesValueReferenceOutput(PythonTimeSeriesReferenceOutput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference output for TS (scalar/value) types - inherits generic wrapper.
    Marker class for type distinction.
    """


@dataclass
class PythonTimeSeriesDictReferenceInput(PythonTimeSeriesReferenceInput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference input for TSD (dict) types - inherits generic wrapper.
    Marker class for type distinction, behaves identically to generic for now.
    """


@dataclass
class PythonTimeSeriesDictReferenceOutput(PythonTimeSeriesReferenceOutput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference output for TSD (dict) types - inherits generic wrapper.
    Marker class for type distinction.
    """


@dataclass
class PythonTimeSeriesSetReferenceInput(PythonTimeSeriesReferenceInput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference input for TSS (set) types - inherits generic wrapper.
    Marker class for type distinction, behaves identically to generic for now.
    """


@dataclass
class PythonTimeSeriesSetReferenceOutput(PythonTimeSeriesReferenceOutput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference output for TSS (set) types - inherits generic wrapper.
    Marker class for type distinction.
    """


@dataclass
class PythonTimeSeriesWindowReferenceInput(PythonTimeSeriesReferenceInput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference input for TSW (window) types - inherits generic wrapper.
    Marker class for type distinction, behaves identically to generic for now.
    """


@dataclass
class PythonTimeSeriesWindowReferenceOutput(PythonTimeSeriesReferenceOutput, Generic[TIME_SERIES_TYPE]):
    """
    Specialized reference output for TSW (window) types - inherits generic wrapper.
    Marker class for type distinction.
    """
