import typing
from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic

from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._impl._types._output import PythonTimeSeriesOutput
from hgraph._runtime._constants import MIN_DT, MIN_ST
from hgraph._types._ref_type import TimeSeriesReference, TimeSeriesReferenceOutput, TimeSeriesReferenceInput
from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import TimeSeriesInput, TIME_SERIES_TYPE, TimeSeriesOutput


__all__ = ("PythonTimeSeriesReference", "PythonTimeSeriesReferenceOutput", "PythonTimeSeriesReferenceInput")


class PythonTimeSeriesReference(TimeSeriesReference):
    def __init__(
        self,
        ts: typing.Optional[TimeSeriesInput | TimeSeriesOutput] = None,
        from_items: typing.Iterable[TimeSeriesReference] = None,
    ):
        self._output = None

        if from_items is not None:  # Put this first to make it clearer that
            self.items = from_items
            self.tp = None
            self.has_peer = False
            self.valid = True
            return

        if ts is None:  # We have already validated that from_items is None, so now if ts is None as well, ...
            self.valid = False
            self.has_peer = False
            return

        if isinstance(ts, TimeSeriesOutput):  # constructing from sn output
            self._output = ts
            tp = type(ts)
            has_peer = True
        elif isinstance(
            ts, TimeSeriesReferenceInput
        ):  # reference input gets the value from a ref output, copy construct
            ref = ts.value
            if has_peer := ref.has_peer:
                self._output = ref.output
            else:
                self.items = ref.items
            self.has_peer = ref.has_peer
            tp = ref.tp
        elif has_peer := ts.has_peer:  # any input with a peer, construct from its output
            self._output = ts.output
            tp = type(ts)
        else:
            # Rely on the assumption that all time-series' that support peering are also iterable.
            # including a reference input that was bound to a free tsl/tsb
            self.items = [PythonTimeSeriesReference(item) for item in ts]
            tp = type(ts)
            has_peer = False

        self.has_peer = has_peer
        self.tp = tp
        self.valid = True

    @property
    def output(self) -> TimeSeriesOutput:
        return self._output

    def bind_input(self, ts_input: TimeSeriesInput):
        if not self.valid:
            ts_input.un_bind_output()
            return

        # NOTE: The ctor remembers the type, this checks the target is the same type unless was constructed from an output
        if self.tp and not issubclass(self.tp, TimeSeriesOutput) and not isinstance(ts_input, self.tp):
            raise TypeError(f"Cannot bind reference of type {self.tp} to {type(ts_input)}")

        if self.has_peer:
            ts_input.bind_output(self.output)
        else:
            for item, r in zip(ts_input, self.items):
                if r:
                    r.bind_input(item)

    def __str__(self) -> str:
        if self.output is not None:
            return (
                f"REF[{self.output.owning_node.signature.name}"
                f"<{', '.join(str(i) for i in self.output.owning_node.node_id)}>.out<{hex(id(self.output))}>]"
            )
        elif self.valid and not self.has_peer and self.items:
            return f"REF[{', '.join(str(i) for i in self.items)}]"
        else:
            return "REF[<UnSet>]"

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

    def invalidate(self):
        self._value = None
        self.mark_invalid()

    def copy_from_output(self, ts_output: TimeSeriesOutput):
        assert isinstance(ts_output, PythonTimeSeriesReferenceOutput)
        self.value = ts_output._value

    def copy_from_input(self, ts_input: TimeSeriesInput):
        assert isinstance(ts_input, PythonTimeSeriesReferenceInput)
        self.value = ts_input.value


@dataclass
class PythonTimeSeriesReferenceInput(PythonBoundTimeSeriesInput, TimeSeriesReferenceInput, Generic[TIME_SERIES_TYPE]):
    """
    Reference input behaves just like a scalar input if bound to a reference output. If bound to a non-reference
    time series it will create a reference to it and show it as its value
    """

    _value: typing.Optional[TimeSeriesReference] = None
    _items: list[TimeSeriesReferenceInput] | None = None

    def bind_output(self, output: TimeSeriesOutput) -> bool:
        peer = self.do_bind_output(output)

        if self.owning_node.is_started and self._output and self._output.valid:
            self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
            if self.active:
                self.notify(self._sample_time)

        return peer

    def do_bind_output(self, output: TimeSeriesOutput) -> bool:
        if isinstance(output, TimeSeriesReferenceOutput):
            return super().do_bind_output(output)
        else:
            self._value = PythonTimeSeriesReference(output)
            if self.owning_node.is_started:
                self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
                self.notify(self._sample_time)
            else:
                self.owning_node.start_inputs.append(self)
            return False

    def do_un_bind_output(self):
        super().do_un_bind_output()
        if self._value is not None:
            self._value = None
            # TODO: Do we need to notify here? should we not only notify if the input is active?
            self._sample_time = (
                self.owning_graph.evaluation_clock.evaluation_time if self.owning_node.is_started else MIN_ST
            )

    def start(self):
        # if the input was scheduled for start it means it wanted to be sampled on node start
        self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
        self.notify(self._sample_time)

    def __getitem__(self, item):
        if self._items is None:
            self._items = []
        while item > len(self._items) - 1:
            new_item = PythonTimeSeriesReferenceInput(_owning_node=self._owning_node, _parent_input=self)
            new_item.set_subscribe_method(subscribe_input=True)
            new_item.make_active()
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
            self._value = PythonTimeSeriesReference(from_items=[i.value for i in self._items])
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
        return self._value is not None or (self._items and any(i.valid for i in self._items)) or super().valid

    @property
    def all_valid(self) -> bool:
        return (self._items and all(i.all_valid for i in self._items)) or self._value is not None or super().all_valid

    @property
    def last_modified_time(self) -> datetime:
        if self._items:
            return max(i.last_modified_time for i in self._items)
        elif self._output is not None:
            return self._output.last_modified_time
        else:
            return self._sample_time
