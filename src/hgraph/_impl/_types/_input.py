from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, TYPE_CHECKING, Union

from hgraph._runtime._constants import MIN_DT, MAX_DT
from hgraph._types._time_series_types import TimeSeriesInput, TimeSeriesOutput
from hgraph._runtime._node import Node

if TYPE_CHECKING:
    from hgraph._builder._graph_builder import Graph
    from hgraph._types._time_series_types import TimeSeries

__all__ = ("PythonTimeSeriesInput", "PythonBoundTimeSeriesInput")


@dataclass
class PythonTimeSeriesInput(TimeSeriesInput, ABC):
    _owning_node: Optional["Node"] = None
    _parent_input: Optional["TimeSeriesInput"] = None

    @property
    def owning_node(self) -> "Node":
        return self._parent_input.owning_node if self._owning_node is None else self._owning_node

    @property
    def owning_graph(self) -> "Graph":
        return self.owning_node.graph

    @property
    def parent_input(self) -> Optional["TimeSeriesInput"]:
        return self._parent_input

    @property
    def has_parent_input(self) -> bool:
        return self._parent_input is not None

    def re_parent(self, parent: Union["Node", "TimeSeries"]):
        if isinstance(parent, Node):
            self._owning_node = parent
            self._parent_input = None
        else:
            self._owning_node = None
            self._parent_input = parent

    @abstractmethod
    def notify(self, modified_time: datetime):
        pass

    def notify_parent(self, child: "TimeSeriesInput", modified_time: datetime):
        self.notify(modified_time)


@dataclass
class PythonBoundTimeSeriesInput(PythonTimeSeriesInput, ABC):
    """
    Bound time-series values will have a single output mapped to it.
    This allows for the simple implementation of the active/passive state.
    Also for the Python implementation we can just drop the typing on the properties for value and delta_value
    and these can be supported directly.
    """

    _output: TimeSeriesOutput | None = None

    _reference_output: TimeSeriesOutput | None = (
        None  # TODO: This might be refactored into a generic binding observer pattern
    )
    # however there is no guarantee that if there were other types of observers they would not clash with the
    # references, so probably this is required to be this way. I am just a little annoyed with the growth of the object

    _subscribe_input: bool = False
    _active: bool = False
    _sample_time: datetime = MIN_DT
    _notify_time: datetime = MIN_DT

    @property
    def active(self) -> bool:
        return self._active

    def set_subscribe_method(self, subscribe_input: bool):
        self._subscribe_input = subscribe_input

    def make_active(self):
        if not self._active:
            self._active = True
            if self.bound:
                self._output.subscribe(self if self._subscribe_input else self.owning_node)
                if self._output.valid and self._output.modified:
                    self.notify(self._output.last_modified_time)
                    return  # If the output is modified we do not need to check if sampled

            if self._sampled:
                self.notify(self._sample_time)

    def make_passive(self):
        if self._active:
            self._active = False
            if self.bound:
                self._output.unsubscribe(self if self._subscribe_input else self.owning_node)

    def notify(self, modified_time: datetime):
        if self._notify_time != modified_time:
            self._notify_time = modified_time
            (
                self.parent_input.notify_parent(self, modified_time)
                if self.parent_input
                else self.owning_node.notify(modified_time)
            )

    @property
    def output(self) -> TimeSeriesOutput:
        return self._output

    def bind_output(self, output: TimeSeriesOutput) -> bool:
        from hgraph import TimeSeriesReferenceOutput

        if isinstance(output, TimeSeriesReferenceOutput):
            if output.value:
                output.value.bind_input(self)
            output.observe_reference(self)
            self._reference_output = output
            peer = False
        else:
            if output is self._output:
                return self.has_peer

            peer = self.do_bind_output(output)

        if (self.owning_node.is_started or self.owning_node.is_starting) and self._output and self._output.valid:
            self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
            if self.active:
                self.notify(
                    self._sample_time
                )  # TODO: This might belong to make_active, or not? THere is a race with setting sample time too

        return peer

    def un_bind_output(self):
        valid = self.valid
        if self.bound:
            from hgraph import TimeSeriesReferenceOutput

            output = self._output
            if isinstance(output, TimeSeriesReferenceOutput):
                output.stop_observing_reference(self)
                self._reference_output = None
            self.do_un_bind_output()

            if self.owning_node.is_started and valid:
                self._sample_time = self.owning_graph.evaluation_clock.evaluation_time
                if self.active:
                    # Notify as the state of the node has changed from bound to un_bound
                    self.owning_node.notify(self._sample_time)

    def do_bind_output(self, output: TimeSeriesOutput) -> bool:
        active = self.active
        self.make_passive()  # Ensure we are unsubscribed from the old output.
        self._output = output
        if active:
            self.make_active()  # If we were active now subscribe to the new output,
            # this is important even if we were not bound previously as this will ensure the new output gets
            # subscribed to
        return True

    def do_un_bind_output(self):
        if self.active:
            self._output.unsubscribe(self.owning_node)
        self._output = None

    @property
    def bound(self) -> bool:
        return self._output is not None

    @property
    def has_peer(self) -> bool:
        # By default we assume that if there is an output then we are peered.
        # This is not always True, but is a good general assumption.
        return self._output is not None

    @property
    def value(self):
        return self._output.value if self._output else None

    @property
    def delta_value(self):
        return self._output.delta_value if self._output else None

    @property
    def modified(self) -> bool:
        return self._output is not None and (self._output.modified or self._sampled)

    @property
    def _sampled(self) -> bool:
        return self._sample_time != MIN_DT and self._sample_time == self.owning_graph.evaluation_clock.evaluation_time

    @property
    def valid(self) -> bool:
        return self.bound and self._output.valid

    @property
    def all_valid(self) -> bool:
        return self.bound and self._output.all_valid

    @property
    def last_modified_time(self) -> datetime:
        return max(self._output.last_modified_time, self._sample_time) if self.bound else MIN_DT
