import typing
from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from hgraph._runtime._constants import MIN_DT
from hgraph._impl._runtime._common import TimeSeriesSubscriber, SUBSCRIBER
from hgraph._types._time_series_types import TimeSeriesOutput

if typing.TYPE_CHECKING:
    from hgraph._runtime._node import Node
    from hgraph._runtime._graph import Graph
    from hgraph._types._time_series_types import TimeSeries


__all__ = ("PythonTimeSeriesOutput",)


@dataclass
class PythonTimeSeriesOutput(TimeSeriesOutput, ABC):
    _owning_node: Optional["Node"] = None
    _parent_output: Optional["TimeSeriesOutput"] = None
    _subscribers: TimeSeriesSubscriber = field(default_factory=TimeSeriesSubscriber)
    _last_modified_time: datetime = MIN_DT

    @property
    def modified(self) -> bool:
        context = self.owning_graph.evaluation_clock
        return context.evaluation_time == self._last_modified_time

    @property
    def last_modified_time(self) -> datetime:
        return self._last_modified_time

    def mark_invalid(self):
        if self._last_modified_time > MIN_DT:
            self._last_modified_time = MIN_DT
            self._notify(self._last_modified_time)

    def mark_modified(self, modified_time: datetime = None):
        if modified_time is None:
            clock = self.owning_graph.evaluation_clock
            modified_time = clock.evaluation_time
        if self._last_modified_time < modified_time:
            self._last_modified_time = modified_time
            if self._parent_output is not None:
                self._parent_output.mark_child_modified(self, modified_time)
            self._notify(modified_time)

    def mark_child_modified(self, child: "TimeSeriesOutput", modified_time: datetime):
        self.mark_modified(modified_time)

    @property
    def valid(self) -> bool:
        return self.last_modified_time > MIN_DT

    @property
    def all_valid(self) -> bool:
        return self.valid  # By default assume that all valid if this output is valid

    @property
    def parent_output(self) -> Optional["TimeSeriesOutput"]:
        return self._parent_output

    @property
    def has_parent_output(self) -> bool:
        return self._parent_output is not None

    def re_parent(self, parent: typing.Union["Node", "TimeSeries"]):
        if isinstance(parent, Node):
            self._owning_node = parent
            self._parent_input = None
        else:
            self._owning_node = None
            self._parent_input = parent

    @property
    def owning_node(self) -> "Node":
        if self._parent_output is not None:
            return self._parent_output.owning_node
        else:
            return self._owning_node

    @property
    def owning_graph(self) -> "Graph":
        return self.owning_node.graph

    def subscribe(self, subscriber: SUBSCRIBER):
        self._subscribers.subscribe(subscriber)

    def unsubscribe(self, subscriber: SUBSCRIBER):
        self._subscribers.unsubscribe(subscriber)

    def _notify(self, modified_time):
        self._subscribers.notify(modified_time)
