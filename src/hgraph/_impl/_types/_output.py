import typing
from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from hgraph._runtime._constants import MIN_DT, MAX_ET
from hgraph._impl._runtime._common import SUBSCRIBER
from hgraph._types._time_series_types import TimeSeriesOutput
from hgraph._runtime._node import Node

if typing.TYPE_CHECKING:
    from hgraph._runtime._graph import Graph
    from hgraph._types._time_series_types import TimeSeries


__all__ = ("PythonTimeSeriesOutput",)


@dataclass
class PythonTimeSeriesOutput(TimeSeriesOutput, ABC):
    _parent_or_node: typing.Union["TimeSeriesOutput", "Node"] = None
    _subscribers: list["TimeSeriesInput"] = field(default_factory=list)
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
            self._notify(self.owning_graph.evaluation_clock.evaluation_time)

    def mark_modified(self, modified_time: datetime = None):
        if modified_time is None:
            if self.owning_node is None:
                modified_time = MAX_ET
            else:
                clock = self.owning_graph.evaluation_clock
                modified_time = clock.evaluation_time
        if self._last_modified_time < modified_time:
            self._last_modified_time = modified_time
            if self.has_parent_output:
                self._parent_or_node.mark_child_modified(self, modified_time)
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
        return self._parent_or_node if not isinstance(self._parent_or_node, Node) else None

    @property
    def has_parent_output(self) -> bool:
        return self._parent_or_node is not None and not isinstance(self._parent_or_node, Node)

    def re_parent(self, parent: typing.Union["Node", "TimeSeries"]):
        self._parent_or_node = parent

    @property
    def owning_node(self) -> "Node":
        if self._parent_or_node is None:
            return None
        if not isinstance(self._parent_or_node, Node):
            return self._parent_or_node.owning_node
        else:
            return self._parent_or_node

    @property
    def owning_graph(self) -> "Graph":
        return self.owning_node.graph

    def subscribe(self, subscriber: SUBSCRIBER):
        self._subscribers.append(subscriber)

    def unsubscribe(self, subscriber: SUBSCRIBER):
        self._subscribers.remove(subscriber)

    def _notify(self, modified_time):
        for ts in self._subscribers:
            ts.notify(modified_time)

    def is_reference(self) -> bool:
        return False

    def has_reference(self, other: "TimeSeries") -> bool:
        return False

