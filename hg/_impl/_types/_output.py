import typing
from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from hg import MIN_DT
from hg._impl._runtime._common import NodeSubscriber
from hg._types._time_series_types import TimeSeriesOutput

if typing.TYPE_CHECKING:
    from hg._runtime._node import Node
    from hg._runtime._graph import Graph


__all__ = ("PythonTimeSeriesOutput",)


@dataclass
class PythonTimeSeriesOutput(TimeSeriesOutput, ABC):
    _owning_node: "Node" = None
    _parent_output: "TimeSeriesOutput" = None
    _subscribers: NodeSubscriber = field(default_factory=NodeSubscriber)
    _last_modified_time: datetime = MIN_DT

    @property
    def modified(self) -> bool:
        context = self.owning_graph.context
        return context.current_engine_time == self._last_modified_time

    @property
    def last_modified_time(self) -> datetime:
        return self._last_modified_time

    def mark_invalid(self):
        self._last_modified_time = MIN_DT
        self._notify()

    def mark_modified(self):
        context = self.owning_graph.context
        et = context.current_engine_time
        if self._last_modified_time < et:
            self._last_modified_time = et
            if self._parent_output is not None:
                self._parent_output.mark_modified()
            self._notify()

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

    @property
    def owning_node(self) -> "Node":
        if self._parent_output is not None:
            return self._parent_output.owning_node
        else:
            return self._owning_node

    @property
    def owning_graph(self) -> "Graph":
        return self.owning_node.graph

    def subscribe_node(self, node: "Node"):
        self._subscribers.subscribe_node(node)

    def un_subscribe_node(self, node: "Node"):
        self._subscribers.un_subscribe_node(node)

    def _notify(self):
        self._subscribers.notify()

