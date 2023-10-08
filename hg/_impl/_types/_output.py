from abc import ABC
from dataclasses import dataclass, field
from typing import Optional

from hg._impl._runtime._common import NodeSubscriber
from hg._types._time_series_types import TimeSeriesOutput


__all__ = ("PythonTimeSeriesOutput",)


@dataclass
class PythonTimeSeriesOutput(TimeSeriesOutput, ABC):
    _owning_node: "Node" = None
    _parent_output: "TimeSeriesOutput" = None
    _subscribers: NodeSubscriber = field(default_factory=NodeSubscriber)

    @property
    def parent_output(self) -> Optional["TimeSeriesOutput"]:
        return self._parent_output

    @property
    def has_parent_output(self) -> bool:
        return self._parent_output is not None

    @property
    def owning_node(self) -> "Node":
        return self._owning_node

    @property
    def owning_graph(self) -> "Graph":
        return self._owning_node.owning_graph

    def subscribe_node(self, node: "Node"):
        self._subscribers.subscribe_node(node)

    def un_subscribe_node(self, node: "Node"):
        self._subscribers.un_subscribe_node(node)

    def _notify(self):
        self._subscribers.notify()

