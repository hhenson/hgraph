from dataclasses import dataclass
from typing import Optional

from hg._types._time_series_types import TimeSeriesInput


__all__ = ("PythonTimeSeriesInput",)


@dataclass
class PythonTimeSeriesInput(TimeSeriesInput):
    _owning_node: "Node" = None
    _parent_input: "TimeSeriesInput" = None

    @property
    def owning_node(self) -> "Node":
        return self._parent_input.owning_graph if self._parent_input is None else self._owning_node

    @property
    def owning_graph(self) -> "Graph":
        return self.owning_node.graph

    @property
    def parent_input(self) -> Optional["TimeSeriesInput"]:
        return self._parent_input

    @property
    def has_parent_input(self) -> bool:
        return self._parent_input is not None

