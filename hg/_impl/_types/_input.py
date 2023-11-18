from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from hg._impl._types._output import PythonTimeSeriesOutput
from hg._types._time_series_types import TimeSeriesInput, TimeSeriesOutput

__all__ = ("PythonTimeSeriesInput",)


@dataclass
class PythonTimeSeriesInput(TimeSeriesInput, ABC):
    _owning_node: "Node" = None
    _parent_input: "TimeSeriesInput" = None

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


@dataclass
class PythonBoundTimeSeriesInput(PythonTimeSeriesInput, ABC):
    """
    Bound time-series values will have a single output mapped to it.
    This allows for the simple implementation of the active/passive state.
    Also for the Python implementation we can just drop the typing on the properties for value and delta_value
    and these can be supported directly.
    """
    _output: TimeSeriesOutput = None
    _active: bool = False

    @property
    def active(self) -> bool:
        return self._active

    def make_active(self):
        if not self._active:
            self._active = True
            if self.bound:
                self._output.subscribe_node(self.owning_node)

    def make_passive(self):
        if self._active:
            self._active = False
            if self.bound:
                self._output.un_subscribe_node(self.owning_node)

    @property
    def output(self) -> TimeSeriesOutput:
        return self._output

    def bind_output(self, output: TimeSeriesOutput):
        active = self.active
        self.make_passive()  # Ensure we are unsubscribed from the old output.
        self._output = output
        if active:
            self.make_active()  # If we were active now subscribe to the new output,
            # this is important even if we were not bound previously as this will ensure the new output gets
            # subscribed to

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
        return self._output.value

    @property
    def delta_value(self):
        return self._output.delta_value

    @property
    def modified(self) -> bool:
        return self._output.modified

    @property
    def valid(self) -> bool:
        if self.bound:
            return self._output.valid
        else:
            return False

    @property
    def all_valid(self) -> bool:
        return self._output.all_valid

    @property
    def last_modified_time(self) -> datetime:
        return self._output.last_modified_time