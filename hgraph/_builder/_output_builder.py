from typing import TYPE_CHECKING

from hgraph._builder._builder import Builder

if TYPE_CHECKING:
    from hgraph._runtime._node import Node
    from hgraph._types._time_series_types import TimeSeriesOutput


__all__ = ("OutputBuilder",)


class OutputBuilder(Builder["TimeSeriesOutput"]):

    def make_instance(self, owning_node: "Node" = None, owning_output: "TimeSeriesOutput" = None) -> "TimeSeriesOutput":
        """One of owning_node or owning_output must be defined."""
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesOutput"):
        raise NotImplementedError()
