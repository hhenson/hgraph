from _pytest.nodes import Node

from hg._builder._builder import Builder
from hg._types._time_series_types import TimeSeriesOutput


class OutputBuilder(Builder[TimeSeriesOutput]):

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None) -> TimeSeriesOutput:
        """One of owning_node or owning_output must be defined."""
        pass

    def release_instance(self, item: TimeSeriesOutput):
        pass

