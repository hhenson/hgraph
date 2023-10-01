from _pytest.nodes import Node

from hg._builder._builder import Builder
from hg._types._time_series_types import TimeSeriesInput


class InputBuilder(Builder[TimeSeriesInput]):

    def make_instance(self, owning_node: Node = None, owning_input: TimeSeriesInput = None) -> TimeSeriesInput:
        """One of owning_node or owning_input must be defined."""
        pass

    def release_instance(self, item: TimeSeriesInput):
        pass
