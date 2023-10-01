from dataclasses import dataclass

from hg._builder._output_builder import OutputBuilder


@dataclass(frozen=True)
class TimeSeriesValueOutputBuilder(OutputBuilder):



    def make_instance(self, owning_node=None, owning_output=None):
        pass

    def release_instance(self, item):
        pass