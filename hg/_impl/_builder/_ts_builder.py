from hg._builder._ts_builder import TimeSeriesValueOutputBuilder


class PythonTimeSeriesValueOutputBuilder(TimeSeriesValueOutputBuilder):

    def make_instance(self, owning_node=None, owning_output=None):
        pass

    def release_instance(self, item):
        pass