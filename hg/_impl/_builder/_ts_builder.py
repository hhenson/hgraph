from hg._builder._ts_builder import TimeSeriesValueOutputBuilder
from hg._impl._types._ts import PythonTimeSeriesValueOutput
from hg._types._ts_type import TimeSeriesValueOutput


class PythonTimeSeriesValueOutputBuilder(TimeSeriesValueOutputBuilder):

    def make_instance(self, owning_node=None, owning_output=None):
        return PythonTimeSeriesValueOutput(_tp=self.value_tp.py_type)

    def release_instance(self, item):
        pass