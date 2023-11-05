from dataclasses import dataclass
from typing import Mapping, cast

from frozendict import frozendict

from hg._builder._ts_builder import (TSOutputBuilder, TimeSeriesBuilderFactory,
                                     TSInputBuilder, TSBInputBuilder, TSSignalInputBuilder, TSBOutputBuilder)
from hg._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hg._types._time_series_types import TimeSeriesOutput
from hg._types._ts_meta_data import HgTSTypeMetaData
from hg._types._ts_signal_meta_data import HgSignalMetaData
from hg._types._tsb_meta_data import HgTSBTypeMetaData
from hg._runtime._node import Node

__all__ = ('PythonTSOutputBuilder', 'PythonTSInputBuilder', 'PythonTimeSeriesBuilderFactory')


class PythonTSOutputBuilder(TSOutputBuilder):

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hg import PythonTimeSeriesValueOutput
        return PythonTimeSeriesValueOutput(_owning_node=owning_node, _parent_output=owning_output,
                                           _tp=self.value_tp.py_type)

    def release_instance(self, item):
        pass


class PythonTSInputBuilder(TSInputBuilder):

    def make_instance(self, owning_node=None, owning_input=None):
        from hg import PythonTimeSeriesValueInput
        return PythonTimeSeriesValueInput(_owning_node=owning_node, _parent_input=owning_input)

    def release_instance(self, item):
        pass


class PythonSignalInputBuilder(TSSignalInputBuilder):

    def make_instance(self, owning_node=None, owning_input=None):
        from hg import PythonTimeSeriesSignal
        return PythonTimeSeriesSignal(_owning_node=owning_node, _parent_input=owning_input)

    def release_instance(self, item):
        pass


@dataclass(frozen=True)
class PythonTSBOutputBuilder(TSBOutputBuilder):
    schema_builders: Mapping[str, TSOutputBuilder] = None

    def __post_init__(self):
        factory = TimeSeriesBuilderFactory.instance()
        object.__setattr__(self, 'schema_builders', frozendict(
            {k: factory.make_output_builder(v) for k, v in self.schema.items()}))

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hg import PythonTimeSeriesBundleOutput
        tsb = PythonTimeSeriesBundleOutput[self.schema](_owning_node=owning_node, _parent_output=owning_output)
        tsb._ts_value = {k: v.make_instance(owning_output=tsb) for k, v in
                         self.schema_builders.items()}
        return tsb

    def release_instance(self, item):
        pass


@dataclass(frozen=True)
class PythonTSBInputBuilder(TSBInputBuilder):
    schema_builders: Mapping[str, TSInputBuilder] = None

    def __post_init__(self):
        factory = TimeSeriesBuilderFactory.instance()
        object.__setattr__(self, 'schema_builders', frozendict(
            {k: factory.make_input_builder(v) for k, v in self.schema.items()}))

    def make_instance(self, owning_node=None, owning_input=None):
        from hg import PythonTimeSeriesBundleInput
        tsb = PythonTimeSeriesBundleInput[self.schema](_owning_node=owning_node, _parent_input=owning_input)
        tsb._ts_value = {k: v.make_instance(owning_input=tsb) for k, v in
                         self.schema_builders.items()}
        return tsb

    def release_instance(self, item):
        pass


def _throw(value_tp):
    raise TypeError(f"Got unexpected value type {type(value_tp)}: {value_tp}")


class PythonTimeSeriesBuilderFactory(TimeSeriesBuilderFactory):

    def make_input_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSInputBuilder:
        return {
            HgTSTypeMetaData: lambda: PythonTSInputBuilder(value_tp=cast(HgTSTypeMetaData, value_tp).value_scalar_tp),
            HgTSBTypeMetaData: lambda: PythonTSBInputBuilder(
                schema=cast(HgTSBTypeMetaData, value_tp).bundle_schema_tp.py_type),
            HgSignalMetaData: lambda: PythonSignalInputBuilder(),
        }.get(type(value_tp), lambda: _throw(value_tp))()

    def make_output_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSOutputBuilder:
        return {
            HgTSTypeMetaData: lambda: PythonTSOutputBuilder(value_tp=value_tp.value_scalar_tp),
            HgTSBTypeMetaData: lambda: PythonTSBOutputBuilder(schema=value_tp.bundle_schema_tp.py_type),
        }.get(type(value_tp), lambda: _throw(value_tp))()
