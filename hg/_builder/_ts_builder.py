from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from _pytest.nodes import Node

from hg._builder._input_builder import InputBuilder
from hg._builder._output_builder import OutputBuilder
from hg._types._scalar_type_meta_data import HgScalarTypeMetaData
from hg._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hg._types._time_series_types import TimeSeriesInput, TimeSeriesOutput
from hg._types._tsb_type import TimeSeriesSchema

__all__ = ("TSOutputBuilder", "TSInputBuilder", "TimeSeriesBuilderFactory")


@dataclass(frozen=True)
class TSOutputBuilder(OutputBuilder):

    value_tp: HgScalarTypeMetaData

    def make_instance(self, owning_node=None, owning_output=None) -> TimeSeriesOutput:
        pass

    def release_instance(self, item: TimeSeriesOutput):
        pass


@dataclass(frozen=True)
class TSInputBuilder(InputBuilder):

    value_tp: HgScalarTypeMetaData

    def make_instance(self, owning_node: Node = None, owning_input: TimeSeriesInput = None) -> TimeSeriesInput:
        ...

    def release_instance(self, item: TimeSeriesInput):
        ...


class TSSignalInputBuilder(InputBuilder):

    def make_instance(self, owning_node: Node = None, owning_input: TimeSeriesInput = None) -> TimeSeriesInput:
        ...

    def release_instance(self, item: TimeSeriesInput):
        ...


@dataclass(frozen=True)
class TSBInputBuilder(InputBuilder):

    schema: TimeSeriesSchema

    def make_instance(self, owning_node: Node = None, owning_input: TimeSeriesInput = None) -> TimeSeriesInput:
        ...

    def release_instance(self, item: TimeSeriesInput):
        ...


@dataclass(frozen=True)
class TSBOutputBuilder(OutputBuilder):

    schema: TimeSeriesSchema

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None) -> TimeSeriesOutput:
        ...

    def release_instance(self, item: TimeSeriesOutput):
        ...


@dataclass(frozen=True)
class TSSOutputBuilder(OutputBuilder):

    value_tp: HgScalarTypeMetaData

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None) -> TimeSeriesOutput:
        ...

    def release_instance(self, item: TimeSeriesOutput):
        ...


@dataclass(frozen=True)
class TSSInputBuilder(InputBuilder):

    value_tp: HgScalarTypeMetaData

    def make_instance(self, owning_node: Node = None, owning_input: TimeSeriesInput = None) -> TimeSeriesInput:
        ...

    def release_instance(self, item: TimeSeriesInput):
        ...


class TimeSeriesBuilderFactory:

    _instance: Optional["TimeSeriesBuilderFactory"] = None

    @staticmethod
    def declare_default_factory():
        from hg._impl._builder._ts_builder import PythonTimeSeriesBuilderFactory
        TimeSeriesBuilderFactory.declare(PythonTimeSeriesBuilderFactory())

    @staticmethod
    def has_instance() -> bool:
        return TimeSeriesBuilderFactory._instance is not None

    @staticmethod
    def instance() -> "TimeSeriesBuilderFactory":
        if TimeSeriesBuilderFactory._instance is None:
            raise RuntimeError("No time-series builder factory has been declared")
        return TimeSeriesBuilderFactory._instance

    @staticmethod
    def declare(factory: "TimeSeriesBuilderFactory"):
        if TimeSeriesBuilderFactory._instance is not None:
            raise RuntimeError("A time-series builder factory has already been declared")
        TimeSeriesBuilderFactory._instance = factory

    @staticmethod
    def un_declare():
        TimeSeriesBuilderFactory._instance = None

    @abstractmethod
    def make_input_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSInputBuilder:
        """Return an instance of an input builder for the given type"""

    @abstractmethod
    def make_output_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSOutputBuilder:
        """Return an instance of an output builder for the given type"""

