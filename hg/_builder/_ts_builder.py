from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Mapping

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


@dataclass(frozen=True)
class TSBInputBuilder(InputBuilder):

    schema: TimeSeriesSchema

    def make_instance(self, owning_node: Node = None, owning_input: TimeSeriesInput = None) -> TimeSeriesInput:
        ...

    def release_instance(self, item: TimeSeriesInput):
        ...


class TimeSeriesBuilderFactory(ABC):
    """
    A factory for creating input and output builders for time series types provided.
    A factory must be declared prior to building a graph.
    """

    _instance: ["TimeSeriesBuilderFactory"] = []

    def __enter__(self):
        self._instance.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._instance.pop()

    @staticmethod
    def instance() -> "TimeSeriesBuilderFactory":
        return TimeSeriesBuilderFactory._instance[-1]

    @abstractmethod
    def make_input_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSInputBuilder:
        """Return an instance of an input builder for the given type"""

    @abstractmethod
    def make_output_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSOutputBuilder:
        """Return an instance of an output builder for the given type"""
