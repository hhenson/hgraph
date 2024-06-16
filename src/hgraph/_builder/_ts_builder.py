from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from _pytest.nodes import Node

from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._builder._input_builder import InputBuilder
from hgraph._builder._output_builder import OutputBuilder
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData

if TYPE_CHECKING:
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
    from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
    from hgraph._types._time_series_types import TimeSeriesInput, TimeSeriesOutput
    from hgraph._types._tsb_type import TimeSeriesSchema

__all__ = (
    "TSOutputBuilder",
    "TSInputBuilder",
    "TimeSeriesBuilderFactory",
    "TSSInputBuilder",
    "TSLOutputBuilder",
    "TSLInputBuilder",
    "TSBOutputBuilder",
    "TSBInputBuilder",
    "TSSOutputBuilder",
    "TSSInputBuilder",
    "TSSignalInputBuilder",
    "TSDOutputBuilder",
    "TSDInputBuilder",
)

"""
This file contains the base signatures for the time-series builders. The implementation needs to provide
valid instances of each of these signatures and the factory should be provided to resolved an instance of the
template to the appropriate builder instance.

This is here to allow us to write generic wiring logic that will allow for multiple implementations of the engine.
"""


@dataclass(frozen=True)
class TSOutputBuilder(OutputBuilder):

    value_tp: "HgScalarTypeMetaData"

    def make_instance(self, owning_node=None, owning_output=None) -> "TimeSeriesOutput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesOutput"):
        raise NotImplementedError()


@dataclass(frozen=True)
class TSInputBuilder(InputBuilder):

    value_tp: "HgScalarTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_input: "TimeSeriesInput" = None) -> "TimeSeriesInput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesInput"):
        raise NotImplementedError()


class TSSignalInputBuilder(InputBuilder):

    def make_instance(self, owning_node: Node = None, owning_input: "TimeSeriesInput" = None) -> "TimeSeriesInput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesInput"):
        raise NotImplementedError()


@dataclass(frozen=True)
class TSBInputBuilder(InputBuilder):

    schema: "TimeSeriesSchema"

    def make_instance(self, owning_node: Node = None, owning_input: "TimeSeriesInput" = None) -> "TimeSeriesInput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesInput"):
        raise NotImplementedError()


@dataclass(frozen=True)
class TSBOutputBuilder(OutputBuilder):

    schema: "TimeSeriesSchema"

    def make_instance(self, owning_node: Node = None, owning_output: "TimeSeriesOutput" = None) -> "TimeSeriesOutput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesOutput"):
        raise NotImplementedError()


@dataclass(frozen=True)
class TSLInputBuilder(InputBuilder):

    value_tp: "HgTimeSeriesTypeMetaData"
    size_tp: "HgScalarTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_input: "TimeSeriesInput" = None) -> "TimeSeriesInput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesInput"):
        raise NotImplementedError()


@dataclass(frozen=True)
class TSLOutputBuilder(OutputBuilder):

    value_tp: "HgTimeSeriesTypeMetaData"
    size_tp: "HgScalarTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_output: "TimeSeriesOutput" = None) -> "TimeSeriesOutput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesOutput"):
        raise NotImplementedError()


class TSDInputBuilder(InputBuilder):

    key_tp: "HgScalarTypeMetaData"
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_input: "TimeSeriesInput" = None) -> "TimeSeriesInput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesInput"):
        raise NotImplementedError()


@dataclass(frozen=True)
class TSDOutputBuilder(OutputBuilder):

    key_tp: "HgScalarTypeMetaData"
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_output: "TimeSeriesOutput" = None) -> "TimeSeriesOutput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesOutput"):
        raise NotImplementedError()


@dataclass(frozen=True)
class TSSOutputBuilder(OutputBuilder):

    value_tp: "HgScalarTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_output: "TimeSeriesOutput" = None) -> "TimeSeriesOutput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesOutput"):
        raise NotImplementedError()


@dataclass(frozen=True)
class TSSInputBuilder(InputBuilder):

    value_tp: "HgScalarTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_input: "TimeSeriesInput" = None) -> "TimeSeriesInput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesInput"):
        raise NotImplementedError()


class REFInputBuilder(InputBuilder):
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_input: "TimeSeriesInput" = None) -> "TimeSeriesInput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesInput"):
        raise NotImplementedError()


@dataclass(frozen=True)
class REFOutputBuilder(OutputBuilder):
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_output: "TimeSeriesOutput" = None) -> "TimeSeriesOutput":
        raise NotImplementedError()

    def release_instance(self, item: "TimeSeriesOutput"):
        raise NotImplementedError()


class TimeSeriesBuilderFactory:

    _instance: Optional["TimeSeriesBuilderFactory"] = None

    @staticmethod
    def declare_default_factory():
        from hgraph._impl._builder._ts_builder import PythonTimeSeriesBuilderFactory

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
    def make_input_builder(self, value_tp: "HgTimeSeriesTypeMetaData") -> TSInputBuilder:
        """Return an instance of an input builder for the given type"""

    @abstractmethod
    def make_output_builder(self, value_tp: "HgTimeSeriesTypeMetaData") -> TSOutputBuilder:
        """Return an instance of an output builder for the given type"""

    def make_error_builder(self, error_tp: "HgTimeSeriesTypeMetaData") -> TSOutputBuilder:
        """
        Return an instance of an output builder for the error type.
        Since the error type is fixed, this does not require a type.
        Additionally, since this is defined in terms of an abstract method,
        we can implement directly.

        The node may require a collection type output. The is_tsd and scalar_tp arguments should be provided
        if the node requires a tsd or is_tsl and size_tp arguments are required if the node requires a list type.
        """
        return self.make_output_builder(error_tp)
