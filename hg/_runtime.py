from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Mapping, Optional, TYPE_CHECKING, Any

from hg._lifecycle import ComponentLifeCycle
from hg._types._tsb_type import TimeSeriesBundleOutput

if TYPE_CHECKING:
    from hg._types import HgScalarTypeMetaData, HgTimeSeriesTypeMetaData
    from hg._types._time_series_types import TimeSeriesInput, TimeSeries, TimeSeriesOutput
    from hg._types._tsb_type import TimeSeriesBundleInput



__all__ = ("NodeSignature", "Node")


@dataclass(frozen=True)
class SourceCodeDetails:
    file: Path
    start_line: int

    def __str__(self):
        return f"{str(self.file)}: {self.start_line}"


class NodeTypeEnum(Enum):
    PUSH_SOURCE_NODE = 0
    PULL_SOURCE_NODE = 1
    COMPUTE_NODE = 2
    SINK_NODE = 3


@dataclass
class NodeSignature:
    name: str
    node_type: NodeTypeEnum
    args: tuple[str, ...]
    scalars: Mapping[str, "HgScalarTypeMetaData"]
    time_series_inputs: Mapping[str, "HgTimeSeriesTypeMetaData"]
    defaults: Mapping[str, Any]
    time_series_outputs: Mapping[str, "HgTimeSeriesTypeMetaData"]


class Node(ComponentLifeCycle, ABC):

    @property
    @abstractmethod
    def signature(self) -> NodeSignature:
        """
        The signature of the Node provides useful information to describe the node.
        This can be used for exception and debugging purposes.
        """

    @property
    @abstractmethod
    def input(self) -> Optional["TimeSeriesBundleInput"]:
        """
        The input as an Unnamed Bundle. This allows the input to be considered as a TSB
        which is helpful for standardising handling of inputs. The bundle schema is the
        collection of inputs that are of time-series types.
        """

    @property
    @abstractmethod
    def inputs(self) -> Optional[Mapping[str, "TimeSeriesInput"]]:
        """
        The inputs associated to this node.
        """

    @property
    @abstractmethod
    def output(self) -> Optional["TimeSeriesOutput"]:
        """
        The output of this node. This could be a TimeSeriesBundleOutput or a single output value.
        """

    @property
    @abstractmethod
    def outputs(self) -> Optional[Mapping[str, "TimeSeriesOutput"]]:
        """
        The outputs of the node. If the node has a single defined output then this is just {"out": Output},
        however if the node was defined with multiple outputs using the "un-named bundle" dictionary format,
        then these are the outputs defined by the dictionary definition.
        """

    def eval(self):
        """Called by the graph evaluation engine when the node has been scheduled for evaluation."""


@dataclass
class Graph(ComponentLifeCycle):
    """ The runtime graph """

    nodes: tuple[Node, ...]  # The nodes of the graph.


@dataclass
class ExecutionContext:
    current_engine_time: datetime
    wall_clock_time: datetime
    engine_lag: timedelta

