import functools
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
    def node_ndx(self) -> int:
        """
        The relative index of this node within the parent graph's list of nodes.
        """

    @property
    @abstractmethod
    def owning_graph_id(self) -> tuple[int, ...]:
        """
        The path from the root graph to the graph containing this node. This is effectively
        the node_id less the last entry. Thus, the root graph is referenced as (),
        the first child if (node_ndx of nested_1), ...
        """

    @property
    @abstractmethod
    def node_id(self) -> tuple[int, ...]:
        """
        The unique path reference to this node from the root graph running in the system.
        For a node directly attached to the root graph, the path will be:
        (node_ndx)
        For a node within a nested graph structure, it will be something like:
        (node_ndx of nested_1, ..., node_ndx of nested_n, node_ndx)
        For nodes with a dynamic nested structure such as a branch, a unique id (integer) is allocated to
        a branch key and this id is used to represent the key in the path.
        This is similar to the categorical concept in dataframes.
        """

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
    graph_id: tuple[int, ...]
    nodes: tuple[Node, ...]  # The nodes of the graph.

    @functools.cached_property
    def push_source_nodes_end(self) -> int:
        """ The index of the first compute node """
        for i in range(len(self.nodes)):
            if self.nodes[i].signature.node_type != NodeTypeEnum.PUSH_SOURCE_NODE:
                return i
        return len(self.nodes) # In the very unlikely event that there are only push source nodes.


class ExecutionContext:

    @property
    @abstractmethod
    def current_engine_time(self) -> datetime:
        """
        The current engine time for this evaulation cycle
        """

    @current_engine_time.setter
    @abstractmethod
    def current_engine_time(self, value: datetime):
        """
        Set the current engine time for this evaluation cycle
        """

    @property
    @abstractmethod
    def wall_clock_time(self) -> datetime:
        """
        The current wall clock time, in a realtime engine, this is the actual wall clock time, in a back test engine
        this is the current engine time + engine lag.
        """

    @property
    @abstractmethod
    def engine_lag(self) -> timedelta:
        """
        The lag between the current engine time and the current wall clock time.
        """

    @property
    @abstractmethod
    def proposed_next_engine_time(self) -> datetime:
        """
        The proposed next engine time, this is the time that the engine will advance to after the current evaluation.
        """

    @property
    @abstractmethod
    def push_has_pending_values(self) -> bool:
        """
        Returns True if there are any pending changes for push nodes.
        """

    @abstractmethod
    def reset_push_has_pending_values(self):
        """
        Reset the push_has_pending_values property.
        """

    @abstractmethod
    def wait_until_proposed_engine_time(self, proposed_engine_time: datetime) -> datetime:
        """
        Wait until the proposed engine time is reached. In the case of a runtime context, this may end early if a
        push node is scheduled whilst waiting for the proposed engine time.
        """
