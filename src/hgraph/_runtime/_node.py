from abc import abstractmethod, ABC
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, IntFlag, auto
from typing import Optional, Mapping, TYPE_CHECKING, Any

from hgraph._runtime._lifecycle import ComponentLifeCycle

if TYPE_CHECKING:
    from hgraph._types import HgTimeSeriesTypeMetaData, HgScalarTypeMetaData, HgRecordableStateType, RECORDABLE_STATE
    from hgraph._types._time_series_types import TimeSeriesInput, TimeSeriesOutput
    from hgraph._types._tsb_type import TimeSeriesBundleInput
    from hgraph._runtime._graph import Graph
    from hgraph._wiring._source_code_details import SourceCodeDetails

__all__ = (
    "Node",
    "NODE",
    "NodeTypeEnum",
    "NodeSignature",
    "SCHEDULER",
    "InjectableTypesEnum",
    "NodeDelegate",
    "ERROR_PATH",
    "STATE_PATH",
)


class NodeTypeEnum(Enum):
    PUSH_SOURCE_NODE = 0
    PULL_SOURCE_NODE = 1
    COMPUTE_NODE = 2
    SINK_NODE = 3


class InjectableTypesEnum(IntFlag):
    NONE = 0
    STATE = auto()
    """This node is stateful, provide a state instance here"""

    RECORDABLE_STATE = auto()

    SCHEDULER = auto()
    """This node will schedule itself outside of its defined inputs for evaluation"""

    OUTPUT = auto()
    """Access to the output is required"""

    CLOCK = auto()
    """Needs to have access to the current engine clock for it's logic"""

    ENGINE_API = auto()
    """Needs access to the engine API, typically start and end times for the engine"""

    LOGGER = auto()
    """This node produces logging output"""

    NODE = auto()
    """Requires access to the node, this should generally be reserved for use in framework nodes"""

    TRAIT = auto()


@dataclass
class NodeSignature:
    """
    This is the generic node signature that can be referenced by all instances of the node.
    The resolved scalar values are stored on the instance only.
    """

    name: str
    node_type: NodeTypeEnum
    args: tuple[str, ...]
    time_series_inputs: Optional[Mapping[str, "HgTimeSeriesTypeMetaData"]]
    time_series_output: Optional["HgTimeSeriesTypeMetaData"]
    scalars: Optional[Mapping[str, "HgScalarTypeMetaData"]]
    src_location: "SourceCodeDetails"
    active_inputs: frozenset[str] | None = None
    valid_inputs: frozenset[str] | None = None
    all_valid_inputs: frozenset[str] | None = None
    context_inputs: frozenset[str] | None = None
    injectable_inputs: Mapping[str, InjectableTypesEnum] | None = None
    injectables: int = 0
    capture_exception: bool = False
    trace_back_depth: int = 1
    wiring_path_name: str = ""
    label: str = ""
    capture_values: bool = False
    record_replay_id: str = ""

    @property
    def uses_scheduler(self) -> bool:
        return (InjectableTypesEnum.SCHEDULER.value & self.injectables) != 0

    @property
    def uses_clock(self) -> bool:
        return (InjectableTypesEnum.CLOCK.value & self.injectables) != 0

    @property
    def uses_engine(self) -> bool:
        return (InjectableTypesEnum.ENGINE_API.value & self.injectables) != 0

    @property
    def uses_state(self) -> bool:
        return (InjectableTypesEnum.STATE.value & self.injectables) != 0

    @property
    def uses_recordable_state(self) -> bool:
        return (InjectableTypesEnum.RECORDABLE_STATE.value & self.injectables) != 0

    def _recordable_state(self) -> tuple[str, "HgRecordableStateType"] | tuple[None, None]:
        from hgraph._types._scalar_type_meta_data import HgRecordableStateType

        return next(((arg, tp) for arg, tp in self.scalars.items() if type(tp) is HgRecordableStateType), (None, None))

    @property
    def recordable_state_arg(self) -> str | None:
        return self._recordable_state()[0]

    @property
    def recordable_state(self) -> "HgRecordableStateType":
        return self._recordable_state()[1]

    @property
    def uses_output_feedback(self) -> bool:
        return (InjectableTypesEnum.OUTPUT & self.injectables) != 0

    @property
    def is_source_node(self) -> bool:
        return self.node_type in (NodeTypeEnum.PUSH_SOURCE_NODE, NodeTypeEnum.PULL_SOURCE_NODE)

    @property
    def is_pull_source_node(self) -> bool:
        return self.node_type is NodeTypeEnum.PULL_SOURCE_NODE

    @property
    def is_push_source_node(self) -> bool:
        return self.node_type is NodeTypeEnum.PUSH_SOURCE_NODE

    @property
    def is_compute_node(self) -> bool:
        return self.node_type is NodeTypeEnum.COMPUTE_NODE

    @property
    def is_sink_node(self) -> bool:
        return self.node_type is NodeTypeEnum.SINK_NODE

    @property
    def is_recordable(self) -> bool:
        return bool(self.record_replay_id)

    @property
    def signature(self) -> str:
        input_types = (self.time_series_inputs or {}) | (self.scalars or {})
        args = (f"{arg}: {input_types[arg].dereference()}" for arg in self.args)
        return_ = "" if self.time_series_output is None else f" -> {self.time_series_output.dereference()}"
        return f"{self.name}({', '.join(args)}){return_}"

    def to_dict(self) -> dict[str, Any]:
        return dict(
            name=self.name,
            node_type=self.node_type,
            args=self.args,
            time_series_inputs=self.time_series_inputs,
            time_series_output=self.time_series_output,
            scalars=self.scalars,
            src_location=self.src_location,
            active_inputs=self.active_inputs,
            valid_inputs=self.valid_inputs,
            all_valid_inputs=self.all_valid_inputs,
            injectable_inputs=self.injectable_inputs,
            injectables=self.injectables,
            capture_exception=self.capture_exception,
            trace_back_depth=self.trace_back_depth,
            wiring_path_name=self.wiring_path_name,
            label=self.label,
            capture_values=self.capture_values,
            record_replay_id=self.record_replay_id,
        )

    def copy_with(self, **kwargs) -> "NodeSignature":
        kwargs_ = self.to_dict() | kwargs
        return NodeSignature(**kwargs_)


ERROR_PATH: int = -1  # The path in the wiring edges representing the error output of the node
STATE_PATH: int = -2  # The path in the wiring edges representing the recordable state output of the node


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
    def scalars(self) -> Mapping[str, Any]:
        """
        The scalar values associated to this node. These are the values that are not time-series.
        """

    @property
    @abstractmethod
    def graph(self) -> "Graph":
        """
        The graph that this node is a member of.
        """

    @graph.setter
    @abstractmethod
    def graph(self, value: "Graph"):
        """
        The graph that this node is a member of.
        """

    @property
    @abstractmethod
    def input(self) -> Optional["TimeSeriesBundleInput"]:
        """
        The input as an Unnamed Bundle. This allows the input to be considered as a TSB
        which is helpful for standardising handling of inputs. The bundle schema is the
        collection of inputs that are of time-series types.
        """

    @input.setter
    @abstractmethod
    def input(self, value: "TimeSeriesBundleInput"):
        """
        This should only be set by the owning graph or a very clever manager.
        """

    @property
    @abstractmethod
    def inputs(self) -> Optional[Mapping[str, "TimeSeriesInput"]]:
        """
        The inputs associated to this node.
        """

    @property
    @abstractmethod
    def start_inputs(self) -> list["TimeSeriesInput"]:
        """
        The inputs scheduled for start callback
        """

    @property
    @abstractmethod
    def output(self) -> Optional["TimeSeriesOutput"]:
        """
        The output of this node. This could be a TimeSeriesBundleOutput or a single output value.
        """

    @output.setter
    @abstractmethod
    def output(self, value: "TimeSeriesOutput"):
        """
        This should only be set by the owning graph or a very clever manager.
        """

    @property
    @abstractmethod
    def recordable_state(self) -> Optional["RECORDABLE_STATE"]:
        """
        The recordable state associated to this node.
        """

    @recordable_state.setter
    @abstractmethod
    def recordable_state(self, value: "RECORDABLE_STATE"):
        """Set the recordable state instance for this node if present."""

    @property
    @abstractmethod
    def scheduler(self) -> "SCHEDULER":
        """
        The scheduler for this node.
        """

    @abstractmethod
    def eval(self):
        """Called by the graph evaluation engine when the node has been scheduled for evaluation."""

    @abstractmethod
    def notify(self, modified_time: datetime):
        """Notify the node that it is need of scheduling"""

    @abstractmethod
    def notify_next_cycle(self):
        """Notify the node to be evaluated in the next evaluation cycle"""

    @property
    @abstractmethod
    def error_output(self) -> Optional["TimeSeriesOutput"]:
        """
        An error output of this node. This will tick when the eval method produces an exception
        instead of a result.
        This is only available when the node is marked as error checking.
        """


class NodeDelegate(Node):
    """Wraps a node delegating all node methods to the underlying implementation."""

    def __init__(self, node: Node):
        super().__init__()
        self._node = node

    @property
    def node_ndx(self) -> int:
        return self._node.node_ndx

    @property
    def owning_graph_id(self) -> tuple[int, ...]:
        return self._node.owning_graph_id

    @property
    def node_id(self) -> tuple[int, ...]:
        return self._node.node_id

    @property
    def signature(self) -> NodeSignature:
        return self._node.signature

    @property
    def scalars(self) -> Mapping[str, Any]:
        return self._node.scalars

    @property
    def graph(self) -> "Graph":
        return self._node.graph

    @property
    def input(self) -> Optional["TimeSeriesBundleInput"]:
        return self._node.input

    @property
    def inputs(self) -> Optional[Mapping[str, "TimeSeriesInput"]]:
        return self._node.inputs

    @property
    def output(self) -> Optional["TimeSeriesOutput"]:
        return self._node.output

    @property
    def error_output(self) -> Optional["TimeSeriesOutput"]:
        return self._node.error_output

    @property
    def scheduler(self) -> "SCHEDULER":
        return self._node.scheduler

    def eval(self):
        self._node.eval()

    def notify(self, modified_time):
        self._node.notify(modified_time)

    def notify_next_cycle(self):
        self._node.notify_next_cycle()


class SCHEDULER(ABC):
    """
    An input that is scheduled to be evaluated at a particular time. This is used for time-series
    inputs that are not bound to an output, but are still required to be evaluated at a particular time.
    """

    @property
    @abstractmethod
    def next_scheduled_time(self) -> datetime:
        """
        The time that this input is scheduled to be evaluated.
        """

    @property
    @abstractmethod
    def is_scheduled(self) -> bool:
        """
        Are there any pending scheduling events.
        """

    @property
    @abstractmethod
    def is_scheduled_now(self) -> bool:
        """
        Was this scheduled in this engine cycle. That is, was the node scheduled for evaluation at the current
        engine_time.
        """

    @abstractmethod
    def has_tag(self, tag: str) -> bool:
        """
        Does this scheduler have the tag specified.
        """

    @abstractmethod
    def pop_tag(self, tag: str, default: datetime = None) -> datetime | None:
        """
        Removes the tag and returns the value associated to it. If the tag is not found, then the default value
        is returned.
        """

    @abstractmethod
    def schedule(self, when: datetime | timedelta, tag: str = None, on_wall_clock: bool = False):
        """
        Schedule the node to be evaluated at the time specified. If tag is set, then the scheduled event will be
        associated to the tag, if a schedule is already set against the tag, it will be replaced with the new entry.
        """

    @abstractmethod
    def un_schedule(self, tag: str = None):
        """
        If tag is set, this will remove the scheduled event associated with this tag, if there is nothing scheduled
        for the tag, nothing is done.
        If the tag is not set, then remove the next scheduled item.
        """

    @abstractmethod
    def reset(self):
        """
        Remove all scheduled events.
        """


NODE = Node
