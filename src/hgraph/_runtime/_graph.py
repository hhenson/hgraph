import typing
from abc import abstractmethod
from datetime import datetime

from hgraph._runtime._lifecycle import ComponentLifeCycle

if typing.TYPE_CHECKING:
    from hgraph._runtime._node import Node
    from hgraph._runtime._evaluation_clock import EvaluationClock, EngineEvaluationClock
    from hgraph._runtime._evaluation_engine import EvaluationEngineApi, EvaluationEngine


__all__ = ("Graph",)


class Graph(ComponentLifeCycle):
    """ The runtime graph """

    @property
    @abstractmethod
    def parent_node(self) -> typing.Optional["Node"]:
        """
        A graph is either a root graph or a nested graph, for nested graphs they are always associated to a
        parent node that operates the graph within the parent graph. This is here for debug traceability.
        The NestedScheduler is used for managing sub-graph scheduling.
        """

    @property
    @abstractmethod
    def graph_id(self) -> tuple[int, ...]:
        """ The graph id """

    @property
    @abstractmethod
    def label(self) -> str:
        """ The graph label/name """

    @property
    @abstractmethod
    def nodes(self) -> tuple["Node", ...]:
        """ The nodes of the graph """

    @property
    @abstractmethod
    def evaluation_clock(self) -> "EvaluationClock":
        """The execution context"""

    @property
    @abstractmethod
    def engine_evaluation_clock(self) -> "EngineEvaluationClock":
        """The engine execution context"""

    @property
    @abstractmethod
    def evaluation_engine_api(self) -> "EvaluationEngineApi":
        """ The evaluation engine api """

    @property
    @abstractmethod
    def evaluation_engine(self) -> "EvaluationEngine":
        """
        This should not be accessed by the user, they should
        instead use the evaluation_engine_api property.
        """

    @evaluation_engine.setter
    @abstractmethod
    def evaluation_engine(self, value: "EvaluationEngine"):
        """ Set the evaluation engine """

    @property
    @abstractmethod
    def push_source_nodes_end(self) -> int:
        """ The index of the first compute node """

    @abstractmethod
    def schedule_node(self, node_ndx, when):
        """Schedule the node with the given node_id to evaluate at the given time"""

    @property
    @abstractmethod
    def schedule(self) -> list[datetime, ...]:
        """The schedule of the graph"""

    @abstractmethod
    def evaluate_graph(self):
        """
        Perform a single cycle of evaluation of this graph.
        This will evaluate all nodes that are scheduled to evaluate at the current evaluation time.
        """

    @abstractmethod
    def copy_with(self, nodes: tuple["Node", ...]) -> "Graph":
        """Create a new instance of the graph with an adjust node set"""
