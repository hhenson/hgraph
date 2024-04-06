import functools
import typing
from datetime import datetime
from typing import Iterable, Sequence

from hgraph._runtime._constants import MIN_DT
from hgraph._runtime._evaluation_clock import EvaluationClock
from hgraph._runtime._evaluation_engine import EvaluationEngine, EvaluationEngineApi
from hgraph._runtime._graph import Graph
from hgraph._runtime._lifecycle import start_guard, stop_guard
from hgraph._runtime._node import NodeTypeEnum, Node

if typing.TYPE_CHECKING:
    from hgraph._builder._graph_builder import GraphBuilder


__all__ = ("PythonGraph",)


class PythonGraph(Graph):
    """
    Provide a reference implementation of the Graph.
    """

    def __init__(self, graph_id: tuple[int, ...], nodes: Iterable[Node], parent_node: Node = None, label: str = None):
        super().__init__()
        self._graph_id: tuple[int, ...] = graph_id
        self._nodes: list[Node] = nodes if type(nodes) is list else list(nodes)
        self._schedule: list[datetime, ...] = [MIN_DT] * len(nodes)
        self._evaluation_engine: EvaluationEngine | None = None
        self._parent_node: Node = parent_node
        self._label: str = label

    def copy_with(self, nodes: tuple[Node, ...]) -> "Graph":
        graph = PythonGraph(self._graph_id, nodes, self._parent_node)
        graph._schedule = self._schedule
        graph._evaluation_engine = self._evaluation_engine
        return graph

    @property
    def parent_node(self) -> Node:
        return self._parent_node

    @property
    def graph_id(self) -> tuple[int, ...]:
        return self._graph_id

    @property
    def label(self) -> str:
        return self._label

    @property
    def nodes(self) -> Sequence[Node]:
        return self._nodes

    @property
    def evaluation_clock(self) -> EvaluationClock:
        return self._evaluation_engine.evaluation_clock

    @property
    def engine_evaluation_clock(self) -> "EngineEvaluationClock":
        return self._evaluation_engine.engine_evaluation_clock

    @property
    def evaluation_engine_api(self) -> EvaluationEngineApi:
        return self._evaluation_engine

    @property
    def evaluation_engine(self) -> EvaluationEngine:
        return self._evaluation_engine

    @evaluation_engine.setter
    def evaluation_engine(self, value):
        if self._evaluation_engine is not None and value is not None:
            raise RuntimeError("Duplicate attempt to set evaluation engine")
        self._evaluation_engine = value

    @property
    def schedule(self) -> list[datetime, ...]:
        return self._schedule

    @functools.cached_property
    def push_source_nodes_end(self) -> int:
        """ The index of the first compute node """
        for i in range(len(self.nodes)):
            if self.nodes[i].signature.node_type != NodeTypeEnum.PUSH_SOURCE_NODE:
                return i
        return len(self.nodes)  # In the very unlikely event that there are only push source nodes.

    def extend_graph(self, graph_builder: "GraphBuilder", delay_start: bool = False) -> None:
        """
        Extend the graph using the nodes produced by the builder supplied. If delayed_start is False, the
        method will call start if the graph is already started, otherwise  the nodes will be initialised only.
        """
        first_node_ndx = len(self._nodes)
        sz = len(graph_builder.node_builders)
        self._nodes.extend(graph_builder.make_and_connect_nodes(self.graph_id, first_node_ndx))
        self._schedule.extend([MIN_DT]*sz)
        self.initialise_subgraph(first_node_ndx, first_node_ndx+sz)
        if not delay_start and self.is_started:
            self.start_subgraph(first_node_ndx, first_node_ndx+sz)

    def reduce_graph(self, start_node: int) -> None:
        end = len(self._nodes)
        if self.is_started:
            self.stop_subgraph(start_node, end)
        self.dispose_subgraph(start_node, end)
        self._nodes = self._nodes[:start_node]
        self._schedule = self._schedule[:start_node]

    def initialise_subgraph(self, start: int, end: int):
        """
        Initialise a subgraph.
        If the graph is dynamically extended, this method is required to initialise the subgraph.
        """
        for node in self.nodes[start:end]:
            node.graph = self
        for node in self.nodes[end:start]:
            node.initialise()

    def initialise(self):
        for node in self.nodes:
            node.graph = self
        for node in self.nodes:
            node.initialise()

    def schedule_node(self, node_ndx, when, force_set: bool = False):
        clock = self._evaluation_engine.engine_evaluation_clock
        if when < (et := clock.evaluation_time):
            raise RuntimeError(
                f"Graph[{self.graph_id}] Trying to schedule node: {self.nodes[node_ndx].signature.signature}[{node_ndx}]"
                f" for {when} but current time is {self.evaluation_clock.evaluation_time}")
        st = self.schedule[node_ndx]
        if force_set or st <= et or st > when:
            self.schedule[node_ndx] = when
        clock.update_next_scheduled_evaluation_time(when)

    def start_subgraph(self, start: int, end: int):
        """Start the subgraph (end is exclusive), i.e. [start, end)"""
        engine = self._evaluation_engine
        for node in self.nodes[start:end]:
            engine.notify_before_start_node(node)
            node.start()
            engine.notify_after_start_node(node)

    @start_guard
    def start(self):
        engine = self._evaluation_engine
        engine.notify_before_start_graph(self)
        for node in self._nodes:
            engine.notify_before_start_node(node)
            node.start()
            engine.notify_after_start_node(node)
        engine.notify_after_start_graph(self)

    @stop_guard
    def stop(self):
        engine = self._evaluation_engine
        engine.notify_before_stop_graph(self)
        for node in self._nodes:
            engine.notify_before_stop_node(node)
            node.stop()
            engine.notify_after_stop_node(node)
        engine.notify_after_stop_graph(self)

    def stop_subgraph(self, start: int, end: int):
        """Stop the subgraph (end is exclusive), i.e. [start, end)"""
        engine = self._evaluation_engine
        for node in self.nodes[start:end]:
            engine.notify_before_stop_node(node)
            node.stop()
            engine.notify_after_stop_node(node)

    def dispose(self):
        for node in self.nodes:  # Since we initialise nodes from within the graph, we need to dispose them here.
            node.dispose()

    def dispose_subgraph(self, start: int, end: int):
        """
        Initialise a subgraph.
        If the graph is dynamically extended, this method is required to initialise the subgraph.
        """
        for node in self.nodes[start:end]:
            node.dispose()

    def evaluate_graph(self):
        self._evaluation_engine.notify_before_graph_evaluation(self)

        now = (clock := self._evaluation_engine.engine_evaluation_clock).evaluation_time
        nodes = self._nodes
        schedule = self._schedule

        if clock.push_node_requires_scheduling:
            clock.reset_push_node_requires_scheduling()
            for i in range(self.push_source_nodes_end):
                nodes[i].eval()  # This is only to move nodes on, won't call the before and after node eval here

        for i in range(self.push_source_nodes_end, len(nodes)):
            scheduled_time, node = schedule[i], nodes[i]
            if scheduled_time == now:
                self._evaluation_engine.notify_before_node_evaluation(node)
                from hgraph._types._error_type import NodeException
                try:
                    node.eval()
                except NodeException as e:
                    raise e
                except Exception as e:
                    raise NodeException.capture_error(e, node, 'During evaluation') from e
                self._evaluation_engine.notify_after_node_evaluation(node)
            elif scheduled_time > now:
                # If the node has a scheduled time in the future, we need to let the execution context know.
                clock.update_next_scheduled_evaluation_time(scheduled_time)

        self._evaluation_engine.notify_after_graph_evaluation(self)
