import functools
from datetime import datetime

from hgraph._runtime._constants import MIN_DT
from hgraph._runtime._evaluation_clock import EvaluationClock
from hgraph._runtime._evaluation_engine import EvaluationEngine, EvaluationEngineApi
from hgraph._runtime._graph import Graph
from hgraph._runtime._lifecycle import start_guard, stop_guard
from hgraph._runtime._node import NodeTypeEnum, Node

__all__ = ("PythonGraph",)


class PythonGraph(Graph):
    """
    Provide a reference implementation of the Graph.
    """

    def __init__(self, graph_id: tuple[int, ...], nodes: tuple[Node, ...], parent_node: Node = None):
        super().__init__()
        self._graph_id: tuple[int, ...] = graph_id
        self._nodes: tuple[Node, ...] = nodes
        self._schedule: list[datetime, ...] = [MIN_DT] * len(nodes)
        self._evaluation_engine: EvaluationEngine = None
        self._parent_node: Node = parent_node

    @property
    def parent_node(self) -> Node:
        return self._parent_node

    @property
    def graph_id(self) -> tuple[int, ...]:
        return self._graph_id

    @property
    def nodes(self) -> tuple[Node, ...]:
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

    def initialise(self):
        for node in self.nodes:
            node.graph = self
        for node in self.nodes:
            node.initialise()

    def schedule_node(self, node_ndx, time):
        clock = self._evaluation_engine.engine_evaluation_clock
        if time < clock.evaluation_time:
            raise RuntimeError(
                f"Graph[{self.graph_id}] Trying to schedule node: {self.nodes[node_ndx].signature.signature}[{node_ndx}]"
                f" for {time} but current time is {self.evaluation_clock.evaluation_time}")
        self.schedule[node_ndx] = time
        clock.update_next_scheduled_evaluation_time(time)

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
            engine.notify_before_start_node(node)
        engine.notify_after_stop_graph(self)

    def dispose(self):
        for node in self.nodes:  # Since we initialise nodes from within the graph, we need to dispose them here.
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
                node.eval()
                self._evaluation_engine.notify_after_node_evaluation(node)
            elif scheduled_time > now:
                # If the node has a scheduled time in the future, we need to let the execution context know.
                clock.update_next_scheduled_evaluation_time(scheduled_time)

        self._evaluation_engine.notify_after_graph_evaluation(self)
