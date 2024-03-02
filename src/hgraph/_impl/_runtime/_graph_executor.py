from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from jinja2.nodes import NodeType

from hgraph._runtime._evaluation_clock import EngineEvaluationClock
from hgraph._runtime._lifecycle import initialise_dispose_context
from hgraph._impl._runtime._evaluation_clock import RealTimeEvaluationClock, SimulationEvaluationClock
from hgraph._impl._runtime._evaluation_engine import PythonEvaluationEngine
from hgraph._runtime._evaluation_engine import EvaluationMode, EvaluationLifeCycleObserver
from hgraph._runtime._graph import Graph
from hgraph._runtime._graph_executor import GraphExecutor
from hgraph._runtime._lifecycle import start_stop_context
from hgraph._runtime._node import Node, NodeTypeEnum

__all__ = ("PythonGraphExecutor",)

from hgraph._runtime._graph_recorder import GraphRecorder


@dataclass
class PythonGraphExecutor(GraphExecutor):
    """
    A graph engine that runs the graph in python.
    """

    def __init__(self, graph: Graph, run_mode: EvaluationMode,
                 observers: Iterable[EvaluationLifeCycleObserver] = None,
                 recorder: GraphRecorder = None):
        self._graph = graph
        self._run_mode = run_mode
        self.observers = observers or []
        self._recorder: GraphRecorder = recorder

    @property
    def run_mode(self) -> EvaluationMode:
        return self._run_mode

    @property
    def graph(self) -> Graph:
        return self._graph

    def run(self, start_time: datetime, end_time: datetime):
        if end_time <= start_time:
            if end_time < start_time:
                raise ValueError("End time cannot be before the start time")
            else:
                raise ValueError("End time cannot be equal to the start time")

        match self.run_mode:
            case EvaluationMode.REAL_TIME:
                clock: EngineEvaluationClock = RealTimeEvaluationClock(start_time)
            case EvaluationMode.SIMULATION:
                clock: EngineEvaluationClock = SimulationEvaluationClock(start_time)
            case EvaluationMode.REPLAY | EvaluationMode.RECORDING:
                clock: EngineEvaluationClock = RealTimeEvaluationClock(start_time)
            case _:
                raise RuntimeError("Unknown run mode")

        evaluation_engine = PythonEvaluationEngine(clock, start_time, end_time, self.run_mode)
        graph = self.graph
        graph.evaluation_engine = evaluation_engine
        for observer in self.observers:
            evaluation_engine.add_life_cycle_observer(observer)
        with initialise_dispose_context(self.graph), start_stop_context(self.graph):
            if self.run_mode == EvaluationMode.REPLAY:
                graph_ = self.replace_replay_nodes(self._recorder, graph)
                switch_time = min(self._recorder.last_recorded_time(), end_time)
                while clock.evaluation_time < switch_time:
                    self.evaluate(evaluation_engine, graph_)
                evaluation_engine.run_mode = EvaluationMode.RECORDING
            if self.run_mode == EvaluationMode.RECORDING:
                graph = self.replace_record_nodes(self._recorder, graph)
            while clock.evaluation_time < end_time:
                self.evaluate(evaluation_engine, graph)

    @staticmethod
    def evaluate(evaluation_engine, graph):
        evaluation_engine.notify_before_evaluation()
        graph.evaluate_graph()
        evaluation_engine.notify_after_evaluation()
        evaluation_engine.advance_engine_time()

    @staticmethod
    def replace_replay_nodes(recorder: GraphRecorder, graph: Graph):
        # We only record the push source nodes or pull source nodes that mare marked for recording.
        def _match(node: Node) -> bool:
            return node.signature.node_type == NodeTypeEnum.PUSH_SOURCE_NODE

        nodes = tuple(recorder.replay_node(node) if _match(node) else node for node in graph.nodes)
        return graph.copy_with(nodes)

    @staticmethod
    def replace_record_nodes(recorder: GraphRecorder, graph: Graph):
        def _match(node: Node) -> bool:
            return node.signature.node_type == NodeTypeEnum.PUSH_SOURCE_NODE

        nodes = tuple(recorder.record_node(node) if _match(node) else node for node in graph.nodes)
        return graph.copy_with(nodes)
