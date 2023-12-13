from dataclasses import dataclass
from datetime import datetime

from hgraph._runtime._evaluation_clock import EngineEvaluationClock
from hgraph._runtime._lifecycle import initialise_dispose_context
from hgraph._impl._runtime._evaluation_clock import RealTimeEvaluationClock, SimulationEvaluationClock
from hgraph._impl._runtime._evaluation_engine import PythonEvaluationEngine
from hgraph._runtime._evaluation_engine import EvaluationMode
from hgraph._runtime._graph import Graph
from hgraph._runtime._graph_executor import GraphExecutor
from hgraph._runtime._lifecycle import start_stop_context

__all__ = ("PythonGraphExecutor",)


@dataclass
class PythonGraphExecutor(GraphExecutor):
    """
    A graph engine that runs the graph in python.
    """

    def __init__(self, graph: Graph, run_mode: EvaluationMode):
        self._graph = graph
        self._run_mode = run_mode

    @property
    def run_mode(self) -> EvaluationMode:
        return self._run_mode

    @property
    def graph(self) -> Graph:
        return self._graph

    def run(self, start_time: datetime, end_time: datetime):
        if end_time < start_time:
            raise ValueError("End time cannot be before the start time")

        match self.run_mode:
            case EvaluationMode.REAL_TIME:
                clock: EngineEvaluationClock = RealTimeEvaluationClock(start_time)
            case EvaluationMode.SIMULATION:
                clock: EngineEvaluationClock  = SimulationEvaluationClock(start_time)
            case _:
                raise RuntimeError("Unknown run mode")

        evaluation_engine = PythonEvaluationEngine(clock, start_time, end_time)
        graph = self.graph
        graph.evaluation_engine = evaluation_engine
        with initialise_dispose_context(self.graph), start_stop_context(self.graph):
            while clock.evaluation_time <= end_time:
                evaluation_engine.notify_before_evaluation()
                graph.evaluate_graph()
                evaluation_engine.notify_after_evaluation()
                evaluation_engine.advance_engine_time()
