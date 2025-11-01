from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from hgraph._impl._runtime._evaluation_clock import RealTimeEvaluationClock, SimulationEvaluationClock
from hgraph._impl._runtime._evaluation_engine import PythonEvaluationEngine
from hgraph._runtime._evaluation_clock import EngineEvaluationClock
from hgraph._runtime._evaluation_engine import EvaluationMode, EvaluationLifeCycleObserver
from hgraph._runtime._graph import Graph
from hgraph._runtime._graph_executor import GraphExecutor
from hgraph._runtime._lifecycle import initialise_dispose_context
from hgraph._runtime._lifecycle import start_stop_context

__all__ = ("PythonGraphExecutor",)


@dataclass
class PythonGraphExecutor(GraphExecutor):
    """
    A graph engine that runs the graph in python.
    """

    def __init__(
        self,
        graph: Graph,
        run_mode: EvaluationMode,
        observers: Iterable[EvaluationLifeCycleObserver] = None,
    ):
        self._graph = graph
        self._run_mode = run_mode
        self.observers = observers or []

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
            case _:
                raise RuntimeError("Unknown run mode")

        evaluation_engine = PythonEvaluationEngine(clock, start_time, end_time, self.run_mode)
        graph = self.graph
        graph.evaluation_engine = evaluation_engine
        for observer in self.observers:
            evaluation_engine.add_life_cycle_observer(observer)
            
        try:
            with initialise_dispose_context(self.graph), start_stop_context(self.graph):
                while clock.evaluation_time < end_time:
                    self.evaluate(evaluation_engine, graph)
        finally:
            evaluation_engine.notify_after_evaluation()  # stop() creates after evaluation events that need to be processed for the shutdown to be clean
        

    @staticmethod
    def evaluate(evaluation_engine, graph):
        evaluation_engine.notify_before_evaluation()
        try:
            graph.evaluate_graph()
        finally:
            evaluation_engine.notify_after_evaluation()
        evaluation_engine.advance_engine_time()
