from datetime import datetime
from typing import Mapping, Any, Callable

from hgraph._runtime._constants import MIN_DT
from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._evaluation_clock import EngineEvaluationClock, EngineEvaluationClockDelegate
from hgraph._runtime._evaluation_engine import EvaluationEngine, EvaluationEngineDelegate
from hgraph._runtime._node import NodeSignature


__all__ = (
    "PythonNestedNodeImpl",
    "NestedEvaluationEngine",
    "NestedEngineEvaluationClock",
)


class NestedEngineEvaluationClock(EngineEvaluationClockDelegate):

    def __init__(self, engine_evaluation_clock: EngineEvaluationClock, nested_node: "PythonNestedNodeImpl"):
        super().__init__(engine_evaluation_clock)
        self._nested_node = nested_node

    @property
    def node(self) -> "PythonNestedNodeImpl":
        return self._nested_node

    def update_next_scheduled_evaluation_time(self, next_time: datetime):
        if (let := self._nested_node.last_evaluation_time) and let >= next_time:
            return

        self._nested_node.graph.schedule_node(self._nested_node.node_ndx, next_time)


class NestedEvaluationEngine(EvaluationEngineDelegate):
    """

    Requesting a stop of the engine will stop the outer engine.
    Stopping an inner graph is a source of bugs and confusion. Instead, the user should create a mechanism to
    remove the key used to create the graph.
    """

    def __init__(self, engine: EvaluationEngine, evaluation_clock: EngineEvaluationClock):
        super().__init__(engine)
        self._engine_evaluation_clock = evaluation_clock
        self._nested_start_time = evaluation_clock.evaluation_time

    @property
    def start_time(self) -> datetime:
        return self._nested_start_time

    @property
    def evaluation_clock(self) -> "EvaluationClock":
        return self._engine_evaluation_clock

    @property
    def engine_evaluation_clock(self) -> "EngineEvaluationClock":
        return self._engine_evaluation_clock


class PythonNestedNodeImpl(NodeImpl):

    def __init__(
        self,
        node_ndx: int,
        owning_graph_id: tuple[int, ...],
        signature: NodeSignature,
        scalars: Mapping[str, Any],
        eval_fn: Callable = None,
        start_fn: Callable = None,
        stop_fn: Callable = None,
    ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn)
        self._last_evaluation_time = MIN_DT

    @property
    def last_evaluation_time(self) -> datetime:
        return self._last_evaluation_time

    def mark_evaluated(self):
        self._last_evaluation_time = self.graph.evaluation_clock.evaluation_time
