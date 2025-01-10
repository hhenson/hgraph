from datetime import datetime
from typing import List

from hgraph._runtime._constants import MIN_TD
from hgraph._runtime._evaluation_clock import EvaluationClock, EngineEvaluationClock
from hgraph._runtime._evaluation_engine import EvaluationEngine, EvaluationLifeCycleObserver, EvaluationMode


class PythonEvaluationEngine(EvaluationEngine):

    def __init__(
        self,
        engine_evaluation_clock: EngineEvaluationClock,
        start_time: datetime,
        end_time: datetime,
        mode: EvaluationMode,
    ):
        super().__init__()
        self._engine_evaluation_clock: EngineEvaluationClock = engine_evaluation_clock
        self._start_time: datetime = start_time
        self._end_time: datetime = end_time
        self._stop_requested: bool = False
        self._life_cycle_observers: List[EvaluationLifeCycleObserver] = []
        self._before_evaluation_notification: List[callable] = []
        self._after_evaluation_notification: List[callable] = []
        self._mode: EvaluationMode = mode

    @property
    def evaluation_mode(self) -> EvaluationMode:
        return self._mode

    @evaluation_mode.setter
    def evaluation_mode(self, value: EvaluationMode):
        self._mode = value

    @property
    def evaluation_clock(self) -> EvaluationClock:
        return self._engine_evaluation_clock

    @property
    def engine_evaluation_clock(self) -> "EngineEvaluationClock":
        return self._engine_evaluation_clock

    @property
    def start_time(self) -> datetime:
        return self._start_time

    @property
    def end_time(self) -> datetime:
        return self._end_time

    @property
    def is_stop_requested(self) -> bool:
        return self._stop_requested

    def request_engine_stop(self):
        self._stop_requested = True

    def add_before_evaluation_notification(self, fn: callable):
        self._before_evaluation_notification.append(fn)

    def add_after_evaluation_notification(self, fn: callable):
        self._after_evaluation_notification.append(fn)

    def add_life_cycle_observer(self, observer: EvaluationLifeCycleObserver):
        self._life_cycle_observers.append(observer)

    def remove_life_cycle_observer(self, observer: EvaluationLifeCycleObserver):
        self._life_cycle_observers.remove(observer)

    def advance_engine_time(self):
        if self._stop_requested:
            self._engine_evaluation_clock.evaluation_time = self._end_time + MIN_TD
            return
        # Ensure we don't run past the end time. So we schedule to the end time + 1 tick.
        self._engine_evaluation_clock.update_next_scheduled_evaluation_time(self._end_time + MIN_TD)

        self._engine_evaluation_clock.advance_to_next_scheduled_time()

    def notify_before_evaluation(self):
        for notification_receiver in self._before_evaluation_notification:
            notification_receiver()
        self._before_evaluation_notification.clear()

    def notify_after_evaluation(self):
        for notification_receiver in reversed(self._after_evaluation_notification):
            notification_receiver()
        self._after_evaluation_notification.clear()

    def notify_before_graph_evaluation(self, graph):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_before_graph_evaluation(graph)

    def notify_after_graph_evaluation(self, graph):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_after_graph_evaluation(graph)

    def notify_before_node_evaluation(self, node):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_before_node_evaluation(node)

    def notify_after_node_evaluation(self, node):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_after_node_evaluation(node)

    def notify_after_push_nodes_evaluation(self, graph):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_after_graph_push_nodes_evaluation(graph)

    def notify_before_start_graph(self, graph):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_before_start_graph(graph)

    def notify_after_start_graph(self, graph):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_after_start_graph(graph)

    def notify_before_stop_graph(self, graph):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_before_stop_graph(graph)

    def notify_after_stop_graph(self, graph):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_after_stop_graph(graph)

    def notify_before_start_node(self, node):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_before_start_node(node)

    def notify_after_start_node(self, node):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_after_start_node(node)

    def notify_before_stop_node(self, node):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_before_stop_node(node)

    def notify_after_stop_node(self, node):
        for life_cycle_observer in self._life_cycle_observers:
            life_cycle_observer.on_after_stop_node(node)
