from abc import abstractmethod, ABC
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from hgraph._runtime._lifecycle import ComponentLifeCycle

if TYPE_CHECKING:
    from hgraph._runtime._evaluation_clock import EvaluationClock, EngineEvaluationClock
    from hgraph._runtime._node import Node
    from hgraph._runtime._graph import Graph

__all__ = ("EvaluationMode", "EvaluationLifeCycleObserver", "EvaluationEngine", "EvaluationEngineApi")

"""
The evaluation engine is split into parts, to clearly indicate the user accessible items vs internal
engine items.
"""


class EvaluationMode(Enum):
    """
    The mode to use when executing the graph.

    The MODES are as follows:

    REAL_TIME
        The graph will run from start time until the graph is either requested to stop or is killed.
        If start time is in the past the graph will potentially replay historical PULL source nodes
        until it has caught up and will then continue from that point on once it catches up to the
        wall clock time it started. PUSH source nodes are only evaluated in REAL_TIME mode, these
        will only be started and evaluated in REAL_TIME mode. Events from PULL source nodes are only
        PULLED into the graph once we are caught up and are processed in a first come - first served basis.
        I.e. there is no cross node arrival time ordering of PUSH source nodes. Time ordering is managed
        within a PUSH source nodes output when the node is queueing, but the ordering is limited to
        when the received value is placed into the graph, not the time-delta between the events.

    SIMULATION
        The graph will run from start time to end time, all events to be processed are introduced
        from PULL source nodes and when there are no new events to process the graph is considered complete.
        In this mode the wall clock time (clock.now) is simulated and updated to be the next events time at the
        end of an evaluation cycle. PUSH source nodes are not supported in this mode of operation.
    """

    REAL_TIME = 0
    SIMULATION = 1


class EvaluationLifeCycleObserver:
    """
    Provide the callbacks that can be received during the evaluation of the graph.
    Use this with care as each additional life-cycle observer will slow down the evaluation of the graph.
    """

    def on_before_start_graph(self, graph: "Graph"):
        """
        Called before the graph is started.
        """

    def on_after_start_graph(self, graph: "Graph"):
        """
        Called after the graph is started.
        """

    def on_before_start_node(self, node: "Node"):
        """
        Called before a node is started.
        """

    def on_after_start_node(self, node: "Node"):
        """
        Called after a node is started.
        """

    def on_before_graph_evaluation(self, graph: "Graph"):
        """
        Called before the graph is evaluated.
        """

    def on_before_node_evaluation(self, node: "Node"):
        """
        Called before a node is evaluated.
        """

    def on_after_node_evaluation(self, node: "Node"):
        """
        Called after a node is evaluated.
        """

    def on_after_graph_push_nodes_evaluation(self, graph: "Graph"):
        """
        Called after the graph has evaluated all its push nodes.
        """

    def on_after_graph_evaluation(self, graph: "Graph"):
        """
        Called after the graph is evaluated.
        """

    def on_before_stop_node(self, node: "Node"):
        """
        Called before a node is stopped.
        """

    def on_after_stop_node(self, node: "Node"):
        """
        Called after a node is stopped.
        """

    def on_before_stop_graph(self, graph: "Graph"):
        """
        Called before the graph is stopped.
        """

    def on_after_stop_graph(self, graph: "Graph"):
        """
        Called after the graph is stopped.
        """


class EvaluationEngineApi(ComponentLifeCycle, ABC):
    """
    The user visible API for the evaluation engine.
    """

    @property
    @abstractmethod
    def evaluation_mode(self) -> EvaluationMode:
        """
        The current mode of evaluation
        """

    @property
    @abstractmethod
    def start_time(self) -> datetime:
        """
        The start time of the evaluation engine.
        """

    @property
    @abstractmethod
    def end_time(self) -> datetime:
        """
        The end time of the evaluation engine.
        """

    @property
    @abstractmethod
    def evaluation_clock(self) -> "EvaluationClock":
        """
        The evaluation clock.
        """

    @abstractmethod
    def request_engine_stop(self):
        """
        Request the evaluation engine to stop processing events and exit.
        This will not stop the graph immediately, and will only be processed
        after the current evaluation cycle has completed.
        """

    @property
    @abstractmethod
    def is_stop_requested(self) -> bool:
        """
        Returns True if the engine has been requested to stop.
        """

    @abstractmethod
    def add_before_evaluation_notification(self, fn: callable):
        """
        Add a before evaluation notification observer.
        The notification is called once before the next evaluation cycle.
        """

    @abstractmethod
    def add_after_evaluation_notification(self, fn: callable):
        """
        Add an after evaluation notification observer.
        The notification is called once after the evaluation of the current cycle.
        """

    @abstractmethod
    def add_life_cycle_observer(self, observer: EvaluationLifeCycleObserver):
        """
        Add a graph engine life-cycle observer.
        Life cycle events will immediately start to be delivered to the observer.
        The observer will continue to receive events until it is removed.
        """

    @abstractmethod
    def remove_life_cycle_observer(self, observer: EvaluationLifeCycleObserver):
        """
        Remove the provided life-cycle observer from the engine.
        This is immediately effective.
        """


class EvaluationEngine(EvaluationEngineApi, ABC):
    """
    This extends the API to include the internal methods that are used to support
    the operations of an evaluation engine. This API is accessible to nested
    graph engines.
    Only the master graph engine implements the API.
    """

    @property
    @abstractmethod
    def engine_evaluation_clock(self) -> "EngineEvaluationClock":
        """
        The engine evaluation clock. This is used by the graph engine
        and nested graph engines.
        """

    def advance_engine_time(self):
        """
        Advance the engine time, this will deal with stopping the engine
        if we have reached the end time or if the engine has been requested
        to be stopped.
        """

    def notify_before_evaluation(self):
        """
        Notify observers before evaluation.
        This is on the outermost graph before any graphs are evaluated.
        """

    def notify_after_evaluation(self):
        """
        Notify observers after evaluation of the outermost graph, that is
        after all graphs have been evaluated.
        """

    def notify_before_graph_evaluation(self, graph: "Graph"):
        """Notify observers before graph evaluation"""

    def notify_after_graph_evaluation(self, graph: "Graph"):
        """Notify observers after graph evaluation"""

    def notify_before_node_evaluation(self, node: "Node"):
        """Notify observers before node evaluation"""

    def notify_after_node_evaluation(self, node: "Node"):
        """Notify observers after node evaluation"""

    def notify_after_push_nodes_evaluation(self, graph):
        """Notify observers after the graph has evaluated all its push nodes"""

    def notify_before_start_graph(self, graph: "Graph"):
        """Notify observers that the graph is about to start"""

    def notify_after_start_graph(self, graph: "Graph"):
        """Notify observers that the graph has started"""

    def notify_before_stop_graph(self, graph: "Graph"):
        """Notify observers that the graph is about to stop"""

    def notify_after_stop_graph(self, graph: "Graph"):
        """
        Notify observers that the graph has stopped
        """

    def notify_before_start_node(self, node: "Node"):
        """
        Notify observers that the node is about to start.
        """

    def notify_after_start_node(self, node: "Node"):
        """
        Notify observers that the node has started.
        """

    def notify_before_stop_node(self, node: "Node"):
        """
        Notify observers that the node is about to stop.
        """

    def notify_after_stop_node(self, node: "Node"):
        """
        Notify observers that the node has stopped.
        """


class EvaluationEngineDelegate(EvaluationEngine):
    """
    A delegate that can be used to extend the evaluation engine with additional behaviour.
    This delegates all calls to the provided engine instance.
    """

    def __init__(self, engine: EvaluationEngine):
        super().__init__()
        self._engine = engine

    @property
    def engine_evaluation_clock(self) -> "EngineEvaluationClock":
        return self._engine.engine_evaluation_clock

    @property
    def start_time(self) -> datetime:
        return self._engine.start_time

    @property
    def end_time(self) -> datetime:
        return self._engine.end_time

    @property
    def evaluation_mode(self) -> EvaluationMode:
        return self._engine.evaluation_mode

    @property
    def evaluation_clock(self) -> "EvaluationClock":
        return self._engine.evaluation_clock

    def request_engine_stop(self):
        self._engine.request_engine_stop()

    @property
    def is_stop_requested(self) -> bool:
        return self._engine.is_stop_requested

    def add_before_evaluation_notification(self, fn: callable):
        self._engine.add_before_evaluation_notification(fn)

    def add_after_evaluation_notification(self, fn: callable):
        self._engine.add_after_evaluation_notification(fn)

    def add_life_cycle_observer(self, observer: EvaluationLifeCycleObserver):
        self._engine.add_life_cycle_observer(observer)

    def remove_life_cycle_observer(self, observer: EvaluationLifeCycleObserver):
        self._engine.remove_life_cycle_observer(observer)

    def advance_engine_time(self):
        self._engine.advance_engine_time()

    def notify_before_evaluation(self):
        self._engine.notify_before_evaluation()

    def notify_after_evaluation(self):
        self._engine.notify_after_evaluation()

    def notify_before_graph_evaluation(self, graph: "Graph"):
        self._engine.notify_before_graph_evaluation(graph)

    def notify_after_graph_evaluation(self, graph: "Graph"):
        self._engine.notify_after_graph_evaluation(graph)

    def notify_before_node_evaluation(self, node: "Node"):
        self._engine.notify_before_node_evaluation(node)

    def notify_after_node_evaluation(self, node: "Node"):
        self._engine.notify_after_node_evaluation(node)

    def notify_before_start_graph(self, graph: "Graph"):
        self._engine.notify_before_start_graph(graph)

    def notify_after_start_graph(self, graph: "Graph"):
        self._engine.notify_after_start_graph(graph)

    def notify_before_stop_graph(self, graph: "Graph"):
        self._engine.notify_before_stop_graph(graph)

    def notify_after_stop_graph(self, graph: "Graph"):
        self._engine.notify_after_stop_graph(graph)

    def notify_before_start_node(self, node: "Node"):
        self._engine.notify_before_start_node(node)

    def notify_after_start_node(self, node: "Node"):
        self._engine.notify_after_start_node(node)

    def notify_before_stop_node(self, node: "Node"):
        self._engine.notify_before_stop_node(node)

    def notify_after_stop_node(self, node: "Node"):
        self._engine.notify_after_stop_node(node)
