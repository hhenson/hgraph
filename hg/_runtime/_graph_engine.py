from abc import abstractmethod
from datetime import datetime
from enum import Enum

from hg._impl._builder._graph_builder import GraphBuilder
from hg._runtime._lifecycle import ComponentLifeCycle
from hg._runtime._graph import Graph

from hg._wiring._graph_builder import wire_graph

__all__ = ("run", "wire_graph", "RunMode", "GraphEngine", "GraphExecutorLifeCycleObserver")


class RunMode(Enum):
    """
    The run mode of the engine.
    """
    REAL_TIME = 0
    BACK_TEST = 1


class GraphExecutorLifeCycleObserver:
    """
    An observer of the graph executor's life cycle.
    """

    def on_before_start(self, graph: Graph):
        """
        Called before the graph is started.
        """

    def on_after_start(self, graph: Graph):
        """
        Called after the graph is started.
        """

    def on_before_start_node(self, node):
        """
        Called before a node is started.
        """

    def on_after_start_node(self, node):
        """
        Called after a node is started.
        """

    def on_before_evaluation(self, graph: Graph):
        """
        Called before the graph is evaluated.
        """

    def on_before_node_evaluation(self, node):
        """
        Called before a node is evaluated.
        """

    def on_after_node_evaluation(self, node):
        """
        Called after a node is evaluated.
        """

    def on_after_evaluation(self, graph: Graph):
        """
        Called after the graph is evaluated.
        """

    def on_before_stop_node(self, node):
        """
        Called before a node is stopped.
        """

    def on_after_stop_node(self, node):
        """
        Called after a node is stopped.
        """


class GraphEngine(ComponentLifeCycle):

    @property
    @abstractmethod
    def run_mode(self) -> RunMode:
        """
        The run mode of the engine.
        """

    @property
    @abstractmethod
    def graph(self) -> Graph:
        """
        The graph associated to this graph executor.
        """

    @abstractmethod
    def run(self, start_time: datetime, end_time: datetime):
        """
        Tell the engine to begin and operate it's run loop.
        :param start_time: The time to start the run loop at
        :param end_time: The time to end the run loop at
        """


def run(graph, *args, run_mode: RunMode=RunMode.BACK_TEST, start_time: datetime, end_time: datetime, **kwargs):
    """
    Use this to initiate the engine run loop.

    The run_mode indicates how the graph engine should evalute the graph, in RunMOde.REAL_TIME the graph will be
    evaluated using the system clock, in RunMode.BACK_TEST the graph will be evaluated using a simulated clock.
    The simulated clock is advanced as fast as possible without following the system clock timings. This allows a
    back-test to be evaluated as fast as possible.

    :param graph: The graph to evaluate
    :param args: Any arguments to pass to the graph
    :param run_mode: The mode to evaluate the graph in
    :param start_time: The time to start the graph
    :param end_time: The time to end the graph
    :param kwargs: Any additional kwargs to pass to the graph.
    """
    # For now this will evaluate the Python engine, as more engines become available there will be an engine factory
    # that can be used to select the engine to use.
    from hg._impl._runtime._graph_engine import PythonGraphEngine
    runtime_graph: GraphBuilder = wire_graph(graph, *args, **kwargs)
    engine: PythonGraphEngine = PythonGraphEngine(runtime_graph.make_instance(tuple()), run_mode)
    engine.run(start_time, end_time)
