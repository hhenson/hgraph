import typing
from abc import abstractmethod
from datetime import datetime
from enum import Enum

from hg._runtime._graph import Graph
from hg._runtime._lifecycle import ComponentLifeCycle

__all__ = ( "RunMode", "GraphEngine", "GraphExecutorLifeCycleObserver")


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


class GraphEngine(ComponentLifeCycle, typing.Protocol):

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


class GraphEngineFactory:

    _graph_engine_class: typing.Optional[typing.Type[GraphEngine]] = None

    @staticmethod
    def default():
        from hg._impl._runtime._graph_engine import  PythonGraphEngine
        return PythonGraphEngine

    @staticmethod
    def is_declared() -> bool:
        return GraphEngineFactory._graph_engine_class is not None

    @staticmethod
    def declared() -> typing.Type[GraphEngine]:
        if GraphEngineFactory._graph_engine_class is None:
            raise RuntimeError("No graph engine type has been declared")
        return GraphEngineFactory._graph_engine_class

    @staticmethod
    def declare(factory: typing.Type[GraphEngine]):
        if GraphEngineFactory._graph_engine_class is not None:
            raise RuntimeError("A graph engine type has already been declared")
        GraphEngineFactory._graph_engine_class = factory

    @staticmethod
    def un_declare():
        GraphEngineFactory._graph_engine_class = None

    @staticmethod
    def make(graph: Graph, run_mode: RunMode) -> GraphEngine:
        """
        Make a new graph engine. If no engine is declared, the default engine will be used.
        :param graph: The graph to make the engine for
        :param run_mode: The run mode of the engine
        :return: A new graph engine
        """
        if GraphEngineFactory.is_declared():
            return GraphEngineFactory.declared()(graph=graph, run_mode=run_mode)
        else:
            return GraphEngineFactory.default()(graph=graph, run_mode=run_mode)