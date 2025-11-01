import typing
from abc import abstractmethod
from datetime import datetime
from typing import Iterable

from hgraph._runtime._evaluation_engine import EvaluationMode, EvaluationLifeCycleObserver
from hgraph._runtime._graph import Graph

__all__ = ("GraphExecutor", "GraphEngineFactory")


class GraphExecutor:
    """
    The component responsible for executing the graph. This is the master run loop engine and provides functionality
    for evaluation push and pull source nodes. Only the graph operated by this component can support pull source nodes.
    Inner graphs only support pull sources nodes in their evaluation.

    The entry point here is the run method. This will begin running the engine on the thread it is called on.
    Note that the engine by default is a single threaded process. It is intended to be thread safe, that is multiple
    engines could be operated in parallel. That said with the current limitations of the python GIL, it is not going
    to be helpful on process bound processes. The intention is to provide a C++ implementation of the engine that
    would be GIL free, additionally, with newer versions of Python promising for GIL free execution, this may become
    more useful in the future.
    """

    @property
    @abstractmethod
    def run_mode(self) -> EvaluationMode:
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
        :param observers: And observers to add to the evaluation engine prior to starting the graph.
        """


class GraphEngineFactory:

    _graph_engine_class: typing.Optional[typing.Type[GraphExecutor]] = None

    @staticmethod
    def default():
        from hgraph._impl._runtime._graph_executor import PythonGraphExecutor

        return PythonGraphExecutor

    @staticmethod
    def is_declared() -> bool:
        return GraphEngineFactory._graph_engine_class is not None

    @staticmethod
    def declared() -> typing.Type[GraphExecutor]:
        if GraphEngineFactory._graph_engine_class is None:
            raise RuntimeError("No graph engine type has been declared")
        return GraphEngineFactory._graph_engine_class

    @staticmethod
    def declare(factory: typing.Type[GraphExecutor]):
        if GraphEngineFactory._graph_engine_class is not None:
            raise RuntimeError("A graph engine type has already been declared")
        GraphEngineFactory._graph_engine_class = factory

    @staticmethod
    def un_declare():
        GraphEngineFactory._graph_engine_class = None

    @staticmethod
    def make(
        graph: Graph, run_mode: EvaluationMode, observers: Iterable[EvaluationLifeCycleObserver] = None
    ) -> GraphExecutor:
        """
        Make a new graph engine. If no engine is declared, the default engine will be used.
        :param graph: The graph to make the engine for
        :param run_mode: The run mode of the engine
        :param observers: The observers to associate with this graph as default observers.
        :return: A new graph engine
        """
        if GraphEngineFactory.is_declared():
            return GraphEngineFactory.declared()(graph=graph, run_mode=run_mode, observers=observers)
        else:
            return GraphEngineFactory.default()(graph=graph, run_mode=run_mode, observers=observers)
