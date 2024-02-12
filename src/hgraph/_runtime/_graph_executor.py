import typing
from abc import abstractmethod
from datetime import datetime
from typing import Iterable

from hgraph._runtime._evaluation_engine import EvaluationMode, EvaluationLifeCycleObserver, StatefulElements
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
    def replay(self, start_time: datetime, end_time: datetime, using: StatefulElements = StatefulElements.SOURCE_NODES):
        """
        Replays the graph using the recorded state, using only the elements specified for replay.
        The replay is only available on graphs that have been created with at least SOURCE_NODES marked as being persistent.
        Replaying can be SOURCE_NODES, SOURCE_NODES + STATEFUL_NODES or just SINK_NODES.

        Replay:
            * SOURCE_NODES - The source nodes are replayed and not evaluated, this effectively replaces the source node
                             with a replay node.
            * STATEFUL_NODES - This will ensure that a node marked with STATE, output or SCHEDULER will be called with
                               the value that it would have been called with on the first pass of evaluation (first time
                               the node is called)
            * SINK_NODES - The graph nodes preceding this node are ignored and replaced with a replay of values as they
                           were previously recorded as receiving.

        If a replay is requested on a modified graph shape, the matching nodes will be replayed and the remainder
        evaluated as normal.
        Push source nodes are replaced with pull source replay nodes.

        Services can be stubbed out in replay mode to a point, but this will have limitations if the code behaves
        differently during replay, for example request-reply would require a request be sent (matching the original recording)
        and the reply will be sent from the recorded store. If the code behaves differently during evaluation, that
        will not work well.
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
    def make(graph: Graph, run_mode: EvaluationMode,
             persistence: StatefulElements = StatefulElements.NONE,
             observers: Iterable[EvaluationLifeCycleObserver] = None) -> GraphExecutor:
        """
        Make a new graph engine. If no engine is declared, the default engine will be used.
        :param graph: The graph to make the engine for
        :param run_mode: The run mode of the engine
        :param persistence: The components to consider as being persistent, the affects what replay options exist.
        :param observers: The observers to associate with this graph as default observers.
        :return: A new graph engine
        """
        if GraphEngineFactory.is_declared():
            return GraphEngineFactory.declared()(graph=graph, run_mode=run_mode, persistence=persistence,
                                                 observers=observers)
        else:
            return GraphEngineFactory.default()(graph=graph, run_mode=run_mode, persistence=persistence,
                                                observers=observers)
