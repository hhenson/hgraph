from datetime import datetime

from hg._runtime import Graph

from hg._wiring._graph_builder import build_runtime_graph

__all__ = ("run", "build_runtime_graph")


class GraphEngine:

    def __init__(self, graph: Graph):
        pass

    def run(self, start_time: datetime, end_time: datetime):
        """
        Tell the engine to begin and operate it's run loop.
        :param start_time: The time to start the run loop at
        :param end_time: The time to end the run loop at
        """


class RealTimeGraphEngine(GraphEngine):
    """
    Evaluates the engine in real time.
    """


class BackTestGraphEngine(GraphEngine):
    """
    Evaluates the engine using a simulated compress time clock.
    This is useful for testing and simulation purposes.
    """


def run(graph, *args, realtime: bool=False, start_time: datetime, end_time: datetime, **kwargs):
    """
    Use this to initiate the engine run loop.

    :param graph: The graph to evaluate
    :param args: Any arguments to pass to the graph
    :param realtime: True will cause the graph to be run in real time, False will cause it to run in back-test mode
    :param start_time: The time to start the graph
    :param end_time: The time to end the graph
    :param kwargs: Any additional kwargs to pass to the graph.
    """

    runtime_graph: Graph = build_runtime_graph(graph, *args, **kwargs)
    engine: GraphEngine
    if realtime:
        engine = RealTimeGraphEngine(graph)
    else:
        engine = BackTestGraphEngine(graph)
    engine.run(start_time, end_time)
