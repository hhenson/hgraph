from datetime import datetime
from typing import Callable

from hg._runtime._constants import MIN_ST, MAX_ET
from hg._runtime._graph_engine import RunMode, GraphEngineFactory


__all__ = ("run_graph",)


def run_graph(graph: Callable, *args, run_mode: RunMode = RunMode.BACK_TEST, start_time: datetime = MIN_ST,
              end_time: datetime = MAX_ET, **kwargs):
    """
    Use this to initiate the graph engine run loop.

    The run_mode indicates how the graph engine should evaluate the graph, in RunMOde.REAL_TIME the graph will be
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
    from hg._builder._graph_builder import GraphBuilder
    from hg._wiring._graph_builder import wire_graph

    if not isinstance(graph, GraphBuilder):
        graph_builder = wire_graph(graph, *args, **kwargs)
    else:
        graph_builder = graph

    engine = GraphEngineFactory.make(graph=graph_builder.make_instance(tuple()), run_mode=run_mode)
    engine.run(start_time, end_time)
