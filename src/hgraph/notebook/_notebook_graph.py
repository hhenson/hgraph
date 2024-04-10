from datetime import datetime
from typing import Sequence, Any

from hgraph import GlobalState, WiringGraphContext, GraphConfiguration, evaluate_graph, create_graph_builder, MIN_ST, \
    MAX_ET
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext
from hgraph.nodes import record, get_recorded_value

__all__ = ("start_wiring_graph", "notebook_evaluate_graph",)


_START_TIME: datetime = None
_END_TIME: datetime = None


def start_wiring_graph(name: str = 'notebook-graph', start_time: datetime = MIN_ST, end_time: datetime = MAX_ET):
    global _START_TIME, _END_TIME
    from hgraph import WiringPort, WiringGraphContext
    WiringPort.eval = notebook_eval_node
    from hgraph._builder._ts_builder import TimeSeriesBuilderFactory
    if not TimeSeriesBuilderFactory.has_instance():
        TimeSeriesBuilderFactory.declare_default_factory()
    # Prepare the contexts required for evaluation
    GlobalState._instance = GlobalState()
    WiringNodeInstanceContext.__stack__ = [WiringNodeInstanceContext()]
    WiringGraphContext.__stack__ = [WiringGraphContext(name)]  # Ensures we reset when this is re-evaluated.
    _START_TIME = start_time
    _END_TIME = end_time


def notebook_evaluate_graph():
    """
    Completes the wiring and evaluates the graph
    """
    context = WiringGraphContext.instance()
    context.build_services()
    # Build graph by walking from sink nodes to parent nodes.
    # Also eliminate duplicate nodes
    sink_nodes = context.sink_nodes
    graph_builder = create_graph_builder(sink_nodes)
    config = GraphConfiguration(start_time=_START_TIME, end_time=_END_TIME)
    evaluate_graph(graph_builder, config)


def notebook_eval_node(self) -> Sequence[tuple[datetime, Any]]:
    global _START_TIME, _END_TIME
    record(self)
    notebook_evaluate_graph()
    return get_recorded_value()

