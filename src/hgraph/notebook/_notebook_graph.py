from datetime import datetime
from logging import ERROR, getLogger
from typing import Sequence, Any

from hgraph import (
    GlobalState,
    WiringGraphContext,
    GraphConfiguration,
    evaluate_graph,
    create_graph_builder,
    MIN_ST,
    MAX_ET,
    record,
    WiringNodeInstanceContext,
    get_recorded_value, WiringPort, HgTSTypeMetaData, HgTSBTypeMetaData, reset_recorded_value,
)

__all__ = (
    "start_wiring_graph",
    "notebook_evaluate_graph",
)


_START_TIME: datetime = None
_END_TIME: datetime = None
_COUNTER: int = 0


def start_wiring_graph(name: str = "notebook-graph", start_time: datetime = MIN_ST, end_time: datetime = MAX_ET):
    global _START_TIME, _END_TIME
    from hgraph import WiringPort, WiringGraphContext

    WiringPort.eval = notebook_eval_node
    WiringPort.plot = notebook_plot_node

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
    config.graph_logger.setLevel(ERROR)
    evaluate_graph(graph_builder, config)


def notebook_eval_node(self) -> Sequence[tuple[datetime, Any]]:
    global _START_TIME, _END_TIME, _COUNTER
    id = f"Eval_{_COUNTER}"
    _COUNTER += 1
    record(self, key=id)
    notebook_evaluate_graph()
    return get_recorded_value(key=id)


def notebook_plot_node(self: WiringPort, title: str = None, ylabel: str = None):
    """
    Adds the ability to plot the time-series.
    Currently, this only supports single time-series values.
    """
    values = notebook_eval_node(self)
    from matplotlib import pyplot as plt
    if type(self.output_type) is HgTSTypeMetaData:
        data = {"date": [v[0] for v in values], "value": [v[1] for v in values]}
        plt.plot(data["date"], data["value"])
        plt.xlabel('datetime')
        if ylabel:
            plt.ylabel(ylabel)
        if title:
            plt.title(title)
    elif type(self.output_type) is HgTSBTypeMetaData:
        ndx, values = zip(*values)
        schema = self.output_type.bundle_schema_tp.meta_data_schema
        data = {key: [d.get(key, None) for d in values] for key in schema.keys()}
        for key in schema.keys():
            plt.plot(ndx, data[key], label=key)
        plt.xlabel('datetime')
        if ylabel:
            plt.ylabel(ylabel)
        if title:
            plt.title(title)
        plt.legend()
    else:
        getLogger("hgraph").error("Unable to plot type: %s", self.output_type)