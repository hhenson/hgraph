import time
from dataclasses import dataclass
from typing import Union, Callable

from frozendict import frozendict
from multimethod import multimethod

from hgraph.debug._inspector_util import str_node_id

try:
    import psutil
except ImportError:
    psutil = None

from hgraph import Graph, PythonTimeSeriesValueOutput, \
    PythonTimeSeriesListOutput, PythonTimeSeriesSetOutput, PythonTimeSeriesBundleOutput, PythonTimeSeriesDictOutput, \
    PythonTimeSeriesReferenceOutput, Node, TimeSeriesOutput, PythonTimeSeriesReferenceInput, TimeSeriesList, \
    TimeSeriesSet, TimeSeriesBundle, TimeSeriesDict, PythonTimeSeriesReference, PythonNestedNodeImpl

__all__ = ("InspectionObserver",)

from hgraph import EvaluationLifeCycleObserver


@dataclass(kw_only=True)
class GraphInfo:
    graph: "Graph"
    id: tuple
    label: str
    parent_graph: int
    stopped: bool = False
    eval_count: int
    eval_begin_time: float
    eval_time: float
    node_eval_counts: []
    node_eval_begin_times: []
    node_eval_times: []


class InspectionObserver(EvaluationLifeCycleObserver):
    def __init__(self, graph: "Graph" = None, callback_node: Callable = None, callback_graph: Callable = None):
        self.graphs = {}
        self.graphs_by_id = {}
        self.current_graph: GraphInfo = None

        self.callback_node = callback_node
        self.callback_graph = callback_graph
        self.subscriptions = set()

        if graph:
            self.walk(graph)
            self.on_before_graph_evaluation(graph)

    def get_graph_info(self, graph_id: tuple) -> GraphInfo | None:
        return self.graphs_by_id.get(graph_id)

    def walk(self, graph: "Graph"):
        from hgraph import PythonNestedNodeImpl

        self.on_before_start_graph(graph)
        for n in graph.nodes:
            if isinstance(n, PythonNestedNodeImpl):
                for k, v in n.enum_nested_graphs():
                    self.walk(v)

        self.on_after_start_graph(graph)

    def subscribe(self, node_id: tuple[int, ...]):
        if node_id not in self.subscriptions:
            self.subscriptions.add(node_id)

    def unsubscribe(self, node_id: tuple[int, ...]):
        self.subscriptions.discard(node_id)

    def on_before_start_graph(self, graph: "Graph"):
        gi = GraphInfo(
            graph=graph,
            id=graph.graph_id,
            label=graph.label,
            parent_graph=id(graph.parent_node.graph) if graph.parent_node else 0,
            eval_count=0,
            eval_begin_time=time.perf_counter_ns(),
            eval_time=0.,
            node_eval_counts=[0] * len(graph.nodes),
            node_eval_begin_times=[0.] * len(graph.nodes),
            node_eval_times=[0.] * len(graph.nodes),
        )

        if self.current_graph:
            assert self.graphs[gi.parent_graph] is self.current_graph

        self.graphs[id(graph)] = gi
        self.graphs_by_id[gi.id] = gi
        self.current_graph = gi

    def on_after_start_graph(self, graph: "Graph"):
        self.current_graph = self.graphs.get(self.current_graph.parent_graph, None)

    def on_before_graph_evaluation(self, graph: "Graph"):
        self.current_graph = self.graphs[id(graph)]
        self.current_graph.eval_begin_time = time.perf_counter_ns()
        if len(graph.nodes) != len(self.current_graph.node_eval_counts):
            self.current_graph.node_eval_counts = [0] * len(graph.nodes)
            self.current_graph.node_eval_begin_times = [0.] * len(graph.nodes)
            self.current_graph.node_eval_times = [0.] * len(graph.nodes)

    def on_before_node_evaluation(self, node: "Node"):
        self.current_graph.node_eval_begin_times[node.node_ndx] = time.perf_counter_ns()

    def on_after_node_evaluation(self, node: "Node"):
        self.current_graph.node_eval_counts[node.node_ndx] += 1
        self.current_graph.node_eval_times[node.node_ndx] += time.perf_counter_ns() - self.current_graph.node_eval_begin_times[node.node_ndx]

        if self.callback_node and node.node_id in self.subscriptions:
            self.callback_node(node)

    def on_after_graph_evaluation(self, graph: "Graph"):
        self.current_graph.eval_count += 1
        self.current_graph.eval_time += time.perf_counter_ns() - self.current_graph.eval_begin_time
        self.current_graph = self.graphs.get(self.current_graph.parent_graph, None)

        if self.callback_graph and graph.graph_id in self.subscriptions:
            self.callback_graph(graph)

    def on_after_stop_graph(self, graph: "Graph"):
        if gi := self.graphs.get(id(graph)):
            gi.stopped = True
