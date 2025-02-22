import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Callable

from hgraph import Graph, Node, NodeTypeEnum
from hgraph.debug._inspector_util import estimate_value_size, estimate_size

__all__ = ("InspectionObserver",)

from hgraph import EvaluationLifeCycleObserver

logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class GraphInfo:
    graph: "Graph"
    id: tuple
    label: str
    parent_graph: int
    stopped: bool = False

    node_count: int
    total_subgraph_count: int
    total_node_count: int
    node_total_subgraph_counts: []
    node_total_node_counts: []

    eval_count: int
    eval_begin_time: float
    os_eval_begin_thread_time: float
    cycle_time: float
    os_cycle_time: float
    observation_time: float
    eval_time: float
    os_eval_time: float
    node_eval_counts: []
    node_eval_begin_times: []
    node_eval_times: []

    node_value_sizes: []
    node_sizes: []
    node_total_value_sizes_begin: []
    node_total_value_sizes: []
    node_total_sizes_begin: []
    node_total_sizes: []
    total_value_size_begin: int = 0
    total_value_size: int = 0
    total_size_begin: int = 0
    total_size: int = 0
    size: int = 0


class InspectionObserver(EvaluationLifeCycleObserver):
    def __init__(self,
                 graph: "Graph" = None,
                 callback_node: Callable = None,
                 callback_graph: Callable = None,
                 callback_progress: Callable = None,
                 progress_interval: float = 0.1,
                 compute_sizes: bool = False,
                 ):
        self.graphs = {}
        self.graphs_by_id = {}
        self.current_graph: GraphInfo = None

        self.callback_node = callback_node
        self.callback_graph = callback_graph
        self.callback_progress = callback_progress
        self.progress_interval = progress_interval
        self.progress_last_time = time.perf_counter_ns()
        self.compute_sizes = compute_sizes

        self.graph_subscriptions = set()
        self.node_subscriptions = set()

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
                for k, v in n.nested_graphs().items():
                    self.walk(v)

        self.on_after_start_graph(graph)

    def subscribe_graph(self, graph_id: tuple[int, ...]):
        self.graph_subscriptions.add(graph_id)

    def unsubscribe_graph(self, graph_id: tuple[int, ...]):
        self.graph_subscriptions.discard(graph_id)

    def subscribe_node(self, node_id: tuple[int, ...]):
        self.node_subscriptions.add(node_id)

    def unsubscribe_node(self, node_id: tuple[int, ...]):
        self.node_subscriptions.discard(node_id)

    def check_progress(self):
        if self.callback_progress and time.perf_counter_ns() - self.progress_last_time > self.progress_interval:
            self.progress_last_time = time.perf_counter_ns()
            try:
                self.callback_progress()
            except Exception:
                logger.exception(f"Error in callback_progress", exc_info=True)

    def on_before_start_graph(self, graph: "Graph"):
        if graph.graph_id == ():
            os_eval_begin_thread_time = time.thread_time()
        else:
            os_eval_begin_thread_time = 0.

        node_count = len(graph.nodes)

        default_size = 0 if self.compute_sizes else None
        size = estimate_size(graph) if self.compute_sizes else None

        gi = GraphInfo(
            graph=graph,
            id=graph.graph_id,
            label=graph.label,
            parent_graph=id(graph.parent_node.graph) if graph.parent_node else 0,
            node_count=node_count,
            total_node_count=node_count,
            total_subgraph_count=0,
            node_total_subgraph_counts=[0] * node_count,
            node_total_node_counts=[0] * node_count,
            eval_count=0,
            eval_begin_time=time.perf_counter_ns(),
            os_eval_begin_thread_time=os_eval_begin_thread_time,
            cycle_time=0.,
            os_cycle_time=0.,
            observation_time=0.,
            eval_time=0.,
            os_eval_time=0.,
            node_eval_counts=[0] * node_count,
            node_eval_begin_times=[0.] * node_count,
            node_eval_times=[0.] * node_count,
            node_value_sizes=[default_size] * node_count,
            node_total_value_sizes_begin=[default_size] * node_count,
            node_total_value_sizes=[default_size] * node_count,
            node_sizes=(node_sizes := [estimate_size(n) if self.compute_sizes else None for n in graph.nodes]),
            node_total_sizes_begin=[default_size] * node_count,
            node_total_sizes=[default_size] * node_count,
            size=size,
            total_size=(size + sum(node_sizes)) if self.compute_sizes else None,
        )

        if self.current_graph:
            assert self.graphs[gi.parent_graph] is self.current_graph

        self.graphs[id(graph)] = gi
        self.graphs_by_id[gi.id] = gi
        self.current_graph = gi

        while gi.parent_graph:
            parent_graph = self.graphs[gi.parent_graph]
            parent_node_id = gi.graph.parent_node.node_ndx
            parent_graph.node_total_subgraph_counts[parent_node_id] += 1
            parent_graph.node_total_node_counts[parent_node_id] += node_count
            parent_graph.total_subgraph_count += 1
            parent_graph.total_node_count += node_count
            if self.compute_sizes:
                parent_graph.node_total_sizes[parent_node_id] += size
            gi = parent_graph

    def on_after_start_graph(self, graph: "Graph"):
        self.current_graph = self.graphs.get(self.current_graph.parent_graph, None)

    def on_before_graph_evaluation(self, graph: "Graph"):
        observation_begin = time.perf_counter_ns()

        self.current_graph = self.graphs[id(graph)]
        self.current_graph.eval_begin_time = time.perf_counter_ns()
        if self.compute_sizes:
            self.current_graph.total_value_size_begin = self.current_graph.total_value_size
            self.current_graph.total_size_begin = self.current_graph.total_size

        if self.current_graph.id == ():
            self.current_graph.os_eval_begin_thread_time = time.thread_time()

        new_node_count = len(graph.nodes)
        if new_node_count != len(self.current_graph.node_eval_counts):
            prev_node_count = len(self.current_graph.node_eval_counts)
            self.current_graph.node_count = new_node_count
            self.current_graph.node_eval_counts = [0] * new_node_count
            self.current_graph.node_eval_begin_times = [0.] * new_node_count
            self.current_graph.node_eval_times = [0.] * new_node_count
            self.current_graph.node_value_sizes = [0] * new_node_count
            self.current_graph.node_total_value_sizes_begin = [0] * new_node_count
            self.current_graph.node_total_value_sizes = [0] * new_node_count
            self.current_graph.node_sizes = [0] * new_node_count
            self.current_graph.node_total_sizes_begin = [0] * new_node_count
            self.current_graph.node_total_sizes = [0] * new_node_count
            self.current_graph.node_total_node_counts = [0] * new_node_count
            self.current_graph.node_total_subgraph_counts = [0] * new_node_count

            gi = self.current_graph
            while gi.parent_graph:
                parent_graph = self.graphs[gi.parent_graph]
                parent_node_id = gi.graph.parent_node.node_ndx
                parent_graph.node_total_node_counts[parent_node_id] += new_node_count - prev_node_count
                parent_graph.total_node_count += new_node_count - prev_node_count
                gi = parent_graph

        self.current_graph.observation_time = time.perf_counter_ns() - observation_begin

    def on_before_node_evaluation(self, node: "Node"):
        self.current_graph.node_eval_begin_times[node.node_ndx] = time.perf_counter_ns()
        if self.compute_sizes:
            self.current_graph.node_total_value_sizes_begin[node.node_ndx] = self.current_graph.node_total_value_sizes[node.node_ndx]
            self.current_graph.node_total_sizes_begin[node.node_ndx] = self.current_graph.node_total_sizes[node.node_ndx]

    def on_after_node_evaluation(self, node: "Node"):
        observation_begin = time.perf_counter_ns()

        self.current_graph.node_eval_counts[node.node_ndx] += 1
        self.current_graph.node_eval_times[node.node_ndx] += time.perf_counter_ns() - self.current_graph.node_eval_begin_times[node.node_ndx]

        if node.signature.node_type != NodeTypeEnum.PUSH_SOURCE_NODE:
            self._process_node_after_eval(node)

        self.current_graph.observation_time += time.perf_counter_ns() - observation_begin

    def on_after_graph_push_nodes_evaluation(self, graph: "Graph"):
        observation_begin = time.perf_counter_ns()

        for node in graph.nodes[:graph.push_source_nodes_end]:
            if node.output.modified:
                self._process_node_after_eval(node)

        self.current_graph.observation_time += time.perf_counter_ns() - observation_begin

    def _process_node_after_eval(self, node):
        if self.compute_sizes:
            value_size = estimate_value_size(node)
            node_size = estimate_size(node)
            self.current_graph.total_value_size += (
                    value_size - self.current_graph.node_value_sizes[node.node_ndx]
                    + self.current_graph.node_total_value_sizes[node.node_ndx]
                    - self.current_graph.node_total_value_sizes_begin[node.node_ndx]
            )
            self.current_graph.total_size += (
                    node_size - self.current_graph.node_sizes[node.node_ndx]
                    + self.current_graph.node_total_sizes[node.node_ndx]
                    - self.current_graph.node_total_sizes_begin[node.node_ndx]
            )
            self.current_graph.node_value_sizes[node.node_ndx] = value_size
            self.current_graph.node_sizes[node.node_ndx] = node_size

        if self.callback_node and node.node_id in self.node_subscriptions:
            try:
                self.callback_node(node)
            except Exception as e:
                logger.exception(f"Error in callback_node", exc_info=True)

    def on_after_graph_evaluation(self, graph: "Graph"):
        observation_begin = time.perf_counter_ns()

        self.current_graph.eval_count += 1
        self.current_graph.cycle_time = time.perf_counter_ns() - self.current_graph.eval_begin_time
        self.current_graph.eval_time += self.current_graph.cycle_time

        if graph.graph_id != ():
            parent_graph = self.graphs_by_id[graph.parent_node.owning_graph_id]
            parent_node_ndx = graph.parent_node.node_ndx
            if self.compute_sizes:
                parent_graph.node_total_value_sizes[parent_node_ndx] += self.current_graph.total_value_size - self.current_graph.total_value_size_begin
                parent_graph.node_total_sizes[parent_node_ndx] += self.current_graph.total_size - self.current_graph.total_size_begin

        if graph.graph_id == ():
            thread_time = time.thread_time() - self.current_graph.os_eval_begin_thread_time
            self.current_graph.os_cycle_time = thread_time
            self.current_graph.os_eval_time += self.current_graph.os_cycle_time

        self.current_graph.observation_time += time.perf_counter_ns() - observation_begin
        prev_observation_time = self.current_graph.observation_time
        self.current_graph = self.graphs.get(self.current_graph.parent_graph, None)

        if self.callback_graph and graph.graph_id in self.graph_subscriptions:
            try:
                self.callback_graph(graph)
            except Exception as e:
                logger.exception(f"Error in callback_graph", exc_info=True)

        self.check_progress()

        if self.current_graph is not None:
            self.current_graph.observation_time += prev_observation_time

    def on_after_stop_graph(self, graph: "Graph"):
        if gi := self.graphs.get(id(graph)):
            gi.stopped = True
