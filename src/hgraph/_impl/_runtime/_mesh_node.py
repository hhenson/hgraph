from collections import defaultdict
from datetime import datetime
from typing import Mapping, Any, Callable, cast

from hgraph import MAX_DT, PythonTsdMapNodeImpl, GlobalState, PythonTimeSeriesReference, MIN_TD
from hgraph._builder._graph_builder import GraphBuilder
from hgraph._impl._runtime._nested_evaluation_engine import (
    NestedEngineEvaluationClock,
    NestedEvaluationEngine,
    PythonNestedNodeImpl,
)
from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._evaluation_clock import EngineEvaluationClock
from hgraph._runtime._graph import Graph
from hgraph._runtime._node import Node, NodeSignature
from hgraph._types._error_type import NodeError
from hgraph._types._time_series_types import TIME_SERIES_TYPE, K
from hgraph._types._ts_type import TS
from hgraph._types._tsd_type import TSD, TSD_OUT
from hgraph._types._tss_type import TSS
from hgraph._wiring._map import KEYS_ARG

__all__ = ("PythonTsdMapNodeImpl",)


class MeshNestedEngineEvaluationClock(NestedEngineEvaluationClock):

    def __init__(self, engine_evaluation_clock: EngineEvaluationClock, key: K, nested_node: "PythonMeshNodeImpl"):
        super().__init__(engine_evaluation_clock, nested_node)
        self._key = key

    @property
    def key(self) -> K:
        return self._key

    def update_next_scheduled_evaluation_time(self, next_time: datetime):
        # First we make sure the key is correctly scheduled, then we call super, which will ensure the
        # node is scheduled if required.
        node: PythonMeshNodeImpl = self._nested_node
        if next_time < node.last_evaluation_time:
            return

        rank = node._active_graphs_rank[self._key]
        if next_time == node.last_evaluation_time and (
            rank == node.current_eval_rank or self.key == node.current_eval_graph
        ):
            return

        tm = node._scheduled_keys_by_rank[rank].get(self._key)
        if tm is None or tm > next_time:
            node.schedule_graph(self._key, next_time)

        if next_time > node.last_evaluation_time:
            super().update_next_scheduled_evaluation_time(next_time)


class PythonMeshNodeImpl(PythonTsdMapNodeImpl):
    def __init__(
        self,
        node_ndx: int,
        owning_graph_id: tuple[int, ...],
        signature: NodeSignature,
        scalars: Mapping[str, Any],
        eval_fn: Callable = None,
        start_fn: Callable = None,
        stop_fn: Callable = None,
        nested_graph_builder: GraphBuilder = None,
        input_node_ids: Mapping[str, int] = None,
        output_node_id: int = None,
        multiplexed_args: frozenset[str] = None,
        key_arg: str = None,
        context_path: str = None,
    ):
        super().__init__(
            node_ndx,
            owning_graph_id,
            signature,
            scalars,
            eval_fn,
            start_fn,
            stop_fn,
            nested_graph_builder,
            input_node_ids,
            output_node_id,
            multiplexed_args,
            key_arg,
        )

        self._full_context_path = f"context-{self.owning_graph_id}-{context_path}"
        self._scheduled_ranks: dict[int, datetime] = {}
        self._scheduled_keys_by_rank: dict[int, dict[K, datetime]] = defaultdict(dict)
        self._active_graphs_rank: dict[K, int] = {}
        self._active_graphs_dependencies: dict[K, set[K]] = defaultdict(set)
        self._re_rank_requests: list[tuple[K, K]] = []
        self.current_eval_rank = None
        self.current_eval_graph = None
        self.max_rank = 0

    def start(self):
        super().start()
        self.output["ref"].value = PythonTimeSeriesReference(self.output["out"])
        GlobalState.instance()[self._full_context_path] = self.output["ref"]
        self._output = self.output["out"]

    def stop(self):
        del GlobalState.instance()[self._full_context_path]

    def eval(self):
        self.mark_evaluated()
        # 1. All inputs should be reference, and we should only be active on the KEYS_ARG input
        keys: TSS[K] = self._kwargs[KEYS_ARG]
        if keys.modified:
            for k in keys.added():
                if k not in self._active_graphs:
                    self._create_new_graph(k)
            for k in keys.removed():
                if not self._active_graphs_dependencies.get(k, None):
                    self._scheduled_keys_by_rank[self._active_graphs_rank[k]].pop(k, None)
                    self._remove_graph(k)
        if self._pending_keys:
            for k in self._pending_keys:
                self._create_new_graph(k, rank=0)
                for d in self._active_graphs_dependencies[k]:
                    self._re_rank(d, k)
            self._pending_keys = set()

        # 2. or one of the nested graphs has been scheduled for evaluation.
        next_time = MAX_DT
        rank = 0
        while rank <= self.max_rank:
            dt = self._scheduled_ranks.pop(rank, None)
            if dt == self.last_evaluation_time:
                graphs = self._scheduled_keys_by_rank.pop(rank, {})
                for k, dtg in graphs.items():
                    if dtg == dt:
                        self.current_eval_graph = k
                        dtg = self._evaluate_graph(k)
                        self.current_eval_graph = None

                    if dtg != MAX_DT and dtg > self.last_evaluation_time:
                        self.schedule_graph(k, dtg)
                        next_time = min(next_time, dtg)
            elif dt and dt > self.last_evaluation_time:
                self._scheduled_ranks[rank] = dt

            rank += 1

        self.current_eval_rank = None

        if self._re_rank_requests:
            for k, d in self._re_rank_requests:
                self._re_rank(k, d)
            self._re_rank_requests = []

        if next_time < MAX_DT:
            self.graph.schedule_node(self.node_ndx, next_time)

    def _create_new_graph(self, key: K, rank=None):
        """Create new graph instance and wire it into the node"""
        graph: Graph = self.nested_graph_builder.make_instance(self.node_id + (self._count,), self, str(key))
        self._count += 1
        self._active_graphs[key] = graph
        self._active_graphs_rank[key] = self.max_rank if rank is None else rank
        graph.evaluation_engine = NestedEvaluationEngine(
            self.graph.evaluation_engine,
            MeshNestedEngineEvaluationClock(self.graph.evaluation_engine.engine_evaluation_clock, key, self),
        )
        graph.initialise()
        self._wire_graph(key, graph)
        self.current_eval_graph = key
        graph.start()
        self.current_eval_graph = None
        self.schedule_graph(key, self.last_evaluation_time)

    def schedule_graph(self, key, tm):
        rank = self._active_graphs_rank[key]
        self._scheduled_keys_by_rank[rank][key] = tm
        self._scheduled_ranks[rank] = min(self._scheduled_ranks.get(rank, MAX_DT), tm)
        self.graph.schedule_node(self.node_ndx, tm)

    def _remove_graph(self, key: K):
        """Un-wire graph and schedule for removal"""
        if self.signature.capture_exception:
            # Remove the error output associated to the graph if there is one.
            cast(TSD_OUT[K, TS[NodeError]], self.error_output).pop(key, None)
        graph: Graph = self._active_graphs.pop(key)
        self._un_wire_graph(key, graph)
        graph.stop()
        self._scheduled_keys_by_rank[self._active_graphs_rank[key]].pop(key, None)
        self._active_graphs_rank.pop(key)
        graph.dispose()

    def _add_graph_dependency(self, key: K, depends_on: K) -> bool:  # returns True if the key is available now
        self._active_graphs_dependencies[depends_on].add(key)

        if depends_on not in self._active_graphs:
            self._pending_keys.add(depends_on)
            self.graph.schedule_node(self.node_ndx, self.last_evaluation_time + MIN_TD)
            return False
        else:
            return self._request_re_rank(key, depends_on)

    def _request_re_rank(self, key: K, depends_on: K):
        if (self._active_graphs_rank[key]) <= (self._active_graphs_rank[depends_on]):
            self._re_rank_requests.append((key, depends_on))
            return False
        else:
            return True

    def _re_rank(self, key: K, depends_on: K):
        if (prev_rank := self._active_graphs_rank[key]) <= (below := self._active_graphs_rank[depends_on]):
            schedule = self._scheduled_keys_by_rank[prev_rank].pop(key, None)
            new_rank = below + 1
            self.max_rank = max(self.max_rank, new_rank)
            self._active_graphs_rank[key] = new_rank
            if schedule:
                self.schedule_graph(key, schedule)
            for k in self._active_graphs_dependencies[key]:
                self._re_rank(k, key)

    def _remove_graph_dependency(self, key: K, depends_on: K):
        self._active_graphs_dependencies[depends_on].remove(key)
        if not self._active_graphs_dependencies[depends_on] and depends_on not in self._kwargs[KEYS_ARG]:
            # no more dependencies and this was not asked for externally (through the keys)
            self._remove_graph(depends_on)
