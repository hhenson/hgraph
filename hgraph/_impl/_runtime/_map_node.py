from datetime import datetime, date, time, timedelta
from typing import Mapping, Any, Callable, cast

from hgraph._operators._record_replay import get_fq_recordable_id, set_parent_recordable_id, has_recordable_id_trait
from hgraph._runtime._constants import MAX_DT
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


class MapNestedEngineEvaluationClock(NestedEngineEvaluationClock):

    def __init__(self, engine_evaluation_clock: EngineEvaluationClock, key: K, nested_node: "PythonTsdMapNodeImpl"):
        super().__init__(engine_evaluation_clock, nested_node)
        self._key = key

    def update_next_scheduled_evaluation_time(self, next_time: datetime):
        # First we make sure the key is correctly scheduled, then we call super, which will ensure the
        # node is scheduled if required.
        node: PythonTsdMapNodeImpl = self._nested_node
        if (let := node.last_evaluation_time) and let >= next_time or node.is_stopping:
            return

        tm = node._scheduled_keys.get(self._key)
        if tm is None or tm > next_time:
            node._scheduled_keys[self._key] = next_time

        super().update_next_scheduled_evaluation_time(next_time)


class PythonTsdMapNodeImpl(PythonNestedNodeImpl):

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
    ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn)
        self.nested_graph_builder: GraphBuilder = nested_graph_builder
        self.input_node_ids: Mapping[str, int] = input_node_ids
        self.output_node_id: int = output_node_id
        self.multiplexed_args: frozenset[str] = multiplexed_args
        self.key_arg: str = key_arg
        self._scheduled_keys: dict[K, datetime] = {}
        self._active_graphs: dict[K, Graph] = {}
        self._pending_keys: set[K] = set()
        self._count: int = 1
        self._recordable_id: str | None = None

    def tsd_output(self):
        return self._output

    def do_start(self):
        super().do_start()
        if has_recordable_id_trait(self.graph.traits):
            recordable_id = self.signature.record_replay_id
            self._recordable_id = get_fq_recordable_id(self.graph.traits, recordable_id if recordable_id else f"map_")

    def eval(self):
        self.mark_evaluated()
        # 1. All inputs should be reference, and we should only be active on the KEYS_ARG input
        keys: TSS[K] = self._kwargs[KEYS_ARG]
        if keys.modified:
            for k in keys.added():
                # There seems to be a case where a set can show a value as added even though it is not.
                # This protects from accidentally creating duplicate graphs
                if k not in self._active_graphs:
                    self._create_new_graph(k)
                else:
                    raise RuntimeError(f"[{self._signature.wiring_path_name}] Key {k} already exists in active graphs")
            for k in keys.removed():
                # Ensure we have a graph before removing.
                if k in self._active_graphs:
                    self._remove_graph(k)
                    self._scheduled_keys.pop(k, None)
                else:
                    raise RuntimeError(f"[{self._signature.wiring_path_name}] Key {k} does not exist in active graphs")
        # 2. or one of the nested graphs has been scheduled for evaluation.
        scheduled_keys = self._scheduled_keys
        self._scheduled_keys = {}
        for k, dt in scheduled_keys.items():
            if dt < self.last_evaluation_time:
                raise RuntimeError(
                    f"Scheduled time is in the past; last evaluation time: {self.last_evaluation_time}, "
                    f"scheduled time: {dt}, evaluation time: {self.graph.evaluation_clock.evaluation_time}"
                )
            elif dt == self.last_evaluation_time:
                dt = self._evaluate_graph(k)

            if dt != MAX_DT and dt > self.last_evaluation_time:
                # Re-schedule for the next time.
                self._scheduled_keys[k] = dt
                self.graph.schedule_node(self.node_ndx, dt)

    def do_stop(self):
        for k in list(self._active_graphs.keys()):
            self._remove_graph(k)
        self._active_graphs.clear()
        self._scheduled_keys.clear()
        self._pending_keys.clear()

    def nested_graphs(self):
        return self._active_graphs

    def _create_new_graph(self, key: K):
        """Create new graph instance and wire it into the node"""
        graph: Graph = self.nested_graph_builder.make_instance(self.node_id + (-self._count,), self, str(key))
        self._count += 1
        self._active_graphs[key] = graph
        graph.evaluation_engine = NestedEvaluationEngine(
            self.graph.evaluation_engine,
            MapNestedEngineEvaluationClock(self.graph.evaluation_engine.engine_evaluation_clock, key, self),
        )
        graph.initialise()
        if self._recordable_id:
            recordable_id = f"{self._recordable_id}[{str(key)}]"
            set_parent_recordable_id(graph, recordable_id)
        self._wire_graph(key, graph)
        graph.start()
        self._scheduled_keys[key] = self.last_evaluation_time

    def _remove_graph(self, key: K):
        """Un-wire graph and schedule for removal"""
        if self.signature.capture_exception:
            # Remove the error output associated to the graph if there is one.
            cast(TSD_OUT[K, TS[NodeError]], self.error_output).pop(key)
        graph: Graph = self._active_graphs.pop(key)
        self._un_wire_graph(key, graph)
        try:
            graph.stop()
        finally:
            self.graph.evaluation_engine_api.add_before_evaluation_notification(
                lambda g=graph: self.nested_graph_builder.release_instance(g)
            )

    def _evaluate_graph(self, key: K):
        """Evaluate the graph for this key"""
        graph: Graph = self._active_graphs[key]
        graph.evaluation_clock.reset_next_scheduled_evaluation_time()
        # TODO: This can be done at start time or initialisation time (decide on behaviour) and then we don't
        # have to pay for the if.
        if self.signature.capture_exception:
            try:
                graph.evaluate_graph()
            except Exception as e:
                error_output = cast(TSD[K, TS[NodeError]], self.error_output).get_or_create(key)
                node_error = NodeError.capture_error(exception=e, node=self, message=f"key: {key}")
                error_output.value = node_error
        else:
            graph.evaluate_graph()

        next = graph.evaluation_clock.next_scheduled_evaluation_time
        graph.evaluation_clock.reset_next_scheduled_evaluation_time()
        return next

    def _un_wire_graph(self, key: K, graph: Graph):
        for arg, node_ndx in self.input_node_ids.items():
            node: NodeImpl = graph.nodes[node_ndx]
            if arg != self.key_arg:
                if arg in self.multiplexed_args:  # Is this a multiplexed input?
                    from hgraph import PythonTimeSeriesReferenceInput

                    tsd : TSD[str, TIME_SERIES_TYPE] = self.input[arg]
                    node.input.ts.re_parent(tsd)
                    node.input = node.input.copy_with(
                        __init_args__=dict(_parent_or_node=node), ts=(ts := PythonTimeSeriesReferenceInput())
                    )
                    ts.re_parent(node.input)
                    if not tsd.key_set.valid or not tsd.key_set.__contains__(key):
                        tsd.on_key_removed(key)

        if self.output_node_id:
            # Replace the nodes output with the map node's output for the key
            del self.tsd_output()[key]

    def _wire_graph(self, key: K, graph: Graph):
        """Connect inputs and outputs to the nodes inputs and outputs"""
        for arg, node_ndx in self.input_node_ids.items():
            node: NodeImpl = graph.nodes[node_ndx]
            node.notify()
            if arg == self.key_arg:
                # The key should be a const node, then we can adjust the scalar values.
                from hgraph._wiring._stub_wiring_node import KeyStubEvalFn

                cast(KeyStubEvalFn, node.eval_fn).key = key
            else:
                if arg in self.multiplexed_args:  # Is this a multiplexed input?
                    # This should create a phantom input if one does not exist.
                    tsd_arg : TSD[str, TIME_SERIES_TYPE] = self.input[arg]
                    ts = tsd_arg.get_or_create(key)
                    node.input = node.input.copy_with(__init_args__=dict(_parent_or_node=node), ts=ts)
                    # Now we need to re-parent the pruned ts input.
                    ts.re_parent(node.input)
                else:
                    from hgraph import TimeSeriesReferenceInput

                    ts: TimeSeriesReferenceInput = self.input[arg]
                    inner_input: TimeSeriesReferenceInput = node.input["ts"]
                    inner_input.clone_binding(ts)

        if self.output_node_id:
            node: Node = graph.nodes[self.output_node_id]
            # Replace the nodes output with the map node's output for the key
            node.output = cast(TSD_OUT[str, TIME_SERIES_TYPE], self.tsd_output()).get_or_create(key)
