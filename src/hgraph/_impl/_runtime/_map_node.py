from datetime import datetime
from typing import Mapping, Any, Callable, cast

from hgraph import MAX_DT
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
        if next_time <= self._nested_node.last_evaluation_time:
            return
        tm = self._nested_node._scheduled_keys.get(self._key)
        if tm is None or tm > next_time:
            self._nested_node._scheduled_keys[self._key] = next_time
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
        self._count = 0

    def eval(self):
        self.mark_evaluated()
        # 1. All inputs should be reference, and we should only be active on the KEYS_ARG input
        keys: TSS[K] = self._kwargs[KEYS_ARG]
        if keys.modified:
            for k in keys.added():
                self._create_new_graph(k)
            for k in keys.removed():
                self._remove_graph(k)
                self._scheduled_keys.pop(k, None)
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

    def _create_new_graph(self, key: K):
        """Create new graph instance and wire it into the node"""
        graph: Graph = self.nested_graph_builder.make_instance(self.node_id + (self._count,), self, str(key))
        self._count += 1
        self._active_graphs[key] = graph
        graph.evaluation_engine = NestedEvaluationEngine(
            self.graph.evaluation_engine,
            MapNestedEngineEvaluationClock(self.graph.evaluation_engine.engine_evaluation_clock, key, self),
        )
        graph.initialise()
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
        graph.stop()
        graph.dispose()

    def _evaluate_graph(self, key: K):
        """Evaluate the graph for this key"""
        graph: Graph = self._active_graphs[key]
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

        return graph.evaluation_clock.next_scheduled_evaluation_time

    def _un_wire_graph(self, key: K, graph: Graph):
        if self.output_node_id:
            # Replace the nodes output with the map node's output for the key
            del self.output[key]

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
                    ts = cast(TSD[str, TIME_SERIES_TYPE], self.input[arg]).get_or_create(key)
                    node.input = node.input.copy_with(__init_args__=dict(owning_node=node), ts=ts)
                    # Now we need to re-parent the pruned ts input.
                    ts.re_parent(node.input)
                else:
                    from hgraph import TimeSeriesReferenceInput

                    ts: TimeSeriesReferenceInput = self.input[arg]
                    if ts.output:
                        node.input["ts"].bind_output(ts.output)
                    else:
                        ts.value.bind_input(node.input["ts"])

        if self.output_node_id:
            node: Node = graph.nodes[self.output_node_id]
            # Replace the nodes output with the map node's output for the key
            node.output = cast(TSD_OUT[str, TIME_SERIES_TYPE], self.output).get_or_create(key)
