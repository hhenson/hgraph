from datetime import datetime
from typing import Mapping, Any, Callable, cast

from hgraph._builder._graph_builder import GraphBuilder
from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._constants import MIN_DT
from hgraph._runtime._evaluation_clock import EngineEvaluationClock
from hgraph._runtime._evaluation_clock import EvaluationClock, EngineEvaluationClockDelegate
from hgraph._runtime._evaluation_engine import EvaluationEngine, EvaluationEngineDelegate
from hgraph._runtime._graph import Graph
from hgraph._runtime._map import KEYS_ARG
from hgraph._runtime._node import Node, NodeSignature
from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._tsd_type import TSD, TSD_OUT
from hgraph._types._tss_type import TSS

__all__ = ("PythonMapNodeImpl",)


class NestedEngineEvaluationClock(EngineEvaluationClockDelegate):

    def __init__(self, engine_evaluation_clock: EngineEvaluationClock, key: SCALAR, map_node: "PythonMapNodeImpl"):
        super().__init__(engine_evaluation_clock)
        self._key = key
        self._map_node = map_node

    def update_next_scheduled_evaluation_time(self, next_time: datetime):
        # NOTE: We only need to schedule if the next time is after the current evaluation time (or if the map_node has
        # not yet been evaluated).
        if next_time < self.evaluation_time or self._map_node._last_evaluation_time == self.evaluation_time:
            # No point doing anything as we are already scheduled to run.
            return
        tm = self._map_node._scheduled_keys.get(self._key)
        if tm is None or tm > next_time:
            self._map_node._scheduled_keys[self._key] = next_time
            self._map_node.graph.schedule_node(self._map_node.node_ndx, next_time)


class NestedEvaluationEngine(EvaluationEngineDelegate):
    """

    Requesting a stop of the engine will stop the outer engine.
    Stopping an inner graph is a source of bugs and confusion. Instead, the user should create a mechanism to
    remove the key used to create the graph.
    """

    def __init__(self, engine: EvaluationEngine, key: SCALAR, map_node: "PythonMapNodeImpl"):
        super().__init__(engine)
        self._engine_evaluation_clock = NestedEngineEvaluationClock(engine.engine_evaluation_clock, key, map_node)

    @property
    def evaluation_clock(self) -> "EvaluationClock":
        return self._engine_evaluation_clock

    @property
    def engine_evaluation_clock(self) -> "EngineEvaluationClock":
        return self._engine_evaluation_clock


class PythonMapNodeImpl(NodeImpl):

    def __init__(self,
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
                 multiplexed_args: frozenset[str] = None
                 ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn)
        self.nested_graph_builder: GraphBuilder = nested_graph_builder
        self.input_node_ids: Mapping[str, int] = input_node_ids
        self.output_node_id: int = output_node_id
        self.multiplexed_args: frozenset[str] = multiplexed_args
        self._scheduled_keys: dict[SCALAR, datetime] = {}
        self._active_graphs: dict[SCALAR, Graph] = {}
        self._count = 0
        self._last_evaluation_time = MIN_DT

    def eval(self):
        self._last_evaluation_time = self.graph.evaluation_clock.evaluation_time
        # 1. All inputs should be reference, and we should only be active on the KEYS_ARG input
        keys: TSS[SCALAR] = self._kwargs[KEYS_ARG]
        if keys.modified:
            for k in keys.added():
                self._create_new_graph(k)
            for k in keys.removed():
                self._remove_graph(k)
        # 2. or one of the nested graphs has been scheduled for evaluation.
        scheduled_keys = self._scheduled_keys
        self._scheduled_keys = {}
        for k, dt in scheduled_keys.items():
            if dt == self._last_evaluation_time:
                self._evaluate_graph(k)
            elif dt < self._last_evaluation_time:
                raise RuntimeError("Scheduled time is in the past")
            else:
                # Re-schedule for the next time.
                self._scheduled_keys[k] = dt
                self.graph.schedule_node(self.node_ndx, dt)

    def _create_new_graph(self, key: SCALAR):
        """Create new graph instance and wire it into the node"""
        graph: Graph = self.nested_graph_builder.make_instance(self.graph.graph_id + (self._count,), self)
        self._count += 1
        self._active_graphs[key] = graph
        graph.evaluation_engine = NestedEvaluationEngine(self.graph.evaluation_engine, key, self)
        graph.initialise()
        self._wire_graph(key, graph)
        graph.start()
        self._evaluate_graph(key)

    def _remove_graph(self, key: SCALAR):
        """Un-wire graph and schedule for removal"""
        graph: Graph = self._active_graphs.pop(key)
        graph.stop()
        graph.dispose()

    def _evaluate_graph(self, key: SCALAR):
        """Evaluate the graph for this key"""
        graph: Graph = self._active_graphs[key]
        graph.evaluate_graph()

    def _wire_graph(self, key: SCALAR, graph: Graph):
        """Connect inputs and outputs to the nodes inputs and outputs"""
        for arg, node_ndx in self.input_node_ids.items():
            node: NodeImpl = graph.nodes[node_ndx]
            node.notify()
            if arg == 'key':
                # The key should be a const node, then we can adjust the scalar values.
                from hgraph._wiring._stub_wiring_node import KeyStubEvalFn
                cast(KeyStubEvalFn, node.eval_fn).key = key
            else:
                if is_multiplexed := arg in self.multiplexed_args:  # Is this a multiplexed input?
                    # This should create a phantom input if one does not exist.
                    ts = cast(TSD[str, TIME_SERIES_TYPE], self.input[arg]).get_or_create(key)
                else:
                    ts = self.input[arg]
                node.input = node.input.copy_with(__init_args__=dict(owning_node=node), ts=ts)
                # Now we need to re-parent the pruned ts input.
                ts.re_parent(node.input)

        if self.output_node_id:
            node: Node = graph.nodes[self.output_node_id]
            # Replace the nodes output with the map node's output for the key
            node.output = cast(TSD_OUT[str, TIME_SERIES_TYPE], self.output).get_or_create(key)
