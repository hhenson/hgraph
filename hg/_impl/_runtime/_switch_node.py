from datetime import datetime
from typing import Mapping, Any, Callable, Optional

from hg._types._ts_type import TS
from hg._runtime._constants import MIN_DT
from hg._runtime._graph import Graph
from hg._types._scalar_types import SCALAR
from hg._builder._graph_builder import GraphBuilder
from hg._runtime._node import NodeSignature
from hg._impl._runtime._node import NodeImpl


class PythonSwitchNodeImpl(NodeImpl):

    def __init__(self,
                 node_ndx: int,
                 owning_graph_id: tuple[int, ...],
                 signature: NodeSignature,
                 scalars: Mapping[str, Any],
                 eval_fn: Callable = None,
                 start_fn: Callable = None,
                 stop_fn: Callable = None,
                 nested_graph_builders: Mapping[SCALAR, GraphBuilder] = None,
                 input_node_ids: Mapping[SCALAR, Mapping[str, int]] = None,
                 output_node_ids: Mapping[SCALAR, int] = None,
                 reload_on_ticked: bool = False
                 ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn)
        self.nested_graph_builders: Mapping[SCALAR, GraphBuilder] = nested_graph_builders
        self.input_node_ids: Mapping[SCALAR, Mapping[str, int]] = input_node_ids
        self.output_node_ids: Mapping[SCALAR, int] = output_node_ids
        self.reload_on_ticked: bool = reload_on_ticked
        self._active_graph: Graph | None = None
        self._active_key: Optional[SCALAR] = None
        self._count: int = 0
        self._last_evaluation_time: datetime = MIN_DT

    def eval(self):
        self._last_evaluation_time = self.graph.evaluation_clock.evaluation_time
        # 1. If the key has ticked we need to create a new graph.
        # (if the value has changed or if reload_on_ticked is True)
        key: TS[SCALAR] = self._kwargs['key']
        if key.modified:
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
                from hg._wiring._stub_wiring_node import KeyStubEvalFn
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
