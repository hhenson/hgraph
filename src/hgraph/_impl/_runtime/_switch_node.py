from datetime import datetime
from typing import Mapping, Any, Callable, Optional, cast

from hgraph._builder._graph_builder import GraphBuilder
from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._constants import MIN_DT
from hgraph._runtime._evaluation_clock import EngineEvaluationClockDelegate, EngineEvaluationClock
from hgraph._runtime._evaluation_engine import EvaluationEngineDelegate, EvaluationEngine
from hgraph._runtime._graph import Graph
from hgraph._runtime._node import NodeSignature, Node
from hgraph._types._scalar_types import SCALAR
from hgraph._types._ts_type import TS


class NestedEngineEvaluationClock(EngineEvaluationClockDelegate):

    def __init__(self, engine_evaluation_clock: EngineEvaluationClock, switch_node: "PythonSwitchNodeImpl"):
        super().__init__(engine_evaluation_clock)
        self._switch_node: "PythonSwitchNodeImpl" = switch_node

    def update_next_scheduled_evaluation_time(self, next_time: datetime):
        # NOTE: We only need to schedule if the next time is after the current evaluation time (or if the map_node has
        # not yet been evaluated).
        if next_time < self.evaluation_time or self._switch_node._last_evaluation_time == self.evaluation_time:
            # No point doing anything as we are already scheduled to run.
            return
        self._switch_node.graph.schedule_node(self._switch_node.node_ndx, next_time)


class NestedEvaluationEngine(EvaluationEngineDelegate):
    """

    Requesting a stop of the engine will stop the outer engine.
    Stopping an inner graph is a source of bugs and confusion. Instead, the user should create a mechanism to
    remove the key used to create the graph.
    """

    def __init__(self, engine: EvaluationEngine, key: SCALAR, switch_node: "PythonSwitchNodeImpl"):
        super().__init__(engine)
        self._engine_evaluation_clock = NestedEngineEvaluationClock(engine.engine_evaluation_clock, switch_node)

    @property
    def evaluation_clock(self) -> "EvaluationClock":
        return self._engine_evaluation_clock

    @property
    def engine_evaluation_clock(self) -> "EngineEvaluationClock":
        return self._engine_evaluation_clock


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
            if self.reload_on_ticked or key.value != self._active_key:
                if self._active_graph:
                    self._active_graph.stop()
                    self._active_graph.dispose()
                self._active_key = key.value
                self._active_graph = self.nested_graph_builders[self._active_key].make_instance(
                    self.node_id + (self._count,))
                self._count += 1
                self._active_graph.evaluation_engine = NestedEvaluationEngine(self.graph.evaluation_engine, self._active_key, self)
                self._active_graph.initialise()
                self._wire_graph(self._active_graph)
                self._active_graph.start()

        if self._active_graph:
            self._active_graph.evaluate_graph()

    def _wire_graph(self, graph: Graph):
        """Connect inputs and outputs to the nodes inputs and outputs"""
        for arg, node_ndx in self.input_node_ids[self._active_key].items():
            node: NodeImpl = graph.nodes[node_ndx]
            node.notify()
            if arg == 'key':
                # The key should be a const node, then we can adjust the scalar values.
                from hgraph._wiring._stub_wiring_node import KeyStubEvalFn
                cast(KeyStubEvalFn, node.eval_fn).key = self._active_key
            else:
                ts = self.input[arg]
                node.input = node.input.copy_with(__init_args__=dict(owning_node=node), ts=ts)
                # Now we need to re-parent the pruned ts input.
                ts.re_parent(node.input)

        if self.output_node_ids:
            node: Node = graph.nodes[self.output_node_ids[self._active_key]]
            # Replace the nodes output with the map node's output for the key
            node.output = self.output
