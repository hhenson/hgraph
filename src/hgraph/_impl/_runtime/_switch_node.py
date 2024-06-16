from typing import Mapping, Any, Callable, Optional, cast

from hgraph._builder._graph_builder import GraphBuilder
from hgraph._impl._runtime._nested_evaluation_engine import (
    PythonNestedNodeImpl,
    NestedEvaluationEngine,
    NestedEngineEvaluationClock,
)
from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._graph import Graph
from hgraph._runtime._node import NodeSignature, Node
from hgraph._types._scalar_types import SCALAR
from hgraph._types._ts_type import TS
from hgraph._types._time_series_types import TimeSeriesOutput


class PythonSwitchNodeImpl(PythonNestedNodeImpl):

    def __init__(
        self,
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
        reload_on_ticked: bool = False,
    ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn)
        self.nested_graph_builders: Mapping[SCALAR, GraphBuilder] = nested_graph_builders
        self.input_node_ids: Mapping[SCALAR, Mapping[str, int]] = input_node_ids
        self.output_node_ids: Mapping[SCALAR, int] = output_node_ids
        self.reload_on_ticked: bool = reload_on_ticked
        self._active_graph: Graph | None = None
        self._active_key: Optional[SCALAR] = None
        self._count: int = 0
        self._old_output: TimeSeriesOutput | None = None

        from hgraph._wiring._switch import DEFAULT

        self._default_graph_builder: GraphBuilder = self.nested_graph_builders.get(DEFAULT)

    def eval(self):
        self.mark_evaluated()
        # 1. If the key has ticked we need to create a new graph.
        # (if the value has changed or if reload_on_ticked is True)
        key: TS[SCALAR] = self._kwargs["key"]
        if key.modified:
            if self.reload_on_ticked or key.value != self._active_key:
                if self._active_graph:
                    self._active_graph.stop()
                    self._unwire_graph(self._active_graph)
                    self._active_graph.dispose()
                self._active_key = key.value

                if builder := self.nested_graph_builders.get(self._active_key, self._default_graph_builder):
                    self._active_graph = builder.make_instance(
                        self.node_id + (self._count,), self, str(self._active_key)
                    )
                    self._count += 1
                    self._active_graph.evaluation_engine = NestedEvaluationEngine(
                        self.graph.evaluation_engine,
                        NestedEngineEvaluationClock(self.graph.engine_evaluation_clock, self),
                    )
                    self._active_graph.initialise()
                    self._wire_graph(self._active_graph)
                    self._active_graph.start()
                else:
                    raise ValueError(f"No graph defined for key {self._active_key}")

        if self._active_graph:
            self._active_graph.evaluate_graph()

    def _wire_graph(self, graph: Graph):
        """Connect inputs and outputs to the nodes inputs and outputs"""
        from hgraph._wiring._switch import DEFAULT

        graph_key = self._active_key if self._active_key in self.nested_graph_builders else DEFAULT

        for arg, node_ndx in self.input_node_ids[graph_key].items():
            node: NodeImpl = graph.nodes[node_ndx]
            node.notify()
            if arg == "key":
                # The key should be a const node, then we can adjust the scalar values.
                from hgraph._wiring._stub_wiring_node import KeyStubEvalFn

                cast(KeyStubEvalFn, node.eval_fn).key = self._active_key
            else:
                ts = self.input[arg]
                if ts.output:
                    node.input["ts"].bind_output(ts.output)
                else:
                    ts.value.bind_input(node.input["ts"])

        if self.output_node_ids:
            node: Node = graph.nodes[self.output_node_ids[graph_key]]
            # Replace the nodes output with the map node's output for the key
            self._old_output = node.output
            node.output = self.output

    def _unwire_graph(self, graph: Graph):
        if self._old_output is not None:
            from hgraph._wiring._switch import DEFAULT

            graph_key = self._active_key if self._active_key in self.nested_graph_builders else DEFAULT
            node: Node = graph.nodes[self.output_node_ids[graph_key]]
            node.output = self._old_output
            self._old_output = None

    def do_stop(self):
        if self._active_graph is not None:
            self._active_graph.stop()
        super().do_stop()

    def dispose(self):
        if self._active_graph is not None:
            self._active_graph.dispose()
        super().dispose()
