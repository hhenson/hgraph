from datetime import datetime
from typing import Mapping, Any

from hgraph._types._tsb_meta_data import HgTSBTypeMetaData
from hgraph._builder._graph_builder import GraphBuilder
from hgraph._impl._runtime._nested_evaluation_engine import PythonNestedNodeImpl, NestedEvaluationEngine, \
    NestedEngineEvaluationClock
from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._graph import Graph
from hgraph._runtime._node import NodeSignature, Node


class PythonTryExceptNodeImpl(PythonNestedNodeImpl):

    def __init__(self,
                 node_ndx: int,
                 owning_graph_id: tuple[int, ...],
                 signature: NodeSignature,
                 scalars: Mapping[str, Any],
                 nested_graph_builder: GraphBuilder = None,
                 input_node_ids: Mapping[str, int] = None,
                 output_node_id: int = None
                 ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars)
        self.nested_graph_builder: GraphBuilder = nested_graph_builder
        self.input_node_ids: Mapping[str, int] = input_node_ids
        self.output_node_id: int = output_node_id
        self._active_graph: Graph | None = None
        self._last_evaluation_time: datetime | None = None

    def _wire_graph(self):
        """Connect inputs and outputs to the nodes inputs and outputs"""
        for arg, node_ndx in self.input_node_ids.items():
            node: NodeImpl = self._active_graph.nodes[node_ndx]
            node.notify()
            ts = self.input[arg]
            node.input = node.input.copy_with(__init_args__=dict(owning_node=node), ts=ts)
            # Now we need to re-parent the pruned ts input.
            ts.re_parent(node.input)

        if self.output_node_id:
            node: Node = self._active_graph.nodes[self.output_node_id]
            # Replace the nodes output with the map node's output
            node.output = self.output.out

    def initialise(self):
        self._active_graph = self.nested_graph_builder.make_instance(self.node_id, self)
        self._active_graph.evaluation_engine = NestedEvaluationEngine(
            self.graph.evaluation_engine,
            NestedEngineEvaluationClock(
                self.graph.engine_evaluation_clock,
                self))
        self._active_graph.initialise()
        self._wire_graph()

    def start(self):
        self._active_graph.start()

    def stop(self):
        self._active_graph.stop()

    def dispose(self):
        self._active_graph.dispose()
        self._active_graph = None

    def eval(self):
        try:
            self.mark_evaluated()
            self._active_graph.evaluate_graph()
        except Exception as e:
            from hgraph._types._error_type import NodeError
            err = NodeError.capture_error(e, self)
            if type(self.signature.time_series_output) is HgTSBTypeMetaData:
                self.output.exception.value = err
            else:
                self.output.value = err

