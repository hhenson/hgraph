from typing import Mapping, Any, Callable

from hgraph._builder._graph_builder import GraphBuilder
from hgraph._impl._runtime._nested_evaluation_engine import PythonNestedNodeImpl, NestedEvaluationEngine, \
    NestedEngineEvaluationClock
from hgraph._runtime._graph import Graph
from hgraph._runtime._node import NodeSignature
from hgraph._types._scalar_types import SCALAR
from hgraph._types._ts_type import TS


class PythonServiceNodeImpl(PythonNestedNodeImpl):

    def __init__(self,
                 node_ndx: int,
                 owning_graph_id: tuple[int, ...],
                 signature: NodeSignature,
                 scalars: Mapping[str, Any],
                 eval_fn: Callable = None,
                 start_fn: Callable = None,
                 stop_fn: Callable = None,
                 nested_graph_builder: GraphBuilder = None,
                 ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn)
        self.nested_graph_builder: GraphBuilder = nested_graph_builder
        self._active_graph: Graph | None = None

    def eval(self):
        self._active_graph.evaluate_graph()

    def do_start(self):
        self._active_graph = self.nested_graph_builder.make_instance(self.graph.graph_id, self)
        self._active_graph.start()

    def do_stop(self):
        self._active_graph.stop()
        self._active_graph = None
