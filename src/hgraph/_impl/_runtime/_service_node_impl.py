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
                self._active_graph.evaluation_engine = NestedEvaluationEngine(self.graph.evaluation_engine,
                                                                              NestedEngineEvaluationClock(
                                                                                  self.graph.engine_evaluation_clock,
                                                                                  self))
                self._active_graph.initialise()
                self._wire_graph(self._active_graph)
                self._active_graph.start()

        if self._active_graph:
            self._active_graph.evaluate_graph()