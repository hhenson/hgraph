from dataclasses import dataclass
from typing import Mapping

from hg import TSS, SCALAR, Graph, EvaluationClock
from hg._runtime._map import KEYS_ARG
from hg._impl._runtime._node import NodeImpl
from hg._builder._graph_builder import GraphBuilder


__all__ = ("PythonMapNodeImpl",)


@dataclass
class CollectionNestedGraphEngine:
    """
    Manage a collection of nested graph engines. All these engines live at the same level as the node wrapping them.
    The nested engine takes a reference to the outer engine to pull observers, etc. from.
    It creates it's own execution context that is supplied to the graphs it manages.
    It is from the actions on the execution context that it determines when to schedule a graph.
    """
    _execution_context: EvaluationClock



@dataclass
class PythonMapNodeImpl(NodeImpl):

    nested_graph_builder: GraphBuilder = None  # This builds the instance graph of the mapped function.
    input_node_ids: Mapping[str, int] = None  # The nodes representing the stub inputs in the nested graph.
    output_node_id: int = None  # The node representing the stub output in the nested graph.
    _scheduled_keys: set[SCALAR] = None
    _active_graphs: dict[SCALAR, Graph] = None  # The currently active graphs

    def eval(self):
        # 1. All inputs should be reference, and we should only be active on the KEYS_ARG input
        keys: TSS[SCALAR] = self._kwargs[KEYS_ARG]
        if keys.modified:
            for k in keys.added():
                self._create_new_graph(k)
            for k in keys.removed():
                self._remove_graph(k)
        # 2. or one of the nested graphs has been scheduled for evaluation.
        for k in self._scheduled_keys:
            self._evaluate_graph(k)

    def _create_new_graph(self, key: SCALAR):
        """Create new graph instance and wire it into the node"""

    def _remove_graph(self, key: SCALAR):
        """Un-wire graph and schedule for removal"""

    def _evaluate_graph(self, key: SCALAR):
        """Evaluate the graph for this key"""


