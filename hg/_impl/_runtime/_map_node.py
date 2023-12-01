from dataclasses import dataclass
from typing import Mapping

from hg._impl._runtime._node import NodeImpl
from hg._builder._graph_builder import GraphBuilder


__all__ = ("PythonMapNodeImpl",)


@dataclass
class PythonMapNodeImpl(NodeImpl):

    nested_graph_builder: GraphBuilder = None  # This builds the instance graph of the mapped function.
    input_node_ids: Mapping[str, int] = None  # The nodes representing the stub inputs in the nested graph.
    output_node_id: int = None  # The node representing the stub output in the nested graph.

    def eval(self):
        # 1. All inputs should be reference, and we should only be active on the key input

        # 2. If we are called it is because the key set has changed.
        #    and / or one of the nested graphs has been scheduled for evaluation.

        # 3. New keys get added, old keys get removed
        ...


