from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from hgraph._impl._builder._node_builder import PythonBaseNodeBuilder
from hgraph._impl._runtime._reduce_node import PythonReduceNodeImpl

if TYPE_CHECKING:
    from hgraph._builder._graph_builder import GraphBuilder


__all__ = ("PythonReduceNodeBuilder",)


@dataclass(frozen=True)
class PythonReduceNodeBuilder(PythonBaseNodeBuilder):
    nested_graph: Optional["GraphBuilder"] = None  # This is the generator function
    input_node_ids: tuple[int, int] | None = None  # The nodes representing the stub inputs in the nested graph.
    output_node_id: int | None = None  # The node representing the stub output in the nested graph.

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> PythonReduceNodeImpl:
        node = PythonReduceNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            nested_graph_builder=self.nested_graph,
            input_node_ids=self.input_node_ids,
            output_node_id=self.output_node_id,
        )

        return self._build_inputs_and_outputs(node)

    def release_instance(self, item: PythonReduceNodeImpl):
        """Nothing to do"""
