from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping, Optional

from hgraph._impl._builder._node_builder import PythonBaseNodeBuilder
from hgraph._impl._runtime._mesh_node import PythonMeshNodeImpl

if TYPE_CHECKING:
    from hgraph._builder._graph_builder import GraphBuilder
    from hgraph._types._scalar_types import HgScalarTypeMetaData


@dataclass(frozen=True)
class PythonMeshNodeBuilder(PythonBaseNodeBuilder):
    nested_graph: Optional["GraphBuilder"] = None  # This is the generator function
    input_node_ids: Mapping[str, int] | None = None  # The nodes representing the stub inputs in the nested graph.
    output_node_id: int | None = None  # The node representing the stub output in the nested graph.
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.
    key_arg: str | None = None  # The key arg to use, None if no key exists
    key_tp: Optional["HgScalarTypeMetaData"] = None
    context_path: str | None = None  # The context path to use

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> PythonMeshNodeImpl:
        node = PythonMeshNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            nested_graph_builder=self.nested_graph,
            input_node_ids=self.input_node_ids,
            output_node_id=self.output_node_id,
            multiplexed_args=self.multiplexed_args,
            key_arg=self.key_arg,
            context_path=self.context_path,
        )
        return self._build_inputs_and_outputs(node)

    def release_instance(self, item: PythonMeshNodeImpl):
        """Nothing to do"""
        super().release_instance(item)
