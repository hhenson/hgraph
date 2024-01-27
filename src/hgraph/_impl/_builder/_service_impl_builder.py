from dataclasses import dataclass
from typing import TYPE_CHECKING

from hgraph import GraphBuilder
from hgraph._impl._builder._node_builder import PythonBaseNodeBuilder
from hgraph._impl._runtime._service_node_impl import PythonServiceNodeImpl
from hgraph._impl._runtime._switch_node import PythonSwitchNodeImpl

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class PythonServiceImplNodeBuilder(PythonBaseNodeBuilder):
    nested_graph: GraphBuilder = None

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> PythonSwitchNodeImpl:
        node = PythonServiceNodeImpl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
            nested_graph_builder=self.nested_graph,
        )

        return self._build_inputs_and_outputs(node)

    def release_instance(self, item: PythonSwitchNodeImpl):
        """Nothing to be done here"""