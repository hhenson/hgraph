from dataclasses import dataclass
from typing import TYPE_CHECKING

from hgraph._impl._builder._node_builder import PythonBaseNodeBuilder, PythonNodeBuilder

if TYPE_CHECKING:
    from hgraph._runtime._node import Node
    from hgraph._impl._runtime._node import BaseNodeImpl


__all__ = ("PythonNodeImplNodeBuilder",)


@dataclass(frozen=True)
class PythonNodeImplNodeBuilder(PythonBaseNodeBuilder):
    node_impl: type["BaseNodeImpl"] = None  # This is the generator function

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx: int) -> "Node":
        node = self.node_impl(
            node_ndx=node_ndx,
            owning_graph_id=owning_graph_id,
            signature=self.signature,
            scalars=self.scalars,
        )

        return self._build_inputs_and_outputs(node)

    def release_instance(self, item: "Node"):
        """Nothing to do"""