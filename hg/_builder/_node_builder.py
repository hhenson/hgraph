from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional, Any

from _pytest.nodes import Node
from frozendict import frozendict

from hg._builder._builder import Builder
from hg._builder._input_builder import InputBuilder
from hg._builder._output_builder import OutputBuilder
from hg._runtime._node import NodeSignature

__all__ = ("NodeBuilder",)


@dataclass(frozen=True)
class NodeBuilder(Builder[Node]):
    node_ndx: int
    signature: NodeSignature
    scalars: frozendict[str, Any]
    input_builder: Optional[InputBuilder] = None
    output_builder: Optional[OutputBuilder] = None

    @abstractmethod
    def make_instance(self, owning_graph_id: tuple[int, ...]) -> Node:
        """
        Construct an instance of a node. The id provided is the id for the node instance to be constructed.
        """

    @abstractmethod
    def release_instance(self, item: Node):
        """
        Release any resources constructed during the build process, plus the node.
        """
