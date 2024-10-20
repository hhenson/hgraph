from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional, Any, Mapping, TypeVar

from hgraph._builder._builder import Builder
from hgraph._builder._input_builder import InputBuilder
from hgraph._builder._output_builder import OutputBuilder
from hgraph._runtime._node import NodeSignature, Node

__all__ = ("NodeBuilder",)


NODE = TypeVar("NODE", bound=Node)


@dataclass(frozen=True)
class NodeBuilder(Builder[NODE]):
    signature: NodeSignature
    scalars: Mapping[str, Any]
    input_builder: Optional[InputBuilder] = None
    output_builder: Optional[OutputBuilder] = None
    error_builder: Optional[OutputBuilder] = None
    recordable_state_builder: Optional[OutputBuilder] = None

    @abstractmethod
    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx) -> NODE:
        """
        Construct an instance of a node. The id provided is the id for the node instance to be constructed.
        """

    @abstractmethod
    def release_instance(self, item: NODE):
        """
        Release any resources constructed during the build process, plus the node.
        """


# TODO: Need to ensure that each type of NodeBuilder is described in the abstract and a factory is provided
#       to provide instances of the builder to allow us to support multiple engines.
