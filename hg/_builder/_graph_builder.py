from abc import abstractmethod
from dataclasses import dataclass

from hg._builder._builder import Builder
from hg._builder._node_builder import NodeBuilder
from hg._runtime._graph import Graph


@dataclass(frozen=True)
class Edge:
    src_node: int
    output_path: tuple[int, ...]
    dst_node: int
    input_path: tuple[int, ...]


@dataclass(frozen=True)
class GraphBuilder(Builder[Graph]):
    node_builders: tuple[NodeBuilder, ...]
    edges: tuple[Edge, ...]

    @abstractmethod
    def make_instance(self, graph_id: tuple[int, ...]) -> Graph:
        """
        Construct an instance of a graph. The id provided is the id for the graph instance to be constructed.
        """

    @abstractmethod
    def release_instance(self, item: Graph):
        """
        Release an resources constructed during the build process, plus the graph.
        """