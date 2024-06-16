import typing
from abc import abstractmethod
from dataclasses import dataclass
from typing import Iterable

from hgraph._builder._builder import Builder

if typing.TYPE_CHECKING:
    from hgraph._runtime._graph import Graph
    from hgraph._runtime._node import Node
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._types._scalar_types import SCALAR

__all__ = ("Edge", "GraphBuilder", "GraphBuilderFactory")


@dataclass(frozen=True)
class Edge:
    src_node: int
    output_path: tuple["SCALAR", ...]
    dst_node: int
    input_path: tuple["SCALAR", ...]


@dataclass(frozen=True)
class GraphBuilder(Builder["Graph"]):
    node_builders: tuple["NodeBuilder", ...]
    edges: tuple[Edge, ...]

    @abstractmethod
    def make_instance(self, graph_id: tuple[int, ...], parent_node: "Node" = None, label: str = None) -> "Graph":
        """
        Construct an instance of a graph. The id provided is the id for the graph instance to be constructed.
        """

    @abstractmethod
    def make_and_connect_nodes(self, graph_id: tuple[int, ...], first_node_ndx: int) -> Iterable["Node"]:
        """
        Make the nodes described in the node builders and connect the edges as described in the edges.
        Return the iterable of newly constructed and wired nodes.
        This can be used to feed into a new graph instance or to extend (or re-initialise) an existing graph.
        """

    @abstractmethod
    def release_instance(self, item: "Graph"):
        """
        Release resources constructed during the build process, plus the graph.
        """


class GraphBuilderFactory:

    _graph_builder_class: typing.Optional[typing.Type[GraphBuilder]] = None

    @staticmethod
    def default():
        from hgraph._impl._builder._graph_builder import PythonGraphBuilder

        return PythonGraphBuilder

    @staticmethod
    def is_declared() -> bool:
        return GraphBuilderFactory._graph_builder_class is not None

    @staticmethod
    def declared() -> typing.Type[GraphBuilder]:
        if GraphBuilderFactory._graph_builder_class is None:
            raise RuntimeError("No graph builder type has been declared")
        return GraphBuilderFactory._graph_builder_class

    @staticmethod
    def declare(cls: typing.Type[GraphBuilder]):
        if GraphBuilderFactory._graph_builder_class is not None:
            raise RuntimeError("A graph builder type has already been declared")
        GraphBuilderFactory._graph_builder_class = cls

    @staticmethod
    def un_declare():
        GraphBuilderFactory._graph_builder_class = None

    @staticmethod
    def make(node_builders: tuple["NodeBuilder", ...], edges: tuple[Edge, ...]) -> GraphBuilder:
        """
        Make a graph builder instance. If no graph builder class is declared, the default builder will be used.
        :param node_builders: The node builders to use
        :param edges: The edges to use for binding the node outputs to inputs.
        :return: The GraphBuilder instance.
        """
        if GraphBuilderFactory.is_declared():
            return GraphBuilderFactory.declared()(node_builders=node_builders, edges=edges)
        else:
            return GraphBuilderFactory.default()(node_builders=node_builders, edges=edges)
