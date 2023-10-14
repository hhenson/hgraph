import typing
from abc import abstractmethod
from dataclasses import dataclass

from hg._builder._builder import Builder

if typing.TYPE_CHECKING:
    from hg._builder._node_builder import NodeBuilder
    from hg._runtime._graph import Graph


__all__ = ("Edge", "GraphBuilder")


@dataclass(frozen=True)
class Edge:
    src_node: int
    output_path: tuple[int, ...]
    dst_node: int
    input_path: tuple[int, ...]


@dataclass(frozen=True)
class GraphBuilder(Builder["Graph"]):
    node_builders: tuple["NodeBuilder", ...]
    edges: tuple[Edge, ...]

    @abstractmethod
    def make_instance(self, graph_id: tuple[int, ...]) -> "Graph":
        """
        Construct an instance of a graph. The id provided is the id for the graph instance to be constructed.
        """

    @abstractmethod
    def release_instance(self, item: "Graph"):
        """
        Release resources constructed during the build process, plus the graph.
        """


class GraphBuilderFactory:

    _instance: typing.Optional[typing.Type[GraphBuilder]] = None

    @staticmethod
    def declare_default_factory():
        from hg._impl._builder._graph_builder import PythonGraphBuilder
        GraphBuilderFactory.declare(PythonGraphBuilder)

    @staticmethod
    def has_instance() -> bool:
        return GraphBuilderFactory._instance is not None

    @staticmethod
    def instance() -> typing.Type[GraphBuilder]:
        if GraphBuilderFactory._instance is None:
            raise RuntimeError("No graph builder type has been declared")
        return GraphBuilderFactory._instance

    @staticmethod
    def declare(factory: typing.Type[GraphBuilder]):
        if GraphBuilderFactory._instance is not None:
            raise RuntimeError("A graph builder type has already been declared")
        GraphBuilderFactory._instance = factory

    @staticmethod
    def undeclare():
        GraphBuilderFactory._instance = None
