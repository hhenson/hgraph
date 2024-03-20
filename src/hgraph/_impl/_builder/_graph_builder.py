from dataclasses import dataclass
from typing import Iterable

from hgraph._builder._graph_builder import GraphBuilder
from hgraph._impl._runtime._graph import PythonGraph
from hgraph._runtime._graph import Graph
from hgraph._runtime._node import Node

__all__ = ("PythonGraphBuilder",)


@dataclass(frozen=True)
class PythonGraphBuilder(GraphBuilder):
    """
    Builds a graph (set of nodes with edges)
    """

    @staticmethod
    def _extract_output(node: Node, path: [int]):
        if not path:
            raise RuntimeError("No path to find an output for")
        output = node.output
        for item in path:
            output = output[item]
        return output

    @staticmethod
    def _extract_input(node: Node, path: [int]):
        if not path:
            raise RuntimeError("No path to find an input for")
        input_ = node.input
        for item in path:
            input_ = input_[item]
        return input_

    def make_instance(self, graph_id: tuple[int, ...], parent_node: Node = None, label: str = None) -> Graph:
        nodes = self.make_and_connect_nodes(graph_id, 0)
        # The nodes are initialised within the context of the graph
        return PythonGraph(graph_id=graph_id, nodes=nodes, parent_node=parent_node, label=label)

    def make_and_connect_nodes(self, graph_id: tuple[int, ...], first_node_ndx: int) -> Iterable[Node]:
        nodes = [nb.make_instance(graph_id, ndx + first_node_ndx) for ndx, nb in enumerate(self.node_builders)]
        for edge in self.edges:
            src_node: Node = nodes[edge.src_node]
            dst_node: Node = nodes[edge.dst_node]
            # TODO: Should we normalise outputs to always be an UnnamedBundleOutput? For now if the path is tuple() assume
            #  the output is the node output [This would be useful dealing with special outputs like error.
            if edge.output_path == (-1,):
                # This is an error handler
                output = src_node.error_output
            else:
                output = src_node.output if edge.output_path == tuple() else self._extract_output(src_node,
                                                                                              edge.output_path)
            input_ = self._extract_input(dst_node, edge.input_path)
            input_.bind_output(output)
        return nodes

    def release_instance(self, item: Graph):
        for node, node_builder in zip(item.nodes, self.node_builders):
            node_builder.release_instance(node)
        item.dispose()

    def __hash__(self):
        return hash(id(self))


    
