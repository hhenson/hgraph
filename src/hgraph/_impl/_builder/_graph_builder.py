from dataclasses import dataclass

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
        input = node.input
        for item in path:
            input = input[item]
        return input

    def make_instance(self, graph_id: tuple[int, ...], parent_node: Node = None) -> Graph:
        nodes = [nb.make_instance(graph_id) for nb in self.node_builders]
        for edge in self.edges:
            src_node: Node = nodes[edge.src_node]
            dst_node: Node = nodes[edge.dst_node]
            # TODO: Should we normalise outputs to always be an UnnamedBundleOutput? For now if the path is tuple() assume
            #  the output is the node output
            output = src_node.output if edge.output_path == tuple() else self._extract_output(src_node, edge.output_path)
            input_ = self._extract_input(dst_node, edge.input_path)
            input_.bind_output(output)
            # The nodes are initialised within the context of the graph
        return PythonGraph(graph_id=graph_id, nodes=tuple(nodes), parent_node=parent_node)

    def release_instance(self, item: Graph):
        for node, node_builder in zip(item.nodes, self.node_builders):
            node_builder.release_instance(node)
        item.dispose()



    
