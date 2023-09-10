from dataclasses import dataclass

from hg._impl._builder._builder import Builder, ITEM
from hg._impl._builder._node_builder import NodeBuilder
from hg._impl._runtime._graph import GraphImpl
from hg._runtime import Node


@dataclass
class Edge:
    src_node: int
    output_path: [int]
    dst_node: int
    input_path: [int]


class GraphBuilder(Builder[GraphImpl]):
    """
    Builds a graph (set of nodes with edges)
    """

    def __init__(self, node_builders: [NodeBuilder], edges: []):
        self.node_builders: [NodeBuilder] = node_builders
        self.edges: [Edge] = edges

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

    def make_instance(self) -> GraphImpl:
        nodes = [nb.make_instance() for nb in self.node_builders]
        for edge in self.edges:
            src_node = nodes[edge.src_node]
            dst_node = nodes[edge.dst_node]
            output = self._extract_output(src_node, edge.output_path)
            input_ = self._extract_input(dst_node, edge.input_path)
            input_.output = output
        for node in nodes:  # TODO: I think we want to initialise the nodes once wiring is complete but not sure, need to think about this
            node.initialise()
        return GraphImpl(nodes=tuple(nodes))

    def release_instance(self, item: GraphImpl):
        for node, node_builder in zip(item.nodes, self.node_builders):
            node_builder.release_instance(node)
        for node in item.nodes:  # TODO: This is not clean if the dispose should be called in the node builder or here
            node.dispose()
        item.dispose()



    
