import functools
from dataclasses import dataclass

from hg._runtime._execution_context import ExecutionContext
from hg._runtime._lifecycle import start_guard, stop_guard
from hg._runtime._graph import Graph
from hg._runtime._node import NodeTypeEnum, Node


__all__ = ("GraphImpl",)


@dataclass
class GraphImpl(Graph):
    """
    Provide a reference implementation of the Graph.
    """
    graph_id: tuple[int, ...]
    nodes: tuple[Node, ...]  # The nodes of the graph.
    context: ExecutionContext

    @functools.cached_property
    def push_source_nodes_end(self) -> int:
        """ The index of the first compute node """
        for i in range(len(self.nodes)):
            if self.nodes[i].signature.node_type != NodeTypeEnum.PUSH_SOURCE_NODE:
                return i
        return len(self.nodes)  # In the very unlikely event that there are only push source nodes.

    is_started: bool = False

    def initialise(self):
        ...

    @start_guard
    def start(self):
        for node in self.nodes:
            node.start()

    @stop_guard
    def stop(self):
        for node in self.nodes:
            node.stop()

    def dispose(self):
        ...

