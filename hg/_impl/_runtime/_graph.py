import functools
from dataclasses import dataclass, field
from datetime import datetime

from hg._runtime._constants import MIN_DT
from hg._runtime._execution_context import ExecutionContext
from hg._runtime._graph import Graph
from hg._runtime._lifecycle import start_guard, stop_guard
from hg._runtime._node import NodeTypeEnum, Node

__all__ = ("GraphImpl",)


@dataclass
class GraphImpl:  # (Graph):
    """
    Provide a reference implementation of the Graph.
    """

    graph_id: tuple[int, ...]
    nodes: tuple[Node, ...]  # The nodes of the graph.
    context: ExecutionContext = None
    is_started: bool = False
    schedule: list[datetime, ...] = field(default_factory=list)

    @functools.cached_property
    def push_source_nodes_end(self) -> int:
        """ The index of the first compute node """
        for i in range(len(self.nodes)):
            if self.nodes[i].signature.node_type != NodeTypeEnum.PUSH_SOURCE_NODE:
                return i
        return len(self.nodes)  # In the very unlikely event that there are only push source nodes.

    def initialise(self):
        self.schedule = [MIN_DT] * len(self.nodes)
        for node in self.nodes:
            node.graph = self

    def schedule_node(self, node_id, time):
        self.schedule[node_id] = time

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

