import functools
from dataclasses import dataclass

from hg._runtime._lifecycle import ComponentLifeCycle
from hg._runtime._node import Node, NodeTypeEnum


@dataclass
class Graph(ComponentLifeCycle):
    """ The runtime graph """
    graph_id: tuple[int, ...]
    nodes: tuple[Node, ...]  # The nodes of the graph.

    @functools.cached_property
    def push_source_nodes_end(self) -> int:
        """ The index of the first compute node """
        for i in range(len(self.nodes)):
            if self.nodes[i].signature.node_type != NodeTypeEnum.PUSH_SOURCE_NODE:
                return i
        return len(self.nodes) # In the very unlikely event that there are only push source nodes.
