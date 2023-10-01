import functools
from abc import abstractmethod
from dataclasses import dataclass

from hg._runtime._execution_context import ExecutionContext
from hg._runtime._lifecycle import ComponentLifeCycle
from hg._runtime._node import Node, NodeTypeEnum


@dataclass
class Graph(ComponentLifeCycle):
    """ The runtime graph """

    @property
    @abstractmethod
    def graph_id(self) -> tuple[int, ...]:
        """ The graph id """

    @property
    @abstractmethod
    def nodes(self) -> tuple[Node, ...]:
        """ The nodes of the graph """

    @property
    @abstractmethod
    def context(self) -> ExecutionContext:
        """The execution context"""

    @property
    @abstractmethod
    def push_source_nodes_end(self) -> int:
        """ The index of the first compute node """
