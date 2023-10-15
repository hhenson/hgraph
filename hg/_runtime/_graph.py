import typing
from abc import abstractmethod

from hg._runtime._execution_context import ExecutionContext
from hg._runtime._lifecycle import ComponentLifeCycle

if typing.TYPE_CHECKING:
    from hg._runtime._node import Node


__all__ = ("Graph",)


class Graph(ComponentLifeCycle, typing.Protocol):
    """ The runtime graph """

    @property
    @abstractmethod
    def graph_id(self) -> tuple[int, ...]:
        """ The graph id """

    @property
    @abstractmethod
    def nodes(self) -> tuple["Node", ...]:
        """ The nodes of the graph """

    @property
    @abstractmethod
    def context(self) -> ExecutionContext:
        """The execution context"""

    @property
    @abstractmethod
    def push_source_nodes_end(self) -> int:
        """ The index of the first compute node """

    @abstractmethod
    def schedule_node(self, node_id, time):
        """Schedule the node with the given node_id to evaluate at the given time"""
