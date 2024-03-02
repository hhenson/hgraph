from abc import ABC, abstractmethod
from datetime import datetime

from hgraph._runtime._node import Node

__all__ = ("GraphRecorder",)


class GraphRecorder(ABC):

    @abstractmethod
    def record_node(self, node: Node) -> Node:
        ...

    @abstractmethod
    def replay_node(self, node: Node) -> Node:
        ...

    @abstractmethod
    def last_recorded_time(self) -> datetime:
        ...
