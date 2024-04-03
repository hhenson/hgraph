from abc import ABC, abstractmethod
from datetime import datetime

from hgraph._runtime._node import Node

__all__ = ("GraphRecorder",)


class GraphRecorder(ABC):

    @abstractmethod
    def record_node(self, node: Node) -> Node:
        """Wrap the node to support recording"""
        ...

    @abstractmethod
    def replay_node(self, node: Node) -> Node:
        """Wrap the node to support recording"""
        ...

    @abstractmethod
    def begin_cycle(self, dt: datetime):
        """Indicates that a cycle to record (only when recording)"""
        ...

    @abstractmethod
    def end_cycle(self, dt: datetime):
        """Indicates that the cycle has ended (only when recording)"""
        ...

    @abstractmethod
    def first_recorded_time(self) -> datetime:
        """The first time the cycle starts"""
        ...

    @abstractmethod
    def last_recorded_time(self) -> datetime:
        """The last time that was recorded"""
        ...

    @abstractmethod
    def begin_replay(self):
        """Start the replay (sets the cycle time to first cycle"""
        ...

    @abstractmethod
    def next_cycle(self) -> datetime | None:
        """The time of the next cycle"""
        ...
