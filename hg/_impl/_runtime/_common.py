from collections import defaultdict
from dataclasses import dataclass, field
import typing

if typing.TYPE_CHECKING:
    from hg._impl._runtime._node import Node


__all__ = ("NodeSubscriber",)


@dataclass
class NodeSubscriber:
    """
    A reference counted subscription of a node.
    """

    _subscribers: dict["Node", int] = field(default_factory=lambda :defaultdict["Node", int](int))

    def subscribe_node(self, node: "Node"):
        self._subscribers[node] += 1

    def un_subscribe_node(self, node: "Node"):
        self._subscribers[node] -= 1
        if self._subscribers[node] == 0:
            del self._subscribers[node]

    def notify(self):
        """Notified the graph executor that the nodes should be scheduled for this engine cycle of evaluation"""
        for node in self._subscribers:
            node.notify()