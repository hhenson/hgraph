from collections import defaultdict
from dataclasses import dataclass, field
import typing

if typing.TYPE_CHECKING:
    from hgraph._impl._runtime._node import Node


__all__ = ("NodeSubscriber",)


@dataclass
class NodeSubscriber:
    """
    A reference counted subscription of a node.
    """

    _subscriber_count: dict[tuple[int, ...], int] = field(default_factory=lambda :defaultdict[tuple[int, ...], int](int))
    _subscribers: list["Node"] = field(default_factory=list)

    def subscribe_node(self, node: "Node"):
        self._subscriber_count[node.node_id] += 1
        if self._subscriber_count[node.node_id] == 1:
            self._subscribers.append(node)

    def un_subscribe_node(self, node: "Node"):
        self._subscriber_count[node.node_id] -= 1
        if self._subscriber_count[node.node_id] == 0:
            self._subscribers.remove(node)

    def notify(self):
        """Notified the graph executor that the nodes should be scheduled for this engine cycle of evaluation"""
        for node in self._subscribers:
            node.notify()
