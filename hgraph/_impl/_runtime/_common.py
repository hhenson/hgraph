from ast import Sub
from collections import defaultdict
from dataclasses import dataclass, field
import sys
import typing
from datetime import datetime

if typing.TYPE_CHECKING:
    from hgraph._impl._runtime._node import Node
    from hgraph._impl._types._input import TimeSeriesInput


__all__ = ("TimeSeriesSubscriber",)


SUBSCRIBER = typing.TypeVar("SUBSCRIBER", "Node", "TimeSeriesInput")


@dataclass
class SubscriberCount:
    subscriber: SUBSCRIBER | None
    count: int

@dataclass
class TimeSeriesSubscriber:
    """
    A reference counted subscription collection.
    """
    _subscribers: dict[int, SubscriberCount] = field(default_factory=lambda: defaultdict(lambda: SubscriberCount(None, 0)))

    def subscribe(self, subscriber: SUBSCRIBER):
        item = self._subscribers[id(subscriber)]
        if item.count == 0:
            item.subscriber = subscriber
            
        item.count += 1

    def unsubscribe(self, subscriber: SUBSCRIBER):
        if not sys.exc_info():  # Check if we are not in an exception context
            assert id(subscriber) in self._subscribers, f"Unsubscribe called with subscriber that is not known: {subscriber}"
            assert self._subscribers[id(subscriber)].count > 0, f"Unsubscribe called with subscriber has already unsubscribed: {subscriber}"

        item = self._subscribers[id(subscriber)]
        item.count -= 1
        if item.count <= 0:
            del self._subscribers[id(subscriber)]        

    def notify(self, modified_time: datetime):
        """Notified the graph executor that the nodes should be scheduled for this engine cycle of evaluation"""
        for s in self._subscribers.values():
            s.subscriber.notify(modified_time)
