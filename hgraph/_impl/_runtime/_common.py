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
class TimeSeriesSubscriber:
    """
    A reference counted subscription collection.
    """

    _subscriber_count: dict[tuple[int, ...], int] = field(default_factory=lambda: defaultdict(int))
    _subscribers: list[SUBSCRIBER] = field(default_factory=list)

    def subscribe(self, subscriber: SUBSCRIBER):
        self._subscriber_count[id(subscriber)] += 1
        if self._subscriber_count[id(subscriber)] == 1:
            self._subscribers.append(subscriber)

    def unsubscribe(self, subscriber: SUBSCRIBER):
        if not sys.exc_info():  # Check if we are not in an exception context
            assert id(subscriber) in self._subscriber_count, f"Unsubscribe called with subscriber that is not known: {subscriber}"
            assert self._subscriber_count[id(subscriber)] > 0, f"Unsubscribe called with subscriber has already unsubscribed: {subscriber}"

        self._subscriber_count[id(subscriber)] -= 1
        if (self._subscriber_count[id(subscriber)]) == 0:
            self._subscribers.remove(subscriber)

    def notify(self, modified_time: datetime):
        """Notified the graph executor that the nodes should be scheduled for this engine cycle of evaluation"""
        for s in self._subscribers:
            s.notify(modified_time)
