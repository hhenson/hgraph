from ast import Sub
from collections import defaultdict
from dataclasses import dataclass, field
import logging
import sys
import typing
from datetime import datetime

if typing.TYPE_CHECKING:
    from hgraph._impl._runtime._node import Node
    from hgraph._impl._types._input import TimeSeriesInput


__all__ = ("TimeSeriesSubscriber",)


logger = logging.getLogger(__name__)


SUBSCRIBER = typing.TypeVar("SUBSCRIBER", "Node", "TimeSeriesInput")


@dataclass
class TimeSeriesSubscriber:
    """
    A reference counted subscription collection.
    """
    _subscribers: dict[int, tuple] = None

    @property
    def subscribers(self):
        if self._subscribers is None:
            self._subscribers = {}
        return self._subscribers

    def subscribe(self, subscriber: SUBSCRIBER):
        item = self.subscribers.get(id(subscriber))
        if item is None:
            item = (subscriber, 1)  # (subscriber, count)
        else:
            item = (item[0], item[1] + 1)
            
        self.subscribers[id(subscriber)] = item

    def unsubscribe(self, subscriber: SUBSCRIBER):
        if self._subscribers is None:
            assert False, "Unsubscribing from a subscriber collection that has not been initialized, this is a bug"
        
        item = self._subscribers[id(subscriber)]
        if item[1] <= 1:
            del self._subscribers[id(subscriber)]        
        else:
            self._subscribers[id(subscriber)] = (item[0], item[1] - 1)

    def notify(self, modified_time: datetime):
        """Notified the graph executor that the nodes should be scheduled for this engine cycle of evaluation"""
        if self._subscribers is not None:
            for s in self._subscribers.values():
                s[0].notify(modified_time)

    def assert_empty(self, owner):
        from hgraph._types._time_series_types import TimeSeries
        if self._subscribers is None:
            return
        
        if len(self._subscribers) != 0: 
            try:
                logger.error(
                    f"Output instance still has subscribers when released, this is a bug. \n"
                    f"output belongs to node {owner.owning_node}\n"
                    f"subscriber nodes are {[i.owning_node if isinstance(i, TimeSeries) else i for i in self._subscribers]}\n\n"
                    f"subscriber inputs are {[i for i in self._subscribers if isinstance(i, TimeSeries)]}\n\n"
                    f"{self}"
                )
            except Exception:
                ...
        