from abc import abstractmethod
from datetime import datetime
from typing import Iterable, Any, Protocol

from hgraph import generator, TIME_SERIES_TYPE, MIN_TD, MIN_ST, GlobalState


__all__ = ("ReplaySource", "replay", "SimpleArrayReplaySource", "set_replay_values",)


class ReplaySource(Protocol):
    """
    A source that replays a sequence of values.
    """

    @abstractmethod
    def __iter__(self) -> Iterable[tuple[datetime, Any]]:
        """Return an iterator over a time-stamp and value tuple"""


class SimpleArrayReplaySource(ReplaySource):

    def __init__(self, values: list[Any], start_time: datetime = MIN_ST):
        self.values = values
        self.start_time = start_time

    def __iter__(self) -> Iterable[tuple[datetime, Any]]:
        next_engine_time = self.start_time
        for value in self.values:
            if value is not None:
                yield next_engine_time, value
            next_engine_time += MIN_TD


def set_replay_values(label: str, value: ReplaySource):
    """
    Set the replay values for the given label.
    """
    GlobalState.instance()[f"nodes.{replay.signature.name}.{label}"] = value


@generator
def replay(label: str, tp: type[TIME_SERIES_TYPE]) -> TIME_SERIES_TYPE:
    """
    This will replay a sequence of values, a None value will be ignored (skip the tick).
    The type of the elements of the sequence must be a delta value of the time series type.

    # TODO: At some point it would be useful to support a time-indexed collection of values to provide
    # More complex replay scenarios.
    """
    source = GlobalState.instance().get(f"nodes.{replay.signature.name}.{label}", None)
    if source is None:
        raise ValueError(f"Replay source with label '{label}' does not exist")
    for ts, v in source:
        if v is not None:
            yield ts, v
