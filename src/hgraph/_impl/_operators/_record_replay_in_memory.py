from abc import abstractmethod
from datetime import datetime
from typing import Protocol, Iterable, Any

from hgraph import (
    MIN_ST,
    MIN_TD,
    GlobalState,
    generator,
    replay,
    record_replay_model,
    IN_MEMORY,
    TIME_SERIES_TYPE,
    sink_node,
    record,
    EvaluationClock,
    STATE,
)

__all__ = (
    "ReplaySource",
    "replay_from_memory",
    "record_to_memory",
    "SimpleArrayReplaySource",
    "set_replay_values",
    "get_recorded_value",
)


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
    GlobalState.instance()[f"nodes.{replay_from_memory.signature.name}.{label}"] = value


@generator(overloads=replay, requires=lambda m, s: record_replay_model() == IN_MEMORY)
def replay_from_memory(key: str, tp: type[TIME_SERIES_TYPE]) -> TIME_SERIES_TYPE:
    """
    This will replay a sequence of values, a None value will be ignored (skip the tick).
    The type of the elements of the sequence must be a delta value of the time series type.

    # TODO: At some point it would be useful to support a time-indexed collection of values to provide
    # More complex replay scenarios.
    """
    source = GlobalState.instance().get(f"nodes.{replay_from_memory.signature.name}.{key}", None)
    if source is None:
        raise ValueError(f"Replay source with label '{key}' does not exist")
    for ts, v in source:
        if v is not None:
            yield ts, v


@sink_node(overloads=record, requires=lambda m, s: record_replay_model() == IN_MEMORY)
def record_to_memory(
    ts: TIME_SERIES_TYPE,
    key: str = "out",
    record_delta_values: bool = True,
    _clock: EvaluationClock = None,
    _state: STATE = None,
):
    """
    This node will record the values of the time series into the provided list.
    """
    _state.record_value.append((_clock.evaluation_time, ts.delta_value if record_delta_values else ts.value))


@record_to_memory.start
def record_to_memory(key: str, _state: STATE):
    value = []
    global_state = GlobalState.instance()
    global_state[f"nodes.{record.signature.name}.{key}"] = value
    _state.record_value = value


def get_recorded_value(label: str = "out") -> list[tuple[datetime, Any]]:
    """
    Returns the recorded values for the given label.
    """
    global_state = GlobalState.instance()
    return global_state[f"nodes.{record.signature.name}.{label}"]
