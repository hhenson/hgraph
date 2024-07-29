from abc import abstractmethod
from datetime import datetime
from typing import Protocol, Iterable, Any

from hgraph import (
    MIN_ST,
    MIN_TD,
    GlobalState,
    generator,
    replay,
    IN_MEMORY,
    TIME_SERIES_TYPE,
    sink_node,
    record,
    EvaluationClock,
    STATE,
    CONTEXT,
    TS,
)

__all__ = (
    "ReplaySource",
    "replay_from_memory",
    "record_to_memory",
    "SimpleArrayReplaySource",
    "set_replay_values",
    "get_recorded_value",
)

from hgraph._operators._record_replay import record_replay_model_restriction
from hgraph._runtime._traits import Traits


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


def set_replay_values(label: str, value: ReplaySource, recordable_id: str = None):
    """
    Set the replay values for the given label.
    """
    if recordable_id is None:
        recordable_id = f"nodes.{replay_from_memory.signature.name}"
    else:
        recordable_id = f":memory:{recordable_id}"
    GlobalState.instance()[f"{recordable_id}.{label}"] = value


@generator(overloads=replay, requires=record_replay_model_restriction(IN_MEMORY))
def replay_from_memory(
    key: str,
    tp: type[TIME_SERIES_TYPE],
    is_operator: bool = False,
    _traits: Traits = None,
) -> TIME_SERIES_TYPE:
    """
    This will replay a sequence of values, a None value will be ignored (skip the tick).
    The type of the elements of the sequence must be a delta value of the time series type.

    # TODO: At some point it would be useful to support a time-indexed collection of values to provide
    # More complex replay scenarios.
    """
    recordable_id = _traits.get_trait_or("recordable_id", None)
    if recordable_id is None:
        recordable_id = f"nodes.{replay_from_memory.signature.name}"
    else:
        recordable_id = f":memory:{recordable_id}"
    source = GlobalState.instance().get(f"{recordable_id}.{key}", None)
    if source is None:
        raise ValueError(f"Replay source with label '{key}' does not exist")
    for ts, v in source:
        if v is not None:
            yield ts, v


@sink_node(overloads=record, requires=record_replay_model_restriction(IN_MEMORY))
def record_to_memory(
    ts: TIME_SERIES_TYPE,
    key: str = "out",
    record_delta_values: bool = True,
    is_operator: bool = False,
    _clock: EvaluationClock = None,
    _state: STATE = None,
    _traits: Traits = None,
):
    """
    This node will record the values of the time series into the provided list.
    """
    _state.record_value.append((_clock.evaluation_time, ts.delta_value if record_delta_values else ts.value))


@record_to_memory.start
def record_to_memory(key: str, _state: STATE, _traits: Traits):
    value = []
    global_state = GlobalState.instance()
    recordable_id = _traits.get_trait_or("recordable_id", None)
    if recordable_id is None:
        recordable_id = f"nodes.{record.signature.name}"
    else:
        recordable_id = f":memory:{recordable_id}"
    global_state[f"{recordable_id}.{key}"] = value
    _state.record_value = value


def get_recorded_value(label: str = "out", recordable_id: str = None) -> list[tuple[datetime, Any]]:
    """
    Returns the recorded values for the given label.
    """
    if recordable_id is None:
        recordable_id = f"nodes.{record.signature.name}"
    else:
        recordable_id = f":memory:{recordable_id}"
    global_state = GlobalState.instance()
    return global_state[f"{recordable_id}.{label}"]
