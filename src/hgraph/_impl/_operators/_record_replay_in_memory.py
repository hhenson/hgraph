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
    TS,
    graph,
    CompoundScalar,
    LOGGER,
)

__all__ = (
    "ReplaySource",
    "replay_from_memory",
    "record_to_memory",
    "replay_const_from_memory",
    "SimpleArrayReplaySource",
    "set_replay_values",
    "get_recorded_value",
)

from hgraph._operators._record_replay import record_replay_model_restriction, compare, replay_const
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
    suffix: str = None,
    is_operator: bool = False,
    _traits: Traits = None,
    _clock: EvaluationClock = None,
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
        recordable_id = f":memory:{recordable_id}{'_' + suffix if suffix else ''}"
    source = GlobalState.instance().get(f"{recordable_id}.{key}", None)
    if source is None:
        raise ValueError(f"Replay source with label '{key}' does not exist")
    tm = _clock.evaluation_time
    for ts, v in source:
        if ts < tm:
            continue
        if v is not None:
            yield ts, v


@generator(overloads=replay_const, requires=record_replay_model_restriction(IN_MEMORY))
def replay_const_from_memory(
    key: str,
    tp: type[TIME_SERIES_TYPE],
    suffix: str = None,
    is_operator: bool = False,
    _traits: Traits = None,
    _clock: EvaluationClock = None,
) -> TIME_SERIES_TYPE:
    recordable_id = f":memory:{_traits.get_trait_or("recordable_id", None)}{'_' + suffix if suffix else ''}"
    source = GlobalState.instance().get(f"{recordable_id}.{key}", None)
    if source is None:
        raise ValueError(f"Replay source with label '{key}' does not exist")
    tm = _clock.evaluation_time
    previous_v = None
    for ts, v in source:
        # This is a slow approach, but since we don't have an index this is the best we can do.
        if ts <= tm:
            previous_v = v
        else:
            break
    yield tm, previous_v


@sink_node(overloads=record, requires=record_replay_model_restriction(IN_MEMORY))
def record_to_memory(
    ts: TIME_SERIES_TYPE,
    key: str = "out",
    record_delta_values: bool = True,
    suffix: str = None,
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
def record_to_memory(key: str, suffix: str, _state: STATE, _traits: Traits):
    value = []
    global_state = GlobalState.instance()
    recordable_id = _traits.get_trait_or("recordable_id", None)
    if recordable_id is None:
        recordable_id = f"nodes.{record.signature.name}"
    else:
        recordable_id = f":memory:{recordable_id}{'_' + suffix if suffix else ''}"
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


@graph(overloads=compare, requires=record_replay_model_restriction(IN_MEMORY))
def compare_in_memory(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE):
    out = lhs == rhs
    record_to_memory(out, key="__COMPARE__")
    _assert_result(out)


class _AssertResult(CompoundScalar):
    has_error: bool = False


@sink_node
def _assert_result(ts: TS[bool], _state: STATE[_AssertResult] = None, _traits: Traits = None, _logger: LOGGER = None):
    _state.has_error |= ts.value


@_assert_result.start
def _assert_result_start(_state: STATE[_AssertResult], _traits: Traits, _logger: LOGGER):
    if _state.has_error:
        raise RuntimeError(f"{_traits.get_trait('recordable_id')} is not equal")
    else:
        _logger.info(f"[COMPARE] '{_traits.get_trait('recordable_id')}' is the same")
