from abc import abstractmethod
from datetime import datetime
from typing import Protocol, Iterable, Any

from hgraph import get_fq_recordable_id
from hgraph._operators import replay, IN_MEMORY, record, record_replay_model_restriction, compare, replay_const
from hgraph._runtime import Traits, MIN_ST, MIN_TD, GlobalState, EvaluationClock, EvaluationEngineApi, Node, Graph
from hgraph._types import (
    AUTO_RESOLVE,
    TS,
    CompoundScalar,
    LOGGER,
    STATE,
    TIME_SERIES_TYPE,
    TimeSeriesOutput,
    HgTimeSeriesTypeMetaData,
)
from hgraph._wiring import generator, sink_node, graph, const_fn


__all__ = (
    "ReplaySource",
    "SimpleArrayReplaySource",
    "set_replay_values",
    "get_recorded_value",
    "reset_recorded_value",
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


def set_replay_values(label: str, value: ReplaySource, recordable_id: str = None):
    """
    Set the replay values for the given label.
    """
    if recordable_id is None:
        recordable_id = f"nodes.{replay_from_memory.signature.name}"
    else:
        recordable_id = f":memory:{recordable_id}"
    GlobalState.instance()[f"{recordable_id}.{label}"] = value


@generator(overloads=replay, requires=record_replay_model_restriction(IN_MEMORY, True))
def replay_from_memory(
    key: str,
    tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
    is_operator: bool = False,
    recordable_id: str = None,
    _traits: Traits = None,
    _clock: EvaluationClock = None,
) -> TIME_SERIES_TYPE:
    """
    This will replay a sequence of values, a None value will be ignored (skip the tick).
    The type of the elements of the sequence must be a delta value of the time series type.

    # TODO: At some point it would be useful to support a time-indexed collection of values to provide
    # More complex replay scenarios.
    """
    try:
        recordable_id = get_fq_recordable_id(_traits, recordable_id)
        recordable_id = f":memory:{recordable_id}"
    except RuntimeError:
        # If there are no parent recordable id's and this was None then we need to catch the exception and set
        recordable_id = f"nodes.{replay_from_memory.signature.name}"

    source = GlobalState.instance().get(f"{recordable_id}.{key}", None)
    if source is None:
        raise ValueError(f"Replay source with label '{key}' does not exist")
    tm = _clock.evaluation_time
    for ts, v in source:
        if ts < tm:
            continue
        if v is not None:
            yield ts, v


@const_fn(overloads=replay_const, requires=record_replay_model_restriction(IN_MEMORY, True))
def replay_const_from_memory(
    key: str,
    tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
    is_operator: bool = False,
    recordable_id: str = None,
    tm: datetime = None,
    as_of: datetime = None,
    _traits: Traits = None,
    _clock: EvaluationClock = None,
    _node: Node = None,
) -> TIME_SERIES_TYPE:
    recordable_id = get_fq_recordable_id(_traits, recordable_id)
    recordable_id = f":memory:{recordable_id}.{key}"
    source = GlobalState.instance().get(recordable_id, None)
    tm = _clock.evaluation_time if tm is None else tm
    if source is not None:
        from hgraph import TimeSeriesBuilderFactory

        # Create an output to match type so we can determine the value through repeated processing of the deltas.
        output: TimeSeriesOutput = (
            TimeSeriesBuilderFactory.instance()
            .make_output_builder(HgTimeSeriesTypeMetaData.parse_type(tp))
            .make_instance(_node, None)
        )
        for ts, v in source:
            # This is a slow approach, but since we don't have an index, this is the best we can do.
            # Additionally, since we are recording delta values, we need to apply the successive results to form the
            # full picture of state.
            if ts <= tm:
                # Combine results when dealing with Collection results
                output.apply_result(v)
            else:
                break
        return output.value


@sink_node(overloads=record, requires=record_replay_model_restriction(IN_MEMORY, True))
def record_to_memory(
    ts: TIME_SERIES_TYPE,
    key: str = "out",
    is_operator: bool = False,
    recordable_id: str = None,
    _api: EvaluationEngineApi = None,
    _state: STATE = None,
    _traits: Traits = None,
):
    """
    This node will record the values of the time series into the provided list.
    """
    _state.record_value.append((_api.evaluation_clock.evaluation_time, ts.delta_value))


@record_to_memory.start
def record_to_memory_start(key: str, is_operator: bool, recordable_id: str, _state: STATE, _traits: Traits):
    try:
        recordable_id = get_fq_recordable_id(_traits, recordable_id)
        recordable_id = f":memory:{recordable_id}.{key}"
        _state.is_operator = True
    except:
        recordable_id = f"nodes.{record.signature.name}.{key}"
        _state.is_operator = False
    _state.recordable_id = recordable_id
    _state.record_value = []


@record_to_memory.stop
def record_to_memory_stop(_state: STATE, _api: EvaluationEngineApi):
    global_state = GlobalState.instance()
    if _state.is_operator and (value := global_state.get(_state.recordable_id, None)):
        result = []
        st = _api.start_time
        for t, v in value:
            if t >= st:
                break
            result.append((t, v))
        result.extend(_state.record_value)
    else:
        result = _state.record_value

    global_state[_state.recordable_id] = result


def get_recorded_value(key: str = "out", recordable_id: str = None) -> list[tuple[datetime, Any]]:
    """
    Returns the recorded values for the given label.
    """
    if recordable_id is None:
        recordable_id = f"nodes.{record.signature.name}"
    else:
        recordable_id = f":memory:{recordable_id}"
    global_state = GlobalState.instance()
    return global_state[f"{recordable_id}.{key}"]


def reset_recorded_value(key: str = "out", recordable_id: str = None):
    """Resets the recorded state"""
    if recordable_id is None:
        recordable_id = f"nodes.{record.signature.name}"
    else:
        recordable_id = f":memory:{recordable_id}"
    global_state = GlobalState.instance()
    global_state.pop(f"{recordable_id}.{key}")


@graph(overloads=compare)
def compare_generic(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE):
    """This just makes use of the generic eq_ operator to perform the comparison."""
    out = lhs == rhs
    record(out, key="__COMPARE__")
    _assert_result(out)


class _AssertResult(CompoundScalar):
    has_error: bool = False


@sink_node
def _assert_result(ts: TS[bool], _state: STATE[_AssertResult] = None, _traits: Traits = None, _logger: LOGGER = None):
    _state.has_error |= not ts.value


@_assert_result.stop
def _assert_result_stop(_state: STATE[_AssertResult], _traits: Traits, _logger: LOGGER):
    if _state.has_error:
        raise RuntimeError(f"{_traits.get_trait('recordable_id')} is not equal")
    else:
        _logger.info(f"[COMPARE] '{_traits.get_trait('recordable_id')}' is the same")
