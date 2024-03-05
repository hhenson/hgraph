from dataclasses import dataclass
from typing import Type

from hgraph import compute_node, SCALAR, SCALAR_1, TS, TIME_SERIES_TYPE, REF, graph, SIGNAL, STATE, CompoundScalar

__all__ = ("cast_", "downcast_", "downcast_ref", "len_", "drop", "take")


@compute_node
def cast_(tp: Type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Casts a time-series to a different type.
    """
    return tp(ts.value)


@compute_node
def downcast_(tp: Type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Downcasts a time-series to the given type.
    """
    assert isinstance(ts.value, tp)
    return ts.value


@compute_node
def downcast_ref(tp: Type[SCALAR], ts: REF[TS[SCALAR_1]]) -> REF[TS[SCALAR]]:
    """
    Downcasts a time-series reference to the given type. This is fast but unsafe as there is no type checking happens here
    """
    return ts.value



@compute_node
def len_(ts: TIME_SERIES_TYPE) -> TS[int]:
    """
    Returns the notion of length for the input time-series.
    By default, it is the length of the value of the time-series.
    """
    return len(ts.value)


@graph
def drop(ts: TIME_SERIES_TYPE, count: int = 1) -> TIME_SERIES_TYPE:
    """
    Drops the first `count` ticks and then returns the remainder of the ticks
    """
    return _drop(ts, ts, count)


@dataclass
class CounterState(CompoundScalar):
    count: int = 0


@compute_node(active=("ts_counter",))
def _drop(ts: REF[TIME_SERIES_TYPE], ts_counter: SIGNAL, count: int = 1, _state: STATE[CounterState] = None) -> REF[TIME_SERIES_TYPE]:
    _state.count += 1
    if _state.count > count:
        ts_counter.make_passive()
        return ts.value


@compute_node
def take(ts: TIME_SERIES_TYPE, count: int = 1, _state: STATE[CounterState] = None) -> TIME_SERIES_TYPE:
    _state.count += 1
    c = _state.count
    if c == count:
        ts.make_passive()
    return ts.delta_value
