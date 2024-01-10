from typing import Type

from hgraph import compute_node, SCALAR, SCALAR_1, TS, TIME_SERIES_TYPE, REF, graph, SIGNAL, STATE

__all__ = ("cast_", "len_", "drop", "take")


@compute_node
def cast_(tp: Type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Casts a time-series to a different type.
    """
    return tp(ts.value)


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


@compute_node(active=("ts_counter",))
def _drop(ts: REF[TIME_SERIES_TYPE], ts_counter: SIGNAL, count: int = 1, _state: STATE = None) -> REF[TIME_SERIES_TYPE]:
    _state.count += 1
    if _state.count > count:
        ts_counter.make_passive()
        return ts.value


@_drop.start
def _drop_start(_state: STATE):
    _state.count = 0


@compute_node
def take(ts: TIME_SERIES_TYPE, count: int = 1, _state: STATE = None) -> TIME_SERIES_TYPE:
    _state.count += 1
    c = _state.count
    if c == count:
        ts.make_passive()
    return ts.delta_value


@take.start
def take_start(count: int, _state: STATE):
    if count < 1:
        raise RuntimeError("count must be greater than 0")
    _state.count = 0
