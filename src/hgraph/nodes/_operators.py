from dataclasses import dataclass
from typing import Type

from hgraph import compute_node, SCALAR, SCALAR_1, TS, TIME_SERIES_TYPE, REF, graph, SIGNAL, STATE, CompoundScalar, \
    contains_, eq_, not_, abs_, len_, and_, or_, mod_, ne_

__all__ = ("cast_", "downcast_", "downcast_ref", "drop", "take")


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


@compute_node(overloads=len_)
def len_ts(ts: TS[SCALAR]) -> TS[int]:
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


@compute_node(overloads=contains_)
def contains_ts(ts: TS[SCALAR], key: TS[SCALAR_1]) -> TS[bool]:
    """Implements using the standard ``in`` Python operator"""
    return key.value in ts.value


@compute_node(overloads=eq_)
def eq_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """Implements using the standard ``==`` Python operator"""
    return bool(lhs.value == rhs.value)


@compute_node(overloads=ne_)
def ne_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """Implements using the standard ``!=`` Python operator"""
    return bool(lhs.value != rhs.value)


@compute_node(overloads=not_)
def not_ts(ts: TS[SCALAR]) -> TS[bool]:
    """Implements not_ using the standard Python ``not`` operator"""
    return not ts.value


@compute_node(overloads=abs_)
def abs_ts(ts: TS[SCALAR]) -> TS[SCALAR]:
    """Implements using the standard ``abs`` Python operator"""
    return abs(ts.value)


@compute_node(overloads=and_)
def and_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """Implements using the standard ``and`` Python operator"""
    return bool(lhs.value and rhs.value)


@compute_node(overloads=or_)
def or_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """Implements using the standard ``or`` Python operator"""
    return bool(lhs.value or rhs.value)


@compute_node(overloads=mod_)
def mod_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """Implements using the standard ``mod`` Python operator"""
    return lhs.value % rhs.value

