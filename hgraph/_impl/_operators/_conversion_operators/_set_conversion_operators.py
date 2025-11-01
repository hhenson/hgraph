from collections import deque
from typing import Tuple, Set, Type

from hgraph import (
    compute_node,
    TS,
    SCALAR,
    STATE,
    SCHEDULER,
    MIN_TD,
    emit,
    TSS,
    collect,
    DEFAULT,
    OUT,
    SIGNAL,
    TS_OUT,
    convert,
)
from hgraph._impl._operators._conversion_operators._conversion_operator_util import _BufferState

__all__ = ()


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type in (TS[Set], TS[set], TS[frozenset])
    or m[OUT].matches_type(TS[Set[m[SCALAR].py_type]]),
)
def convert_ts_to_set(ts: TS[SCALAR], to: Type[OUT] = DEFAULT[OUT]) -> TS[Set[SCALAR]]:
    return {ts.value}


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type in (TS[Set], TS[set], TS[frozenset])
    or m[OUT].matches_type(TS[Set[m[SCALAR].py_type]]),
)
def convert_tuple_to_set(ts: TS[Tuple[SCALAR, ...]], to: Type[OUT] = DEFAULT[OUT]) -> TS[Set[SCALAR]]:
    return set(ts.value)


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type in (TS[Set], TS[set], TS[frozenset])
    or m[OUT].matches_type(TS[Set[m[SCALAR].py_type]]),
)
def convert_tss_to_set(ts: TSS[SCALAR], to: Type[OUT] = DEFAULT[OUT]) -> TS[Set[SCALAR]]:
    return set(ts.value)


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type in (TS[Set], TS[set], TS[frozenset])
    or m[OUT].matches_type(TS[Set[m[SCALAR].py_type]]),
    valid=("ts",),
)
def collect_set(
    ts: TS[SCALAR], *, reset: SIGNAL = None, tp_: Type[OUT] = DEFAULT[OUT], _output: TS_OUT[Set[SCALAR]] = None
) -> TS[Set[SCALAR]]:
    prev = _output.value if _output.valid and not reset.modified else set()
    new = {ts.value} if ts.modified else set()
    return prev | new


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type in (TS[Set], TS[set], TS[frozenset])
    or m[OUT].matches_type(TS[Set[m[SCALAR].py_type]]),
    valid=("ts",),
)
def collect_set_from_tuples(
    ts: TS[Tuple[SCALAR, ...]],
    *,
    reset: SIGNAL = None,
    tp_: Type[OUT] = DEFAULT[OUT],
    _output: TS_OUT[Set[SCALAR]] = None,
) -> TS[Set[SCALAR]]:
    prev = _output.value if _output.valid and not reset.modified else set()
    new = set(ts.value) if ts.modified else set()
    return prev | new


@compute_node(overloads=emit)
def emit_set(ts: TS[frozenset[SCALAR]], _state: STATE[_BufferState] = None, _schedule: SCHEDULER = None) -> TS[SCALAR]:
    """
    Converts a tuple of SCALAR values in a stream of individual SCALAR values.
    """
    if ts.modified:
        _state.buffer.extend(ts.value)

    if _state.buffer:
        d: deque[SCALAR] = _state.buffer
        v = d.popleft()
        if d:
            _schedule.schedule(MIN_TD)
        return v
