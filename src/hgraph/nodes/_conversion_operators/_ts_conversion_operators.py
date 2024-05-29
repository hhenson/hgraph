from collections import deque
from dataclasses import dataclass, field
from typing import Mapping

from hgraph import compute_node, TS, SCALAR, STATE, CompoundScalar, SCHEDULER, MIN_TD, SCALAR_1, graph, \
    AUTO_RESOLVE, TSS
from hgraph.nodes._conversion_operators._conversion_operator_templates import emit, convert

__all__ = ("emit_tuple", "convert_ts", "convert_ts_to_tss")


@graph(overloads=convert)
def convert_ts(
        ts: TS[SCALAR],
        to: type[TS[SCALAR_1]],
        s_tp: type[SCALAR] = AUTO_RESOLVE,
        s1_type: type[SCALAR_1] = AUTO_RESOLVE
) -> TS[SCALAR_1]:
    """
    Check types, if they are the same, then return the value.
    """
    if s_tp == s1_type:
        return ts
    else:
        return _convert_ts_generic(ts, s1_type)


@compute_node
def _convert_ts_generic(ts: TS[SCALAR], s1_type: type[SCALAR_1]) -> TS[SCALAR_1]:
    return s1_type(ts.value)


@compute_node(overloads=convert)
def convert_ts_to_tss(ts: TS[SCALAR], to: type[TSS[SCALAR]]) -> TSS[SCALAR]:
    return {ts.value}


@dataclass
class _BufferState(CompoundScalar):
    buffer: deque = field(default_factory=deque)


@compute_node(overloads=emit)
def emit_tuple(ts: TS[tuple[SCALAR, ...]],
               _state: STATE[_BufferState] = None,
               _schedule: SCHEDULER = None) -> TS[SCALAR]:
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


@compute_node(overloads=emit)
def emit_set(ts: TS[frozenset[SCALAR]],
             _state: STATE[_BufferState] = None,
             _schedule: SCHEDULER = None) -> TS[SCALAR]:
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


@compute_node(overloads=emit)
def emit_mapping(ts: TS[Mapping[SCALAR, SCALAR_1]],
                 _state: STATE[_BufferState] = None,
                 _schedule: SCHEDULER = None) -> TS[SCALAR_1]:
    """
    Converts a tuple of SCALAR values in a stream of individual SCALAR values.
    """
    if ts.modified:
        _state.buffer.extend(ts.value.values())

    if _state.buffer:
        d: deque[SCALAR] = _state.buffer
        v = d.popleft()
        if d:
            _schedule.schedule(MIN_TD)
        return v
