from collections import deque
from dataclasses import dataclass, field
from typing import Mapping

from hgraph import compute_node, TS, SCALAR, STATE, CompoundScalar, SCHEDULER, MIN_TD, SCALAR_1, TSS
from hgraph.nodes._conversion_operators._conversion_operator_templates import emit

__all__ = ("emit_tuple",)


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
