from collections import deque
from dataclasses import dataclass, field
from typing import Mapping

from hgraph import compute_node, TS, SCALAR, STATE, CompoundScalar, SCHEDULER, MIN_TD, SCALAR_1, graph, \
    AUTO_RESOLVE, TSS, OUT
from hgraph._types._scalar_types import DEFAULT
from hgraph.nodes._conversion_operators._conversion_operator_templates import emit, convert

__all__ = ("emit_tuple", "convert_ts_generic", "convert_ts_to_tss")


@graph(overloads=convert)
def convert_ts_noop(
        ts: TS[SCALAR],
        to: type[TS[SCALAR]] = DEFAULT[OUT],
) -> TS[SCALAR]:
    """
    if types are the same, then return the value.
    """
    return ts


@compute_node(overloads=convert)
def convert_ts_generic(ts: TS[SCALAR], to: type[SCALAR_1] = DEFAULT[OUT], s1_type: type[SCALAR_1] = AUTO_RESOLVE) -> TS[SCALAR_1]:
    return s1_type(ts.value)


@compute_node(overloads=convert)
def convert_ts_to_tss(ts: TS[SCALAR], to: type[TSS[SCALAR]]) -> TSS[SCALAR]:
    return {ts.value}  # AB: this looks like `collect` to me


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
