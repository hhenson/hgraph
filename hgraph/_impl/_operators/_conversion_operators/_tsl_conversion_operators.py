from collections import deque
from typing import Type, Tuple

from hgraph import (
    graph,
    TSL,
    TIME_SERIES_TYPE,
    SIZE,
    combine,
    OUT,
    TIME_SERIES_TYPE_1,
    DEFAULT,
    convert,
    SCALAR,
    TS,
    compute_node,
    emit,
    SCHEDULER,
    MIN_TD,
)
from hgraph._types._scalar_types import TUPLE, STATE
from hgraph._impl._operators._conversion_operators._conversion_operator_util import _BufferState


@graph(overloads=combine, requires=lambda m, s: OUT not in m or m[OUT].py_type == TSL)
def combine_tsl(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    return tsl


@graph(overloads=combine)
def combine_tsl(
    *tsl: TSL[TIME_SERIES_TYPE, SIZE], tp_: Type[TSL[TIME_SERIES_TYPE_1, SIZE]] = DEFAULT[OUT]
) -> TSL[TIME_SERIES_TYPE_1, SIZE]:
    return tsl


@compute_node(
    overloads=convert,
    requires=lambda m, s: SIZE in m and (m[OUT].py_type is TSL or m[OUT].value_tp.scalar_type().matches(m[SCALAR])),
    resolvers={
        TIME_SERIES_TYPE: lambda m, s: TS[m[SCALAR]] if m[OUT].py_type is TSL else m[OUT].value_tp,
        SIZE: lambda m, s: None if m[OUT].py_type is TSL else m[OUT].size(),
    },
)
def convert_tuple_to_tsl(
    ts: TS[Tuple[SCALAR, ...]], to: Type[OUT] = DEFAULT[OUT], __strict__: bool = True
) -> TSL[TIME_SERIES_TYPE, SIZE]:
    return ts.value


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSL or m[OUT].value_tp.scalar_type().matches(m[SCALAR]),
    resolvers={
        TIME_SERIES_TYPE: lambda m, s: TS[m[SCALAR]] if m[OUT].py_type is TSL else m[OUT].value_tp,
        SIZE: lambda m, s: m[TUPLE].size(),
    },
)
def convert_tuple_to_tsl(
    ts: TS[TUPLE], to: Type[OUT] = DEFAULT[OUT], __strict__: bool = True
) -> TSL[TIME_SERIES_TYPE, SIZE]:
    return ts.value


@compute_node(
    overloads=emit,
    resolvers={SCALAR: lambda m, s: m[TIME_SERIES_TYPE].scalar_type()},
)
def emit_tsl(
    ts: TSL[TIME_SERIES_TYPE, SIZE], _state: STATE[_BufferState] = None, _schedule: SCHEDULER = None
) -> TS[SCALAR]:
    """
    Converts TSL values to a stream of individual SCALAR values.
    """
    if ts.modified:
        _state.buffer.extend([v.value for v in ts.modified_values()])

    if _state.buffer:
        d: deque[SCALAR] = _state.buffer
        v = d.popleft()
        if d:
            _schedule.schedule(MIN_TD)
        return v
