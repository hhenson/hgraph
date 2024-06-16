from collections import deque
from typing import Tuple, Type, Set

from hgraph import (
    compute_node,
    combine,
    TSL,
    SIZE,
    TS,
    SCALAR,
    DEFAULT,
    OUT,
    collect,
    TS_OUT,
    emit,
    STATE,
    SCHEDULER,
    MIN_TD,
    TSB,
    TS_SCHEMA,
    HgTupleFixedScalarType,
    SIGNAL,
    HgTypeMetaData,
    convert,
    TSS,
    graph,
)

__all__ = ()

from hgraph._impl._operators._conversion_operators._conversion_operator_util import _BufferState


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type == TS[Tuple] or m[OUT].matches_type(TS[Tuple[m[SCALAR], ...]]),
)
def convert_ts_to_tuple(ts: TS[SCALAR], to: Type[OUT] = DEFAULT[OUT]) -> OUT:
    return (ts.value,)


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type == TS[Tuple] or m[OUT].matches_type(TS[Tuple[m[SCALAR], ...]]),
)
def convert_set_to_tuple(ts: TS[Set[SCALAR]], to: Type[OUT] = DEFAULT[OUT]) -> OUT:
    return tuple(ts.value)


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type == TS[Tuple] or m[OUT].matches_type(TS[Tuple[m[SCALAR], ...]]),
)
def convert_tss_to_tuple(ts: TSS[SCALAR], to: Type[OUT] = DEFAULT[OUT]) -> OUT:
    return tuple(ts.value)


@graph(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type == TS[Tuple] or m[OUT].matches_type(TS[Tuple[m[SCALAR], ...]]),
)
def convert_tsl_to_tuple(ts: TSL[TS[SCALAR], SIZE], to: Type[OUT] = DEFAULT[OUT], __strict__: bool = True) -> OUT:
    return combine[to](tsl=ts, __strict__=__strict__)


@compute_node(
    overloads=combine,
    requires=lambda m, s: m[OUT].py_type == TS[Tuple],
    all_valid=lambda m, s: ("tsl",) if s["__strict__"] else None,
)
def combine_tuple_generic(*tsl: TSL[TS[SCALAR], SIZE], __strict__: bool = True) -> TS[Tuple[SCALAR, ...]]:
    return tuple(v.value for v in tsl)


@compute_node(
    overloads=combine,
    requires=lambda m, s: HgTypeMetaData.parse_type(TS[Tuple[m[SCALAR], ...]]).matches(m[OUT]),
    all_valid=lambda m, s: ("tsl",) if s["__strict__"] else None,
)
def combine_tuple_specific(*tsl: TSL[TS[SCALAR], SIZE], __strict__: bool = True) -> OUT:
    return tuple(v.value for v in tsl)


@compute_node(
    overloads=combine,
    requires=lambda m, s: isinstance(m[OUT].value_scalar_tp, HgTupleFixedScalarType),
    all_valid=lambda m, s: ("tsl",) if s["__strict__"] else None,
)
def combine_tuple_specific_nonuniform(*tsl: TSB[TS_SCHEMA], __strict__: bool = True) -> OUT:
    return tuple(v.value for v in tsl.values())


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type == TS[Tuple] or m[OUT].matches_type(TS[Tuple[m[SCALAR].py_type, ...]]),
    valid=("ts",),
)
def collect_tuple(
    ts: TS[SCALAR], *, reset: SIGNAL = None, tp_: Type[OUT] = DEFAULT[OUT], _output: TS_OUT[Tuple[SCALAR, ...]] = None
) -> TS[Tuple[SCALAR, ...]]:
    prev_value = _output.value if _output.valid and not reset.modified else ()
    new_value = (ts.value,) if ts.modified else ()
    return prev_value + new_value


@compute_node(overloads=emit)
def emit_tuple(
    ts: TS[tuple[SCALAR, ...]], _state: STATE[_BufferState] = None, _schedule: SCHEDULER = None
) -> TS[SCALAR]:
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
