from collections import deque
from typing import Tuple, Type

from hgraph import compute_node, combine, TSL, TIME_SERIES_TYPE, SIZE, TS, SCALAR, DEFAULT, OUT, collect, TS_OUT, \
    SIGNAL, OUT_1, HgTypeMetaData, emit, STATE, SCHEDULER, MIN_TD, TSB, TS_SCHEMA, HgTupleFixedScalarType

__all__ = ()

from hgraph.nodes._conversion_operators._conversion_operator_util import _BufferState


@compute_node(overloads=combine,
              requires=lambda m, s: m[OUT].py_type == TS[Tuple],
              all_valid=lambda m, s: ('tsl',) if s['__strict__'] else None)
def combine_tuple_generic(*tsl: TSL[TS[SCALAR], SIZE], __strict__: bool = True) -> TS[Tuple[SCALAR, ...]]:
    return tuple(v.value for v in tsl)


@compute_node(overloads=combine,
              requires=lambda m, s: HgTypeMetaData.parse_type(TS[Tuple[m[SCALAR], ...]]).matches(m[OUT]),
              all_valid=lambda m, s: ('tsl',) if s['__strict__'] else None)
def combine_tuple_specific(*tsl: TSL[TS[SCALAR], SIZE], __strict__: bool = True) -> OUT:
    return tuple(v.value for v in tsl)


@compute_node(overloads=combine,
              requires=lambda m, s: isinstance(m[OUT].value_scalar_tp, HgTupleFixedScalarType),
              all_valid=lambda m, s: ('tsl',) if s['__strict__'] else None)
def combine_tuple_specific_nonuniform(*tsl: TSB[TS_SCHEMA], __strict__: bool = True) -> OUT:
    return tuple(v.value for v in tsl.values())


@compute_node(overloads=collect,
              requires=lambda m, s: m[OUT].py_type == TS[Tuple] or m[OUT].matches_type(TS[Tuple[m[SCALAR].py_type, ...]]),
              valid=('ts',)
              )
def collect_tuple(ts: TS[SCALAR], *, reset: SIGNAL = None, tp_: Type[OUT] = DEFAULT[OUT],
                  _output: TS_OUT[Tuple[SCALAR, ...]] = None) -> TS[Tuple[SCALAR, ...]]:
    prev_value = _output.value if _output.valid and not reset.modified else ()
    new_value = (ts.value,) if ts.modified else ()
    return prev_value + new_value


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


