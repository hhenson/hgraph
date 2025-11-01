from collections import deque
from typing import Type, Tuple, Set

from hgraph import AUTO_RESOLVE
from hgraph._impl._operators._conversion_operators._conversion_operator_util import _BufferState
from hgraph._operators._time_series_conversion import convert, combine, emit, collect
from hgraph._runtime._constants import MIN_TD
from hgraph._runtime._node import SCHEDULER
from hgraph._types._scalar_types import SCALAR, STATE, DEFAULT
from hgraph._types._time_series_types import OUT, SIGNAL, TIME_SERIES_TYPE
from hgraph._types._ts_type import TS, TS_OUT
from hgraph._types._tsl_type import TSL, SIZE
from hgraph._types._tss_type import TSS, TSS_OUT, set_delta
from hgraph._wiring._decorators import compute_node

_all__ = tuple()


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
)
def convert_ts_to_tss(
    ts: TS[SCALAR], to: Type[OUT] = DEFAULT[OUT], _output: TSS_OUT[SCALAR] = None, _tp: type[SCALAR] = AUTO_RESOLVE
) -> TSS[SCALAR]:
    return set_delta({ts.value}, _output.value if _output.valid else set(), _tp)


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
)
def convert_tuple_to_tss(
    ts: TS[Tuple[SCALAR, ...]],
    to: Type[OUT] = DEFAULT[OUT],
    _output: TSS_OUT[SCALAR] = None,
    _tp: type[SCALAR] = AUTO_RESOLVE,
) -> TSS[SCALAR]:
    prev = _output.value if _output.valid else set()
    new = set(ts.value)
    return set_delta(new - prev, prev - new, _tp)


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
)
def convert_set_to_tss(
    ts: TS[Set[SCALAR]], to: Type[OUT] = DEFAULT[OUT], _output: TSS_OUT[SCALAR] = None, _tp: type[SCALAR] = AUTO_RESOLVE
) -> TSS[SCALAR]:
    prev = _output.value if _output.valid else set()
    new = ts.value
    return set_delta(new - prev, prev - new, _tp)


@compute_node(
    overloads=combine,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
    resolvers={SCALAR: lambda m, s: m[TIME_SERIES_TYPE].scalar_type()},
)
def combine_tss(
    *tsl: TSL[TIME_SERIES_TYPE, SIZE],
    to: Type[OUT] = DEFAULT[OUT],
    _output: TSS_OUT[SCALAR] = None,
    _tp: type[SCALAR] = AUTO_RESOLVE,
) -> TSS[SCALAR]:
    prev = _output.value if _output.valid else set()
    new = {v.value for v in tsl.valid_values()}
    return set_delta(new - prev, prev - new, _tp)


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
    valid=("ts",),
)
def collect_tss_from_ts(
    ts: TS[SCALAR],
    *,
    reset: SIGNAL = None,
    tp_: Type[OUT] = DEFAULT[OUT],
    _output: TS_OUT[Set[SCALAR]] = None,
    _tp: type[SCALAR] = AUTO_RESOLVE,
) -> TSS[SCALAR]:
    remove = _output.value if _output.valid and reset.modified else set()
    add = {ts.value} if ts.modified else set()
    return set_delta(add, remove, _tp)


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
    valid=("ts",),
)
def collect_tss_from_tuples(
    ts: TS[Tuple[SCALAR, ...]],
    *,
    reset: SIGNAL = None,
    tp_: Type[OUT] = DEFAULT[OUT],
    _output: TS_OUT[Set[SCALAR]] = None,
    _tp: type[SCALAR] = AUTO_RESOLVE,
) -> TSS[SCALAR]:
    remove = _output.value if _output.valid and reset.modified else set()
    new = set(ts.value) if ts.modified else set()
    return set_delta(new, remove, _tp)


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
    valid=("ts",),
)
def collect_tss_from_sets(
    ts: TS[Set[SCALAR]],
    *,
    reset: SIGNAL = None,
    tp_: Type[OUT] = DEFAULT[OUT],
    _output: TS_OUT[Set[SCALAR]] = None,
    _tp: type[SCALAR] = AUTO_RESOLVE,
) -> TSS[SCALAR]:
    remove = _output.value if _output.valid and reset.modified else set()
    new = ts.value if ts.modified else set()
    return set_delta(new, remove, _tp)


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
    valid=("tss",),
)
def collect_tss_from_tss(
    tss: TSS[SCALAR],
    *,
    reset: SIGNAL = None,
    tp_: Type[OUT] = DEFAULT[OUT],
    _output: TSS_OUT[SCALAR] = None,
    _tp: type[SCALAR] = AUTO_RESOLVE,
) -> TSS[SCALAR]:
    remove = _output.value if _output.valid and reset.modified else set()
    new = tss.value if tss.modified else set()
    return set_delta(new, remove, _tp)


@compute_node(overloads=emit)
def emit_tss(ts: TSS[SCALAR], _state: STATE[_BufferState] = None, _schedule: SCHEDULER = None) -> TS[SCALAR]:
    """
    Converts a tuple of SCALAR values in a stream of individual SCALAR values.
    """
    if ts.modified:
        _state.buffer.extend(ts.added())

    if _state.buffer:
        d: deque[SCALAR] = _state.buffer
        v = d.popleft()
        if d:
            _schedule.schedule(MIN_TD)
        return v
