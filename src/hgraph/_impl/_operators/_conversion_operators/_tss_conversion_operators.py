from collections import deque
from typing import Type, Tuple, Set

from hgraph._operators._time_series_conversion import convert, combine, emit, collect
from hgraph._wiring._decorators import compute_node
from hgraph._types._time_series_types import OUT, SIGNAL, TIME_SERIES_TYPE
from hgraph._types._ts_type import TS, TS_OUT
from hgraph._types._tss_type import TSS, TSS_OUT
from hgraph._types._tsl_type import TSL, SIZE
from hgraph._types._scalar_types import SCALAR, STATE, DEFAULT
from hgraph._impl._types._tss import PythonSetDelta
from hgraph._runtime._node import SCHEDULER
from hgraph._runtime._constants import MIN_TD

from hgraph._impl._operators._conversion_operators._conversion_operator_util import _BufferState

_all__ = tuple()


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
)
def convert_ts_to_tss(ts: TS[SCALAR], to: Type[OUT] = DEFAULT[OUT], _output: TSS_OUT[SCALAR] = None) -> TSS[SCALAR]:
    return PythonSetDelta({ts.value}, _output.value if _output.valid else set())


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
)
def convert_tuple_to_tss(
    ts: TS[Tuple[SCALAR, ...]], to: Type[OUT] = DEFAULT[OUT], _output: TSS_OUT[SCALAR] = None
) -> TSS[SCALAR]:
    prev = _output.value if _output.valid else set()
    new = set(ts.value)
    return PythonSetDelta(new - prev, prev - new)


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
)
def convert_set_to_tss(
    ts: TS[Set[SCALAR]], to: Type[OUT] = DEFAULT[OUT], _output: TSS_OUT[SCALAR] = None
) -> TSS[SCALAR]:
    prev = _output.value if _output.valid else set()
    new = ts.value
    return PythonSetDelta(new - prev, prev - new)


@compute_node(
    overloads=combine,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
    resolvers={SCALAR: lambda m, s: m[TIME_SERIES_TYPE].scalar_type()},
)
def combine_tss(
    *tsl: TSL[TIME_SERIES_TYPE, SIZE], to: Type[OUT] = DEFAULT[OUT], _output: TSS_OUT[SCALAR] = None
) -> TSS[SCALAR]:
    prev = _output.value if _output.valid else set()
    new = {v.value for v in tsl.valid_values()}
    return PythonSetDelta(new - prev, prev - new)


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
    valid=("ts",),
)
def collect_tss_from_ts(
    ts: TS[SCALAR], *, reset: SIGNAL = None, tp_: Type[OUT] = DEFAULT[OUT], _output: TS_OUT[Set[SCALAR]] = None
) -> TSS[SCALAR]:
    remove = _output.value if _output.valid and reset.modified else set()
    add = {ts.value} if ts.modified else set()
    return PythonSetDelta(add, remove)


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
) -> TSS[SCALAR]:
    remove = _output.value if _output.valid and reset.modified else set()
    new = set(ts.value) if ts.modified else set()
    return PythonSetDelta(new, remove)


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[SCALAR].py_type]),
    valid=("ts",),
)
def collect_tss_from_sets(
    ts: TS[Set[SCALAR]], *, reset: SIGNAL = None, tp_: Type[OUT] = DEFAULT[OUT], _output: TS_OUT[Set[SCALAR]] = None
) -> TSS[SCALAR]:
    remove = _output.value if _output.valid and reset.modified else set()
    new = ts.value if ts.modified else set()
    return PythonSetDelta(new, remove)


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
