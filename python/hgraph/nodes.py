"""hgraph.nodes - helper nodes (hgraph parity; python impls as upstream)."""
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, Type

from ._wiring import compute_node, graph, wire, REMOVE_IF_EXISTS, operator_function
from ._types import (Array, SIZE, TS, TSB, TSD, TSS, K, K_1,
                     COMPOUND_SCALAR, NUMBER, SCALAR, TIME_SERIES_TYPE,
                     TIME_SERIES_TYPE_1, TimeSeriesSchema)

__all__ = (
    "NpRollingWindowResult", "NpRollingWindowState", "np_rolling_window",
    "np_quantile", "np_std", "pct_change", "rolling_window",
    "rolling_average", "make_tsd", "make_tsd_scalar", "flatten_tsd",
    "extract_tsd", "keys_where_true", "where_true", "flatten_tsl_values",
    "tsl_to_tsd",
    "request_id"
)


request_id = operator_function("request_id")


@dataclass
class NpRollingWindowResult(TimeSeriesSchema, Generic[SCALAR, SIZE]):
    buffer: TS[Array[SCALAR, SIZE]]
    index: TS[Array[datetime, SIZE]]


@dataclass
class NpRollingWindowState:
    """Compatibility state shape retained for code that imports it directly."""

    capacity: int = None
    buffer: Array[SCALAR] = None
    index: Array[datetime] = None
    start: int = 0
    length: int = 0


_to_window_native = operator_function("to_window")
_rolling_window_arrays_native = operator_function("rolling_window_arrays")
_quantile_native = operator_function("quantile")
_std_native = operator_function("np_std")
pct_change = operator_function("pct_change")
rolling_window = operator_function("window")
rolling_average = operator_function("rolling_average")


@graph
def np_rolling_window(ts: TS[SCALAR], period: SIZE,
                      min_window_period: int = None) -> TSB[NpRollingWindowResult]:
    """Return native shaped arrays for a tick window's values and timestamps."""
    window = (_to_window_native(ts, period) if min_window_period is None
              else _to_window_native(ts, period, min_window_period))
    return _rolling_window_arrays_native(window)


np_quantile = _quantile_native
np_std = _std_native


def _requires_python_descriptor(mapping, attr):
    """Select the Python fallback only for non-storage attributes.

    Stored CompoundScalar fields are native Bundle projections. A descriptor
    such as hg_oap's ExprClass fields is evaluated on the reconstructed Python
    object because it has no C++ storage field to project.
    """
    value_type = mapping[COMPOUND_SCALAR]
    return attr not in {name for name, _ in value_type.fields}


@compute_node(overloads="getattr_", requires=_requires_python_descriptor)
def _getattr_compound_descriptor(
        ts: TS[COMPOUND_SCALAR], attr: str,
        default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    value = getattr(ts.value, attr, None)
    if value is not None:
        return value
    return default_value.value if default_value.valid else None


make_tsd = operator_function("make_tsd")
_const = operator_function("const")


@graph
def make_tsd_scalar(
        key: K_1,
        value: TIME_SERIES_TYPE,
        remove_key: TS[bool] = None,
        ts_type: Type[TIME_SERIES_TYPE_1] = TIME_SERIES_TYPE,
) -> TSD[K_1, TIME_SERIES_TYPE_1]:
    """Const-lift a scalar key and delegate TSD updates to the native node."""
    return make_tsd(_const(key), value, remove_key)


@compute_node
def flatten_tsd(tsd: TSD[object, TS[object]]) -> TS[object]:
    """A time-series of the TSD's delta dictionaries (frozendict values)."""
    from frozendict import frozendict

    return frozendict(tsd.delta_value)


@compute_node
def extract_tsd(ts: TS[object]) -> TSD[object, TS[object]]:
    """Extracts a TSD from a stream of delta dictionaries."""
    return dict(ts.value)


class _KeySubscripted:
    """upstream shape: helper[K: int] specializes the key type (the py
    node rebuilds with substituted annotations; cached per type)."""

    def __init__(self, builder):
        self._builder = builder
        self._cache = {}

    def _for(self, tp):
        if tp not in self._cache:
            self._cache[tp] = self._builder(tp)
        return self._cache[tp]

    def __getitem__(self, item):
        tp = item.stop if isinstance(item, slice) else item
        return self._for(tp)

    def __call__(self, *args, **kwargs):
        return self._for(K)(*args, **kwargs)


def _keys_where_true_for(tp):
    @compute_node
    def keys_where_true(ts: TSD[tp, TS[bool]]) -> TSS[tp]:
        from ._wiring import Removed

        delta = set()
        for key in ts.removed_keys():
            delta.add(Removed(key))
        for key, value in ts.modified_items():
            if value.value:
                delta.add(key)
            else:
                delta.add(Removed(key))
        return delta

    return keys_where_true


def _where_true_for(tp):
    @compute_node
    def where_true(ts: TSD[tp, TS[bool]]) -> TSD[tp, TS[bool]]:
        from ._wiring import REMOVE_IF_EXISTS

        out = {}
        for key, value in ts.modified_items():
            if value.value:
                out[key] = value.value
            else:
                out[key] = REMOVE_IF_EXISTS
        for key in ts.removed_keys():
            out[key] = REMOVE_IF_EXISTS
        return out

    return where_true


keys_where_true = _KeySubscripted(_keys_where_true_for)
where_true = _KeySubscripted(_where_true_for)


def tsl_to_tsd(tsl, keys: tuple = None):
    """upstream shape: tsl_to_tsd(tsl, keys) - convert a TSL to a TSD with
    the given keys (modified elements only, hgraph parity)."""
    from ._wiring import wire

    return wire("combine_tsd", tuple(keys), *[tsl[i] for i in range(len(keys))], __strict__=False)


from ._wiring import compute_node as _compute_node
from ._types import TIME_SERIES_TYPE as _TST


@_compute_node
def pass_through_node(ts: _TST) -> _TST:
    """hgraph's pass_through_node: forward each tick unchanged."""
    return ts.delta_value


class _FlattenTslValues:
    """hgraph's flatten_tsl_values: a TSL as a TS of tuples - rides the
    tuple-combine kernel (strict = all_valid; non-strict leaves None holes).
    Subscription (``flatten_tsl_values[SCALAR: int]``) is accepted for
    upstream parity; the C++ registry infers the tuple type from the wired
    TSL, so the pin carries no extra information."""

    def __getitem__(self, _item):
        return self

    def __call__(self, tsl, all_valid: bool = False):
        from . import combine
        from ._types import TS
        from typing import Tuple

        return combine[TS[Tuple]](*tsl, __strict__=bool(all_valid))


flatten_tsl_values = _FlattenTslValues()
