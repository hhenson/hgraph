"""hgraph.nodes - helper nodes (hgraph parity; python impls as upstream)."""
from typing import Type

from ._wiring import compute_node, graph, wire, REMOVE_IF_EXISTS, operator_function
from ._types import (TS, TSD, TSS, K, K_1,
                     COMPOUND_SCALAR, NUMBER, SCALAR, TIME_SERIES_TYPE,
                     TIME_SERIES_TYPE_1)
from ._analytics_compat import (
    np_quantile, np_rolling_window, np_std, pct_change, rolling_average,
)

__all__ = (
    "np_quantile", "np_rolling_window", "np_std", "pct_change",
    "rolling_average", "rolling_window", "make_tsd", "make_tsd_scalar", "flatten_tsd",
    "extract_tsd", "keys_where_true", "where_true", "flatten_tsl_values",
    "tsl_to_tsd",
    "request_id"
)


request_id = operator_function("request_id")


rolling_window = operator_function("window")
def _requires_python_descriptor(mapping, attr):
    """Select the Python fallback for non-storage or retyped attributes.

    Stored CompoundScalar fields are native Bundle projections. A descriptor
    such as hg_oap's ExprClass fields is evaluated on the reconstructed Python
    object because it has no C++ storage field to project. An explicitly
    requested output type that differs from the declared field type also reads
    the Python object, preserving the legacy pre-inflation accessor behavior.
    """
    value_type = mapping[COMPOUND_SCALAR]
    field_type = dict(value_type.fields).get(attr)
    return field_type is None or field_type != mapping[SCALAR]


def _python_descriptor_type(mapping, attr):
    import dataclasses
    import inspect
    import typing

    from ._types import _value_type_python_type

    scalar_type = _value_type_python_type(mapping[COMPOUND_SCALAR])
    annotation = typing.get_type_hints(scalar_type).get(attr)
    if isinstance(annotation, dataclasses.InitVar):
        return annotation.type
    if annotation is not None:
        return annotation

    descriptor = inspect.getattr_static(scalar_type, attr, None)
    if isinstance(descriptor, property) and descriptor.fget is not None:
        return typing.get_type_hints(descriptor.fget).get("return", object)
    raise AttributeError(
        f"{scalar_type.__module__}.{scalar_type.__qualname__} has no typed "
        f"attribute '{attr}'")


@compute_node(
    overloads="getattr_",
    requires=_requires_python_descriptor,
    resolvers={SCALAR: _python_descriptor_type},
)
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
