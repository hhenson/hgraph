from collections import defaultdict
from dataclasses import field, dataclass
from statistics import stdev, variance
from typing import Type, cast, Tuple, Set

from hgraph import union, ts_schema
from hgraph._impl._types._ref import PythonTimeSeriesReference
from hgraph._operators import (
    add_,
    bit_and,
    bit_or,
    bit_xor,
    collapse_keys,
    contains_,
    div_,
    eq_,
    flip,
    flip_keys,
    getattr_,
    getitem_,
    is_empty,
    keys_,
    len_,
    max_,
    mean,
    merge,
    min_,
    not_,
    partition,
    rekey,
    std,
    str_,
    sub_,
    sum_,
    uncollapse_keys,
    unpartition,
    var,
    zero,
)
from hgraph._types._frame_scalar_type_meta_data import SCHEMA
from hgraph._types._ref_type import REF, TimeSeriesReferenceOutput
from hgraph._types._scalar_types import SCALAR, STATE, CompoundScalar, NUMBER
from hgraph._types._time_series_types import TIME_SERIES_TYPE, OUT, K_1, V
from hgraph._types._ts_type import TS
from hgraph._types._tsb_type import TSB, TS_SCHEMA
from hgraph._types._tsd_type import TSD, K, REMOVE_IF_EXISTS, TSD_OUT
from hgraph._types._tsl_type import TSL, SIZE
from hgraph._types._tss_type import TSS
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._wiring._decorators import compute_node, graph
from hgraph._wiring._map import map_
from hgraph._wiring._reduce import reduce

__all__ = tuple()


@dataclass
class KeyValueRefState:
    reference: object = field(default_factory=object)
    tsd: TSD[SCALAR, TIME_SERIES_TYPE] | None = None
    key: SCALAR | None = None


@compute_node(overloads=getitem_, valid=("key",))
def tsd_get_item_default(
    ts: REF[TSD[K, TIME_SERIES_TYPE]],
    key: TS[K],
    _ref: REF[TIME_SERIES_TYPE] = None,
    _ref_ref: REF[TIME_SERIES_TYPE] = None,
    _value_tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
    _state: STATE[KeyValueRefState] = None,
) -> REF[TIME_SERIES_TYPE]:
    """
    Returns the time-series associated to the key provided.
    """
    # Use tsd as a reference to avoid the cost of the input wrapper
    # If we got here something was modified so release any previous value and replace
    if ts.modified or key.modified:
        if _state.tsd is not None:
            _ref.make_passive()
            _state.tsd.release_ref(_state.key, _state.reference)
        if ts.valid and ts.value.valid:
            _state.tsd = ts.value.output
            _state.key = key.value
        else:
            _state.tsd = None
            _state.key = None
        if _state.tsd is not None:
            output = _state.tsd.get_ref(_state.key, _state.reference)
            _ref.bind_output(output)
            _ref.make_active()

    # This is required if tsd is a TSD of references, the TIME_SERIES_TYPE is captured dereferenced so
    # we cannot tell if we got one, but in that case tsd_get_ref will return a reference to reference
    # and the below 'if' deals with that by subscribing to the inner reference too
    if _ref.modified and _ref.value.has_peer and isinstance(_ref.value.output, TimeSeriesReferenceOutput):
        _ref_ref.bind_output(_ref.value.output)
        _ref_ref.make_active()

    result = _ref_ref.value if _ref_ref.bound else _ref.value
    if result is None or not ts.value.valid:
        # We can't have a valid ref if the ts value is not valid.
        result = PythonTimeSeriesReference()
    return result


@compute_node(overloads=getitem_, valid=())
def tsd_get_items(
    ts: REF[TSD[K, TIME_SERIES_TYPE]],
    key: TSS[K],
    _ref: TSD[K, REF[TIME_SERIES_TYPE]] = None,
    _ref_ref: TSD[K, REF[TIME_SERIES_TYPE]] = None,
    _value_tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
    _state: STATE[KeyValueRefState] = None,
) -> TSD[K, REF[TIME_SERIES_TYPE]]:
    """
    Returns TSD of the time-series associated to the keys provided.
    """
    # Use tsd as a reference to avoid the cost of the input wrapper
    # If we got here something was modified so release any previous value and replace
    if ts.modified:
        if _state.tsd is not None:
            for k in _state.key:
                _ref.on_key_removed(k)
                _ref_ref.on_key_removed(k)
                _state.tsd.release_ref(k, _state.reference)

        if ts.valid and ts.value.valid:
            _state.tsd = ts.value.output
            _state.key = (key.value - key.added()) if key.valid else set()
        else:
            _state.tsd = None
            _state.key = set()

        if _state.tsd is not None:
            for k in _state.key:
                output = _state.tsd.get_ref(k, _state.reference)
                _ref._create(k)
                _ref[k].bind_output(output)
                _ref[k].make_active()

    if _state.tsd is None or not key.valid:
        return

    for k in key.added():
        output = _state.tsd.get_ref(k, _state.reference)
        _ref._create(k)
        _ref[k].bind_output(output)
        _ref[k].make_active()

    out = {}

    for k in key.removed():
        _state.tsd.release_ref(k, _state.reference)
        _ref.on_key_removed(k)
        _ref_ref.on_key_removed(k)
        out[k] = REMOVE_IF_EXISTS

    # This is required if tsd is a TSD of references, the TIME_SERIES_TYPE is captured dereferenced so
    # we cannot tell if we got one, but in that case tsd_get_ref will return a reference to reference
    # and the below 'if' deals with that by subscribing to the inner reference too
    for k, v in _ref.modified_items():
        if k in _state.tsd.key_set.removed():
            out[k] = REMOVE_IF_EXISTS
        elif v.value.has_peer and isinstance(v.value.output, TimeSeriesReferenceOutput):
            _ref_ref._create(k)
            _ref_ref[k].bind_output(v.value.output)
            _ref_ref[k].make_active()
        elif v.value.valid or k in _state.tsd.key_set:
            out[k] = v.value

    for k, v in _ref_ref.modified_items():
        out[k] = v.value

    return out


@compute_node(
    overloads=keys_,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[K].py_type]),
    resolvers={OUT: lambda m, s: TSS[m[K].py_type]},
)
def keys_tsd_as_tss(tsd: REF[TSD[K, TIME_SERIES_TYPE]]) -> REF[TSS[K]]:
    # Use tsd as a reference to avoid the cost of the input wrapper
    # If we got here the TSD got rebound so get the key set and return
    if tsd.value.valid:
        return cast(REF, PythonTimeSeriesReference(tsd.value.output.key_set))
    else:
        return cast(REF, PythonTimeSeriesReference())


@compute_node(
    overloads=keys_,
    requires=lambda m, s: m[OUT].py_type in (TS[Set], TS[set], TS[frozenset])
    or m[OUT].matches_type(TS[Set[m[K].py_type]]),
)
def keys_tsd_as_set(tsd: TSD[K, TIME_SERIES_TYPE]) -> TS[Set[K]]:
    return set(tsd.keys())


@graph(overloads=contains_)
def contains_tsd(ts: TSD[K, TIME_SERIES_TYPE], item: TS[K]) -> TS[bool]:
    """
    Contains for TSD delegates to the key-set contains
    """
    return contains_(ts.key_set, item)


@graph(overloads=not_)
def not_tsd(ts: TSD[K, TIME_SERIES_TYPE]) -> TS[bool]:
    return not_(ts.key_set)


@graph(overloads=is_empty)
def is_empty_tsd(ts: TSD[K, TIME_SERIES_TYPE]) -> TS[bool]:
    return is_empty(ts.key_set)


@compute_node(overloads=len_)
def len_tsd(ts: TSD[K, TIME_SERIES_TYPE]) -> TS[int]:
    return len(ts)


@graph(overloads=sub_)
def sub_tsds(lhs: TSD[K, TIME_SERIES_TYPE], rhs: TSD[K, TIME_SERIES_TYPE]) -> TSD[K, TIME_SERIES_TYPE]:
    keys = lhs.key_set - rhs.key_set
    return tsd_get_items(lhs, keys)


@graph(overloads=bit_and)
def bit_and_tsds(lhs: TSD[K, TS[SCALAR]], rhs: TSD[K, TS[SCALAR]]) -> TSD[K, TS[SCALAR]]:
    keys = lhs.key_set & rhs.key_set
    return tsd_get_items(lhs, keys)


@graph(overloads=bit_or)
def bit_or_tsds(lhs: TSD[K, TS[SCALAR]], rhs: TSD[K, TS[SCALAR]]) -> TSD[K, TS[SCALAR]]:
    return merge(lhs, rhs)


@graph(overloads=bit_xor)
def bit_xor_tsds(lhs: TSD[K, TS[SCALAR]], rhs: TSD[K, TS[SCALAR]]) -> TSD[K, TS[SCALAR]]:
    keys = lhs.key_set ^ rhs.key_set
    return merge(tsd_get_items(lhs, keys), tsd_get_items(rhs, keys))


@compute_node(overloads=eq_)
def eq_tsds(lhs: TSD[K, TS[SCALAR]], rhs: TSD[K, TS[SCALAR]]) -> TS[bool]:
    # TODO - optimise this
    return lhs.value == rhs.value


def get_schema_type(schema: Type[TS_SCHEMA], key: str) -> Type[TIME_SERIES_TYPE]:
    return schema[key].py_type


@compute_node(
    overloads=getattr_,
    resolvers={TIME_SERIES_TYPE: lambda mapping, scalars: get_schema_type(mapping[TS_SCHEMA], scalars["key"])},
)
def tsd_get_bundle_item(
    tsd: TSD[K, REF[TSB[TS_SCHEMA]]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE
) -> TSD[K, REF[TIME_SERIES_TYPE]]:
    """
    Returns a TSD of the given items from the bundles in the original TSD
    """
    out = {}
    for k, v in tsd.modified_items():
        if v.value.valid:
            if v.value.has_peer:
                out[k] = PythonTimeSeriesReference(v.value.output[key])
            else:
                out[k] = v.value.items[_schema._schema_index_of(key)]
        else:
            out[k] = PythonTimeSeriesReference()

    for k in tsd.removed_keys():
        out[k] = REMOVE_IF_EXISTS

    return out


@compute_node(
    overloads=getattr_,
    resolvers={SCALAR: lambda mapping, scalars: get_schema_type(mapping[SCHEMA].meta_data_schema, scalars["key"])},
)
def tsd_get_cs_item(tsd: TSD[K, TS[SCHEMA]], key: str, _schema: Type[SCHEMA] = AUTO_RESOLVE) -> TSD[K, TS[SCALAR]]:
    """
    Returns a TSD of the given items from the compound scalars in the original TSD
    """
    out = {}
    for k, v in tsd.modified_items():
        if v.valid:
            out[k] = getattr(v.value, key)

    for k in tsd.removed_keys():
        out[k] = REMOVE_IF_EXISTS

    return out


@compute_node(overloads=collapse_keys)
def collapse_keys_tsd(ts: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]]) -> TSD[Tuple[K, K_1], REF[TIME_SERIES_TYPE]]:
    """
    Collapse the nested TSDs to a TSD with a tuple key.
    """
    out = {}

    for k, v in ts.removed_items():
        out.update({(k, k1): REMOVE_IF_EXISTS for k1 in v.keys()})

    for k, v in ts.modified_items():
        out.update({(k, k1): v1.value for k1, v1 in v.modified_items()})
        out.update({(k, k1): REMOVE_IF_EXISTS for k1 in v.removed_keys()})

    return out


@compute_node(overloads=uncollapse_keys)
def uncollapse_keys_tsd(
    ts: TSD[Tuple[K, K_1], REF[TIME_SERIES_TYPE]], _output: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]] = None
) -> TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]]:
    """
    Un-Collapse the nested TSDs to a TSD with a tuple key.
    """
    out = defaultdict(dict)

    removed_keys = defaultdict(set)
    for k in ts.removed_keys():
        removed_keys[k[0]].add(k[1])
        out[k[0]][k[1]] = REMOVE_IF_EXISTS

    removed = set()
    for k, v in removed_keys.items():
        if v == _output[k].key_set.value:
            removed.add(k)

    for k, v in ts.modified_items():
        if k[0] in removed:
            removed.remove(k[0])
        out[k[0]][k[1]] = v.value

    for k in removed:
        out[k] = REMOVE_IF_EXISTS

    return out


@dataclass
class TsdRekeyState(CompoundScalar):
    prev: dict = field(default_factory=dict)  # Copy of previous ticks


@compute_node(overloads=rekey, valid=("new_keys",))
def rekey_tsd(
    ts: TSD[K, REF[TIME_SERIES_TYPE]], new_keys: TSD[K, TS[K_1]], _state: STATE[TsdRekeyState] = None
) -> TSD[K_1, REF[TIME_SERIES_TYPE]]:
    """
    Rekey a TSD to the new keys.

    The expectation is that the set of new keys are distinct producing a 1-1 mapping.
    """
    out = {}
    prev = _state.prev

    # Clear up existing mapping before we track new key mappings
    for k in ts.removed_keys():
        k_new = prev.get(k)
        if k_new is not None:
            out[k_new] = REMOVE_IF_EXISTS

    # Track changes in new keys
    for ts_key in new_keys.removed_keys():
        prev_key = prev.pop(ts_key, None)
        if prev_key is not None:
            out[prev_key] = REMOVE_IF_EXISTS
    for ts_key, new_key in new_keys.modified_items():
        new_key = new_key.value  # Get the value from the ts
        prev_key = prev.get(ts_key, None)
        if prev_key is not None and new_key != prev_key:
            out[prev_key] = REMOVE_IF_EXISTS
        prev[ts_key] = new_key
        v = ts.get(ts_key)
        if v is not None:
            out[new_key] = v.value

    for k, v in ts.modified_items() if ts.valid else ():
        k_new = prev.get(k, None)
        if k_new is not None:
            out[k_new] = v.value

    return out if out else None


@compute_node(overloads=flip)
def flip_tsd(ts: TSD[K, TS[K_1]], _state: STATE[TsdRekeyState] = None) -> TSD[K_1, TS[K]]:
    """
    Flip the TSD to have the time-series as the key and the key as the time-series.
    """

    out = {}
    prev = _state.prev

    # Clear up existing mapping before we track new key mappings
    for k in ts.removed_keys():
        k_new = prev.pop(k, None)
        if k_new is not None:
            out[k_new] = REMOVE_IF_EXISTS

    for k, v in ts.modified_items():
        v = v.value
        k_new = prev.pop(k, None)
        if k_new is not None:
            out[k_new] = REMOVE_IF_EXISTS
        out[v] = k
        prev[k] = v

    return out


@compute_node(overloads=flip, requires=lambda m, s: s["unique"] is False)
def flip_tsd_non_unique(
    ts: TSD[K, TS[K_1]], unique: bool, _state: STATE[TsdRekeyState] = None, _output: TSD_OUT[K_1, TSS[K]] = None
) -> TSD[K_1, TSS[K]]:
    """
    Flip the TSD to have the time-series as the key and the key as the time-series. Collect keys for duplicate values into TSS
    """
    prev = _state.prev

    # Clear up existing mapping before we track new key mappings
    for k in ts.removed_keys():
        k_old = prev.pop(k, None)
        if k_old is not None:
            _output[k_old].remove(k)

    for k, v in ts.modified_items():
        v = v.value
        k_old = prev.pop(k, None)
        if k_old is not None and k_old != v:
            _output[k_old].remove(k)
        _output.get_or_create(v).add(k)
        prev[k] = v

    drop = {k for k, v in _output.modified_items() if not v}
    for k in drop:
        del _output[k]


@compute_node(overloads=flip_keys)
def flip_keys_tsd(
    ts: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]], _output: TSD[K_1, TSD[K, REF[TIME_SERIES_TYPE]]] = None
) -> TSD[K_1, TSD[K, REF[TIME_SERIES_TYPE]]]:
    """
    Switch the keys on a TSD of TSD's. This can be considered as a pivot.
    """
    ts = ts.value
    prev = _output.value

    # Create the target dictionary
    new = defaultdict(dict)
    for k, v in ts.items():
        for k1, v1 in v.items():
            new[k1][k] = v1

    # Now work out the delta
    out = defaultdict(dict)
    for k_n, inner_n in new.items():
        if inner_prev := prev.get(k_n):
            for k1_n, ref_n in inner_n.items():
                if (ref_prev := inner_prev.get(k1_n)) is None or ref_n != ref_prev:
                    out[k_n][k1_n] = ref_n
            for k1_p in inner_prev.keys():
                if k1_p not in inner_n:
                    out[k_n][k1_p] = REMOVE_IF_EXISTS
        else:
            out[k_n] = inner_n
    for k_p in prev.keys():
        if k_p not in new:
            out[k_p] = REMOVE_IF_EXISTS

    return out


@graph
def _re_index(
    key: TS[K],
    tsl: REF[TSL[TSD[K, TIME_SERIES_TYPE], SIZE]],
    _sz: type[SIZE] = AUTO_RESOLVE,
    _v_tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
) -> TSL[TIME_SERIES_TYPE, SIZE]:
    return TSL[_v_tp, _sz].from_ts(*[tsl[i][key] for i in range(_sz.SIZE)])


@graph
def _merge(tsl: TSL[TIME_SERIES_TYPE, SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TIME_SERIES_TYPE:
    return merge(*[tsl[i] for i in range(_sz.SIZE)])


@graph(overloads=merge)
def merge_tsd(
    *tsl: TSL[TSD[K, TIME_SERIES_TYPE], SIZE],
    _k_tp: type[K] = AUTO_RESOLVE,
    _v_tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
    _sz: type[SIZE] = AUTO_RESOLVE,
) -> TSD[K, TIME_SERIES_TYPE]:
    """
    Merge TSD elements together
    """
    keys = union(*[tsd.key_set for tsd in tsl])
    re_index = map_(_re_index, tsl, __keys__=keys)
    return map_(_merge, re_index)


@compute_node(overloads=partition)
def partition_tsd(
    ts: TSD[K, REF[TIME_SERIES_TYPE]], partitions: TSD[K, TS[K_1]], _state: STATE[TsdRekeyState] = None
) -> TSD[K_1, TSD[K, REF[TIME_SERIES_TYPE]]]:
    """
    Partition a TSD into partitions by the given mapping.
    """
    out = defaultdict(dict)
    prev = _state.prev

    # Clear up existing mapping before we track new key mappings
    for k in ts.removed_keys():
        partition = prev.get(k)
        if partition is not None:
            out[partition][k] = REMOVE_IF_EXISTS

    # Track changes in partitions
    for k, partition in partitions.removed_items():
        out[partition.value][k] = REMOVE_IF_EXISTS

    for k, partition in partitions.modified_items():
        partition = partition.value
        prev_partition = prev.get(k, None)
        if prev_partition is not None and partition != prev_partition:
            out[prev_partition][k] = REMOVE_IF_EXISTS
        prev[k] = partition
        v = ts.get(k)
        if v is not None:
            out[partition][k] = v.value

    for k, v in ts.modified_items():
        partition = prev.get(k, None)
        if partition is not None:
            out[partition][k] = v.value

    return out


@compute_node(overloads=unpartition)
def unpartition_tsd(tsd: TSD[K_1, TSD[K, REF[TIME_SERIES_TYPE]]]) -> TSD[K, REF[TIME_SERIES_TYPE]]:
    """
    Union of TSDs - given
    """
    out = {}
    removed = {}
    for k, v in tsd.modified_items():
        out |= {k: v.delta_value for k, v in v.modified_items()}
        removed |= {k: REMOVE_IF_EXISTS for k in v.removed_keys()}

    return removed | out


@graph(overloads=zero)
def zero_tsd(ts: Type[TSD[SCALAR, TIME_SERIES_TYPE]], op: object) -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    This is a helper generator to create a zero time-series for the reduce function.
    """
    from hgraph import nothing

    return nothing(ts)


@compute_node(overloads=min_)
def min_tsd_unary(tsd: TSD[K, V], tp: Type[V] = AUTO_RESOLVE) -> V:
    return reduce(min_, tsd, zero(tp, min_))


@compute_node(overloads=min_)
def min_tsd_unary_number(tsd: TSD[K, TS[NUMBER]], default_value: TS[NUMBER] = None) -> TS[NUMBER]:
    return min((v.value for v in tsd.valid_values()), default=default_value.value)


@compute_node(overloads=max_)
def max_tsd_unary(tsd: TSD[K, V], tp: Type[V] = AUTO_RESOLVE) -> V:
    return reduce(max_, tsd, zero(tp, max_))


@compute_node(overloads=max_)
def max_tsd_unary_number(tsd: TSD[K, TS[NUMBER]], default_value: TS[NUMBER] = None) -> TS[NUMBER]:
    return max((v.value for v in tsd.valid_values()), default=default_value.value)


@graph(overloads=sum_)
def sum_tsd_unary(tsd: TSD[K, V], tp: Type[V] = AUTO_RESOLVE) -> V:
    return reduce(add_, tsd, zero(tp, add_))


@graph(overloads=sum_)
def sum_tsd_unary_number(tsd: TSD[K, TS[NUMBER]], tp: Type[TS[NUMBER]] = AUTO_RESOLVE) -> TS[NUMBER]:
    return _sum_tsd_unary(tsd, zero(tp, sum_))


@compute_node
def _sum_tsd_unary(tsd: TSD[K, TS[NUMBER]], zero_ts: TS[NUMBER]) -> TS[NUMBER]:
    return sum((v.value for v in tsd.valid_values()), start=zero_ts.value)


@graph(overloads=mean)
def mean_tsd_unary_number(ts: TSD[K, TS[NUMBER]]) -> TS[float]:
    from hgraph import DivideByZero, default

    return default(div_(sum_(ts), len_(ts), divide_by_zero=DivideByZero.NAN), float("NaN"))


@compute_node(overloads=std)
def std_tsd_unary_number(tsd: TSD[K, TS[NUMBER]]) -> TS[float]:
    values = [v.value for v in tsd.valid_values()]
    if len(values) <= 1:
        return 0.0
    else:
        return float(stdev(values))


@compute_node(overloads=var)
def var_tsd_unary_number(tsd: TSD[K, TS[NUMBER]]) -> TS[float]:
    values = [v.value for v in tsd.valid_values()]
    if len(values) <= 1:
        return 0.0
    else:
        return float(variance(values))


@compute_node(overloads=str_)
def str_tsd(tsd: TSD[K, V]) -> TS[str]:
    return str(dict(tsd.value))
