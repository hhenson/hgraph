from collections import defaultdict
from dataclasses import field, dataclass
from statistics import stdev, variance
from typing import Type, Mapping, cast, Tuple, Set

from hgraph import TS, SCALAR, TIME_SERIES_TYPE, TSD, compute_node, REMOVE_IF_EXISTS, REF, STATE, graph, contains_, \
    not_, K, TSS, PythonTimeSeriesReference, CompoundScalar, TS_SCHEMA, TSB, getattr_, zero, len_, AUTO_RESOLVE, TSL, \
    SIZE, TimeSeriesReferenceOutput, operator, is_empty, K_1, TIME_SERIES_TYPE_1, sub_, bit_or, bit_and, bit_xor, eq_, \
    keys_, OUT, rekey, flip, partition, flip_keys, collapse_keys, uncollapse_keys, min_, \
    max_, str_, sum_, V, merge, mean, NUMBER, div_, std, var

__all__ = ("make_tsd", "make_tsd_scalar", "flatten_tsd", "extract_tsd", "tsd_get_item", "tsd_get_bundle_item",
           "merge_nested_tsds", "get_schema_type", "tsd_get_items", "merge_tsds")


@operator
def make_tsd(key: TS[K_1], value: TIME_SERIES_TYPE, remove_key: TS[bool] = None,
             ts_type: Type[TIME_SERIES_TYPE_1] = TIME_SERIES_TYPE) -> TSD[K_1, TIME_SERIES_TYPE_1]:
    """
    Make a TSD from a time-series of key and value, if either key or value ticks an entry in the TSD will be
    created / update. It is also possible to remove a key by setting remove_key to True.
    In this scenario a key will be removed if the remove_key ticked True or if the key ticks and remove_key is already
    set to True.
    """


@compute_node(overloads=make_tsd, valid=("key",))
def make_tsd_default(key: TS[K_1], value: TIME_SERIES_TYPE, remove_key: TS[bool] = None,
                     ts_type: Type[TIME_SERIES_TYPE_1] = TIME_SERIES_TYPE) -> TSD[K_1, TIME_SERIES_TYPE_1]:
    """
    Make a TSD from a time-series of key and value, if either key or value ticks an entry in the TSD will be
    created / update. It is also possible to remove a key by setting remove_key to True.
    In this scenario a key will be removed if the remove_key ticked True or if the key ticks and remove_key is already
    set to True.
    """

    if remove_key.valid:
        if remove_key.value and remove_key.modified or key.modified:
            return {key.value: REMOVE_IF_EXISTS}
        elif key.modified or value.modified:
            return {key.value: value.delta_value}
    else:
        return {key.value: value.delta_value}


@graph(overloads=make_tsd)
def make_tsd_scalar(key: K_1, value: TIME_SERIES_TYPE, remove_key: TS[bool] = None,
                    ts_type: Type[TIME_SERIES_TYPE_1] = TIME_SERIES_TYPE) -> TSD[K_1, TIME_SERIES_TYPE_1]:
    from hgraph import const
    return make_tsd(const(key), value, remove_key, ts_type)


@compute_node
def flatten_tsd(tsd: TSD[K_1, TIME_SERIES_TYPE]) -> TS[Mapping[K_1, SCALAR]]:
    """
    Flatten a TSD into a time-series of frozen dicts (equivalent to the delta dictionary)
    """
    return tsd.delta_value


@compute_node
def extract_tsd(ts: TS[Mapping[K_1, SCALAR]]) -> TSD[K_1, TIME_SERIES_TYPE]:
    """
    Extracts a TSD from a stream of delta dictionaries.
    """
    return ts.value


@dataclass
class KeyValueRefState:
    reference: object = field(default_factory=object)
    tsd: TSD[SCALAR, TIME_SERIES_TYPE] | None = None
    key: SCALAR | None = None


@operator
def tsd_get_item(tsd: TSD[K, TIME_SERIES_TYPE], key: TS[K]) -> TIME_SERIES_TYPE:
    """
    Returns the time-series associated to the key provided.
    """


@compute_node(overloads=tsd_get_item, valid=("key",))
def tsd_get_item_default(tsd: REF[TSD[K, TIME_SERIES_TYPE]], key: TS[K],
                         _ref: REF[TIME_SERIES_TYPE] = None,
                         _ref_ref: REF[TIME_SERIES_TYPE] = None,
                         _value_tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
                         _state: STATE[KeyValueRefState] = None) -> REF[TIME_SERIES_TYPE]:
    """
    Returns the time-series associated to the key provided.
    """
    # Use tsd as a reference to avoid the cost of the input wrapper
    # If we got here something was modified so release any previous value and replace
    if tsd.modified or key.modified:
        if _state.tsd is not None:
            _ref.make_passive()
            _state.tsd.release_ref(_state.key, _state.reference)
        if tsd.valid and tsd.value.valid:
            _state.tsd = tsd.value.output
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
    return result


@compute_node(overloads=tsd_get_item)
def tsd_get_items(tsd: TSD[K, REF[TIME_SERIES_TYPE]], keys: TSS[K]) -> TSD[K, REF[TIME_SERIES_TYPE]]:
    """
    Filters the tsd to the given keys.
    """
    return {
        **{k: v.value for k, v in tsd.modified_items() if k in keys},
        **{k: REMOVE_IF_EXISTS for k in tsd.removed_keys()},
        **{k: tsd[k].value for k in keys.added() if k in tsd},
        **{k: REMOVE_IF_EXISTS for k in keys.removed()}
    }


@compute_node(overloads=keys_,
              requires=lambda m, s: m[OUT].py_type is TSS or
              m[OUT].matches_type(TSS[m[K].py_type]),
              resolvers={OUT: lambda m, s: TSS[m[K].py_type]})
def keys_tsd_as_tss(tsd: REF[TSD[K, TIME_SERIES_TYPE]]) -> REF[TSS[K]]:
    # Use tsd as a reference to avoid the cost of the input wrapper
    # If we got here the TSD got rebound so get the key set and return
    if tsd.value.valid:
        return cast(REF, PythonTimeSeriesReference(tsd.value.output.key_set))
    else:
        return cast(REF, PythonTimeSeriesReference())


@compute_node(overloads=keys_,
              requires=lambda m, s: m[OUT].py_type in (TS[Set], TS[set], TS[frozenset]) or
              m[OUT].matches_type(TS[Set[m[K].py_type]]))
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


@graph(overloads=len_)
def len_tsd(ts: TSD[K, TIME_SERIES_TYPE]) -> TS[int]:
    return len_(ts.key_set)


@graph(overloads=sub_)
def sub_tsds(lhs: TSD[K, TS[SCALAR]], rhs: TSD[K, TS[SCALAR]]) -> TSD[K, TS[SCALAR]]:
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


@compute_node(overloads=getattr_, resolvers={
    TIME_SERIES_TYPE: lambda mapping, scalars: get_schema_type(mapping[TS_SCHEMA], scalars['key'])})
def tsd_get_bundle_item(tsd: TSD[K, REF[TSB[TS_SCHEMA]]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE) \
        -> TSD[K, REF[TIME_SERIES_TYPE]]:
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
def uncollapse_keys_tsd(ts: TSD[Tuple[K, K_1], REF[TIME_SERIES_TYPE]],
                        _output: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]] = None) \
        -> TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]]:
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


class TsdRekeyState(CompoundScalar):
    prev: dict = {}  # Copy of previous ticks


@compute_node(overloads=rekey, valid=("new_keys",))
def rekey_tsd(ts: TSD[K, REF[TIME_SERIES_TYPE]], new_keys: TSD[K, TS[K_1]],
              _state: STATE[TsdRekeyState] = None) -> TSD[K_1, REF[TIME_SERIES_TYPE]]:
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
        prev_key = prev.pop(ts_key)
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

    return out


@compute_node(overloads=flip)
def flip_tsd(ts: TSD[K, TS[K_1]], _state: STATE[TsdRekeyState] = None) -> TSD[K_1, TS[K]]:
    """
    Flip the TSD to have the time-series as the key and the key as the time-series.
    """

    out = {}
    prev = _state.prev

    # Clear up existing mapping before we track new key mappings
    for k in ts.removed_keys():
        k_new = prev.pop(k)
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


@compute_node(overloads=flip_keys)
def flip_keys_tsd(ts: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]], _output: TSD[K_1, TSD[K, REF[TIME_SERIES_TYPE]]] = None) \
        -> TSD[K_1, TSD[K, REF[TIME_SERIES_TYPE]]]:
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


@compute_node(overloads=merge)
def merge_tsds(*tsl: TSL[TSD[K, REF[TIME_SERIES_TYPE]], SIZE]) -> TSD[K, REF[TIME_SERIES_TYPE]]:
    """
    Merge TSDs.  If more than one TSD ticks, all items will tick to the output assuming they are on different keys.
    If they are on the same key then the left-most item is preferred.
    """
    out = {}
    removals = set()

    for v in reversed(list(tsl.modified_values())):
        out.update({k: v.value for k, v in v.modified_items()})
        removals.update(v.removed_keys())

    for k in removals:
        for v in tsl.values():
            if k in v:
                out[k] = v[k].value
                break
        else:
            out[k] = REMOVE_IF_EXISTS

    return out


@compute_node(overloads=merge)
def merge_nested_tsds(*tsl: TSL[TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]], SIZE]) -> TSD[
    K, TSD[K_1, REF[TIME_SERIES_TYPE]]]:
    out = defaultdict(dict)
    removals = set()
    nested_removals = defaultdict(set)

    for tsd in reversed(list(tsl.modified_values())):
        for k, v in tsd.modified_items():
            for k1, v1 in v.modified_items():
                out[k].update({k1: v1.value})
            nested_removals[k].update(v.removed_keys())
            out[k].update({k1: REMOVE_IF_EXISTS for k1 in v.removed_keys()})
        removals.update(tsd.removed_keys())
        for k, v1 in tsd.removed_items():
            nested_removals[k].update(v1.keys())

    for k in removals:
        for v in reversed(tsl.values()):
            if k in v:
                break
        else:
            out[k] = REMOVE_IF_EXISTS

    for k, v in nested_removals.items():
        for v1 in reversed(tsl.values()):
            if k in v1:
                for k1 in v:
                    if k1 in v1[k] and out[k].get(k1, REMOVE_IF_EXISTS) is REMOVE_IF_EXISTS:
                        out[k][k1] = v1[k][k1].value

    return out


@compute_node(overloads=partition)
def partition_tsd(ts: TSD[K, REF[TIME_SERIES_TYPE]], partitions: TSD[K, TS[K_1]],
                  _state: STATE[TsdRekeyState] = None) -> TSD[K_1, TSD[K, REF[TIME_SERIES_TYPE]]]:
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


@graph(overloads=zero)
def zero_tsd(ts: Type[TSD[SCALAR, TIME_SERIES_TYPE]], op: object) -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    This is a helper generator to create a zero time-series for the reduce function.
    """
    from hgraph import nothing
    return nothing(ts)


@compute_node(overloads=min_)
def min_tsd_unary(tsd: TSD[K, V], default_value: V = None) -> V:
    """
    The minimum value in the TSD
    """
    return min(tsd.value.values(), default=default_value.value)


@compute_node(overloads=max_)
def max_tsd_unary(tsd: TSD[K, V], default_value: V = None) -> V:
    """
    The maximum value in the TSD
    """
    return max(tsd.value.values(), default=default_value.value)



@graph(overloads=sum_)
def sum_tsd_unary(tsd: TSD[K, V], tp: Type[V] = AUTO_RESOLVE) -> V:
    return _sum_tsd_unary(tsd, zero(tp, sum_))


@compute_node
def _sum_tsd_unary(tsd: TSD[K, V], zero_ts: V) -> V:
    return sum(tsd.value.values(), start=zero_ts.value)


@graph(overloads=mean)
def mean_tsd_unary_number(ts: TSD[K, TS[NUMBER]]) -> TS[float]:
    from hgraph import DivideByZero, default
    return default(div_(sum_(ts), len_(ts), divide_by_zero=DivideByZero.NAN), float('NaN'))


@compute_node(overloads=std)
def std_tsd_unary_number(ts: TSD[K, TS[NUMBER]]) -> TS[float]:
    if len(ts) <= 1:
        return 0.0
    else:
        return float(stdev(ts.value.values()))


@compute_node(overloads=var)
def var_tsd_unary_number(ts: TSD[K, TS[NUMBER]]) -> TS[float]:
    if len(ts) <= 1:
        return 0.0
    else:
        return float(variance(ts.value.values()))


@compute_node(overloads=str_)
def str_tsd(tsd: TSD[K, V]) -> TS[str]:
    return str(dict(tsd.value))
