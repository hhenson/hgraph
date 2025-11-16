from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from statistics import stdev, variance
from typing import Callable, Set, Tuple, Type, cast

from hgraph import K_2, TIME_SERIES_TYPE_1, HgTSDTypeMetaData, HgTupleFixedScalarType, and_, default
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
from hgraph._operators._stream import filter_by
from hgraph._types._frame_scalar_type_meta_data import SCHEMA
from hgraph._types._ref_type import REF, TimeSeriesReference, TimeSeriesReferenceOutput
from hgraph._types._scalar_types import NUMBER, SCALAR, STATE, CompoundScalar
from hgraph._types._time_series_types import K_1, OUT, TIME_SERIES_TYPE, V
from hgraph._types._ts_type import TS
from hgraph._types._tsb_type import TS_SCHEMA, TSB
from hgraph._types._tsd_type import REMOVE_IF_EXISTS, TSD, TSD_OUT, K
from hgraph._types._tsl_type import SIZE, TSL
from hgraph._types._tss_type import TSS
from hgraph._types._type_meta_data import AUTO_RESOLVE, HgTypeMetaData
from hgraph._wiring._decorators import compute_node, graph
from hgraph._wiring._map import map_
from hgraph._wiring._reduce import reduce

__all__ = ("merge_tsd_disjoint",)


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
        if ts.valid and not ts.value.is_empty:
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
    if _ref.modified:
        # TODO: Is the instanceof check required or can it just be dumped.
        if _ref.value.has_output and _ref.value.output.is_reference:
            _ref_ref.bind_output(_ref.value.output)
            _ref_ref.make_active()
        elif _ref_ref.bound:
            _ref_ref.make_passive()
            _ref_ref.un_bind_output(unbind_refs=True)

    result = _ref_ref.value if _ref_ref.bound else _ref.value
    if result is None or ts.value.is_empty:
        # We can't have a valid ref if the ts value is not valid.
        result = TimeSeriesReference.make()
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

        if ts.valid and not ts.value.is_empty:
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
    remove_ref_refs = []
    for k, v in _ref.modified_items():
        if k in _state.tsd.key_set.removed():
            out[k] = REMOVE_IF_EXISTS
        elif v.value.has_output and isinstance(v.value.output, TimeSeriesReferenceOutput):
            _ref_ref._create(k)
            _ref_ref[k].bind_output(v.value.output)
            _ref_ref[k].make_active()
            continue
        elif not v.value.is_empty or k in _state.tsd.key_set:
            out[k] = v.value
            
        if k in _ref_ref:
            remove_ref_refs.append(k)
            
    for k in remove_ref_refs:
        _ref_ref.on_key_removed(k)

    for k, v in _ref_ref.modified_items():
        out[k] = v.value

    return out


@tsd_get_items.stop
def tsd_get_items_stop(
        _ref: TSD[K, REF[TIME_SERIES_TYPE]],
        _ref_ref: TSD[K, REF[TIME_SERIES_TYPE]]):
    
    for k, _ in list(_ref.items()):
        _ref.on_key_removed(k)
    for k, _ in list(_ref_ref.items()):
        _ref_ref.on_key_removed(k)
    

@compute_node(
    overloads=keys_,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[K].py_type]),
    resolvers={OUT: lambda m, s: TSS[m[K].py_type]},
)
def keys_tsd_as_tss(tsd: REF[TSD[K, TIME_SERIES_TYPE]]) -> REF[TSS[K]]:
    # Use tsd as a reference to avoid the cost of the input wrapper
    # If we got here the TSD got rebound so get the key set and return
    if not tsd.value.is_empty:
        return cast(REF, TimeSeriesReference.make(tsd.value.output.key_set))
    else:
        return cast(REF, TimeSeriesReference.make())


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
def bit_and_tsds(lhs: TSD[K, TIME_SERIES_TYPE], rhs: TSD[K, TIME_SERIES_TYPE]) -> TSD[K, TIME_SERIES_TYPE]:
    keys = lhs.key_set & rhs.key_set
    return tsd_get_items(lhs, keys)


@graph(overloads=bit_or)
def bit_or_tsds(lhs: TSD[K, TIME_SERIES_TYPE], rhs: TSD[K, TIME_SERIES_TYPE]) -> TSD[K, TIME_SERIES_TYPE]:
    return merge(lhs, rhs)


@graph(overloads=bit_xor)
def bit_xor_tsds(lhs: TSD[K, TIME_SERIES_TYPE], rhs: TSD[K, TIME_SERIES_TYPE]) -> TSD[K, TIME_SERIES_TYPE]:
    keys = lhs.key_set ^ rhs.key_set
    return merge(tsd_get_items(lhs, keys), tsd_get_items(rhs, keys))


@graph(overloads=eq_)
def eq_tsds(lhs: TSD[K, TIME_SERIES_TYPE], rhs: TSD[K, TIME_SERIES_TYPE], epsilon: TS[float] = None) -> TS[bool]:
    return reduce(
        lambda l, r: and_(l, r),
        map_(lambda l, r: default(eq_(l, r) if epsilon is None else eq_(l, r, epsilon=epsilon), False), lhs, rhs),
        True,
    )


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
        if not v.value.is_empty:
            if v.value.has_output:
                out[k] = TimeSeriesReference.make(v.value.output[key])
            else:
                out[k] = v.value.items[_schema._schema_index_of(key)]
        else:
            out[k] = TimeSeriesReference.make()
    for k in tsd.removed_keys():
        out[k] = REMOVE_IF_EXISTS

    return out


@graph
def tsd_get_bundle_item_nested(
    tsd: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]], key: str
) -> TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE_1]]]:
    collapsed_tsd = collapse_keys(tsd)
    tsd_out = getattr_(collapsed_tsd, key)
    return uncollapse_keys(tsd_out)


@graph(
    overloads=getattr_,
    resolvers={TIME_SERIES_TYPE: lambda mapping, scalars: get_schema_type(mapping[TS_SCHEMA], scalars["key"])},
)
def tsd_get_bundle_item_2(
    tsd: TSD[K, TSD[K_1, REF[TSB[TS_SCHEMA]]]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE
) -> TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]]:
    return tsd_get_bundle_item_nested(tsd, key)


@graph(
    overloads=getattr_,
    resolvers={TIME_SERIES_TYPE: lambda mapping, scalars: get_schema_type(mapping[TS_SCHEMA], scalars["key"])},
)
def tsd_get_bundle_item_3(
    tsd: TSD[K, TSD[K_1, TSD[K_2, REF[TSB[TS_SCHEMA]]]]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE
) -> TSD[K, TSD[K_1, TSD[K_2, REF[TIME_SERIES_TYPE]]]]:
    return tsd_get_bundle_item_nested(tsd, key)


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


@compute_node
def _collapse_keys_tsd_impl(ts: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]]) -> TSD[Tuple[K, K_1], REF[TIME_SERIES_TYPE]]:
    """
    Collapse the nested TSDs to a TSD with a tuple key.
    """
    out = {}

    for k, v in ts.removed_items():
        if v is not None:
            out.update({(k, k1): REMOVE_IF_EXISTS for k1 in v.removed_keys()})

    for k, v in ts.modified_items():
        if v is not None:
            out.update({(k, k1): v1.value for k1, v1 in v.modified_items()})
            out.update({(k, k1): REMOVE_IF_EXISTS for k1 in v.removed_keys()})

    return out


def _key_type_as_tuple(tp):
    return tp.element_types if isinstance(tp, HgTupleFixedScalarType) else (tp.py_type,)


@compute_node(resolvers={SCALAR: lambda m, s: Tuple[*(_key_type_as_tuple(m[K]) + _key_type_as_tuple(m[K_1]))]})
def _collapse_merge_keys_tsd(ts: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]]) -> TSD[SCALAR, REF[TIME_SERIES_TYPE]]:
    """
    Collapse the nested TSDs to a TSD with a tuple key, merging keys if they are tuples.
    """
    out = {}

    def merge_key(a, b):
        return ((a,) if not isinstance(a, tuple) else a) + ((b,) if not isinstance(b, tuple) else b)

    for k, v in ts.removed_items():
        out.update({merge_key(k, k1): REMOVE_IF_EXISTS for k1 in v.removed_keys()})

    for k, v in ts.modified_items():
        out.update({merge_key(k, k1): v1.value for k1, v1 in v.modified_items()})
        out.update({merge_key(k, k1): REMOVE_IF_EXISTS for k1 in v.removed_keys()})

    return out


@graph(overloads=collapse_keys)
def collapse_keys_tsd(
    ts: TSD[K, TSD[K_1, TIME_SERIES_TYPE]], max_depth: int = -1, v_tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE
) -> OUT:
    assert max_depth >= 2 or max_depth == -1, (
        "max_depth must be greater than or equal to 2, or -1 for full depth of the TSD"
    )

    if max_depth == 2 or not isinstance(HgTypeMetaData.parse_type(v_tp), HgTSDTypeMetaData):
        return _collapse_keys_tsd_impl(ts)
    else:
        return _collapse_merge_keys_tsd(
            map_(lambda x: collapse_keys(x, max_depth - 1 if max_depth > 0 else max_depth), ts)
        )


@compute_node(overloads=uncollapse_keys)
def uncollapse_keys_tsd(
    ts: TSD[Tuple[K, K_1], REF[TIME_SERIES_TYPE]],
    remove_empty: bool = True,
    _output: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]] = None,
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

    if remove_empty:
        for k in removed:
            out[k] = REMOVE_IF_EXISTS

    return out


def _make_recursive_tsd_type(keys, value):
    return _make_recursive_tsd_type(keys[:-1], TSD[keys[-1], value]) if len(keys) > 1 else TSD[keys[0], value]


@compute_node(
    overloads=uncollapse_keys,
    requires=lambda m, s: isinstance(m[K], HgTupleFixedScalarType),
    resolvers={OUT: lambda m, s: _make_recursive_tsd_type(m[K].element_types, REF[m[TIME_SERIES_TYPE].py_type])},
)
def uncollapse_more_keys_tsd(ts: TSD[K, REF[TIME_SERIES_TYPE]], remove_empty: bool = True, _output: TSD = None) -> OUT:
    """
    Un-Collapse the nested TSDs to a TSD with a tuple key of any length.
    """
    nested_dict = lambda: defaultdict(nested_dict)
    out = nested_dict()
    removed_keys = nested_dict()

    if remove_empty:
        for k in ts.removed_keys():
            r = removed_keys
            o = out
            for i in k[:-1]:
                r = r[i]
                o = o[i]
            r[k[-1]] = True
            o[k[-1]] = REMOVE_IF_EXISTS

        def process_removed(base_path, remove_dict, _out):
            removed_paths = set()
            all_removed = True
            for key, value in remove_dict.items():
                path = base_path + (key,)
                out_val = _out.get(key)
                if out_val is None:
                    continue
                if isinstance(value, dict) and set(value).issubset(out_val.key_set.value):
                    child_paths_removed, all_children_removed = process_removed(path, value, out_val)
                    if all_children_removed:
                        removed_paths.add(path)
                    else:
                        removed_paths.update(child_paths_removed)
                    all_removed = all_children_removed and all_removed
                else:
                    removed_paths.add(path)
            return removed_paths, len(removed_paths) == len(_out) and all_removed

        removed = process_removed((), removed_keys, _output)[0]
    else:
        removed = set(ts.removed_keys())

    for k, v in ts.modified_items():
        o = out
        for i in range(len(k) - 1):
            # This is needed for the case where the child is removed leaving the parent node empty and a new child
            # is added to the parent node in the same cycle
            if k[:i] in removed:
                removed.remove(k[:i])
            o = o[k[i]]
        o[k[-1]] = v.value

    for k in removed:
        o = out
        for i in range(len(k) - 1):
            o = o[k[i]]
        o[k[-1]] = REMOVE_IF_EXISTS

    return out


@dataclass
class TsdRekeyState(CompoundScalar):
    prev: dict = field(default_factory=dict)  # Previous new keys


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

    # Removed tsd items
    for k in ts.removed_keys():
        k_new = prev.get(k)
        if k_new is not None:
            out[k_new] = REMOVE_IF_EXISTS

    # Removed key mappings
    for ts_key in new_keys.removed_keys():
        prev_key = prev.pop(ts_key, None)
        if prev_key is not None:
            out[prev_key] = REMOVE_IF_EXISTS

    # Modified key mappings
    for ts_key, new_key in new_keys.modified_items():
        new_key = new_key.value
        prev_key = prev.get(ts_key, None)
        if prev_key is not None and new_key != prev_key:
            out[prev_key] = REMOVE_IF_EXISTS
        prev[ts_key] = new_key
        v = ts.get(ts_key)
        if v is not None:
            out[new_key] = v.value

    # Modified tsd items
    if ts.valid:
        for k, v in ts.modified_items():
            k_new = prev.get(k, None)
            if k_new is not None:
                out[k_new] = v.value

    if out:
        return out


@compute_node(overloads=rekey, valid=("new_keys",))
def rekey_tsd_with_set(
    tsd: TSD[K, REF[TIME_SERIES_TYPE]], new_keys: TSD[K, TSS[K_1]], _state: STATE[TsdRekeyState] = None
) -> TSD[K_1, REF[TIME_SERIES_TYPE]]:
    prev = _state.prev

    # Removed tsd items
    out = {k: REMOVE_IF_EXISTS for tsd_key in tsd.removed_keys() for k in prev.get(tsd_key, ())}

    # Removed key mappings
    for tsd_key in new_keys.removed_keys():
        for k in prev.get(tsd_key, ()):
            out[k] = REMOVE_IF_EXISTS
        prev.pop(tsd_key, None)

    # Modified key mappings
    for tsd_key, key_set in new_keys.modified_items():
        key_set = key_set.value
        prev_key_set = prev.get(tsd_key, set())
        for k in prev_key_set - key_set:
            out[k] = REMOVE_IF_EXISTS
        for k in key_set - prev_key_set:
            if tsd_key in tsd:
                out[k] = tsd[tsd_key].value
        prev[tsd_key] = {s for s in key_set}  # need a copy of the set

    # Modified tsd items
    for tsd_key, v in tsd.modified_items():
        key_set = new_keys.get(tsd_key)
        if key_set:
            for k in key_set.value:
                out[k] = v.value

    if out:
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
    Flip the TSD to have the time-series as the key and the key as the time-series.
    Collect keys for duplicate values into TSS
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
    return map_(merge, *tsl)


@compute_node(overloads=merge, requires=lambda m, s: s["disjoint"])
def merge_tsd_disjoint(
    *tsl: TSL[TSD[K, REF[TIME_SERIES_TYPE]], SIZE],
    disjoint: bool = False,
    _output: TSD_OUT[K, REF[TIME_SERIES_TYPE]] = None,
) -> TSD[K, REF[TIME_SERIES_TYPE]]:
    """
    Merge TSD of references assuming there is no overlap in key sets, otherwise only the leftmost values will be forwarded
    """
    out = {}
    modified = set()
    removed = set()

    for v in reversed(list(tsl.modified_values())):
        modified.update(v.modified_keys())
        removed.update(v.removed_keys())

    for k in removed:
        for v in tsl.values():
            if k in v:
                out[k] = v[k].value
                break
        else:
            out[k] = REMOVE_IF_EXISTS

    for k in modified - removed:
        for v in tsl.values():
            if k in v:
                out[k] = v[k].value
                break
        if k in _output and _output[k].value == out[k]:
            del out[k]

    return out


@compute_node(overloads=partition, valid=())
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


@compute_node(overloads=min_)
def min_tsd_unary_datetime(tsd: TSD[K, TS[datetime]], default_value: TS[datetime] = None) -> TS[datetime]:
    return min((v.value for v in tsd.valid_values()), default=default_value.value)


@compute_node(overloads=min_)
def min_tsd_unary_date(tsd: TSD[K, TS[date]], default_value: TS[date] = None) -> TS[date]:
    return min((v.value for v in tsd.valid_values()), default=default_value.value)


@compute_node(overloads=max_)
def max_tsd_unary(tsd: TSD[K, V], tp: Type[V] = AUTO_RESOLVE) -> V:
    return reduce(max_, tsd, zero(tp, max_))


@compute_node(overloads=max_)
def max_tsd_unary_number(tsd: TSD[K, TS[NUMBER]], default_value: TS[NUMBER] = None) -> TS[NUMBER]:
    return max((v.value for v in tsd.valid_values()), default=default_value.value)


@compute_node(overloads=max_)
def max_tsd_unary_datetime(tsd: TSD[K, TS[datetime]], default_value: TS[datetime] = None) -> TS[datetime]:
    return max((v.value for v in tsd.valid_values()), default=default_value.value)


@compute_node(overloads=max_)
def max_tsd_unary_date(tsd: TSD[K, TS[date]], default_value: TS[date] = None) -> TS[date]:
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


@graph(overloads=filter_by)
def filter_by_tsd(ts: TSD[K, V], expr: Callable[[V, ...], bool], **kwargs: TSB[TS_SCHEMA]) -> TSD[K, V]:
    """
    Applies the filter, removing keys when the filter returns false.
    If you instead wish to remove values from the underlying stream, use map_ with a filter for
    the individual time-series instead.
    """
    matches = map_(expr, ts, **kwargs)
    return _filter_by_tsd(ts, matches)


@compute_node(active=("matches",))
def _filter_by_tsd(ts: TSD[K, REF[V]], matches: TSD[K, TS[bool]]) -> TSD[K, REF[V]]:
    """
    We only care about matches ticking, since any change in ts will be reflected in matches.
    """
    out = {}
    for k, v in matches.modified_items():
        if v.value:
            out[k] = ts[k].value
        else:
            out[k] = REMOVE_IF_EXISTS
    for k in matches.removed_keys():
        out[k] = REMOVE_IF_EXISTS
    return out
