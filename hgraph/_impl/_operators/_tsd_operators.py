from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from statistics import stdev, variance
from typing import Callable, Set, Tuple, Type, cast

from hgraph import K_2, TIME_SERIES_TYPE_1, HgTSDTypeMetaData, HgTupleFixedScalarType, and_, default, set_delta
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
                _ref.create(k)
                _ref[k].bind_output(output)
                _ref[k].make_active()

    if _state.tsd is None or not key.valid:
        return

    for k in key.added():
        output = _state.tsd.get_ref(k, _state.reference)
        _ref.create(k)
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
            _ref_ref.create(k)
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
    requires=lambda m: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[K].py_type]),
    resolvers={OUT: lambda m: TSS[m[K].py_type]},
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
    requires=lambda m: m[OUT].py_type in (TS[Set], TS[set], TS[frozenset])
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


@dataclass
class TsdEqState(CompoundScalar):
    lhs: dict = field(default_factory=dict)
    rhs: dict = field(default_factory=dict)


def _update_tsd_snapshot(ts: TSD[K, TIME_SERIES_TYPE], snapshot: dict) -> None:
    for key in ts.removed_keys():
        snapshot.pop(key, None)

    for key in ts.modified_keys():
        child = ts[key]
        snapshot[key] = child if hasattr(child, "modified_keys") else child.value


@compute_node(overloads=eq_)
def eq_tsds(
    lhs: TSD[K, TIME_SERIES_TYPE],
    rhs: TSD[K, TIME_SERIES_TYPE],
    epsilon: TS[float] = None,
    _state: STATE[TsdEqState] = None,
) -> TS[bool]:
    _update_tsd_snapshot(lhs, _state.lhs)
    _update_tsd_snapshot(rhs, _state.rhs)

    lhs_keys = set(_state.lhs.keys())
    rhs_keys = set(_state.rhs.keys())
    if lhs_keys != rhs_keys:
        return False

    epsilon_value = epsilon.value if epsilon is not None and epsilon.valid else None
    for key in lhs_keys:
        left = _state.lhs[key]
        right = _state.rhs[key]
        if (
            epsilon_value is not None
            and isinstance(left, (int, float))
            and isinstance(right, (int, float))
            and abs(float(left) - float(right)) > float(epsilon_value)
        ):
            return False
        if epsilon_value is None and left != right:
            return False
        if epsilon_value is not None and not isinstance(left, (int, float)) and left != right:
            return False

    return True


def get_schema_type(schema: Type[TS_SCHEMA], key: str) -> Type[TIME_SERIES_TYPE]:
    return schema[key].py_type


def _as_ref(ts_like) -> TimeSeriesReference:
    """
    Normalize REF wrappers and directly-bound TS inputs to a TimeSeriesReference.
    """
    candidates = [ts_like]
    for attr in ("output", "reference_output"):
        try:
            candidate = getattr(ts_like, attr)
        except Exception:
            candidate = None
        if candidate is not None:
            candidates.append(candidate)

    for candidate in candidates:
        try:
            ref = TimeSeriesReference.make(candidate)
            if not ref.is_empty:
                return ref
        except Exception:
            continue

    return TimeSeriesReference.make()


@compute_node(
    overloads=getattr_,
    resolvers={TIME_SERIES_TYPE: lambda mapping, key: get_schema_type(mapping[TS_SCHEMA], key)},
)
def tsd_get_bundle_item_direct(
    tsd: TSD[K, TSB[TS_SCHEMA]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE
) -> TSD[K, TIME_SERIES_TYPE]:
    """
    Direct bundle-field extraction for non-REF TSD elements.
    """
    out = {}
    for k, v in tsd.modified_items():
        if v.valid:
            field = v[key]
            if field.valid:
                out[k] = field.value

    for k in tsd.removed_keys():
        out[k] = REMOVE_IF_EXISTS

    return out


@graph(
    overloads=getattr_,
    resolvers={TIME_SERIES_TYPE: lambda mapping, key: get_schema_type(mapping[TS_SCHEMA], key)},
)
def tsd_get_bundle_item_2_direct(
    tsd: TSD[K, TSD[K_1, TSB[TS_SCHEMA]]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE
) -> TSD[K, TSD[K_1, TIME_SERIES_TYPE]]:
    return map_(lambda x: getattr_(x, key), tsd)


@graph(
    overloads=getattr_,
    resolvers={TIME_SERIES_TYPE: lambda mapping, key: get_schema_type(mapping[TS_SCHEMA], key)},
)
def tsd_get_bundle_item_3_direct(
    tsd: TSD[K, TSD[K_1, TSD[K_2, TSB[TS_SCHEMA]]]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE
) -> TSD[K, TSD[K_1, TSD[K_2, TIME_SERIES_TYPE]]]:
    return map_(lambda x: getattr_(x, key), tsd)


@compute_node(
    resolvers={TIME_SERIES_TYPE: lambda mapping, key: get_schema_type(mapping[TS_SCHEMA], key)},
)
def tsd_get_bundle_item(
    tsd: TSD[K, REF[TSB[TS_SCHEMA]]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE
) -> TSD[K, REF[TIME_SERIES_TYPE]]:
    """
    Returns a TSD of the given items from the bundles in the original TSD
    """
    out = {}
    for k, v in tsd.modified_items():
        ref = _as_ref(v)
        if not ref.is_empty:
            if ref.has_output and ref.output is not None:
                out[k] = TimeSeriesReference.make(ref.output[key])
            else:
                out[k] = ref.items[_schema._schema_index_of(key)]
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
    resolvers={TIME_SERIES_TYPE: lambda mapping, key: get_schema_type(mapping[TS_SCHEMA], key)},
)
def tsd_get_bundle_item_2(
    tsd: TSD[K, TSD[K_1, REF[TSB[TS_SCHEMA]]]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE
) -> TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]]:
    return tsd_get_bundle_item_nested(tsd, key)


@graph(
    resolvers={TIME_SERIES_TYPE: lambda mapping, key: get_schema_type(mapping[TS_SCHEMA], key)},
)
def tsd_get_bundle_item_3(
    tsd: TSD[K, TSD[K_1, TSD[K_2, REF[TSB[TS_SCHEMA]]]]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE
) -> TSD[K, TSD[K_1, TSD[K_2, REF[TIME_SERIES_TYPE]]]]:
    return tsd_get_bundle_item_nested(tsd, key)


@compute_node(
    overloads=getattr_,
    resolvers={SCALAR: lambda mapping, key: get_schema_type(mapping[SCHEMA].meta_data_schema, key)},
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


@dataclass
class NestedTsdState(CompoundScalar):
    nested: dict = field(default_factory=dict)
    output: dict = field(default_factory=dict)


def _update_nested_tsd_snapshot(ts: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]], snapshot: dict) -> None:
    for outer_key in ts.removed_keys():
        snapshot.pop(outer_key, None)

    for outer_key, inner in ts.modified_items():
        if inner is None:
            snapshot.pop(outer_key, None)
            continue

        inner_snapshot = dict(snapshot.get(outer_key, {}))
        had_inner_delta = False

        for inner_key in inner.removed_keys():
            inner_snapshot.pop(inner_key, None)
            had_inner_delta = True

        for inner_key, inner_value in inner.modified_items():
            inner_snapshot[inner_key] = _as_ref(inner_value)
            had_inner_delta = True

        if inner_snapshot:
            snapshot[outer_key] = inner_snapshot
        else:
            snapshot.pop(outer_key, None)


def _diff_flat_tsd(previous: dict, current: dict) -> dict:
    out = {key: REMOVE_IF_EXISTS for key in previous.keys() - current.keys()}
    out.update({key: value for key, value in current.items() if previous.get(key) != value})
    return out


@compute_node
def _collapse_keys_tsd_impl(
    ts: TSD[K, TSD[K_1, TIME_SERIES_TYPE]], _state: STATE[NestedTsdState] = None
) -> TSD[Tuple[K, K_1], TIME_SERIES_TYPE]:
    """
    Collapse the nested TSDs to a TSD with a tuple key.
    """
    snapshot_value = ts.value
    _state.nested = {outer_key: dict(inner.items()) for outer_key, inner in (snapshot_value or {}).items()}
    current = {
        (outer_key, inner_key): value
        for outer_key, inner_values in _state.nested.items()
        for inner_key, value in inner_values.items()
    }

    out = _diff_flat_tsd(_state.output, current)
    _state.output = dict(current)
    if out:
        return out


def _key_type_as_tuple(tp):
    return tp.element_types if isinstance(tp, HgTupleFixedScalarType) else (tp.py_type,)


@compute_node(resolvers={SCALAR: lambda m: Tuple[*(_key_type_as_tuple(m[K]) + _key_type_as_tuple(m[K_1]))]})
def _collapse_merge_keys_tsd(
    ts: TSD[K, TSD[K_1, TIME_SERIES_TYPE]], _state: STATE[NestedTsdState] = None
) -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    Collapse the nested TSDs to a TSD with a tuple key, merging keys if they are tuples.
    """
    def merge_key(a, b):
        return ((a,) if not isinstance(a, tuple) else a) + ((b,) if not isinstance(b, tuple) else b)

    snapshot_value = ts.value
    _state.nested = {outer_key: dict(inner.items()) for outer_key, inner in (snapshot_value or {}).items()}
    current = {
        merge_key(outer_key, inner_key): value
        for outer_key, inner_values in _state.nested.items()
        for inner_key, value in inner_values.items()
    }

    out = _diff_flat_tsd(_state.output, current)
    _state.output = dict(current)
    if out:
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
    remove_outer_keys: TS[Set[K]] = None,
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

    if remove_outer_keys is not None and remove_outer_keys.valid:
        for k in remove_outer_keys.value:
            out[k] = REMOVE_IF_EXISTS

    return out


def _make_recursive_tsd_type(keys, value):
    return _make_recursive_tsd_type(keys[:-1], TSD[keys[-1], value]) if len(keys) > 1 else TSD[keys[0], value]


@compute_node(
    overloads=uncollapse_keys,
    requires=lambda m: isinstance(m[K], HgTupleFixedScalarType),
    resolvers={OUT: lambda m: _make_recursive_tsd_type(m[K].element_types, REF[m[TIME_SERIES_TYPE].py_type])},
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
    ts_values: dict = field(default_factory=dict)
    output: dict = field(default_factory=dict)


@dataclass
class TsdPartitionState(CompoundScalar):
    ts_values: dict = field(default_factory=dict)
    partition_values: dict = field(default_factory=dict)
    output: dict = field(default_factory=dict)


@dataclass
class TsdUnpartitionState(CompoundScalar):
    flattened: dict = field(default_factory=dict)
    sources: dict = field(default_factory=dict)


def _unpartition_snapshot_impl(
    tsd: TSD[K_1, TSD[K, TIME_SERIES_TYPE]], _state: STATE[TsdUnpartitionState]
) -> TSD[K, TIME_SERIES_TYPE]:
    snapshot_value = tsd.value
    snapshot = dict(snapshot_value.items()) if snapshot_value is not None else {}

    flattened = {}
    sources = {}
    for partition, values in snapshot.items():
        if values is None:
            continue
        for key, value in values.items():
            flattened[key] = value
            sources[key] = partition

    previous_flattened = _state.flattened
    previous_sources = _state.sources
    removed = {k: REMOVE_IF_EXISTS for k in previous_flattened.keys() - flattened.keys()}
    out = {
        key: value
        for key, value in flattened.items()
        if previous_flattened.get(key) != value or previous_sources.get(key) != sources.get(key)
    }

    _state.flattened = dict(flattened)
    _state.sources = dict(sources)

    if removed or out:
        return removed | out


@compute_node(overloads=rekey, valid=("new_keys",))
def rekey_tsd(
    ts: TSD[K, REF[TIME_SERIES_TYPE]], new_keys: TSD[K, TS[K_1]], _state: STATE[TsdRekeyState] = None
) -> TSD[K_1, REF[TIME_SERIES_TYPE]]:
    """
    Rekey a TSD to the new keys.

    The expectation is that the set of new keys are distinct producing a 1-1 mapping.
    """
    prev = _state.prev
    ts_values = _state.ts_values

    for k in ts.removed_keys():
        ts_values.pop(k, None)
    for k, v in ts.modified_items():
        ts_values[k] = v.value
    if not ts_values and ts.valid:
        ts_values.update({k: v.value for k, v in ts.valid_items()})

    for ts_key in new_keys.removed_keys():
        prev.pop(ts_key, None)
    for ts_key, new_key in new_keys.modified_items():
        prev[ts_key] = new_key.value
    if not prev and new_keys.valid:
        prev.update({ts_key: key.value for ts_key, key in new_keys.valid_items()})

    current_output = {}
    for ts_key, value in ts_values.items():
        key = prev.get(ts_key)
        if key is not None:
            current_output[key] = value

    previous_output = _state.output
    out = {k: REMOVE_IF_EXISTS for k in previous_output.keys() - current_output.keys()}
    out.update({k: v for k, v in current_output.items() if previous_output.get(k) != v})
    _state.output = dict(current_output)

    if out:
        return out


@compute_node(overloads=rekey, valid=("new_keys",))
def rekey_tsd_with_set(
    tsd: TSD[K, REF[TIME_SERIES_TYPE]], new_keys: TSD[K, TSS[K_1]], _state: STATE[TsdRekeyState] = None
) -> TSD[K_1, REF[TIME_SERIES_TYPE]]:
    prev = _state.prev
    ts_values = _state.ts_values

    for ts_key in tsd.removed_keys():
        ts_values.pop(ts_key, None)
    for ts_key, v in tsd.modified_items():
        ts_values[ts_key] = v.value
    if not ts_values and tsd.valid:
        ts_values.update({ts_key: value.value for ts_key, value in tsd.valid_items()})

    for ts_key in new_keys.removed_keys():
        prev.pop(ts_key, None)
    for ts_key, key_set in new_keys.modified_items():
        prev[ts_key] = set(key_set.value)
    if not prev and new_keys.valid:
        prev.update({ts_key: set(key_set.value) for ts_key, key_set in new_keys.valid_items()})

    current_output = {}
    for ts_key, value in ts_values.items():
        for key in prev.get(ts_key, ()):
            current_output[key] = value

    previous_output = _state.output
    out = {k: REMOVE_IF_EXISTS for k in previous_output.keys() - current_output.keys()}
    out.update({k: v for k, v in current_output.items() if previous_output.get(k) != v})
    _state.output = dict(current_output)

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


@compute_node(overloads=flip, requires=lambda m, unique: unique is False)
def flip_tsd_non_unique(
    ts: TSD[K, TS[K_1]],
    unique: bool,
    _state: STATE[TsdRekeyState] = None,
    _k_tp: type[K] = AUTO_RESOLVE,
) -> TSD[K_1, TSS[K]]:
    """
    Flip the TSD to have the time-series as the key and the key as the time-series.
    Collect keys for duplicate values into TSS
    """
    prev = _state.prev

    for k in ts.removed_keys():
        prev.pop(k, None)
    for k, v in ts.modified_items():
        prev[k] = v.value
    if not prev and ts.valid:
        prev.update({k: v.value for k, v in ts.valid_items()})

    current = defaultdict(set)
    for k, v in prev.items():
        current[v].add(k)

    previous_output = _state.output
    out = {v: REMOVE_IF_EXISTS for v in previous_output.keys() - current.keys()}

    for v, current_set in current.items():
        previous_set = previous_output.get(v)
        if previous_set is None:
            out[v] = set(current_set)
            continue
        added = current_set - previous_set
        removed = previous_set - current_set
        if added or removed:
            out[v] = set_delta(added, removed, _k_tp)

    _state.output = {v: set(values) for v, values in current.items()}

    if out:
        return out


@compute_node
def _flip_keys_tsd_flat(
    ts: TSD[K, TSD[K_1, TIME_SERIES_TYPE]], _state: STATE[NestedTsdState] = None
) -> TSD[Tuple[K_1, K], TIME_SERIES_TYPE]:
    """
    Build a flat swapped-key view to avoid emitting nested TSD deltas directly from Python.
    """
    snapshot_value = ts.value
    _state.nested = {outer_key: dict(inner.items()) for outer_key, inner in (snapshot_value or {}).items()}
    current = {
        (inner_key, outer_key): value
        for outer_key, inner_values in _state.nested.items()
        for inner_key, value in inner_values.items()
    }
    out = _diff_flat_tsd(_state.output, current)
    _state.output = dict(current)
    if out:
        return out


@graph(overloads=flip_keys)
def flip_keys_tsd(ts: TSD[K, TSD[K_1, TIME_SERIES_TYPE]]) -> TSD[K_1, TSD[K, TIME_SERIES_TYPE]]:
    return uncollapse_keys(_flip_keys_tsd_flat(ts))


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
    v_meta = HgTypeMetaData.parse_type(_v_tp)
    if isinstance(v_meta, HgTSDTypeMetaData):
        flat_merged = _merge_nested_tsd_flat_stateful(*tsl)
        collapse_outer_keys = _nested_outer_remove_candidates(*tsl)
        return uncollapse_keys(flat_merged, remove_empty=False, remove_outer_keys=collapse_outer_keys)
    return _merge_tsd_stateful(*tsl)


@dataclass
class TsdMergeState(CompoundScalar):
    sources: dict = field(default_factory=dict)
    versions: dict = field(default_factory=dict)
    output: dict = field(default_factory=dict)
    tick: int = 0


@compute_node
def _merge_tsd_stateful(
    *tsl: TSL[TSD[K, TIME_SERIES_TYPE], SIZE], _state: STATE[TsdMergeState] = None
) -> TSD[K, TIME_SERIES_TYPE]:
    _state.tick += 1
    tick = _state.tick
    values = list(tsl.values())
    sources = _state.sources
    versions = _state.versions
    for index in range(len(values), len(sources)):
        sources.pop(index, None)
        versions.pop(index, None)

    for index, ts in enumerate(values):
        source = sources.setdefault(index, {})
        source_versions = versions.setdefault(index, {})
        for key in ts.removed_keys():
            source.pop(key, None)
            source_versions.pop(key, None)
        modified_keys = list(ts.modified_keys())
        for key in modified_keys:
            child = ts[key]
            source[key] = child if hasattr(child, "modified_keys") else child.value
            source_versions[key] = tick
        if not modified_keys and ts.valid:
            snapshot_value = ts.value
            snapshot = dict(snapshot_value.items()) if snapshot_value is not None else {}
            for key in set(source.keys()) - set(snapshot.keys()):
                source.pop(key, None)
                source_versions.pop(key, None)
            for key, value in snapshot.items():
                if source.get(key) != value:
                    source[key] = value
                    source_versions[key] = tick

    all_keys = set()
    for source in sources.values():
        all_keys.update(source.keys())

    current = {}
    for key in all_keys:
        best_index = None
        best_version = -1
        for index in range(len(values)):
            source = sources.get(index, {})
            if key not in source:
                continue
            version = versions.get(index, {}).get(key, -1)
            if version > best_version or (version == best_version and (best_index is None or index < best_index)):
                best_index = index
                best_version = version
        if best_index is not None:
            current[key] = sources[best_index][key]

    out = _diff_flat_tsd(_state.output, current)
    _state.output = dict(current)
    if out:
        return out


@compute_node
def _merge_nested_tsd_flat_stateful(
    *tsl: TSL[TSD[K, TSD[K_1, TIME_SERIES_TYPE]], SIZE], _state: STATE[TsdMergeState] = None
) -> TSD[Tuple[K, K_1], TIME_SERIES_TYPE]:
    _state.tick += 1
    tick = _state.tick
    values = list(tsl.values())
    sources = _state.sources
    versions = _state.versions
    for index in range(len(values), len(sources)):
        sources.pop(index, None)
        versions.pop(index, None)

    for index, ts in enumerate(values):
        source = sources.setdefault(index, {})
        source_versions = versions.setdefault(index, {})

        snapshot_value = ts.value if ts.valid else None
        snapshot = {
            (outer_key, inner_key): value
            for outer_key, inner_values in (snapshot_value or {}).items()
            for inner_key, value in inner_values.items()
        }

        for key in set(source.keys()) - set(snapshot.keys()):
            source.pop(key, None)
            source_versions.pop(key, None)

        for key, value in snapshot.items():
            if source.get(key) != value:
                source[key] = value
                source_versions[key] = tick

    all_keys = set()
    for source in sources.values():
        all_keys.update(source.keys())

    current = {}
    for key in all_keys:
        best_index = None
        best_version = -1
        for index in range(len(values)):
            source = sources.get(index, {})
            if key not in source:
                continue
            version = versions.get(index, {}).get(key, -1)
            if version > best_version or (version == best_version and (best_index is None or index < best_index)):
                best_index = index
                best_version = version
        if best_index is not None:
            current[key] = sources[best_index][key]

    out = _diff_flat_tsd(_state.output, current)
    _state.output = dict(current)
    if out:
        return out


@dataclass
class TsdNestedOuterRemoveState(CompoundScalar):
    source_outer_keys: dict = field(default_factory=dict)


@compute_node
def _nested_outer_remove_candidates(
    *tsl: TSL[TSD[K, TSD[K_1, TIME_SERIES_TYPE]], SIZE],
    _state: STATE[TsdNestedOuterRemoveState] = None,
) -> TS[Set[K]]:
    removed_outer_keys = set()
    source_outer_keys = _state.source_outer_keys
    values = list(tsl.values())

    for index in range(len(values), len(source_outer_keys)):
        source_outer_keys.pop(index, None)

    for index, ts in enumerate(values):
        previous_keys = set(source_outer_keys.get(index, set()))
        snapshot_value = ts.value if ts.valid else None
        current_keys = set((snapshot_value or {}).keys())
        removed_outer_keys.update(previous_keys - current_keys)
        source_outer_keys[index] = current_keys

    current_outer_keys = set()
    for keys in source_outer_keys.values():
        current_outer_keys.update(keys)

    return {key for key in removed_outer_keys if key not in current_outer_keys}



@compute_node(overloads=merge, requires=lambda m, disjoint: disjoint)
def merge_tsd_disjoint(
    *tsl: TSL[TSD[K, TIME_SERIES_TYPE], SIZE],
    disjoint: bool = False,
    _state: STATE[TsdMergeState] = None,
) -> TSD[K, TIME_SERIES_TYPE]:
    """
    Merge TSD of references assuming there is no overlap in key sets, otherwise only the leftmost values will be forwarded
    """
    values = list(tsl.values())
    sources = _state.sources
    for index in range(len(values), len(sources)):
        sources.pop(index, None)

    for index, ts in enumerate(values):
        source = sources.setdefault(index, {})
        for key in ts.removed_keys():
            source.pop(key, None)
        modified_keys = list(ts.modified_keys())
        for key in modified_keys:
            child = ts[key]
            source[key] = child if hasattr(child, "modified_keys") else child.value
        if not modified_keys and ts.valid:
            snapshot_value = ts.value
            snapshot = dict(snapshot_value.items()) if snapshot_value is not None else {}
            for key in set(source.keys()) - set(snapshot.keys()):
                source.pop(key, None)
            for key, value in snapshot.items():
                source[key] = value

    current = {}
    for index in range(len(values)):
        for key, value in sources.get(index, {}).items():
            current.setdefault(key, value)

    out = _diff_flat_tsd(_state.output, current)
    _state.output = dict(current)
    if out:
        return out


@compute_node(overloads=partition, valid=())
def partition_tsd(
    ts: TSD[K, REF[TIME_SERIES_TYPE]], partitions: TSD[K, TS[K_1]], _state: STATE[TsdPartitionState] = None
) -> TSD[K_1, TSD[K, REF[TIME_SERIES_TYPE]]]:
    """
    Partition a TSD into partitions by the given mapping.
    """
    ts_values = _state.ts_values
    partition_values = _state.partition_values

    for key in ts.removed_keys():
        ts_values.pop(key, None)
    for key, value in ts.modified_items():
        ts_values[key] = value.value

    for key in partitions.removed_keys():
        partition_values.pop(key, None)
    for key, partition in partitions.modified_items():
        partition_values[key] = partition.value

    new_output = defaultdict(dict)
    for key, value in ts_values.items():
        partition = partition_values.get(key)
        if partition is not None:
            new_output[partition][key] = value

    previous_output = _state.output
    out = {}
    for partition in set(previous_output.keys()) | set(new_output.keys()):
        previous_values = previous_output.get(partition, {})
        current_values = new_output.get(partition, {})
        partition_delta = {}

        for key in previous_values.keys() - current_values.keys():
            partition_delta[key] = REMOVE_IF_EXISTS

        for key, value in current_values.items():
            if previous_values.get(key) != value:
                partition_delta[key] = value

        if partition_delta:
            out[partition] = partition_delta

    _state.output = {partition: dict(values) for partition, values in new_output.items()}

    if out:
        return out


@compute_node(overloads=unpartition)
def unpartition_tsd(
    tsd: TSD[K_1, TSD[K, TIME_SERIES_TYPE]], _state: STATE[TsdUnpartitionState] = None
) -> TSD[K, TIME_SERIES_TYPE]:
    """
    Union of TSDs - given
    """
    return _unpartition_snapshot_impl(tsd, _state)


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


@graph(overloads=max_)
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


@dataclass
class TsdFilterByState(CompoundScalar):
    selected: set = field(default_factory=set)


@compute_node(active=("matches",))
def _filter_by_tsd(
    ts: TSD[K, REF[V]], matches: TSD[K, TS[bool]], _state: STATE[TsdFilterByState] = None
) -> TSD[K, REF[V]]:
    """
    We only care about matches ticking, since any change in ts will be reflected in matches.
    """
    def resolve_match(v) -> bool | None:
        value = v.value
        if hasattr(value, "has_output") and hasattr(value, "is_empty") and hasattr(value, "output"):
            if value.is_empty or not value.has_output:
                return None
            output = value.output
            if output is None:
                return None
            if hasattr(output, "valid") and not output.valid:
                return None
            return bool(output.value)
        return bool(value)

    current_selected = set()
    for key, value in matches.items():
        if resolve_match(value):
            current_selected.add(key)

    out = {}
    for key in _state.selected - current_selected:
        out[key] = REMOVE_IF_EXISTS

    for key in current_selected - _state.selected:
        out[key] = ts[key].value

    for key in current_selected & _state.selected:
        if key in ts.modified_keys():
            out[key] = ts[key].value

    _state.selected = current_selected
    if out:
        return out
