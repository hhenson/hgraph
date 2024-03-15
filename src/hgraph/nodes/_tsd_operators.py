from collections import defaultdict
from dataclasses import field, dataclass
from typing import Type, Mapping, cast, Tuple, Dict

from hgraph import TS, SCALAR, TIME_SERIES_TYPE, TSD, compute_node, REMOVE_IF_EXISTS, REF, \
    STATE, graph, contains_, not_, K, NUMBER, TSS, PythonTimeSeriesReference, CompoundScalar, TS_SCHEMA, TSB, \
    AUTO_RESOLVE, map_, TS_OUT, TSD_OUT
from hgraph._runtime._operators import getattr_, mul_
from hgraph._types._time_series_types import K_1, TIME_SERIES_TYPE_1
from hgraph.nodes import sum_, const
from hgraph.nodes._operators import len_
from hgraph.nodes._set_operators import is_empty

__all__ = ("make_tsd", "flatten_tsd", "extract_tsd", "tsd_get_item", "tsd_get_key_set", "tsd_contains", "tsd_not",
           "tsd_is_empty", "tsd_collapse_keys", "tsd_uncollapse_keys", "tsd_get_bundle_item", "tsd_rekey", "tsd_flip")


@compute_node(valid=("key",))
def make_tsd(key: TS[K_1], value: TIME_SERIES_TYPE, remove_key: TS[bool] = None,
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


@compute_node
def tsd_get_item(tsd: REF[TSD[K, TIME_SERIES_TYPE]], key: TS[K], _ref: REF[TIME_SERIES_TYPE] = None,
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
        if tsd.value.valid:
            _state.tsd = tsd.value.output
            _state.key = key.value
        else:
            _state.tsd = None
            _state.key = None
        output = _state.tsd.get_ref(_state.key, _state.reference)
        _ref.bind_output(output)
        _ref.make_active()
    return _ref.value


@compute_node
def tsd_get_key_set(tsd: REF[TSD[K, TIME_SERIES_TYPE]]) -> REF[TSS[K]]:
    """
    Returns the key set time-series associated to the TSD pointed to by the reference.
    """
    # Use tsd as a reference to avoid the cost of the input wrapper
    # If we got here the TSD got rebound so get the key set and return
    if tsd.value.valid:
        return cast(REF, PythonTimeSeriesReference(tsd.value.output.key_set))
    else:
        return cast(REF, PythonTimeSeriesReference())


@graph(overloads=contains_)
def tsd_contains(ts: TSD[K, TIME_SERIES_TYPE], item: TS[K]) -> TS[bool]:
    """Contains for TSD delegates to the key-set contains"""
    return contains_(ts.key_set, item)


@graph(overloads=not_)
def tsd_not(ts: TSD[K, TIME_SERIES_TYPE]) -> TS[bool]:
    return not_(ts.key_set)


@graph(overloads=is_empty)
def tsd_is_empty(ts: TSD[K, TIME_SERIES_TYPE]) -> TS[bool]:
    return is_empty(ts.key_set)


@graph(overloads=len_)
def tsd_len(ts: TSD[K, TIME_SERIES_TYPE]) -> TS[int]:
    return len_(ts.key_set)


@compute_node(overloads=sum_)
def sum_tsd(ts: TSD[K, TS[NUMBER]]) -> TS[NUMBER]:
    return sum(i.value for i in ts.valid_values())


@graph(overloads=mul_)
def mul_tsd(tsd: TSD[K, TIME_SERIES_TYPE], other: TIME_SERIES_TYPE) -> TSD[K, TIME_SERIES_TYPE]:
    return map_(lambda x, y: x * y, tsd, other)


def get_schema_type(schema: Type[TS_SCHEMA], key: str) -> Type[TIME_SERIES_TYPE]:
    return schema[key].py_type


@compute_node(overloads=getattr_, resolvers={TIME_SERIES_TYPE: lambda mapping, scalars: get_schema_type(mapping[TS_SCHEMA], scalars['key'])})
def tsd_get_bundle_item(tsd: TSD[K, REF[TSB[TS_SCHEMA]]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE) \
        -> TSD[K, REF[TIME_SERIES_TYPE]]:
    """
    Returns a TSD of teh given items from the bundles in the original TSD
    """
    out = {}
    for k, v in tsd.modified_items():
        if v.value.valid:
            if v.value.has_peer:
                out[k] = PythonTimeSeriesReference(v.value.output[key])
            else:
                out[k] = PythonTimeSeriesReference(v.value[_schema.index_of(key)])
        else:
            out[k] = PythonTimeSeriesReference()

    for k in tsd.removed_keys():
        out[k] = REMOVE_IF_EXISTS

    return out


@compute_node
def tsd_collapse_keys(ts: TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]]) -> TSD[Tuple[K, K_1], REF[TIME_SERIES_TYPE]]:
    """
    Collapse the nested TSDs to a TSD with a tuple key.
    """
    out = {}
    for k, v in ts.modified_items():
        out.update({(k, k1): v1.value for k1, v1 in v.modified_items()})
        out.update({(k, k1): REMOVE_IF_EXISTS for k1 in v.removed_keys()})

    for k in ts.removed_keys():
        out.update({(k, k1): REMOVE_IF_EXISTS for k1 in ts[k].keys()})

    return out


@compute_node
def tsd_uncollapse_keys(ts: TSD[Tuple[K, K_1], REF[TIME_SERIES_TYPE]]) -> TSD[K, TSD[K_1, REF[TIME_SERIES_TYPE]]]:
    """
    Un-Collapse the nested TSDs to a TSD with a tuple key.
    """
    out = defaultdict(defaultdict)
    for k, v in ts.modified_items():
        out[k[0]][k[1]] = v.delta_value

    for k in ts.removed_keys():
        out[k[0]][k[1]] = REMOVE_IF_EXISTS

    return out


class TsdRekeyState(CompoundScalar):
    known_keys: Dict = {}


@compute_node
def tsd_rekey(ts: TSD[K, REF[TIME_SERIES_TYPE]], new_keys: TSD[K, TS[K_1]], state: STATE[TsdRekeyState] = None) -> TSD[K_1, REF[TIME_SERIES_TYPE]]:
    """
    Rekey a TSD to the new keys.
    """
    out = {}
    for k, v in ts.modified_items():
        if k in new_keys:
            out[new_keys[k].value] = v.value

    for k in ts.removed_keys():
        out[new_keys[k].value] = REMOVE_IF_EXISTS

    for k, k1 in new_keys.modified_items():
        if k0 := state.known_keys.get(k):
            out[state.keys[k0]] = REMOVE_IF_EXISTS
        out[k1.value] = ts[k].value

    for k, k1 in new_keys.removed_items():
        out[k1.value] = REMOVE_IF_EXISTS

    return out


@compute_node
def tsd_flip(ts: TSD[K, TS[K_1]]) -> TSD[K_1, TS[K]]:
    """
    Flip the TSD to have the time-series as the key and the key as the time-series.
    """

    # TODO: Introduce state to track changed values
    out = {}
    for k, v in ts.modified_items():
        out[v.value] = k

    for k, v in ts.removed_items():
        out[v.value] = REMOVE_IF_EXISTS

    return out
