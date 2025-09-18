from typing import Type, Mapping

from hgraph import (
    TS,
    SCALAR,
    TIME_SERIES_TYPE,
    TSD,
    TSS,
    compute_node,
    REMOVE_IF_EXISTS,
    graph,
    operator,
    K,
    K_1,
    TIME_SERIES_TYPE_1,
    set_delta,
    AUTO_RESOLVE,
)

__all__ = (
    "make_tsd",
    "make_tsd_scalar",
    "flatten_tsd",
    "extract_tsd",
    "keys_where_true",
    "where_true",
)


@operator
def make_tsd(
    key: TS[K_1],
    value: TIME_SERIES_TYPE,
    remove_key: TS[bool] = None,
    ts_type: Type[TIME_SERIES_TYPE_1] = TIME_SERIES_TYPE,
) -> TSD[K_1, TIME_SERIES_TYPE_1]:
    """
    Make a TSD from a time-series of key and value, if either key or value ticks an entry in the TSD will be
    created / update. It is also possible to remove a key by setting remove_key to True.
    In this scenario a key will be removed if the remove_key ticked True or if the key ticks and remove_key is already
    set to True.
    """


@compute_node(overloads=make_tsd, valid=("key",))
def make_tsd_default(
    key: TS[K_1],
    value: TIME_SERIES_TYPE,
    remove_key: TS[bool] = None,
    ts_type: Type[TIME_SERIES_TYPE_1] = TIME_SERIES_TYPE,
) -> TSD[K_1, TIME_SERIES_TYPE_1]:
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
def make_tsd_scalar(
    key: K_1, value: TIME_SERIES_TYPE, remove_key: TS[bool] = None, ts_type: Type[TIME_SERIES_TYPE_1] = TIME_SERIES_TYPE
) -> TSD[K_1, TIME_SERIES_TYPE_1]:
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


@compute_node
def keys_where_true(ts: TSD[K, TS[bool]], _tp: type[K] = AUTO_RESOLVE) -> TSS[K]:
    added = set()
    removed = ts.key_set.removed()

    for key, value in ts.modified_items():
        if value.value:
            added.add(key)
        else:
            removed.add(key)

    return set_delta(added, removed, _tp)


@compute_node
def where_true(ts: TSD[K, TS[bool]]) -> TSD[K, TS[bool]]:
    out = {}

    for key, value in ts.modified_items():
        if value.value:
            out[key] = value.value
        else:
            out[key] = REMOVE_IF_EXISTS

    for key in ts.removed_keys():
        out[key] = REMOVE_IF_EXISTS

    return out
