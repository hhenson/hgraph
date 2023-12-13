from typing import Type, Mapping

from frozendict import frozendict

from hgraph import TS, SCALAR, TIME_SERIES_TYPE, TSD, compute_node, REMOVE_IF_EXISTS, SCALAR_1, SCALAR_2


@compute_node(valid=("key",))
def make_tsd(key: TS[SCALAR_1], value: TS[SCALAR_2], remove_key: TS[bool] = None,
             ts_type: Type[TIME_SERIES_TYPE] = TS[SCALAR_2]) -> TSD[SCALAR_1, TIME_SERIES_TYPE]:
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


@compute_node
def flatten_tsd(tsd: TSD[SCALAR_1, TIME_SERIES_TYPE]) -> TS[Mapping[SCALAR_1, SCALAR_2]]:
    """
    Flatten a TSD into a time-series of frozen dicts (equivalent to the delta dictionary)
    """
    return tsd.delta_value


@compute_node
def extract_tsd(ts: TS[Mapping[SCALAR_1, SCALAR_2]]) -> TSD[SCALAR_1, TIME_SERIES_TYPE]:
    """
    Extracts a TSD from a stream of delta dictionaries.
    """
    return ts.value
