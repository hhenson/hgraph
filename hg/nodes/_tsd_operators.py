from typing import Type

from hg import TS, SCALAR, TIME_SERIES_TYPE, TSD, compute_node, REMOVE_KEY_IF_EXISTS, SCALAR_1, SCALAR_2


@compute_node(valid=("key",))
def make_tsd(key: TS[SCALAR_1], value: TS[SCALAR_2], remove_key: TS[bool] = None,
             ts_type: Type[TIME_SERIES_TYPE] = TS[SCALAR_2]) -> TSD[SCALAR_1, TIME_SERIES_TYPE]:

    if remove_key.valid:
        return {key.value: REMOVE_KEY_IF_EXISTS if remove_key.value else value.delta_value}
    else:
        return {key.value: value.delta_value}